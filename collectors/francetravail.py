
import uuid
import os
import time
import json
import logging
import re
import requests
import pandas as pd

from datetime import datetime, timezone

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

from collectors.base import JobOffer, extract_skills


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

AUTH_URL = (
    "https://entreprise.francetravail.fr/"
    "connexion/oauth2/access_token"
)

SEARCH_URL = (
    "https://api.francetravail.io/"
    "partenaire/offresdemploi/v2/offres/search"
)

MAX_RESULTS = 150
RESULTS_PER_PAGE = 50
RATE_LIMIT_DELAY = 1.0


# ─────────────────────────────────────────────────────────────
# SALARY PARSING — FRANCE TRAVAIL ONLY
# ─────────────────────────────────────────────────────────────

def parse_salary_france_travail(
    salary_text: str | None,
):
    """Extract salary and normalize it to gross annual salary."""

    if not salary_text:
        return None, None, "unknown"

    text = salary_text.replace("\xa0", " ").replace("\u202f", " ")

    # Detect salary period
    if re.search(r"\bAnnuel\b", text, re.IGNORECASE):
        period = "yearly"
        factor = 1

    elif re.search(r"\bMensuel\b", text, re.IGNORECASE):
        period = "monthly"
        factor = 12

    else:
        return None, None, "unknown"

    # Extract amounts expressed in Euros
    amounts = re.findall(
        r"(\d+(?:[.,]\d+)?)\s*Euros?",
        text,
        re.IGNORECASE,
    )

    if not amounts:
        return None, None, "unknown"

    values = [
        float(amount.replace(",", "."))
        for amount in amounts
    ]

    annual = [
        round(value * factor, 2)
        for value in values
    ]

    salary_min = annual[0]
    salary_max = annual[1] if len(annual) > 1 else None

    return salary_min, salary_max, period


# ─────────────────────────────────────────────────────────────
# COLLECTOR
# ─────────────────────────────────────────────────────────────

class FranceTravailCollector:

    def __init__(self):
        self.client_id = os.environ["FRANCETRAVAIL_CLIENT_ID"]
        self.client_secret = os.environ["FRANCETRAVAIL_CLIENT_SECRET"]

        self.session = requests.Session()

        self._token = None
        self._token_expiry = 0.0

        self.invalid_publication_dates = 0

    # ─────────────────────────────────────────────────────────
    # AUTH
    # ─────────────────────────────────────────────────────────

    def _get_token(self) -> str:

        if self._token and time.time() < self._token_expiry - 30:
            return self._token

        scope = os.environ.get(
            "FRANCETRAVAIL_SCOPE",
            "api_offresdemploiv2 o2dsoffre",
        )

        resp = self.session.post(
            AUTH_URL,
            params={"realm": "/partenaire"},
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": scope,
            },
            timeout=10,
        )

        if not resp.ok:
            logger.error(
                "Auth error: %s - %s",
                resp.status_code,
                resp.text,
            )

        resp.raise_for_status()

        payload = resp.json()

        self._token = payload["access_token"]
        self._token_expiry = (
            time.time() + payload.get("expires_in", 1490)
        )

        logger.info("France Travail token obtained")

        return self._token

    # ─────────────────────────────────────────────────────────
    # API CALL
    # ─────────────────────────────────────────────────────────

    def _get(self, start: int, query: str) -> dict:

        end = min(
            start + RESULTS_PER_PAGE - 1,
            MAX_RESULTS - 1,
        )

        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }

        params = {
            "motsCles": query,
            "typeContrat": "CDI",
            "range": f"{start}-{end}",
            "sort": 1,
        }

        retry_statuses = (429, 500, 502, 503, 504)

        for attempt in range(3):

            try:
                resp = self.session.get(
                    SEARCH_URL,
                    headers=headers,
                    params=params,
                    timeout=10,
                )

            except (
                requests.ConnectionError,
                requests.Timeout,
                requests.exceptions.ChunkedEncodingError,
            ) as e:

                logger.warning(
                    "Network error, attempt %s/3: %s",
                    attempt + 1,
                    e,
                )

                if attempt == 2:
                    raise RuntimeError(
                        f"Network failure start={start}"
                    ) from e

                time.sleep(2 ** attempt)
                continue

            if resp.status_code == 204:
                return {"resultats": []}

            if resp.status_code in retry_statuses:

                logger.warning(
                    "Temporary HTTP error %s, attempt %s/3",
                    resp.status_code,
                    attempt + 1,
                )

                if attempt < 2:
                    time.sleep(2 ** attempt)
                    continue

            resp.raise_for_status()

            return resp.json()

        raise RuntimeError(f"API failure start={start}")

    # ─────────────────────────────────────────────────────────
    # PARSING
    # ─────────────────────────────────────────────────────────

    def _parse_job(self, raw: dict) -> JobOffer:

        title = raw.get("intitule") or ""
        description = raw.get("description") or ""

        # ─── CONTRACT ────────────────────────────────────────

        contract_type = raw.get("typeContrat") or ""

        if contract_type != "CDI":
            raise ValueError(
                f"Unexpected contract type: {contract_type}"
            )

        # ─── SALARY ──────────────────────────────────────────

        salary = raw.get("salaire") or {}
        salary_text = salary.get("libelle", "")

        salary_min, salary_max, _ = (
            parse_salary_france_travail(salary_text)
        )

        # ─── LOCATION ────────────────────────────────────────

        location_data = raw.get("lieuTravail") or {}
        location = location_data.get("libelle", "")

        # ─── COMPANY ─────────────────────────────────────────

        company_data = raw.get("entreprise") or {}
        company = company_data.get("nom", "")

        # ─── DATE ────────────────────────────────────────────

        date_str = (
            raw.get("dateCreation")
            or raw.get("dateActualisation")
        )

        try:
            published_at = (
                datetime.fromisoformat(
                    date_str.replace("Z", "+00:00")
                )
                if date_str
                else None
            )

        except (ValueError, TypeError, AttributeError):
            published_at = None

        if published_at is None:
            self.invalid_publication_dates += 1

        # ─── URL ─────────────────────────────────────────────

        origin_data = raw.get("origineOffre") or {}
        url = origin_data.get("urlOrigine", "")

        # ─── BUILD OBJECT ────────────────────────────────────

        return JobOffer(
            id=f"ft_{raw.get('id')}",
            title=title,
            company=company,
            location=location,
            contract_type=contract_type,
            salary_min=salary_min,
            salary_max=salary_max,
            skills=extract_skills(f"{title} {description}"),
            source="francetravail",
            url=url,
            published_at=published_at,
            collected_at=datetime.now(timezone.utc),
        )

    # ─────────────────────────────────────────────────────────
    # COLLECT
    # ─────────────────────────────────────────────────────────

    def collect(
        self,
        query: str = "data engineer",
    ) -> list[JobOffer]:

        jobs = []
        self.invalid_publication_dates = 0

        logger.info(
            "Collecting France Travail CDI offers: %s",
            query,
        )

        for start in range(0, MAX_RESULTS, RESULTS_PER_PAGE):

            data = self._get(start, query)
            results = data.get("resultats") or []

            if not results:
                break

            for raw in results:

                # Defensive check: keep only CDI offers
                if raw.get("typeContrat") != "CDI":
                    logger.warning(
                        "Unexpected contract type for offer %s: %s",
                        raw.get("id"),
                        raw.get("typeContrat"),
                    )
                    continue

                jobs.append(self._parse_job(raw))

            logger.info(
                "Fetched %s jobs (start=%s)",
                len(results),
                start,
            )

            time.sleep(RATE_LIMIT_DELAY)

        logger.info(
            "Total collected for '%s': %s",
            query,
            len(jobs),
        )

        logger.warning(
            "Invalid publication dates for query '%s': %s",
            query,
            self.invalid_publication_dates,
        )

        return jobs

    # ─────────────────────────────────────────────────────────
    # SAVE TO SNOWFLAKE
    # ─────────────────────────────────────────────────────────

    def save_to_snowflake(self, jobs: list[JobOffer]) -> None:

        if not jobs:
            logger.info("No jobs to save")
            return

        df = (
            pd.DataFrame([job.to_dict() for job in jobs])
            .drop_duplicates(subset=["id"])
            .reset_index(drop=True)
        )

        df.columns = [column.upper() for column in df.columns]

        if "ID" not in df.columns:
            raise ValueError("Missing ID column in jobs DataFrame")

        # Serialize skills for the staging table
        df["SKILLS"] = df["SKILLS"].apply(
            lambda skills: json.dumps(skills, ensure_ascii=False)
            if isinstance(skills, list)
            else "[]"
        )

        staging_table = f"STG_RAW_JOBS_{uuid.uuid4().hex.upper()}"
        staging_full_name = f"JOBMARKET.PUBLIC.{staging_table}"
        target_table = "JOBMARKET.PUBLIC.RAW_JOBS"

        hook = SnowflakeHook(
            snowflake_conn_id="jmt_snowflake_default"
        )

        conn = hook.get_conn()

        try:
            # 1. Load data into staging
            success, _, nrows, _ = write_pandas(
                conn,
                df,
                table_name=staging_table,
                database="JOBMARKET",
                schema="PUBLIC",
                overwrite=False,
                auto_create_table=True,
            )

            if not success:
                raise RuntimeError(
                    "Failed to load jobs into Snowflake staging"
                )

            logger.info("%s jobs loaded into staging", nrows)

            columns = list(df.columns)

            quoted_columns = ", ".join(
                f'"{column}"' for column in columns
            )

            update_columns = [
                column for column in columns if column != "ID"
            ]

            update_set = ", ".join(
                (
                    f'target."{column}" = PARSE_JSON(source."{column}")'
                    if column == "SKILLS"
                    else f'target."{column}" = source."{column}"'
                )
                for column in update_columns
            )

            insert_values = ", ".join(
                (
                    f'PARSE_JSON(source."{column}")'
                    if column == "SKILLS"
                    else f'source."{column}"'
                )
                for column in columns
            )

            with conn.cursor() as cursor:

                # 2. Create RAW_JOBS if it does not exist,
                # with SKILLS as VARIANT
                target_definitions = []

                for column in columns:
                    if column == "SKILLS":
                        target_definitions.append('"SKILLS" VARIANT')
                    else:
                        target_definitions.append(
                            f'"{column}" {df[column].dtype}'
                        )

                # The table already exists, so this statement
                # does not change its existing structure.
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {target_table}
                    LIKE {staging_full_name}
                """)

                # 3. Merge all jobs, converting SKILLS to VARIANT
                cursor.execute(f"""
                    MERGE INTO {target_table} AS target
                    USING {staging_full_name} AS source
                        ON target."ID" = source."ID"

                    WHEN MATCHED THEN UPDATE SET
                        {update_set}

                    WHEN NOT MATCHED THEN INSERT (
                        {quoted_columns}
                    )
                    VALUES (
                        {insert_values}
                    )
                """)

            logger.info(
                "Incremental MERGE completed: %s jobs processed",
                nrows,
            )

        finally:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"DROP TABLE IF EXISTS {staging_full_name}"
                    )
            except Exception:
                logger.exception("Could not drop staging table")

            conn.close()
