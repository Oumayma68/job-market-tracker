import os
import time
import json
import logging
import re
import requests
import pandas as pd
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

from collectors.base import JobOffer, extract_skills

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

AUTH_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token"
SEARCH_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"

MAX_RESULTS = 150
RESULTS_PER_PAGE = 50
RATE_LIMIT_DELAY = 1.0

# ─────────────────────────────────────────────────────────────
# CONTRACT MAP
# ─────────────────────────────────────────────────────────────

CONTRACT_TYPE_MAP = {
    "CDI": "CDI",
    "CDD": "CDD",
    "MIS": "Intérim",
    "SAI": "Saisonnier",
    "LIB": "Freelance",
    "TTI": "Travail temporaire",
    "": "Non précisé",
}

# ─────────────────────────────────────────────────────────────
# SALARY PARSER 
# ─────────────────────────────────────────────────────────────

def parse_salary_france_travail(salary_text: str):
    """
    Parse le salaire France Travail de manière robuste.
    Extrait les nombres isolés d'abord, puis détecte la période.
    """
    if not salary_text:
        return None, None, "unknown"

    text = salary_text.lower()

    t_clean = text.replace("\xa0", " ").replace(",", ".")
    raw_finds = re.findall(r"\d+(?:\s*\d+)*(?:\.\d+)?", t_clean)
    
    numbers = []
    for num_str in raw_finds:
        clean_num = num_str.replace(" ", "")
        if "." in clean_num:
            clean_num = clean_num.split(".")[0]
            
        if clean_num:
            val = int(clean_num)
            # Élimination du "12" de "sur 12 mois"
            if val > 100: 
                numbers.append(val)

    if not numbers:
        return None, None, "unknown"

    if len(numbers) >= 2:
        salary_min = numbers[0]
        salary_max = numbers[1]
    else:
        salary_min = numbers[0]
        salary_max = None

  
    if salary_min >= 10000:
        period = "yearly"
    elif "annuel" in text or "année" in text or "an" in text:
        period = "yearly"
    elif "horaire" in text or "/h" in text or salary_min < 50:
        period = "hourly"
    elif "mensuel" in text or "mois" in text:
        period = "monthly"
    else:
        period = "unknown"


    if period == "monthly":
        salary_min *= 12
        if salary_max and salary_max < 10000:
            salary_max *= 12

    elif period == "hourly":
        HOURS_PER_WEEK = 35
        WEEKS_PER_YEAR = 52
        salary_min *= HOURS_PER_WEEK * WEEKS_PER_YEAR
        if salary_max:
            salary_max *= HOURS_PER_WEEK * WEEKS_PER_YEAR

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

        resp = requests.post(
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
            logger.error(f"Auth error: {resp.status_code} - {resp.text}")

        resp.raise_for_status()

        payload = resp.json()
        self._token = payload["access_token"]
        self._token_expiry = time.time() + payload.get("expires_in", 1490)

        logger.info("France Travail token obtained")
        return self._token

    # ─────────────────────────────────────────────────────────
    # API CALL
    # ─────────────────────────────────────────────────────────

    def _get(self, start: int, query: str) -> dict:
        end = min(start + RESULTS_PER_PAGE - 1, MAX_RESULTS - 1)

        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }

        params = {
            "motsCles": query,
            "range": f"{start}-{end}",
            "sort": 1,
        }

        for attempt in range(3):
            try:
                resp = self.session.get(
                    SEARCH_URL,
                    headers=headers,
                    params=params,
                    timeout=10,
                )

                if resp.status_code == 204:
                    return {"resultats": []}

                resp.raise_for_status()
                return resp.json()

            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt+1}/3 failed: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)

        raise RuntimeError(f"API failure start={start}")

    # ─────────────────────────────────────────────────────────
    # PARSING
    # ─────────────────────────────────────────────────────────

    def _parse_job(self, raw: dict) -> JobOffer:
        title = raw.get("intitule", "")
        description = raw.get("description", "")
        title_lower = title.lower()

        # ─── FILTRE D'EXCLUSION : STAGES ET ALTERNANCES ─────
        internship_keywords = ["stage", "internship", "alternance", "apprentissage", "trainee"]
        if any(kw in title_lower for kw in internship_keywords):
            raise ValueError(f"Offre ignorée : Stage ou Alternance détecté ({title})")

        # ─── SALARY ───────────────────────────────
        salary_text = raw.get("salaire", {}).get("libelle", "")
        salary_min, salary_max, salary_period = parse_salary_france_travail(salary_text)

        # ─── LOCATION ─────────────────────────────
        location = raw.get("lieuTravail", {}).get("libelle", "")

        # ─── CONTRACT ─────────────────────────────
        contrat_code = raw.get("typeContrat", "")
        contract_type = CONTRACT_TYPE_MAP.get(contrat_code, contrat_code)

        # ─── SÉCURITÉ CONTRAT "NON PRÉCISÉ" ───────
        if contract_type == "Non précisé" and salary_min and salary_min < 15000:
            raise ValueError("Offre ignorée : Contrat non précisé avec salaire incohérent")

        # ─── DATE ──────────────────────────────────
        date_str = raw.get("dateCreation", raw.get("dateActualisation", ""))

        try:
            published_at = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            published_at = datetime.utcnow()

        # ─── BUILD OBJECT ─────────────────────────
        return JobOffer(
            id=f"ft_{raw.get('id')}",
            title=title,
            company=raw.get("entreprise", {}).get("nom", ""),
            location=location,
            contract_type=contract_type,
            salary_min=salary_min,
            salary_max=salary_max,
            skills=extract_skills(f"{title} {description}"),
            source="francetravail",
            url=raw.get("origineOffre", {}).get("urlOrigine", ""),
            published_at=published_at,
            collected_at=datetime.utcnow(),
        )

    # ─────────────────────────────────────────────────────────
    # COLLECT
    # ─────────────────────────────────────────────────────────

    def collect(self, query: str = "data engineer") -> list[JobOffer]:
        jobs = []

        logger.info(f"Collecting France Travail: {query}")

        for start in range(0, MAX_RESULTS, RESULTS_PER_PAGE):
            data = self._get(start, query)
            results = data.get("resultats", [])

            if not results:
                break

            for raw in results:
                try:
                    jobs.append(self._parse_job(raw))
                except Exception as e:
                    logger.info(f"Offre filtrée/erreur {raw.get('id')}: {e}")

            logger.info(f"Fetched {len(results)} jobs (start={start})")
            time.sleep(RATE_LIMIT_DELAY)

        logger.info(f"Total collected: {len(jobs)}")
        return jobs

    # ─────────────────────────────────────────────────────────
    # SAVE TO SNOWFLAKE
    # ─────────────────────────────────────────────────────────

    def save_to_snowflake(self, jobs: list[JobOffer]) -> None:
        if not jobs:
            raise ValueError("No jobs to save")

        df = pd.DataFrame([j.to_dict() for j in jobs]).drop_duplicates(subset=["id"])
        df.columns = [c.upper() for c in df.columns]

        df["SKILLS"] = df["SKILLS"].apply(
            lambda x: json.dumps(x) if isinstance(x, list) else "[]"
        )

        hook = SnowflakeHook(snowflake_conn_id="jmt_snowflake_default")
        conn = hook.get_conn()

        try:
            _, _, nrows, _ = write_pandas(
                conn,
                df,
                table_name="RAW_JOBS",
                database="JOBMARKET",
                schema="PUBLIC",
                overwrite=False,
                auto_create_table=True,
            )

            logger.info(f"{nrows} jobs inserted into RAW_JOBS")

        finally:
            conn.close()
