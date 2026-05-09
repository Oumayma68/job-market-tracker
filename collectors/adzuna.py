"""
Collecteur Adzuna → Snowflake (table RAW_JOBS
"""
import os
import time
import json
import logging
import requests
import pandas as pd
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from collectors.base import JobOffer, extract_skills

logger = logging.getLogger(__name__)

ADZUNA_BASE_URL = "https://api.adzuna.com/v1/api/jobs"
COUNTRY = "fr"
RESULTS_PER_PAGE = 50
MAX_PAGES = 10
RATE_LIMIT_DELAY = 1.0


class AdzunaCollector:
    def __init__(self):
        self.app_id = os.environ["ADZUNA_APP_ID"]
        self.app_key = os.environ["ADZUNA_APP_KEY"]
        self.session = requests.Session()

    def _get(self, page: int, query: str) -> dict:
        url = f"{ADZUNA_BASE_URL}/{COUNTRY}/search/{page}"
        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": RESULTS_PER_PAGE,
            "what": query,
            "content-type": "application/json",
        }
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=10)
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1}/3 failed: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
        raise RuntimeError(f"Adzuna API échec après 3 tentatives (page={page})")

    def _parse_job(self, raw: dict) -> JobOffer:
        title = raw.get("title", "")
        description = raw.get("description", "")
        return JobOffer(
            id=f"adzuna_{raw['id']}",
            title=title,
            company=raw.get("company", {}).get("display_name", ""),
            location=raw.get("location", {}).get("display_name", ""),
            contract_type=raw.get("contract_type", ""),
            salary_min=raw.get("salary_min"),
            salary_max=raw.get("salary_max"),
            skills=extract_skills(f"{title} {description}"),
            source="adzuna",
            url=raw.get("redirect_url", ""),
            published_at=datetime.fromisoformat(raw["created"].replace("Z", "+00:00")),
            collected_at=datetime.utcnow(),
        )

    def collect(self, query: str = "data engineer") -> list[JobOffer]:
        jobs = []
        logger.info(f"Début collecte Adzuna: query='{query}'")
        for page in range(1, MAX_PAGES + 1):
            data = self._get(page, query)
            results = data.get("results", [])
            if not results:
                break
            for raw in results:
                try:
                    jobs.append(self._parse_job(raw))
                except Exception as e:
                    logger.warning(f"Parsing échoué: {e}")
            logger.info(f"Page {page}: {len(results)} offres")
            time.sleep(RATE_LIMIT_DELAY)
        logger.info(f"Total collecté: {len(jobs)} offres")
        return jobs

    def save_to_snowflake(self, jobs: list[JobOffer]) -> None:
        """Charge les offres brutes dans Snowflake RAW_JOBS (append)."""
        if not jobs:
            raise ValueError("Aucune offre à sauvegarder")

        df = pd.DataFrame([j.to_dict() for j in jobs]).drop_duplicates(subset=["id"])
        df.columns = [c.upper() for c in df.columns]

        df["SKILLS"] = df["SKILLS"].apply(
            lambda x: json.dumps(x) if isinstance(x, list) else "[]"
        )

        hook = SnowflakeHook(snowflake_conn_id="jmt_snowflake_default")
        conn = hook.get_conn()

        try:
            _, _, nrows, _ = write_pandas(
                conn, df,
                table_name="RAW_JOBS",
                database="JOBMARKET",
                schema="PUBLIC",
                overwrite=False,
                auto_create_table=True,
            )
            logger.info(f"{nrows} offres chargées dans RAW_JOBS")
        finally:
            conn.close()
