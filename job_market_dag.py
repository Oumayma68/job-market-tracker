
import os
import sys
import logging

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Variable


logger = logging.getLogger(__name__)

sys.path.insert(0, "/opt/airflow/projects/job-market-tracker")


# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "oumayma",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

QUERIES = [
    "data engineer",
    "ingénieur données",
    "data analytics engineer",
    "analytics engineer",
]

DBT_PROJECT_DIR = "/opt/airflow/projects/job-market-tracker/dbt"

MIN_TOTAL_JOBS = 10


# ------------------------------------------------------------
# FRANCE TRAVAIL: COLLECT ALL QUERIES
# ------------------------------------------------------------

def collect_all_jobs():

    from collectors.francetravail import FranceTravailCollector

    # Load API credentials from Airflow Variables
    os.environ["FRANCETRAVAIL_CLIENT_ID"] = Variable.get(
        "FRANCETRAVAIL_CLIENT_ID"
    )
    os.environ["FRANCETRAVAIL_CLIENT_SECRET"] = Variable.get(
        "FRANCETRAVAIL_CLIENT_SECRET"
    )

    collector = FranceTravailCollector()

    all_jobs = []
    failed_queries = []

    # Collect offers for each keyword
    for query in QUERIES:
        try:
            logger.info("Starting collection for query: %s", query)

            jobs = collector.collect(query=query)

            all_jobs.extend(jobs)

            logger.info(
                "Query '%s': %s jobs collected",
                query,
                len(jobs),
            )

        except Exception:
            failed_queries.append(query)

            logger.exception(
                "Collection failed for query: %s",
                query,
            )

    # Remove duplicates using the France Travail job ID
    unique_jobs = {
        job.id: job
        for job in all_jobs
    }

    jobs_to_save = list(unique_jobs.values())

    logger.info(
        "Collection completed: %s unique jobs from %s queries",
        len(jobs_to_save),
        len(QUERIES),
    )

    # Save successful results, even if some queries failed
    if jobs_to_save:
        collector.save_to_snowflake(jobs_to_save)

        logger.info(
            "%s unique jobs successfully saved to Snowflake",
            len(jobs_to_save),
        )
    else:
        logger.warning("No jobs collected. Nothing to save.")

    # Check minimum number of unique offers
    if len(jobs_to_save) < MIN_TOTAL_JOBS:
        logger.error(
            "Too few unique jobs collected: %s (minimum: %s)",
            len(jobs_to_save),
            MIN_TOTAL_JOBS,
        )

    # Report collection failures after saving successful results
    if failed_queries:
        raise AirflowException(
            f"Collection failed for queries: {failed_queries}. "
            f"{len(jobs_to_save)} unique jobs were saved."
        )

    # Fail if the total number of unique offers is insufficient
    if len(jobs_to_save) < MIN_TOTAL_JOBS:
        raise AirflowException(
            f"Too few unique jobs collected: "
            f"{len(jobs_to_save)} (minimum: {MIN_TOTAL_JOBS})"
        )

    logger.info("France Travail collection and quality check successful.")

    return len(jobs_to_save)


# ------------------------------------------------------------
# DAG
# ------------------------------------------------------------

with DAG(
    dag_id="job_market_tracker",
    description="France Travail → Snowflake RAW → dbt → Metabase",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["job-market", "data-engineering", "dbt"],
) as dag:

    
    # Collect all keywords, deduplicate and merge into Snowflake
    collect_jobs = PythonOperator(
        task_id="collect_all_jobs",
        python_callable=collect_all_jobs,
    )

    # Check freshness of RAW_JOBS
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=(
            f"dbt source freshness --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    # dbt build: seeds + models + tests
    dbt_build_task = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"dbt build --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    # Generate dbt documentation
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=(
            f"dbt docs generate --project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
    )

    # Task dependencies
    collect_jobs >> dbt_source_freshness >> dbt_build_task >> dbt_docs
