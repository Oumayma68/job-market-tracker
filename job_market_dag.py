import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.sdk import Variable

sys.path.insert(0, "/opt/airflow/projects/job-market-tracker")

DEFAULT_ARGS = {
    "owner": "oumayma",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

QUERIES = ["data engineer", "data engineer python", "ingénieur données"]

DBT_PROJECT_DIR = "/opt/airflow/projects/job-market-tracker/dbt"


def load_snowflake_env():
    conn = BaseHook.get_connection("jmt_snowflake_default")
    os.environ["SNOWFLAKE_ACCOUNT"] = conn.extra_dejson.get("account")
    os.environ["SNOWFLAKE_USER"] = conn.login
    os.environ["SNOWFLAKE_PASSWORD"] = conn.password
    os.environ["SNOWFLAKE_DATABASE"] = conn.schema or "JOBMARKET"
    os.environ["SNOWFLAKE_SCHEMA"] = conn.extra_dejson.get("snowflake_schema", "PUBLIC")
    os.environ["SNOWFLAKE_WAREHOUSE"] = conn.extra_dejson.get("warehouse", "COMPUTE_WH")


def collect_and_load_francetravail(**context):
    sys.path.insert(0, "/opt/airflow/projects/job-market-tracker")

    from collectors.francetravail import FranceTravailCollector

    # API KEYS
    os.environ["FRANCETRAVAIL_CLIENT_ID"] = Variable.get("FRANCETRAVAIL_CLIENT_ID")
    os.environ["FRANCETRAVAIL_CLIENT_SECRET"] = Variable.get("FRANCETRAVAIL_CLIENT_SECRET")

    load_snowflake_env()

    collector = FranceTravailCollector()
    all_jobs = []

    for query in QUERIES:
        jobs = collector.collect(query=query)
        all_jobs.extend(jobs)

    if not all_jobs:
        raise ValueError("Aucune offre collectée — vérifier l'API France Travail")

    collector.save_to_snowflake(all_jobs)
    print(f"{len(all_jobs)} jobs collectés et chargés")

    return len(all_jobs)


def check_data_quality(**context):
    ti = context["ti"]
    ft_count = ti.xcom_pull(task_ids="collect_and_load_francetravail") or 0

    if ft_count < 10:
        raise ValueError(f"Trop peu d'offres collectées: {ft_count}")

    print(f"Quality check OK: {ft_count} offres France Travail chargées")


with DAG(
    dag_id="job_market_tracker",
    description="France Travail → Snowflake RAW → dbt → Streamlit",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["job-market", "data-engineering", "dbt"],
) as dag:

    collect_ft_task = PythonOperator(
        task_id="collect_and_load_francetravail",
        python_callable=collect_and_load_francetravail,
    )

    quality_task = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        dbt run \
        --project-dir {DBT_PROJECT_DIR} \
        --profiles-dir {DBT_PROJECT_DIR}
        """,
    )

    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        dbt test \
        --project-dir {DBT_PROJECT_DIR} \
        --profiles-dir {DBT_PROJECT_DIR}
        """,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"""
        dbt docs generate \
        --project-dir {DBT_PROJECT_DIR} \
        --profiles-dir {DBT_PROJECT_DIR}
        """,
    )

    collect_ft_task >> quality_task >> dbt_run_task >> dbt_test_task >> dbt_docs
