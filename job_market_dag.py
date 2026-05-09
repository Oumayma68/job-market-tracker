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
DBT_DIR = "/opt/airflow/projects/job-market-tracker/dbt"



def load_snowflake_env():
    conn = BaseHook.get_connection("jmt_snowflake_default")

    os.environ["SNOWFLAKE_ACCOUNT"] = conn.extra_dejson.get("account")
    os.environ["SNOWFLAKE_USER"] = conn.login
    os.environ["SNOWFLAKE_PASSWORD"] = conn.password
    os.environ["SNOWFLAKE_DATABASE"] = conn.schema or "JOBMARKET"
    os.environ["SNOWFLAKE_SCHEMA"] = conn.extra_dejson.get("snowflake_schema", "PUBLIC")
    os.environ["SNOWFLAKE_WAREHOUSE"] = conn.extra_dejson.get("warehouse", "COMPUTE_WH")


def collect_and_load(**context):
    sys.path.insert(0, "/opt/airflow/projects/job-market-tracker")

    from collectors.adzuna import AdzunaCollector

    # API KEYS (Variables)
    os.environ["ADZUNA_APP_ID"] = Variable.get("ADZUNA_APP_ID")
    os.environ["ADZUNA_APP_KEY"] = Variable.get("ADZUNA_APP_KEY")

    # Snowflake (Connection)
    load_snowflake_env()

    collector = AdzunaCollector()

    all_jobs = []
    for query in QUERIES:
        jobs = collector.collect(query=query)
        all_jobs.extend(jobs)

    if not all_jobs:
        raise ValueError("Aucune offre collectée — vérifier l'API Adzuna")

    collector.save_to_snowflake(all_jobs)

    print(f"{len(all_jobs)} jobs collectés et chargés")

    return len(all_jobs)


def check_data_quality(**context):
    ti = context["ti"]
    job_count = ti.xcom_pull(task_ids="collect_and_load")

    if job_count < 10:
        raise ValueError(f"Trop peu d'offres collectées: {job_count}")

    print(f" Quality check OK: {job_count} offres chargées")



DBT_PROJECT_DIR = "/opt/airflow/projects/job-market-tracker/dbt"
with DAG(
    dag_id="job_market_tracker",
    description="Adzuna → Snowflake RAW → dbt → Streamlit",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["job-market", "data-engineering", "dbt"],
) as dag:

    collect_task = PythonOperator(
        task_id="collect_and_load",
        python_callable=collect_and_load,
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
    bash_command=f"dbt docs generate --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    ) 
    collect_task >> quality_task >> dbt_run_task >> dbt_test_task >> dbt_docs
