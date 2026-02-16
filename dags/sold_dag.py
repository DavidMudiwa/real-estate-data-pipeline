"""
Airflow DAG: Sold Property ETL Pipeline

Description:
End-to-end automated pipeline that ingests sold property data,
cleans and normalizes addresses, performs GNAF matching,
upserts transactions, updates current property state,
and syncs production property collections.

Designed for incremental processing and production stability.
"""

import os
import pendulum
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from pymongo import MongoClient


# =============================================================================
# Configuration (Environment Driven)
# =============================================================================

BASE_DIR = os.getenv("PIPELINE_BASE_DIR", "/opt/airflow")
DATA_DIR = f"{BASE_DIR}/data"
SCRIPTS_DIR = f"{BASE_DIR}/scripts"
MATCHING_DIR = f"{BASE_DIR}/matching"

TODAY_STR = "{{ macros.datetime.now().strftime('%Y%m%dT%H') }}"
RUN_TIMESTAMP = "{{ data_interval_start.strftime('%Y%m%d_%H%M') }}"

PYTHON_EXEC = os.getenv("PIPELINE_PYTHON", "/usr/bin/python3")

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


# =============================================================================
# Run Summary Logging
# =============================================================================

def create_run_summary(**context):
    """
    Creates a MongoDB document for each DAG run.
    Used for monitoring ingestion metrics and pipeline health.
    """
    mongo_uri = Variable.get("PIPELINE_MONGO_URI")
    client = MongoClient(mongo_uri)

    db = client["pipeline_monitoring"]
    coll = db["sold_pipeline_runs"]

    doc = {
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"],
        "execution_time": context["ts"],
    }

    result = coll.insert_one(doc)
    return str(result.inserted_id)


# =============================================================================
# DAG Definition
# =============================================================================

with DAG(
    dag_id="sold_property_etl_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="Australia/Sydney"),
    schedule="0 16 * * *",
    catchup=False,
    tags=["etl", "real-estate", "mongodb", "airflow"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    create_summary = PythonOperator(
        task_id="create_run_summary",
        python_callable=create_run_summary,
    )

    ingest_task = BashOperator(
        task_id="ingest_sold_data",
        bash_command=f"""
            {PYTHON_EXEC} {SCRIPTS_DIR}/load_ssr_csv.py sold --date {TODAY_STR}
        """,
    )

    cleanse_task = BashOperator(
        task_id="cleanse_addresses",
        bash_command=f"""
            {PYTHON_EXEC} {SCRIPTS_DIR}/process_addresses.py
        """,
    )

    match_task = BashOperator(
        task_id="match_to_gnaf",
        bash_command=f"""
            {PYTHON_EXEC} {MATCHING_DIR}/run_address_matching.py
        """,
    )

    upsert_task = BashOperator(
        task_id="upsert_transactions",
        bash_command=f"""
            {PYTHON_EXEC} {MATCHING_DIR}/sold_agent_property_matching.py
                --timestamp "{RUN_TIMESTAMP}"
        """,
    )

    current_state_task = BashOperator(
        task_id="update_current_state",
        bash_command=f"""
            {PYTHON_EXEC} {MATCHING_DIR}/transaction_processor.py
        """,
    )

    update_collections_task = BashOperator(
        task_id="update_property_collections",
        bash_command=f"""
            {PYTHON_EXEC} {SCRIPTS_DIR}/update_property_collections.py
        """,
    )

    (
        start
        >> create_summary
        >> ingest_task
        >> cleanse_task
        >> match_task
        >> upsert_task
        >> current_state_task
        >> update_collections_task
        >> end
    )

