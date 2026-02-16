import pendulum
import os
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import timedelta
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
from bson import ObjectId

# =============================================================================
# Configuration
# =============================================================================

BASE_DIR = os.getenv("PIPELINE_BASE_DIR", "/opt/airflow")
SOURCE_CSV_DIR = os.getenv("SOURCE_CSV_DIR", f"{BASE_DIR}/dags")
DATA_DIR = os.getenv("DATA_DIR", f"{BASE_DIR}/data")
LIBPOSTAL_DIR = os.getenv("LIBPOSTAL_DIR", f"{BASE_DIR}/libpostal")
MATCHING_DIR = os.getenv("MATCHING_DIR", f"{BASE_DIR}/matching")

AIRFLOW_PYTHON_EXEC = os.getenv("AIRFLOW_PYTHON_EXEC", "/usr/bin/python3")
LIBPOSTAL_PYTHON_EXEC = os.getenv("LIBPOSTAL_PYTHON_EXEC", "/usr/bin/python3")

RUN_TIMESTAMP = "{{ macros.datetime.now().strftime('%Y%m%dT%H') }}"
TODAY_STR = "{{ macros.datetime.now().strftime('%Y%m%dT%H') }}"

INGESTION_SCRIPT_PATH = f"{SOURCE_CSV_DIR}/load_leased_data.py"
LIBPOSTAL_SCRIPT_PATH = f"{LIBPOSTAL_DIR}/process_addresses.py"
FINALIZE_SCRIPT_PATH = f"{DATA_DIR}/finalize_addresses.py"
FIELD_CLEANING_SCRIPT_PATH = f"{DATA_DIR}/clean_leased_data.py"
ADDRESS_MATCHING_SCRIPT_PATH = f"{MATCHING_DIR}/run_address_matching.py"
UPSERT_LEASED_SCRIPT_PATH = f"{SOURCE_CSV_DIR}/upsert_leased_data.py"
UPDATE_PROPERTY_COLLECTIONS_SCRIPT_PATH = f"{DATA_DIR}/update_property_collections.py"
TRANSACTION_PROCESSOR_SCRIPT_PATH = f"{MATCHING_DIR}/transaction_processor.py"
SILVER_COLLECTIONS_SCRIPT_PATH = f"{BASE_DIR}/csv_ingest.py"


def create_run_summary(**context):
    mongo_uri = Variable.get("MONGO_URI")
    db_name = "airflow_summaries"
    coll_name = "leased_pipeline"

    client = MongoClient(mongo_uri)
    coll = client[db_name][coll_name]

    doc = {
        "date": context["ts"],
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"],
    }

    res = coll.insert_one(doc)
    return str(res.inserted_id)


with DAG(
    dag_id="leased_processing_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="Australia/Sydney"),
    schedule="0 15 * * *",
    catchup=False,
    doc_md="""
    Leased Data Processing Pipeline
    - Ingestion
    - Address Cleansing
    - Post Processing
    - Agent and Agency Processing
    - Field Cleaning
    - Address Matching
    - Upsert
    - Collection Update
    - Transaction Processing
    """,
    tags=['leased', 'etl', 'processing'],
) as dag:

    env_vars = {
        'REMOTE_URI': Variable.get('MONGO_URI'),
        "AIRFLOW_SUMMARY_MONGO_URI": Variable.get("MONGO_URI"),
        "AIRFLOW_SUMMARY_COLLECTION": "leased_pipeline"
    }

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    create_run_summary_task = PythonOperator(
        task_id="create_run_summary",
        python_callable=create_run_summary,
    )

    def with_summary_env(base_env: dict):
        e = dict(base_env)
        e["AIRFLOW_RUN_SUMMARY_ID"] = "{{ ti.xcom_pull(task_ids='create_run_summary') }}"
        return e

    source_prefix = "leased_data"
    clean_jsonl = f"{LIBPOSTAL_DIR}/clean_{source_prefix}_{RUN_TIMESTAMP}.jsonl"
    finalized_csv = f"{DATA_DIR}/final_{source_prefix}_{RUN_TIMESTAMP}.csv"
    field_cleaned_csv = f"{DATA_DIR}/fields_clean_{source_prefix}_{RUN_TIMESTAMP}.csv"
    matched_csv = f"{MATCHING_DIR}/matched_{source_prefix}_{RUN_TIMESTAMP}.csv"

    ingest_leased_task = BashOperator(
        task_id="ingest_leased_data",
        bash_command=f"""
            {AIRFLOW_PYTHON_EXEC} {INGESTION_SCRIPT_PATH} --date {TODAY_STR}
        """,
        env=with_summary_env(env_vars),
        execution_timeout=timedelta(minutes=30),
    )

    process_leased_task = BashOperator(
        task_id="process_addresses",
        bash_command=f"""
            if [ -f "{SOURCE_CSV_DIR}/{source_prefix}_{TODAY_STR}.csv" ]; then
                {LIBPOSTAL_PYTHON_EXEC} {LIBPOSTAL_SCRIPT_PATH} \
                    --input "{SOURCE_CSV_DIR}/{source_prefix}_{TODAY_STR}.csv" \
                    --success-output "{clean_jsonl}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    finalize_leased_task = BashOperator(
        task_id="postlib_addresses",
        bash_command=f"""
            if [ -f "{clean_jsonl}" ]; then
                {LIBPOSTAL_PYTHON_EXEC} {FINALIZE_SCRIPT_PATH} \
                    --input "{clean_jsonl}" \
                    --output "{finalized_csv}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    field_cleaning_task = BashOperator(
        task_id="fields_cleaning",
        bash_command=f"""
            if [ -f "{finalized_csv}" ]; then
                {AIRFLOW_PYTHON_EXEC} {FIELD_CLEANING_SCRIPT_PATH} \
                    --input "{finalized_csv}" \
                    --output "{field_cleaned_csv}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    match_leased_task = BashOperator(
        task_id="matching_leased_data",
        bash_command=f"""
            if [ -f "{field_cleaned_csv}" ]; then
                {AIRFLOW_PYTHON_EXEC} {ADDRESS_MATCHING_SCRIPT_PATH} \
                    --input "{field_cleaned_csv}" \
                    --output "{matched_csv}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    upsert_leased_task = BashOperator(
        task_id="upsert_leased",
        bash_command=f"""
            if [ -f "{matched_csv}" ]; then
                {AIRFLOW_PYTHON_EXEC} {UPSERT_LEASED_SCRIPT_PATH} \
                    --input "{matched_csv}" \
                    --timestamp "{RUN_TIMESTAMP}" \
                    --limited;
            fi
        """,
        execution_timeout=timedelta(minutes=60),
        env=with_summary_env(env_vars),
    )

    process_transactions_task = BashOperator(
        task_id="update_current_state",
        bash_command=f"""
            if [ -f "{matched_csv}" ]; then
                {AIRFLOW_PYTHON_EXEC} {TRANSACTION_PROCESSOR_SCRIPT_PATH} \
                    --input "{matched_csv}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    update_property_collections_task = BashOperator(
        task_id="update_property_collections",
        bash_command=f"""
            if [ -f "{matched_csv}" ]; then
                {AIRFLOW_PYTHON_EXEC} {UPDATE_PROPERTY_COLLECTIONS_SCRIPT_PATH} \
                    --input "{matched_csv}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    start >> create_run_summary_task >> ingest_leased_task >> process_leased_task >> finalize_leased_task >> field_cleaning_task >> match_leased_task >> upsert_leased_task >> process_transactions_task >> update_property_collections_task >> end
