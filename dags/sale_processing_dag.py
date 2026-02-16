import pendulum
import os
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from pymongo import MongoClient
from bson import ObjectId

# =============================================================================
# --- Configuration & Paths  ---
# =============================================================================

BASE_DIR = os.getenv("PIPELINE_BASE_DIR", "/opt/airflow")
DATA_DIR = os.getenv("DATA_DIR", f"{BASE_DIR}/data")
MATCHING_DIR = os.getenv("MATCHING_DIR", f"{BASE_DIR}/matching")
LIBPOSTAL_DIR = os.getenv("LIBPOSTAL_DIR", f"{BASE_DIR}/libpostal")
SOURCE_CSV_DIR = os.getenv("SOURCE_CSV_DIR", f"{BASE_DIR}/dags")

AIRFLOW_PYTHON_EXEC = os.getenv("AIRFLOW_PYTHON_EXEC", "/usr/bin/python3")
LIBPOSTAL_PYTHON_EXEC = os.getenv("LIBPOSTAL_PYTHON_EXEC", "/usr/bin/python3")

TODAY_STR = "{{ macros.datetime.now().strftime('%Y%m%dT%H') }}"
RUN_TIMESTAMP = "{{ data_interval_start.strftime('%Y%m%d_%H%M') }}"

INGESTION_SCRIPT_PATH = f"{SOURCE_CSV_DIR}/load_ssr_csv.py"
LIBPOSTAL_SCRIPT_PATH = f"{LIBPOSTAL_DIR}/process_addresses.py"
FINALIZE_SCRIPT_PATH = f"{DATA_DIR}/finalize_addresses.py"
CLEAN_SALE_DATA_SCRIPT_PATH = f"{DATA_DIR}/clean_sale_data.py"
ADDRESS_MATCHING_SCRIPT_PATH = f"{MATCHING_DIR}/run_address_matching.py"
SILVER_COLLECTIONS_SCRIPT_PATH = f"{BASE_DIR}/csv_ingest.py"
SALE_UPSERT_SCRIPT_PATH = f"{MATCHING_DIR}/sale_upsert.py"
UPDATE_PROPERTY_COLLECTIONS_SCRIPT_PATH = f"{DATA_DIR}/update_property_collections.py"
TRANSACTION_PROCESSOR_SCRIPT_PATH = f"{MATCHING_DIR}/transaction_processor.py"

# =============================================================================
# --- DAG Definition ---
# =============================================================================

default_args = {
    'owner': 'data-team',
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

def create_run_summary(**context):
    mongo_uri = Variable.get("MONGO_URI")
    db_name = "airflow_summaries"
    coll_name = "sale_pipeline"

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
    dag_id="sale_processing_pipeline",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="Australia/Sydney"),
    schedule="0 15 * * *",
    catchup=False,
    doc_md="""
    Sale Data Processing Pipeline
    - Ingestion
    - Address Cleansing
    - Post-processing
    - Agent & Agency Processing
    - Field Cleaning
    - Address Matching
    - Sale Upsert
    - Collection Update
    - Transaction Processing
    """,
    tags=['sale', 'etl', 'processing'],
) as dag:

    env_vars = {
        'REMOTE_URI': Variable.get('MONGO_URI'),
        "AIRFLOW_SUMMARY_MONGO_URI": Variable.get("MONGO_URI"),
        "AIRFLOW_SUMMARY_COLLECTION": "sale_pipeline",
    }

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    create_run_summary_task = PythonOperator(
        task_id="create_run_summary",
        python_callable=create_run_summary,
    )

    sale_prefix = "sale_data"
    sale_ingest_out = f"{SOURCE_CSV_DIR}/{sale_prefix}_{TODAY_STR}.csv"
    sale_proc_out = f"{LIBPOSTAL_DIR}/clean_{sale_prefix}_{TODAY_STR}.jsonl"
    sale_finalize_out = f"{DATA_DIR}/final_{sale_prefix}_{TODAY_STR}.csv"
    sale_data_clean_out = f"{DATA_DIR}/data_clean_{sale_prefix}_{TODAY_STR}.csv"
    sale_matched_out = f"{MATCHING_DIR}/matched_{sale_prefix}_{TODAY_STR}.csv"

    def with_summary_env(base_env: dict):
        e = dict(base_env)
        e["AIRFLOW_RUN_SUMMARY_ID"] = "{{ ti.xcom_pull(task_ids='create_run_summary') }}"
        return e

    ingest_sale_task = BashOperator(
        task_id="ingest_sale",
        bash_command=f"""
            {AIRFLOW_PYTHON_EXEC} {INGESTION_SCRIPT_PATH} sale --date {TODAY_STR}
        """,
        env=with_summary_env(env_vars),
    )

    process_sale_task = BashOperator(
        task_id="cleanse_sale_addresses",
        bash_command=f"""
            if [ -f "{sale_ingest_out}" ]; then
                {LIBPOSTAL_PYTHON_EXEC} {LIBPOSTAL_SCRIPT_PATH} \
                    --input "{sale_ingest_out}" \
                    --success-output "{sale_proc_out}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    finalize_sale_task = BashOperator(
        task_id="postlib_sale_addresses",
        bash_command=f"""
            if [ -f "{sale_proc_out}" ]; then
                {LIBPOSTAL_PYTHON_EXEC} {FINALIZE_SCRIPT_PATH} \
                    --input "{sale_proc_out}" \
                    --output "{sale_finalize_out}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    clean_sale_data_task = BashOperator(
        task_id="clean_sale_fields",
        bash_command=f"""
            if [ -f "{sale_finalize_out}" ]; then
                {AIRFLOW_PYTHON_EXEC} {CLEAN_SALE_DATA_SCRIPT_PATH} \
                    --input "{sale_finalize_out}" \
                    --output "{sale_data_clean_out}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    match_sale_task = BashOperator(
        task_id="match_sale",
        bash_command=f"""
            if [ -f "{sale_data_clean_out}" ]; then
                {AIRFLOW_PYTHON_EXEC} {ADDRESS_MATCHING_SCRIPT_PATH} \
                    --input "{sale_data_clean_out}" \
                    --output "{sale_matched_out}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    process_transactions_task = BashOperator(
        task_id="update_current_state",
        bash_command=f"""
            if [ -f "{sale_matched_out}" ]; then
                {AIRFLOW_PYTHON_EXEC} {TRANSACTION_PROCESSOR_SCRIPT_PATH} \
                    --input "{sale_matched_out}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    update_property_collections_task = BashOperator(
        task_id="update_property_collections",
        bash_command=f"""
            if [ -f "{sale_matched_out}" ]; then
                {AIRFLOW_PYTHON_EXEC} {UPDATE_PROPERTY_COLLECTIONS_SCRIPT_PATH} \
                    --input "{sale_matched_out}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    start >> create_run_summary_task >> ingest_sale_task >> process_sale_task >> finalize_sale_task >> clean_sale_data_task >> match_sale_task >> process_transactions_task >> update_property_collections_task >> end
