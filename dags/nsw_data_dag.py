import pendulum
import os
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from pymongo import MongoClient
from bson import ObjectId


# =============================================================================
# Configuration (Environment Driven)
# =============================================================================

BASE_DIR = os.getenv("PIPELINE_BASE_DIR", "/opt/airflow")
DAGS_FOLDER_PATH = os.getenv("DAGS_FOLDER_PATH", f"{BASE_DIR}/dags")
DATA_DIR = os.getenv("DATA_DIR", f"{BASE_DIR}/data")
LIBPOSTAL_DIR = os.getenv("LIBPOSTAL_DIR", f"{BASE_DIR}/libpostal")
MATCHING_DIR = os.getenv("MATCHING_DIR", f"{BASE_DIR}/matching")

VENV_PATH = os.getenv("AIRFLOW_VENV_PATH", "/usr/bin")
LIBPOSTAL_VENV_PATH = os.getenv("LIBPOSTAL_VENV_PATH", "/usr/bin")
LIBPOSTAL_PYTHON_EXEC = os.getenv("LIBPOSTAL_PYTHON_EXEC", "/usr/bin/python3")

TIMESTAMP = "{{ data_interval_end.in_timezone(dag.timezone).strftime('%Y%m%dT%H') }}"
UPSERT_TIMESTAMP = "{{ data_interval_end.in_timezone(dag.timezone).strftime('%Y%m%d_%H%M') }}"

INGESTION_OUTPUT_FILE = f"{DAGS_FOLDER_PATH}/nsw_weekly_data_{TIMESTAMP}.csv"
PROCESSED_OUTPUT_FILE = f"{DATA_DIR}/nsw_processed_{TIMESTAMP}.csv"
NSW_LIBPOSTAL_SUCCESS_OUT = f"{LIBPOSTAL_DIR}/clean_nsw_{TIMESTAMP}.jsonl"
NSW_LIBPOSTAL_FAILURE_OUT = f"{LIBPOSTAL_DIR}/unsplit_nsw_{TIMESTAMP}.jsonl"
NSW_FINALIZED_OUT = f"{DATA_DIR}/final_nsw_{TIMESTAMP}.csv"
NSW_CLEANED_OUT = f"{DATA_DIR}/cleaned_nsw_{TIMESTAMP}.csv"
NSW_MATCHED_OUT = f"{MATCHING_DIR}/matched_nsw_{TIMESTAMP}.csv"

ADDRESS_LOG_FILE = f"{DATA_DIR}/log_address_{TIMESTAMP}.txt"
NSW_FINALIZE_LOG = f"{DATA_DIR}/log_finalize_{TIMESTAMP}.txt"
NSW_CLEAN_LOG = f"{DATA_DIR}/log_clean_{TIMESTAMP}.txt"
UPSERT_SUCCESS_LOG = f"{MATCHING_DIR}/log_upsert_success_{UPSERT_TIMESTAMP}.csv"
UPSERT_FAILURE_LOG = f"{MATCHING_DIR}/log_upsert_failure_{UPSERT_TIMESTAMP}.csv"
TRANSACTION_PROCESSOR_LOG = f"{MATCHING_DIR}/log_transaction_processor_{UPSERT_TIMESTAMP}.log"


default_args = {
    'retries': 4,
    'retry_delay': timedelta(minutes=6),
}


def create_run_summary(**context):
    mongo_uri = Variable.get("MONGO_URI")
    db_name = "airflow_summaries"
    coll_name = "nsw_pipeline"

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
    dag_id="nsw_weekly_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="Australia/Sydney"),
    schedule="0 14 * * 1",
    default_args=default_args,
    catchup=False,
    doc_md="""
    NSW Weekly Data ETL Pipeline
    - Ingestion
    - Address Processing
    - Cleansing
    - Matching
    - Upsert
    - Current State Update
    """,
    tags=['nsw', 'etl', 'pipeline'],
) as dag:

    env_vars = {
        "MONGO_URI": Variable.get("MONGO_URI"),
        "MONGO_DB": Variable.get("MONGO_DB", "property_database"),
        "GNAF_DB": Variable.get("GNAF_DB", "gnaf_database"),
        "AIRFLOW_SUMMARY_MONGO_URI": Variable.get("MONGO_URI"),
        "AIRFLOW_SUMMARY_COLLECTION": "nsw_pipeline",
    }

    start = EmptyOperator(task_id="start")

    create_run_summary_task = PythonOperator(
        task_id="create_run_summary",
        python_callable=create_run_summary,
    )

    def with_summary_env(base_env: dict):
        e = dict(base_env)
        e["AIRFLOW_RUN_SUMMARY_ID"] = "{{ ti.xcom_pull(task_ids='create_run_summary') }}"
        return e

    run_nsw_ingestion_task = BashOperator(
        task_id="nsw_data_ingestion",
        bash_command=f"""
            python {DAGS_FOLDER_PATH}/load_nsw_data.py --output "{INGESTION_OUTPUT_FILE}"
        """,
        env=with_summary_env(env_vars),
    )

    combine_address_task = BashOperator(
        task_id="combine_address_field",
        bash_command=f"""
            if [ -f "{INGESTION_OUTPUT_FILE}" ]; then
                python {DATA_DIR}/full_address.py \
                    --input "{INGESTION_OUTPUT_FILE}" \
                    --output "{PROCESSED_OUTPUT_FILE}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    process_nsw_addresses_task = BashOperator(
        task_id="cleanse_nsw_addresses",
        bash_command=f"""
            if [ -f "{PROCESSED_OUTPUT_FILE}" ]; then
                {LIBPOSTAL_PYTHON_EXEC} {LIBPOSTAL_DIR}/process_addresses.py \
                    --input "{PROCESSED_OUTPUT_FILE}" \
                    --success-output "{NSW_LIBPOSTAL_SUCCESS_OUT}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    finalize_nsw_task = BashOperator(
        task_id="finalize_nsw_addresses",
        bash_command=f"""
            if [ -f "{NSW_LIBPOSTAL_SUCCESS_OUT}" ]; then
                {LIBPOSTAL_PYTHON_EXEC} {DATA_DIR}/finalize_addresses.py \
                    --input "{NSW_LIBPOSTAL_SUCCESS_OUT}" \
                    --output "{NSW_FINALIZED_OUT}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    clean_nsw_data_task = BashOperator(
        task_id="clean_nsw_data",
        bash_command=f"""
            if [ -f "{NSW_FINALIZED_OUT}" ]; then
                python {DATA_DIR}/nsw_field_cleaner.py \
                    --input "{NSW_FINALIZED_OUT}" \
                    --output "{NSW_CLEANED_OUT}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    match_nsw_task = BashOperator(
        task_id="match_nsw_addresses",
        bash_command=f"""
            if [ -f "{NSW_CLEANED_OUT}" ]; then
                python {MATCHING_DIR}/run_address_matching.py \
                    --input "{NSW_CLEANED_OUT}" \
                    --output "{NSW_MATCHED_OUT}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    upsert_dat_task = BashOperator(
        task_id="upsert_dat",
        bash_command=f"""
            if [ -f "{NSW_MATCHED_OUT}" ]; then
                python {MATCHING_DIR}/dat_upsert.py \
                    --input "{NSW_MATCHED_OUT}" \
                    --success-log "{UPSERT_SUCCESS_LOG}" \
                    --failure-log "{UPSERT_FAILURE_LOG}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    update_current_state_task = BashOperator(
        task_id="update_current_state",
        bash_command=f"""
            if [ -f "{NSW_MATCHED_OUT}" ]; then
                python {MATCHING_DIR}/transaction_processor.py \
                    --input "{NSW_MATCHED_OUT}" \
                    --log "{TRANSACTION_PROCESSOR_LOG}";
            fi
        """,
        env=with_summary_env(env_vars),
    )

    end = EmptyOperator(task_id="end")

    (
        start >> create_run_summary_task >>
        run_nsw_ingestion_task >>
        combine_address_task >>
        process_nsw_addresses_task >>
        finalize_nsw_task >>
        clean_nsw_data_task >>
        match_nsw_task >>
        upsert_dat_task >>
        update_current_state_task >>
        end
    )
