"""
Address Cleansing Script

Description:
- Cleans and normalizes raw address data from CSV
- Uses libpostal for parsing
- Applies custom normalization rules
- Outputs:
    - Success records (JSONL)
    - Failed records (CSV)
    - Failure logs (JSONL)
"""

import os
import sys
import json
import csv
import re
import argparse
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Tuple, List, Dict
from postal.parser import parse_address
from pymongo import MongoClient
from bson.objectid import ObjectId


# =============================================================================
# Configuration
# =============================================================================

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_WORKERS = int(os.getenv("ADDRESS_MAX_WORKERS", "4"))
BATCH_SIZE = int(os.getenv("ADDRESS_BATCH_SIZE", "1000"))

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


# =============================================================================
# Airflow Summary Support
# =============================================================================

def update_airflow_summary(fields: dict):
    mongo_uri = os.getenv("AIRFLOW_SUMMARY_MONGO_URI")
    coll_name = os.getenv("AIRFLOW_SUMMARY_COLLECTION")
    run_summary_id = os.getenv("AIRFLOW_RUN_SUMMARY_ID")

    if not mongo_uri or not coll_name or not run_summary_id:
        return

    try:
        client = MongoClient(mongo_uri)
        coll = client["airflow_summaries"][coll_name]
        coll.update_one(
            {"_id": ObjectId(run_summary_id)},
            {"$set": fields},
            upsert=False
        )
    except Exception as e:
        logger.warning("Failed updating Airflow summary: %s", e)


# =============================================================================
# Normalization Helpers
# =============================================================================

DOUBLE_SPACE_PATTERN = re.compile(r"(\d)\s{2,}(\d)")

def normalize_address(raw: str) -> str:
    if not raw:
        return raw

    raw = re.sub(r'^None\s+', '', raw, flags=re.IGNORECASE)
    raw = DOUBLE_SPACE_PATTERN.sub(r"\1/\2", raw)
    raw = re.sub(r"\s+,", ",", raw)
    raw = re.sub(r",\s*(\S)", r", \1", raw)
    raw = re.sub(r'^\s*NÂ°\s*', '', raw, flags=re.IGNORECASE)
    raw = re.sub(r"(['\"]).*?\1", "", raw)
    raw = raw.replace(".", "")
    raw = re.sub(r'^\s*(lot|lt)[\s]*(?=\d)', '', raw, flags=re.IGNORECASE)
    return raw.strip()


# =============================================================================
# Batch Processor
# =============================================================================

def process_batch(records: List[Dict], header: List[str]) -> Tuple[List, List, List]:

    successes = []
    failures_csv = []
    failures_log = []

    for rec in records:
        raw_address = rec.get("Field5", "")

        if not raw_address or not raw_address.strip():
            failures_csv.append({k: rec.get(k) for k in header})
            failures_log.append({"reason": "empty_address"})
            continue

        try:
            cleaned = normalize_address(raw_address)
            parsed = parse_address(cleaned)

            if not parsed:
                raise ValueError("libpostal_parse_failed")

            final_record = rec.copy()
            final_record["parsed_components"] = parsed

            successes.append(final_record)

        except Exception as e:
            failures_csv.append({k: rec.get(k) for k in header})
            failures_log.append({
                "reason": str(e),
                "address": raw_address
            })

    return successes, failures_csv, failures_log


# =============================================================================
# CSV Batch Iterator
# =============================================================================

def batch_iterator_csv(file_path: str):
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        batch = []

        for row in reader:
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                yield batch
                batch = []

        if batch:
            yield batch


# =============================================================================
# Main Execution
# =============================================================================

def main(args):

    logger.info("Starting address cleansing process")
    logger.info("Input file: %s", args.input_file)

    if not os.path.exists(args.input_file):
        logger.warning("Input file not found. Nothing to process.")
        sys.exit(0)

    try:
        with open(args.input_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            total_records = sum(1 for _ in reader)
    except Exception as e:
        logger.error("Failed reading input file: %s", e)
        sys.exit(1)

    if total_records == 0:
        logger.info("Input file is empty.")
        sys.exit(0)

    total_success = 0
    total_fail = 0

    with open(args.success_file, "w", encoding="utf-8") as f_success, \
         open(args.failure_file, "w", encoding="utf-8", newline="") as f_failure_csv, \
         open(args.failure_log, "w", encoding="utf-8") as f_failure_log:

        failure_writer = csv.DictWriter(f_failure_csv, fieldnames=header)
        failure_writer.writeheader()

        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(process_batch, batch, header)
                for batch in batch_iterator_csv(args.input_file)
            ]

            for future in as_completed(futures):
                try:
                    successes, failures_csv, failures_log = future.result()
                except Exception as e:
                    logger.error("Batch failed: %s", e)
                    continue

                for rec in successes:
                    f_success.write(json.dumps(rec, ensure_ascii=False) + "\n")

                if failures_csv:
                    failure_writer.writerows(failures_csv)

                for log_entry in failures_log:
                    f_failure_log.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

                total_success += len(successes)
                total_fail += len(failures_csv)

    success_rate = (total_success / total_records) * 100

    logger.info("Processing complete")
    logger.info("Total processed: %s", total_records)
    logger.info("Successful: %s (%.2f%%)", total_success, success_rate)
    logger.info("Failed: %s", total_fail)

    update_airflow_summary({
        "records_processed": total_records,
        "success_count": total_success,
        "failure_count": total_fail,
        "success_rate": round(success_rate, 2)
    })

    if args.delete_input:
        try:
            os.remove(args.input_file)
            logger.info("Deleted input file.")
        except OSError as e:
            logger.warning("Failed to delete input file: %s", e)

    sys.exit(0)


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Clean addresses from CSV using libpostal."
    )

    parser.add_argument("--input", required=True)
    parser.add_argument("--success-output", required=True)
    parser.add_argument("--failure-output", required=True)
    parser.add_argument("--failure-log", required=True)
    parser.add_argument("--delete-input", action="store_true")

    args = parser.parse_args()

    main(args)
