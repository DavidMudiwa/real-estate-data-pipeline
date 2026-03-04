"""
Incremental MongoDB Ingestion Script
Description:
Fetches new documents from remote MongoDB collections using
last processed ObjectId tracking. Writes results to CSV and
updates ingestion metadata for incremental processing.
"""
import os
import re
import pytz
from datetime import datetime
from pymongo import MongoClient, ASCENDING
from bson.objectid import ObjectId
import sys
import csv
import argparse

# --- Configuration ---
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'aggregate_ssr.log')
SNAPSHOT_PREFIX = {"sale": "sale_au", "rent": "rent_au", "sold": "sold_au"}
METADATA_COLL = "aggregate_state"
AU_TZ = pytz.timezone("Australia/Sydney")

# --- Helper Functions ---
def log(msg):
    """Logs a message to both the console and a log file."""
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{timestamp}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
    except Exception as e:
        print(f"[LOGGING ERROR] {e}")

def _update_airflow_summary(fields: dict):
    mongo_uri = os.getenv("AIRFLOW_SUMMARY_MONGO_URI")
    coll_name = os.getenv("AIRFLOW_SUMMARY_COLLECTION")  # sold/sale/rent
    run_summary_id = os.getenv("AIRFLOW_RUN_SUMMARY_ID")

    if not mongo_uri or not coll_name or not run_summary_id:
        return

    try:
        client = MongoClient(mongo_uri)
        coll = client["airflow_summaries"][coll_name]
        coll.update_one({"_id": ObjectId(run_summary_id)}, {"$set": fields}, upsert=False)
    except Exception as e:
        print(f"[SUMMARY_UPDATE_ERROR] {e}")

def get_au_batch_id():
    """Return current Sydney timestamp as ibis-compliant YYYYMMDDHHmmSS."""
    return datetime.now(AU_TZ).strftime("%Y%m%d%H%M%S")

def write_csv(filename, docs):
    """Writes a list of dictionaries to a CSV file."""
    if not docs:
        log(f"INFO: No documents to write for {filename}.")
        return
    headers = []
    for d in docs:
        for k in d.keys():
            if k not in headers:
                headers.append(k)
    try:
        with open(filename, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(docs)
        log(f"SUCCESS: Wrote {len(docs)} records to {filename}")
    except IOError as e:
        log(f"FATAL: Failed to write CSV file {filename} ({e})")

# --- Main Processing Function ---
def process_database(db_name, date_str, remote_client):
    """Fetch new docs from remote DB and write them to a CSV file."""
    log(f"=== Processing remote DB: '{db_name}' for date {date_str} ===")
    try:
        remote_db = remote_client[db_name]
        metadata = remote_db[METADATA_COLL]
        metadata.create_index([("agency_name", ASCENDING)])
    except Exception as e:
        log(f"ERROR: Cannot access remote DB '{db_name}' ({e}). Skipping.")
        return

    prefix = SNAPSHOT_PREFIX.get(db_name)
    if not prefix:
        log(f"WARNING: No SNAPSHOT_PREFIX for '{db_name}'; skipping.")
        return

    output_csv_filename = f"{prefix}_{date_str}.csv"
    log(f"→ Output will be written to: '{output_csv_filename}'")

    try:
        source_cols = [c for c in remote_db.list_collection_names() if c != METADATA_COLL]
    except Exception as e:
        log(f"ERROR: Cannot list collections in remote DB '{db_name}' ({e}); skipping.")
        return

    batch_id = get_au_batch_id()
    all_new_docs_for_csv = []
    
    for src_name in sorted(source_cols):
        log(f"• Processing source collection: '{src_name}'")
        try:
            # Find the previous state to get old values
            previous_state = metadata.find_one({"agency_name": src_name})
            if not previous_state:
                previous_state = {} # Handle first-run case

            last_id = previous_state.get("last_id")
            query = {"_id": {"$gt": ObjectId(last_id)}} if last_id else {}
            
            src_coll = remote_db[src_name]
            new_docs = list(src_coll.find(query).sort("_id", ASCENDING))

            if new_docs:
                last_insert_count = len(new_docs)
                last_written_id = str(new_docs[-1]['_id'])
                
                # Create the new, detailed metadata document
                new_metadata_doc = {
                    "agency_name": src_name,
                    "last_run": datetime.utcnow(),
                    "last_id": last_written_id,
                    "batchId": batch_id,
                    "last_insert_count": last_insert_count,
                    "previous_last_run_id": previous_state.get("last_id"),
                    "previous_last_run": previous_state.get("last_run"),
                    "previous_batchId": previous_state.get("batchId")
                }

                metadata.replace_one(
                    {"agency_name": src_name},
                    new_metadata_doc,
                    upsert=True
                )
                
                all_new_docs_for_csv.extend(new_docs)
                log(f"– Found {last_insert_count} new docs. Updated metadata with last_id: {last_written_id}")
            else:
                log(f"– No new documents found for '{src_name}'.")
        except Exception as e:
            log(f"ERROR: Failed during processing of '{src_name}' ({e}); skipping source.")
            continue

    write_csv(output_csv_filename, all_new_docs_for_csv)
    _update_airflow_summary({
    "total_records_ingested": len(all_new_docs_for_csv),
    })
    log(f"✅ Finished processing '{db_name}'.")

def main(args):
    """Main function to connect to remote DB and start processing."""
    remote_uri = os.getenv('REMOTE_URI')
    if not remote_uri:
        log("FATAL: REMOTE_URI environment variable not set. Exiting.")
        sys.exit(1)

    try:
        remote_client = MongoClient(remote_uri)
        remote_client.admin.command('ping')
    except Exception as e:
        log(f"FATAL: Cannot connect to remote MongoDB ({e}); exiting.")
        sys.exit(1)

    date_str = args.date_str if args.date_str else datetime.now(AU_TZ).strftime("%Y%m%d")
    
    process_database(args.db_type, date_str, remote_client)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest data from SSR databases.")
    parser.add_argument('db_type', choices=['rent', 'sale', 'sold'], help="The type of data to process.")
    parser.add_argument('--date', dest='date_str', help="Date string in YYYYMMDD format for the output filename.")
    args = parser.parse_args()
    main(args)
