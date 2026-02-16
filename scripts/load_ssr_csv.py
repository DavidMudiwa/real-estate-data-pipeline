#!/usr/bin/env python3
"""
Incremental MongoDB Ingestion Script

"""

import os
import sys
import csv
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Optional
import pytz
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError
from bson.objectid import ObjectId


# =============================================================================
# Configuration
# =============================================================================

AU_TZ = pytz.timezone("Australia/Sydney")
METADATA_COLLECTION = os.getenv("METADATA_COLLECTION", "aggregate_state")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./output")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MONGO_TIMEOUT_MS = int(os.getenv("MONGO_TIMEOUT_MS", "10000"))

SNAPSHOT_PREFIX = {
    "sale": "sale_au",
    "rent": "rent_au",
    "sold": "sold_au",
}


# =============================================================================
# Logging Setup
# =============================================================================

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


# =============================================================================
# Utilities
# =============================================================================

def get_batch_id() -> str:
    return datetime.now(AU_TZ).strftime("%Y%m%d%H%M%S")


def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def safe_object_id(value: Optional[str]) -> Optional[ObjectId]:
    try:
        return ObjectId(value) if value else None
    except Exception:
        return None


def write_csv(filepath: str, docs: List[Dict]):
    if not docs:
        logger.info("No documents to write.")
        return

    headers = list({k for doc in docs for k in doc.keys()})

    with open(filepath, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(docs)

    logger.info("Wrote %s records to %s", len(docs), filepath)


# =============================================================================
# Core Logic
# =============================================================================

def process_database(
    db_name: str,
    date_str: str,
    mongo_uri: str,
    dry_run: bool = False
):
    logger.info("Starting ingestion for database: %s", db_name)

    prefix = SNAPSHOT_PREFIX.get(db_name)
    if not prefix:
        logger.error("Invalid db_type provided.")
        sys.exit(1)

    client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=MONGO_TIMEOUT_MS,
    )

    try:
        client.admin.command("ping")
    except PyMongoError as e:
        logger.error("MongoDB connection failed: %s", e)
        sys.exit(1)

    db = client[db_name]
    metadata = db[METADATA_COLLECTION]
    metadata.create_index([("agency_name", ASCENDING)])

    collections = [
        c for c in db.list_collection_names()
        if c != METADATA_COLLECTION
    ]

    batch_id = get_batch_id()
    all_new_docs = []

    for collection_name in sorted(collections):
        logger.info("Processing collection: %s", collection_name)

        previous_state = metadata.find_one(
            {"agency_name": collection_name}
        ) or {}

        last_id = safe_object_id(previous_state.get("last_id"))

        query = {"_id": {"$gt": last_id}} if last_id else {}

        try:
            new_docs = list(
                db[collection_name]
                .find(query)
                .sort("_id", ASCENDING)
            )
        except PyMongoError as e:
            logger.error("Query failed for %s: %s", collection_name, e)
            continue

        if not new_docs:
            logger.info("No new documents for %s", collection_name)
            continue

        last_written_id = str(new_docs[-1]["_id"])

        if not dry_run:
            metadata.replace_one(
                {"agency_name": collection_name},
                {
                    "agency_name": collection_name,
                    "last_run": datetime.utcnow(),
                    "last_id": last_written_id,
                    "batch_id": batch_id,
                    "record_count": len(new_docs),
                },
                upsert=True,
            )

        logger.info(
            "Found %s new records for %s",
            len(new_docs),
            collection_name,
        )

        all_new_docs.extend(new_docs)

    ensure_output_dir()
    output_file = os.path.join(
        OUTPUT_DIR,
        f"{prefix}_{date_str}.csv",
    )

    if not dry_run:
        write_csv(output_file, all_new_docs)

    logger.info("Ingestion complete. Total records: %s", len(all_new_docs))


# =============================================================================
# Entry Point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Incremental MongoDB ingestion."
    )
    parser.add_argument(
        "db_type",
        choices=["rent", "sale", "sold"],
        help="Database type to process.",
    )
    parser.add_argument(
        "--date",
        dest="date_str",
        help="Date in YYYYMMDD format.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without writing data or updating metadata.",
    )

    args = parser.parse_args()

    mongo_uri = os.getenv("REMOTE_URI")
    if not mongo_uri:
        logger.error("REMOTE_URI environment variable not set.")
        sys.exit(1)

    date_str = args.date_str or datetime.now(AU_TZ).strftime("%Y%m%d")

    process_database(
        db_name=args.db_type,
        date_str=date_str,
        mongo_uri=mongo_uri,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
