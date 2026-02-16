```md
# Property Data Processing – ETL Pipeline Documentation

## Overview

This repository contains a production-grade property transaction data pipeline.

The pipeline processes property data through:

1. Address cleansing (libpostal parsing)  
2. Address finalization and normalization  
3. Field-level data cleaning  
4. GNAF address matching  
5. Transaction upsert into MongoDB  
6. Current state recalculation  

The same core architecture is reused across multiple property data types:

- Sold data  
- Sale listings  
- Leased data  
- Rent listings  

Each data type has its own Airflow DAG, but they share common processing scripts to ensure consistency and maintainability.

---

## Execution Modes

The scripts can be executed in two ways:

### 1️⃣ Manual Execution (Step-by-Step)

Each script can be run independently via command line.

This is useful for:

- Local development  
- Debugging  
- Backfilling historical data  
- Testing individual pipeline stages  

Scripts must be executed in sequence because each stage depends on the previous output.

---

### 2️⃣ Scheduled Execution via Apache Airflow

The `dags/` folder contains multiple DAGs, including:

- Sold data pipeline  
- Sale data pipeline  
- Leased data pipeline  

Although each DAG handles a different data source, they reuse many of the same core scripts (address processing, matching, transaction logic).

Each DAG:

- Runs scripts in the correct order  
- Tracks run metadata  
- Logs summary statistics  
- Handles retries  
- Enables scheduled automation (daily or weekly)  

This allows the pipeline to operate as a fully automated ETL workflow.

---

## High-Level Pipeline Flow

```

Ingestion
↓
Address Cleansing (libpostal)
↓
Address Finalization
↓
Field Cleaning
↓
GNAF Address Matching
↓
Transaction Upsert
↓
Current State Processing

```

---

## Shared Script Architecture

Multiple DAGs reuse the same core processing modules:

- `process_addresses.py`
- `finalize_addresses.py`
- `run_address_matching.py`
- `transaction_processor.py`

Data-type-specific scripts include:

- `clean_sold_data.py`
- `clean_sale_data.py`
- `clean_leased_data.py`
- `<data_type>_agent_property_matching.py`

This modular design ensures:

- Code reusability  
- Consistent data standards  
- Easier maintenance  
- Lower duplication  
- Safer future enhancements  

---

## Environment Requirements

### Python Environments

- **Libpostal Environment**
  - Required for `process_addresses.py`
  - Must run in Linux or WSL environment
  - Requires libpostal installed

- **Standard Python Environment**
  - Used for all other scripts
  - Compatible with Windows, Linux, macOS

---

## Required Environment Variables

### For Address Matching (GNAF)

- `MONGO_URI`
- `GNAF_DB`
- `GNAF_COLLECTION`

### For Transaction Processing

- `MONGO_URI`
- `MONGO_DB_NAME`
- `MONGO_PROPERTY_COLLECTION`
- `MONGO_AGENTS_COLLECTION`
- `MONGO_COMPANIES_COLLECTION`

When running via Airflow, these are managed through Airflow Variables.

---

## Script Execution Order

Scripts must be executed in this order:

1. `process_addresses.py`
2. `finalize_addresses.py`
3. `clean_<data_type>_data.py`
4. `run_address_matching.py`
5. `<data_type>_agent_property_matching.py`
6. `transaction_processor.py`

Replace `<data_type>` with:

- sold
- sale
- leased
- rent

---

## Performance Characteristics

- Batch processing for large datasets  
- Concurrent upsert logic  
- Bulk MongoDB operations  
- Memory-controlled batching  
- Idempotent transaction recalculation  
- Shared script reuse across multiple DAGs  

---

## Repository Structure

```

dags/
sold_data_dag.py
sale_data_dag.py
leased_data_dag.py

src/
process_addresses.py
finalize_addresses.py
run_address_matching.py
transaction_processor.py
clean_sold_data.py
clean_sale_data.py
clean_leased_data.py

```

---

## Summary

This repository provides:

- A modular property data ETL pipeline  
- Manual execution capability for development  
- Airflow orchestration for production automation  
- Reusable processing logic across multiple data pipelines  
- Deterministic address matching with GNAF  
- Intelligent transaction merging and state management  
```
