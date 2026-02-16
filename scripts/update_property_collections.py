"""
Property Collections Update Script
- Updates propertyHub collection with cleaned property types from Field35
- Updates propertyFeatures collection with cleaned property features
- Runs after the matching task when ADDRESS_DETAIL_PID is available

"""
import re
import csv
import os
import sys
import argparse
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from multiprocessing import Pool, cpu_count
from functools import partial

# --- Database Configuration ---
# Use environment variables; defaults are documentation-safe placeholders
DIGITAL_OCEAN_URI = os.environ.get("MONGO_URI")
DB_NAME = os.environ.get("MONGO_DB_NAME")
COLLECTION_NAME = os.environ.get("MONGO_PROPERTY_HUB_COLLECTION")
PROPERTY_FEATURES_COLLECTION_NAME = os.environ.get("MONGO_PROPERTY_FEATURES_COLLECTION")

# --- Parallel Processing Configuration ---
NUM_CORES = 8
BATCH_SIZE = 1000

# --- Universal Cleaning Functions ---

def clean_date(raw_date):
    """Converts a date string to 'yyyy-mm-dd' format."""
    if not raw_date or pd.isna(raw_date):
        return None
    try:
        # pd.to_datetime is robust and handles many formats
        date_obj = pd.to_datetime(raw_date)
        return date_obj.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None # Return None if parsing fails

def custom_paragraph_segmenter_v5(text):
    """Segments a paragraph of text using various rules."""
    if (pd.isna(text)) or (text is None):
        return text

    text = str(text).strip()
    text = re.sub(r'(?<!\n)[•●*]\s*', r'\n\g<0> ', text)
    text = re.sub(r'(?<!\n)(\d+\.\s+[A-Z][a-zA-Z &]{0,50}):', r'\n\1\n', text)
    text = re.sub(r'(:)(?!//)\s*', r'\1\n', text)
    text = re.sub(r'(?<!\n)\s*-\s+(?=[A-Z])', r'\n- ', text)
    text = re.sub(r'([.!?])\s*(?=([A-Z][a-zA-Z]+(?:\s[A-Z][a-zA-Z]+){0,4}):)', r'\1\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

def parse_price_value(value_str):
    """Converts a string with 'k' or 'm' to a numeric value."""
    if not isinstance(value_str, str): return None
    value_str = value_str.lower()
    multiplier = 1
    if 'k' in value_str:
        multiplier = 1000
        value_str = value_str.replace('k', '')
    elif 'm' in value_str or 'mil' in value_str:
        multiplier = 1000000
        value_str = value_str.replace('m', '').replace('il', '')

    try:
        cleaned_value = re.sub(r'[^\d.]', '', value_str)
        return int(float(cleaned_value) * multiplier)
    except (ValueError, TypeError):
        return None

def clean_price(raw_price):
    """
    Parses a raw price string to extract up to two numeric price values.
    """
    if not isinstance(raw_price, str) or not raw_price.strip():
        return None, None

    s = raw_price.replace(',', '').replace('$', '').replace(' ', '')
    s = re.sub(r'(?i)(offersover|plusgstifappliable|buyerguide|private(sale)?|eoi|guide|auction|northfacing|\(.*\)|[|+]|to)', '-', s)

    price_parts = re.findall(r'(\d+\.?\d*k|\d+\.?\d*m|\d+\.?\d*mil|\d+\.?\d*)', s, re.IGNORECASE)

    if not price_parts:
        return None, None

    numeric_prices = [p for p in [parse_price_value(p) for p in price_parts if p] if p is not None]

    if not numeric_prices:
        return None, None

    price1 = numeric_prices[0]
    price2 = numeric_prices[1] if len(numeric_prices) > 1 else None

    return price1, price2

def clean_area(raw_area):
    """
    Parses a raw area string to extract a numeric area and its unit.
    """
    if not isinstance(raw_area, str) or not raw_area.strip():
        return None, None

    s = raw_area.lower().replace(',', '').replace(' ', '').replace('(', '').replace(')', '')

    area_unit = None
    if 'hectare' in s or 'ha' in s:
        area_unit = 'hectares'
    elif 'acre' in s or 'ac' in s:
        area_unit = 'acres'

    match = re.search(r'(\d+\.?\d*)', s)
    if match:
        try:
            numeric_area = float(match.group(1))
            return numeric_area, area_unit
        except (ValueError, TypeError):
            return None, None

    return None, None

def clean_field35(row):
    """
    Cleans the 'Field35' value based on a set of specific rules, checking
    other fields like 'flat_number' and 'Field12' (description) as needed.
    """
    field35_val = str(row.get('Field35', '')).strip()
    flat_number = str(row.get('flat_number', '')).strip()
    description = str(row.get('Field12', '')).lower()

    # Standardize to proper case first
    cased_field35 = field35_val.title()

    # Rule 1: If value is already one of the desired final values, keep it.
    if cased_field35 in ["Apartment", "House", "Villa", "Townhouse", "Land"]:
        return cased_field35

    # Direct mappings for synonyms and specified values
    mapping = {
        "town house": "Townhouse",
        "vacant land": "Land",
        "semi-detached": "Duplex",
        "duplexsemi-detached": "Duplex",
        "apartment": "Apartment",
        "unit": "Apartment",
        "blockofunits": "Apartment",
        " units": "Apartment" # Handles value with a leading space
    }
    if field35_val.lower() in mapping:
        return mapping[field35_val.lower()]

    # Check if flat_number contains any numeric character
    has_numeric_flat_number = bool(re.search(r'\d', flat_number))

    def find_type_in_description(desc):
        """Searches description for property types in a specific order."""
        # Order is important to find specific terms first (e.g., 'townhouse' before 'house')
        search_order = {
            "townhouse": "Townhouse",
            "town house": "Townhouse",
            "villa": "Villa",
            "home": "House",
            "house": "House",
            "land": "Land",
            "units": "Apartment",
            "apartment": "Apartment"
        }
        for keyword, prop_type in search_order.items():
            if re.search(r'\b' + re.escape(keyword) + r'\b', desc):
                return prop_type
        return None

    # Rule 2: Logic for "Residential"
    if field35_val.lower() == 'residential':
        if has_numeric_flat_number:
            return "Apartment"
        
        # Check description only for the specified subset of types
        found_type = find_type_in_description(description)
        if found_type in ["House", "Villa", "Townhouse","Apartment"]:
            return found_type
        return None # Return None if it's residential but no other criteria match

    # Rule 3: Logic for empty Field35
    if not field35_val:
        if has_numeric_flat_number:
            return "Apartment"
        return find_type_in_description(description)

    # If the value is not empty but doesn't match any rules, return it as is.
    return field35_val if field35_val else None

# --- MongoDB Update Functions ---

def extract_gnaf_id_from_row(row: dict):
    """Attempt to extract a GNAF ID from a CSV row using common key variants."""
    if not isinstance(row, dict):
        return None
    possible_keys = [
        'gnafId', 'GNAF_ID', 'GNAF ID', 'gnaf_id', 'reference_id.gnafId',
        'GNAFId', 'gnafid'
    ]
    for key in possible_keys:
        if key in row and row.get(key):
            return str(row.get(key)).strip()
    # As a last resort, if ADDRESS_DETAIL_PID looks exactly like a GNAF (often starts with 'GA')
    address_pid = row.get('ADDRESS_DETAIL_PID')
    if isinstance(address_pid, str) and address_pid.strip().upper().startswith('GA'):
        return address_pid.strip()
    return None

def get_mongodb_connections():
    """Establish connection to Digital Ocean MongoDB database."""
    try:
        client = MongoClient(DIGITAL_OCEAN_URI)
        
        # Test connection
        client.admin.command('ping')
        
        db = client[DB_NAME]
        
        propertyhub = db[COLLECTION_NAME]
        propertyfeatures = db[PROPERTY_FEATURES_COLLECTION_NAME]
        
        return (propertyhub, propertyfeatures, client)
    except Exception as e:
        print(f"ERROR: Failed to connect to Digital Ocean MongoDB database: {e}")
        return None, None, None

def update_propertyhub_type(collection, address_pid, gnaf_id, cleaned_type, dry_run=False):
    """Update the type field in propertyHub collection in Digital Ocean database.

    Matches primarily by reference_id.gnafId. If not provided, falls back to ADDRESS_DETAIL_PID if present.
    """
    if not cleaned_type:
        return False
    
    current_time = datetime.utcnow()
    update_data = {
        "$set": {
            "type": cleaned_type,
            "updatedAt": current_time
        }
    }
    
    # Require GNAF ID; match strictly on reference_id.gnafId
    if not gnaf_id:
        print("  SKIP propertyHub: Missing GNAF ID; no fallback query will be used")
        return False
    query = {"reference_id.gnafId": gnaf_id}
    
    if dry_run:
        print(f" DRY RUN: Would update propertyHub with type '{cleaned_type}' using query: {query}")
        return True
    
    try:
        # Update Digital Ocean database
        result = collection.update_many(query, update_data)
        if result.modified_count > 0:
            print(f" DIGITAL OCEAN DB: Updated {result.modified_count} records with type '{cleaned_type}' using query: {query}")
            return True
        else:
            print(f"  DIGITAL OCEAN DB: No records found to update using query: {query}")
            return False
            
    except Exception as e:
        print(f" ERROR: Failed to update propertyHub collection for ADDRESS_DETAIL_PID: {address_pid}, GNAF_ID: {gnaf_id}: {e}")
        return False

def update_propertyfeatures(collection, address_pid, gnaf_id, beds, baths, car_spaces, land_area, dry_run=False):
    """Update the propertyFeatures collection in Digital Ocean database."""
    # Only proceed if we have at least one valid field to update
    if not any([beds, baths, car_spaces, land_area]):
        return False
    
    current_time = datetime.utcnow()
    update_data = {"$set": {"updatedAt": current_time}}
    
    # Add fields to update only if they have valid values
    if beds is not None:
        update_data["$set"]["beds"] = beds
    if baths is not None:
        update_data["$set"]["baths"] = baths
    if car_spaces is not None:
        update_data["$set"]["carSpaces"] = car_spaces
    if land_area is not None:
        update_data["$set"]["landArea"] = land_area
    
    # Query to match on gnafId field
    query = {
        "gnafId": gnaf_id
    }
    
    if dry_run:
        print(f" DRY RUN: Would update propertyFeatures for ADDRESS_DETAIL_PID: {address_pid}, GNAF_ID: {gnaf_id}")
        return True
    
    try:
        # Update Digital Ocean database
        result = collection.update_many(query, update_data)
        if result.modified_count > 0:
            print(f" DIGITAL OCEAN DB (propertyFeatures): Updated {result.modified_count} records for ADDRESS_DETAIL_PID: {address_pid}, GNAF_ID: {gnaf_id}")
            return True
        else:
            print(f" DIGITAL OCEAN DB (propertyFeatures): No records found to update for ADDRESS_DETAIL_PID: {address_pid}, GNAF_ID: {gnaf_id}")
            return False
            
    except Exception as e:
        print(f" ERROR: Failed to update propertyFeatures collection for ADDRESS_DETAIL_PID: {address_pid}, GNAF_ID: {gnaf_id}: {e}")
        return False

def process_batch(batch_data, dry_run=False):
    """Process a batch of records and return summary statistics."""
    batch_records = batch_data['records']
    
    # Create database connections inside the worker process to avoid SSLContext pickling issues
    propertyhub, propertyfeatures, client = get_mongodb_connections()
    if propertyhub is None or propertyfeatures is None:
        print("ERROR: Could not connect to MongoDB in worker process. Skipping batch.")
        return {
            'records_processed': 0,
            'updates_attempted': 0,
            'updates_successful': 0,
            'type_update_counts': {},
            'propertyfeatures_updates_attempted': 0,
            'propertyfeatures_updates_successful': 0
        }
    
    # Initialize batch tracking variables
    batch_records_processed = 0
    batch_updates_attempted = 0
    batch_updates_successful = 0
    batch_type_update_counts = {}
    batch_propertyfeatures_updates_attempted = 0
    batch_propertyfeatures_updates_successful = 0
    
    for row in batch_records:
        batch_records_processed += 1
        new_row = {k: v for k, v in row.items() if v != ''}

        # Clean price data
        price1, price2 = clean_price(new_row.get('Field9'))
        new_row['price'] = price1
        new_row['price2'] = price2

        # If the price has less than 5 digits, set it to null (None)
        if new_row.get('price') is not None and new_row['price'] < 10000:
            new_row['price'] = None

        # Clean area data
        area, area_unit = clean_area(new_row.get('Field36'))
        new_row['Area'] = area
        new_row['areaUnit'] = area_unit

        # Clean numeric fields
        for field in ['Field6', 'Field7', 'Field8']:
            if field in new_row:
                try:
                    new_row[field] = int(float(new_row[field]))
                except (ValueError, TypeError):
                    new_row[field] = None
        
        # Clean description
        new_row['Field12'] = custom_paragraph_segmenter_v5(new_row.get('Field12'))
        
        # Clean date
        new_row['Field11'] = clean_date(new_row.get('Field11'))

        # Clean Field35 using the new function
        cleaned_type = clean_field35(new_row)
        new_row['Field35'] = cleaned_type
        
        # Update propertyHub collections
        if cleaned_type:
            address_pid = new_row.get('ADDRESS_DETAIL_PID')
            # Prefer GNAF ID from CSV if available
            gnaf_id = extract_gnaf_id_from_row(new_row)
            if not gnaf_id and address_pid:
                # Fall back only if ADDRESS_DETAIL_PID looks like GNAF
                gnaf_id = address_pid if str(address_pid).upper().startswith('GA') else None
            
            if address_pid or gnaf_id:
                batch_updates_attempted += 1
                try:
                    update_success = update_propertyhub_type(propertyhub, address_pid, gnaf_id, cleaned_type, dry_run)
                    if update_success:
                        batch_updates_successful += 1
                        # Track counts by type
                        if cleaned_type in batch_type_update_counts:
                            batch_type_update_counts[cleaned_type] += 1
                        else:
                            batch_type_update_counts[cleaned_type] = 1
                except Exception as e:
                    print(f"WARNING: Failed to update propertyHub for record ADDRESS_DETAIL_PID={address_pid}, GNAF_ID={gnaf_id}: {e}")
            else:
                print(f" SKIP propertyHub: No identifier present (ADDRESS_DETAIL_PID or GNAF_ID) for row")

        # Update propertyFeatures collections
        address_pid = new_row.get('ADDRESS_DETAIL_PID')
        # Use ADDRESS_DETAIL_PID as the GNAF ID for matching against gnafId
        gnaf_id = address_pid
        
        if address_pid or gnaf_id:
            # Get cleaned values for property features
            beds = new_row.get('Field6')
            baths = new_row.get('Field7')
            car_spaces = new_row.get('Field8')
            land_area = new_row.get('Area')  # This is the cleaned area from Field36
            
            # Only attempt update if we have at least one valid field
            if any([beds, baths, car_spaces, land_area]):
                batch_propertyfeatures_updates_attempted += 1
                try:
                    update_success = update_propertyfeatures(propertyfeatures, 
                                                           address_pid, gnaf_id, beds, baths, car_spaces, land_area, dry_run)
                    if update_success:
                        batch_propertyfeatures_updates_successful += 1
                except Exception as e:
                    print(f"WARNING: Failed to update propertyFeatures for record ADDRESS_DETAIL_PID={address_pid}, GNAF_ID={gnaf_id}: {e}")
            else:
                print(f"  SKIP propertyFeatures: No valid feature fields present for row")

    # Close MongoDB connection
    if client is not None:
        client.close()
    
    return {
        'records_processed': batch_records_processed,
        'updates_attempted': batch_updates_attempted,
        'updates_successful': batch_updates_successful,
        'type_update_counts': batch_type_update_counts,
        'propertyfeatures_updates_attempted': batch_propertyfeatures_updates_attempted,
        'propertyfeatures_updates_successful': batch_propertyfeatures_updates_successful
    }

def main(args):
    """Main function to update MongoDB collections with cleaned data using parallel processing."""
    print('===========================================')
    print('Property Collections Update Script running...')
    print(f'Input file: {args.input_file}')
    print(f'Mode: {"DRY RUN" if args.dry_run else "EXECUTE"}')
    print(f'Parallel processing: {NUM_CORES} cores, batch size: {BATCH_SIZE}')
    print('===========================================')
    print('🔧 This script will:')
    print('   1. Read matched CSV with ADDRESS_DETAIL_PID')
    print('   2. Clean Field35 values for property types')
    print('   3. Clean Field6, Field7, Field8, Field36 for property features')
    print('   4. Update propertyHub collection in Digital Ocean database')
    print('   5. Update propertyFeatures collection in Digital Ocean database')
    print('   6. Update updatedAt timestamp for modified records')
    print('   7. Process data in parallel batches for improved performance')
    print('===========================================')
    
    # Note: Database connections will be established in each worker process to avoid SSLContext pickling issues
    print("Database connections will be established in each worker process...")

    # Read and prepare data
    try:
        with open(args.input_file, 'r', encoding='utf-8') as f_in:
            reader = csv.DictReader(f_in)
            data = list(reader)
            if not data:
                print("Input file is empty. Nothing to process.")
                return
            print(f" Loaded {len(data)} records from input file")
    except FileNotFoundError:
        print(f"ERROR: Input file not found at {args.input_file}. Aborting.")
        sys.exit(1)

    # Split data into batches
    batches = []
    for i in range(0, len(data), BATCH_SIZE):
        batch_records = data[i:i + BATCH_SIZE]
        batch_data = {
            'records': batch_records
        }
        batches.append(batch_data)
    
    print(f" Split data into {len(batches)} batches of up to {BATCH_SIZE} records each")
    print(f" Starting parallel processing with {NUM_CORES} cores...")
    
    # Process batches in parallel
    with Pool(processes=NUM_CORES) as pool:
        # Create partial function with dry_run parameter
        process_batch_with_dry_run = partial(process_batch, dry_run=args.dry_run)
        
        # Process all batches in parallel
        batch_results = pool.map(process_batch_with_dry_run, batches)
    
    print(" Parallel processing completed!")
    
    # Aggregate results from all batches
    total_records_processed = 0
    total_updates_attempted = 0
    total_updates_successful = 0
    type_update_counts = {}
    propertyfeatures_updates_attempted = 0
    propertyfeatures_updates_successful = 0
    
    for result in batch_results:
        total_records_processed += result['records_processed']
        total_updates_attempted += result['updates_attempted']
        total_updates_successful += result['updates_successful']
        propertyfeatures_updates_attempted += result['propertyfeatures_updates_attempted']
        propertyfeatures_updates_successful += result['propertyfeatures_updates_successful']
        
        # Merge type update counts
        for prop_type, count in result['type_update_counts'].items():
            if prop_type in type_update_counts:
                type_update_counts[prop_type] += count
            else:
                type_update_counts[prop_type] = count

    # Print comprehensive summary report
    print("\n" + "="*60)
    print(" DATABASE UPDATE SUMMARY REPORT")
    print("="*60)
    print(f" Total records processed: {total_records_processed}")
    print(f" Processed using {NUM_CORES} cores in {len(batches)} batches")
    
    print(f"\n PROPERTYHUB COLLECTION (Digital Ocean):")
    print(f" Total update attempts: {total_updates_attempted}")
    print(f" Total successful updates: {total_updates_successful}")
    print(f" Total failed updates: {total_updates_attempted - total_updates_successful}")
    
    if type_update_counts:
        print(f"\n Updates by property type:")
        for prop_type, count in sorted(type_update_counts.items()):
            print(f"   • {prop_type}: {count} records updated")
    else:
        print(f"\n No property type updates were made")
    
    if total_updates_attempted > 0:
        success_rate = (total_updates_successful / total_updates_attempted) * 100
        print(f"\n PropertyHub success rate: {success_rate:.1f}%")
    
    print(f"\n PROPERTYFEATURES COLLECTION (Digital Ocean):")
    print(f" Total update attempts: {propertyfeatures_updates_attempted}")
    print(f" Total successful updates: {propertyfeatures_updates_successful}")
    print(f" Total failed updates: {propertyfeatures_updates_attempted - propertyfeatures_updates_successful}")
    
    if propertyfeatures_updates_attempted > 0:
        features_success_rate = (propertyfeatures_updates_successful / propertyfeatures_updates_attempted) * 100
        print(f"\n PropertyFeatures success rate: {features_success_rate:.1f}%")
    
    print("="*60)
    print("All worker processes have completed and closed their database connections.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update propertyHub and propertyFeatures collections with cleaned data.")
    parser.add_argument('--input', dest='input_file', required=True, help="Path to the input CSV file with ADDRESS_DETAIL_PID.")
    parser.add_argument('--dry-run', action='store_true', help="Dry run mode - show what would be updated without making changes.")

    args = parser.parse_args()
    main(args)
