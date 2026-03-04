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
import json
import os
import sys
import csv
import argparse
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from postal.parser import parse_address
from pymongo import MongoClient
from bson.objectid import ObjectId
from tqdm import tqdm
import re


# Address normalization patterns and functions from normalize_field5.py
COMMA_SPACE_PATTERN = re.compile(r",\s+")
# Pattern to match double spaces that are between digits (numbers)
DOUBLE_SPACE_PATTERN = re.compile(r"(\d)\s{2,}(\d)")
STREET_TO_SUBURB_PATTERN = re.compile(
    r"\b(Street|St|Avenue|Ave|Road|Rd|Court|Ct|Drive|Dr|Lane|Ln|Boulevard|Bvd|Way|Place|Pl|Terrace|Tce|Crescent|Cres|Close|Cl)([A-Z])"
)

def _update_airflow_summary(fields: dict):
    mongo_uri = os.getenv("AIRFLOW_SUMMARY_MONGO_URI")
    coll_name = os.getenv("AIRFLOW_SUMMARY_COLLECTION")
    run_summary_id = os.getenv("AIRFLOW_RUN_SUMMARY_ID")

    if not mongo_uri or not coll_name or not run_summary_id:
        return

    try:
        client = MongoClient(mongo_uri)
        coll = client["airflow_summaries"][coll_name]
        coll.update_one({"_id": ObjectId(run_summary_id)}, {"$set": fields}, upsert=False)
    except Exception as e:
        print(f"[SUMMARY_UPDATE_ERROR] {e}")

# improved: To Remove "NÂ°" prefix which is a common encoding error for "Nº"
# NOTE: These paths should be configured correctly in your environment
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCALITIES_FILE = os.path.join(BASE_DIR, 'localities.json')
STREETS_FILE = os.path.join(BASE_DIR, 'streets.json')
STREET_TYPE_FILE = os.path.join(BASE_DIR, 'STREET_TYPE_AUT_psv.psv')

def load_localities():
    try:
        with open(LOCALITIES_FILE, 'r', encoding='utf-8') as f:
            localities = json.load(f)
        return set((l['locality'].strip().lower(), l['state'].strip().lower()) for l in localities)
    except FileNotFoundError:
        print(f"Warning: Localities file not found at {LOCALITIES_FILE}. Suburb detection may be affected.")
        return set()

def load_streets():
    try:
        with open(STREETS_FILE, 'r', encoding='utf-8') as f:
            streets = json.load(f)
        if isinstance(streets, list):
            if streets and isinstance(streets[0], dict):
                return set(s['street'].strip().lower() for s in streets if 'street' in s)
            else:
                return set(s.strip().lower() for s in streets)
        elif isinstance(streets, dict):
            return set(streets.keys())
    except FileNotFoundError:
        print(f"Warning: Streets file not found at {STREETS_FILE}. This is optional.")
        return set()
    return set()

def load_street_types():
    street_types = set()
    try:
        with open(STREET_TYPE_FILE, 'r', encoding='utf-8') as f:
            next(f)
            for line in f:
                parts = line.strip().split('|')
                if len(parts) >= 3:
                    street_types.add(parts[0].strip().lower())
                    street_types.add(parts[1].strip().lower())
                    street_types.add(parts[2].strip().lower())
    except FileNotFoundError:
        print(f"CRITICAL: Street type file not found at {STREET_TYPE_FILE}. This is required for processing.")
        sys.exit(1)
    return street_types

def postprocess_libpostal(parsed, localities_set, streets_set, street_types):
    comp = {k: v for v, k in parsed}
    if 'road' not in comp or not comp['road'].strip(): return None, 'missing_street_name'
    if 'house_number' not in comp or not comp['house_number'].strip(): return None, 'missing_street_number'
    flat_number, house_number, house_number_suffix, house_number_first, house_number_last = None, None, None, None, None
    hn = comp.get('house_number', '')
    unit_house_match = re.match(r'(?P<flat>\d+)\s*/\s*(?P<house>\d+)', hn)
    range_match = re.match(r'(?P<first>\d+)\s*[-–]\s*(?P<last>\d+)', hn)
    if unit_house_match:
        flat_number, house_number = unit_house_match.groups()
    elif range_match:
        house_number_first, house_number_last = range_match.groups()
        house_number = house_number_first
    else:
        match = re.match(r'^(\d+)([A-Z]+)?$', hn.strip(), re.IGNORECASE)
        if match: house_number, house_number_suffix = match.groups()
        else: house_number = hn
    suburb = comp.get('suburb') or comp.get('city')
    state = comp.get('state')
    postcode = comp.get('postcode')
    if not postcode:
        state_postcode_regex = re.compile(r'\s*([A-Z]{2,3})(\d{4})$', re.IGNORECASE)
        for field_name in ['suburb', 'city', 'state']:
            field_value = comp.get(field_name)
            if not field_value: continue
            match = state_postcode_regex.search(field_value)
            if match:
                potential_state, potential_postcode = match.groups()
                if potential_state.upper() in {'NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'ACT', 'NT'}:
                    state, postcode = potential_state.upper(), potential_postcode
                    if field_name in ['suburb', 'city']: suburb = field_value[:match.start()].strip()
                    break
    location_tokens = (comp.get('road', '').split() + (suburb.split() if suburb else []))
    found_suburb, road_token_count = None, len(location_tokens)
    for i in range(min(3, len(location_tokens)), 0, -1):
        candidate_suburb = ' '.join(location_tokens[-i:]).lower()
        is_locality = (candidate_suburb, state.lower()) in localities_set if state else any(loc_name == candidate_suburb for loc_name, _ in localities_set)
        if is_locality:
            found_suburb = ' '.join(location_tokens[-i:])
            road_token_count = len(location_tokens) - i
            break
    if found_suburb:
        suburb = found_suburb
        comp['road'] = ' '.join(location_tokens[:road_token_count])
    else: suburb = comp.get('suburb') or comp.get('city')
    comp['road'] = re.sub(r"\s*['\"].*['\"]\s*$", "", comp.get('road', '')).strip()
    street_name, street_type = (' '.join(comp['road'].split()[:-1]), comp['road'].split()[-1]) if comp['road'].split() else (comp['road'], None)
    if not street_type: return None, 'missing_street_type'
    expansions = {'ba': 'banan', 'wd': 'wood', 'app': 'approach', 'arc': 'arcade', 'av': 'avenue', 'ave': 'avenue', 'bay': 'bay', 'bch': 'beach', 'bvd': 'boulevard', 'bvde': 'boulevard', 'blv': 'boulevard', 'bv': 'boulevard', 'br': 'brace', 'brk': 'break', 'ctr': 'centre', 'ch': 'chase', 'cir': 'circle', 'clt': 'circlet', 'cct': 'circuit', 'crt': 'court', 'cc': 'circuit', 'ci': 'circuit', 'cl': 'close', 'con': 'concourse', 'cps': 'copse', 'cnr': 'corner', 'cso': 'corso', 'ct': 'court', 'cr': 'cresent', 'crs': 'cresent', 'crf': 'crief', 'crk': 'crook', 'cut': 'cut', 'de': 'deviation', 'dip': 'dip', 'div': 'divide', 'dom': 'domain', 'dv': 'drive', 'elb': 'elbow', 'end': 'end', 'ent': 'entrance', 'esp': 'esplanade', 'est': 'estate', 'exp': 'expressway', 'fwy': 'freeway', 'gap': 'gap', 'gdn': 'garden', 'gs': 'gardens', 'gte': 'gate', 'gwy': 'gateway', 'gld': 'glade', 'gl': 'glen', 'gln': 'glen', 'gra': 'grange', 'grn': 'green', 'gr': 'grove', 'gly': 'gully', 'hvn': 'haven', 'hth': 'heath', 'hts': 'heights', 'hwy': 'highway', 'hy': 'highway', 'hub': 'hub', 'id': 'island', 'jnc': 'junction', 'key': 'key', 'ldg': 'landing', 'la': 'lane', 'l': 'lane', 'ln': 'lane', 'lkt': 'lookout', 'mew': 'mew', 'nth': 'north', 'pde': 'parade', 'pd': 'parade', 'pwy': 'parkway', 'pl': 'place', 'pkt': 'pocket', 'pnt': 'point', 'qy': 'quay', 'qys': 'quays', 'rch': 'reach', 'res': 'reserve', 'rtt': 'retreat', 'rtn': 'return', 'rvr': 'river', 'rd': 'road', 'rds': 'roads', 'rty': 'rotary', 'rnd': 'round', 'rte': 'route', 'row': 'row', 'rue': 'rue', 'run': 'run', 'sth': 'south', 'sq': 'square', 'st': 'street', 'tce': 'terrace', 'top': 'top', 'tor': 'tor', 'trk': 'track', 'trl': 'trail', 'way': 'way', 'wy': 'way', 'wyn': 'way', 'bdy': 'boundary', 'mz': 'maze', 'wk':'walk'}
    tokens = street_type.strip().split()
    last = tokens[-1].lower()
    if last in expansions: tokens[-1] = expansions[last]
    else: tokens[-1] = tokens[-1].capitalize()
    street_type = ' '.join(tokens)
    cleansed = {'flat_number': flat_number, 'house_number': house_number, 'house_number_suffix': house_number_suffix, 'house_number_first': house_number_first, 'house_number_last': house_number_last, 'road': comp.get('road'), 'street_name': street_name, 'street_type': street_type, 'suburb': suburb, 'state': state, 'postcode': postcode, 'unit': comp.get('unit'), 'level': comp.get('level'), 'po_box': comp.get('po_box'), 'attention': comp.get('attention'), 'category': comp.get('category'), 'country': comp.get('country')}
    for key, value in cleansed.items():
        if isinstance(value, str):
            cleansed[key] = value.upper()
    return cleansed, None

def process_batch(records, localities_set, streets_set, street_types, original_header=None):
    successes = []
    failures_csv = []
    failures_log = []
    for rec in records:
        try:
            raw_address = rec.get('Field5', '')
            def fail_record(reason):
                # Only include fields that are in the original CSV header to avoid fieldnames error
                if original_header:
                    # Filter to only include fields that exist in the original header
                    failed_record = {k: v for k, v in rec.items() if k in original_header}
                else:
                    # Fallback: filter out None keys and ensure all keys are strings
                    failed_record = {str(k): v for k, v in rec.items() if k is not None and str(k).strip() != ''}
                failures_csv.append(failed_record)
                log_entry = {"id": {"$oid": rec.get("id")},"reason": reason,"address": raw_address}
                failures_log.append(log_entry)
            if not raw_address or not raw_address.strip():
                fail_record('empty_address_field')
                continue
            
            # --- MODIFICATION START ---
            # Apply a series of cleaning steps to the raw address string
            raw_address_cleaned = raw_address
            
            # 1. Apply comprehensive normalization from normalize_field5.py
            raw_address_cleaned = normalize_field5(raw_address_cleaned)
            
            # 2. Remove "NÂ°" prefix which is a common encoding error for "Nº"
            raw_address_cleaned = re.sub(r'^\s*NÂ°\s*', '', raw_address_cleaned, flags=re.IGNORECASE)
            
            # 3. Remove any text within quotes
            raw_address_cleaned = re.sub(r"(['\"]).*?\1", "", raw_address_cleaned)
            
            # 4. Remove periods
            raw_address_cleaned = raw_address_cleaned.replace('.', '')
            
            # 5. Remove "lot" or "lt" prefixes
            raw_address_cleaned = re.sub(r'^\s*(lot|lt)[\s]*(?=\d)', '', raw_address_cleaned, flags=re.IGNORECASE)
            # --- MODIFICATION END ---

            parsed = parse_address(raw_address_cleaned)
            cleansed, err = postprocess_libpostal(parsed, localities_set, streets_set, street_types)
            if err:
                fail_record(err)
                continue
            final_record = rec.copy()
            new_fields_with_values = {k: v for k, v in cleansed.items() if v is not None and v != ''}
            final_record.update(new_fields_with_values)
            successes.append(final_record)
        except Exception as e:
            fail_record(f"exception: {str(e)}")
    return successes, failures_csv, failures_log

def batch_iterator_csv(file_path, batch_size):
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append(row)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

def main(args):
    print('===========================================')
    print('Address Cleansing Script is now running...')
    print(f'Input file: {args.input_file}')
    print(f'Success output: {args.success_file}')
    print(f'Failure CSV output: {args.failure_file}')
    print(f'Failure LOG output: {args.failure_log}')
    print('===========================================')

    localities_set = load_localities()
    streets_set = load_streets()
    street_types = load_street_types()
    
    if not os.path.exists(args.input_file):
        print(f"\nINFO: Input file not found at {args.input_file}. No records to process.")
        total, total_successes, total_failures = 0, 0, 0
    else:
        try:
            with open(args.input_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                total = sum(1 for _ in reader)
        except StopIteration:
            total = 0

        if total == 0:
            print("\nINFO: Input file is empty. No records to process.")
            total_successes, total_failures = 0, 0
        else:
            total_successes = 0
            total_failures = 0

            with open(args.success_file, 'w', encoding='utf-8') as f_success, \
                 open(args.failure_file, 'w', encoding='utf-8', newline='') as f_failure_csv, \
                 open(args.failure_log, 'w', encoding='utf-8') as f_failure_log:
                
                failure_writer = csv.DictWriter(f_failure_csv, fieldnames=header)
                failure_writer.writeheader()
                
                with tqdm(total=total, desc='Processing', unit='rec') as pbar:
                    with ProcessPoolExecutor(max_workers=8) as executor:
                        batch_iter = batch_iterator_csv(args.input_file, 1000)
                        futures = {executor.submit(process_batch, batch, localities_set, streets_set, street_types, header): batch for batch in batch_iter}

                        for future in as_completed(futures):
                            successes, failures_csv, failures_log = future.result()
                            
                            if successes:
                                total_successes += len(successes)
                                for rec in successes:
                                    f_success.write(json.dumps(rec, ensure_ascii=False) + '\n')
                            
                            if failures_csv:
                                total_failures += len(failures_csv)
                                failure_writer.writerows(failures_csv)

                            if failures_log:
                                for log_entry in failures_log:
                                    f_failure_log.write(json.dumps(log_entry, ensure_ascii=False) + '\n')

                            pbar.update(len(successes) + len(failures_csv))
            print('Processing complete.')

    print('\n' + '='*40)
    print('--- CLEANSING SUMMARY ---')
    if total > 0:
        success_percentage = (total_successes / total) * 100
        print(f"Total Records Processed: {total}")
        print(f"Successfully Cleaned:    {total_successes} ({success_percentage:.2f}%)")
        print(f"Failed to Clean:         {total_failures}")

        _update_airflow_summary({
            "successfully_cleaned": f"{total_successes}({success_percentage:.2f}%)",
            "failed_to_clean": total_failures,
            "failed_output_file": args.failure_file
            })
    else:
        print("No records were processed.")
        _update_airflow_summary({
            "successfully_cleaned": "0 (0.00%)",
            "failed_to_clean": 0,
            "failed_output_file": args.failure_file
            })
    print('='*40 + '\n')
    
    if args.delete_input and os.path.exists(args.input_file):
        try:
            os.remove(args.input_file)
            print(f"Successfully deleted input file: {args.input_file}")
        except OSError as e:
            print(f"Error deleting input file {args.input_file}: {e}")

    print('Script finished.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Clean addresses from a CSV file, splitting output into success and failure files.")
    parser.add_argument('--input', dest='input_file', required=True, help="Path to the source CSV file.")
    parser.add_argument('--success-output', dest='success_file', required=True, help="Path for the successfully processed records (JSONL format).")
    parser.add_argument('--failure-output', dest='failure_file', required=True, help="Path for the records that failed processing (CSV format).")
    parser.add_argument('--failure-log', dest='failure_log', required=True, help="Path for the failure log file (JSON objects per line).")
    parser.add_argument('--delete-input', action='store_true', help="If set, delete the input file after successful processing.")
    
    args = parser.parse_args()
    main(args)
