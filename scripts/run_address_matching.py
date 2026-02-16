import os
import csv
import argparse
import re
import hashlib
import string
from pymongo import MongoClient
from bson.objectid import ObjectId
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
import pytz
from tqdm import tqdm

NUM_WORKERS = 10
BATCH_SIZE = 500
GNAF_FIELDS = [
    'ADDRESS_DETAIL_PID', 'STREET_LOCALITY_PID', 'LOCALITY_PID',
    'FULL_ADDRESS', 'LOT_NUMBER'
]
MATCH_FIELDS = [
    ('house_number', 'NUMBER_FIRST'),
    ('house_number_suffix', 'NUMBER_FIRST_SUFFIX'),
    ('flat_number', 'FLAT_NUMBER'),
    ('street_name', 'STREET_NAME'),
    ('street_type', 'STREET_TYPE_CODE'),
    ('suburb', 'LOCALITY_NAME'),
    ('postcode', 'POSTCODE'),
]
FIELD_WEIGHTS = {
    'house_number': 25,
    'street_name': 25,
    'suburb': 20,
    'street_type': 10,
    'postcode': 10,
    'flat_number': 5,
    'house_number_suffix': 5,
}

MONGODB_URI = os.getenv('MONGODB_URI')
MONGODB_DB = os.getenv('MONGODB_DB', 'property_database')
TARGET_COLLECTION = os.getenv('TARGET_COLLECTION', 'GNAF_addresses')


def get_client():
    return MongoClient(MONGODB_URI)

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

def build_gnaf_query(doc):
    query = {}
    src_hns_val = doc.get('house_number_suffix')
    is_lot_in_suffix = src_hns_val and isinstance(src_hns_val, str) and "LOT" in src_hns_val.upper()
    lot_num = re.search(r'\d+', src_hns_val).group(0) if is_lot_in_suffix else None
    flat_val = doc.get('flat_number')
    ignore_flat = flat_val and isinstance(flat_val, str) and "LOT" in flat_val.upper()
    hn_val = doc.get('house_number')
    use_lot = hn_val and isinstance(hn_val, str) and "LOT" in hn_val.upper()
    street_name_val = doc.get('street_name', '')
    skip_street_type = street_name_val.upper().startswith("THE ")

    for src, tgt in MATCH_FIELDS:
        val = doc.get(src)
        if val is None or str(val).strip() == '':
            continue
        val = str(val).upper()

        if is_lot_in_suffix:
            if src == 'flat_number' and lot_num:
                query['FLAT_NUMBER'] = lot_num
            elif src not in ['house_number', 'house_number_suffix']:
                if src == 'street_type' and skip_street_type:
                    continue
                query[tgt] = val
        else:
            if src == 'flat_number':
                if ignore_flat:
                    continue
                if use_lot:
                    query['LOT_NUMBER'] = val
                else:
                    query[tgt] = val
            elif src == 'street_type' and skip_street_type:
                continue
            else:
                query[tgt] = val
    return query


def calculate_confidence_score(src, tgt):
    score, total = 0, 0
    src_hns_val = src.get('house_number_suffix')
    is_lot_suffix = src_hns_val and isinstance(src_hns_val, str) and "LOT" in src_hns_val.upper()
    lot_val = re.search(r'\d+', src_hns_val).group(0) if is_lot_suffix else None
    flat_val = src.get('flat_number')
    flat_has_lot = flat_val and isinstance(flat_val, str) and "LOT" in flat_val.upper()
    hn_val = src.get('house_number')
    hn_has_lot = hn_val and isinstance(hn_val, str) and "LOT" in hn_val.upper()
    skip_street_type = src.get('street_name', '').upper().startswith("THE ")

    for field, weight in FIELD_WEIGHTS.items():
        src_val = src.get(field)
        tgt_field = dict(MATCH_FIELDS).get(field)
        scorable = True
        src_comp = None

        if is_lot_suffix:
            if field in ['house_number', 'house_number_suffix']:
                scorable = False
            elif field == 'flat_number' and lot_val:
                src_comp = lot_val
                tgt_field = 'FLAT_NUMBER'
            elif field == 'street_type' and skip_street_type:
                scorable = False
            else:
                src_comp = str(src_val).upper()
        else:
            if field == 'flat_number':
                if flat_has_lot:
                    scorable = False
                elif hn_has_lot:
                    tgt_field = 'LOT_NUMBER'
                    src_comp = str(src_val).upper()
                else:
                    src_comp = str(src_val).upper()
            elif field == 'street_type' and skip_street_type:
                scorable = False
            else:
                src_comp = str(src_val).upper() if src_val is not None else None

        if not src_comp:
            scorable = False

        if scorable:
            tgt_val = str(tgt.get(tgt_field, '')).upper()
            if src_comp == tgt_val:
                score += weight
            total += weight

    return int(round(100 * score / total)) if total > 0 else 0

def _norm(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.strip().upper()
    # keep letters/numbers/spaces only
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def deterministic_9_digits(key: str) -> str:
    """
    Deterministically map a string key to 7 digits (zero-padded).
    Same key => same 7 digits across runs/days.
    """
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()
    n = int(h[:12], 16) % (10**7)  # 7 digits
    return str(n).zfill(7)

def generate_custom_address_detail_pid(doc):
    """
    Custom ADDRESS_DETAIL_PID format (deterministic):
      <house_number>GA<first 3 letters of suburb or street_name><7 digits>

    Example: 11GALIL1234567
    """
    # --- house_number ---
    house = doc.get("house_number", "")
    house = "" if house is None else str(house).strip()
    house = re.sub(r"\.0$", "", house)  # handle "11.0"
    # --- suburb preferred for letters; fallback to street_name ---
    suburb = doc.get("suburb")
    street_name = doc.get("street_name")
    base = suburb if suburb and str(suburb).strip() else street_name
    base = "" if base is None else str(base).strip()

    letters_only = "".join(ch for ch in base.upper() if ch in string.ascii_uppercase)
    letters3 = letters_only[:3] if letters_only else "XXX"

    # --- build deterministic key (prefer Field5 if present) ---
    street_type = doc.get("street_type")
    road = doc.get("road")
    field5 = doc.get("Field5")
    key_parts = [
        _norm(field5) if field5 else "",
        _norm(house),
        _norm(street_name),
        _norm(street_type),
        _norm(road),
        _norm(suburb),
    ]
    key = "|".join([p for p in key_parts if p])

    digits7 = deterministic_9_digits(key)
    # Remove all non-alphanumeric characters from house number
    house_clean = re.sub(r'[^A-Za-z0-9]', '', house)
    # Build final PID and ensure it only contains alphanumeric characters
    pid = f"{house_clean}GA{letters3}{digits7}"
    # Final sanitization: remove any remaining non-alphanumeric characters
    pid = re.sub(r'[^A-Za-z0-9]', '', pid)
    return pid

def process_batch(batch):
    client = get_client()
    col = client[MONGODB_DB][TARGET_COLLECTION]
    projection = {tgt: 1 for _, tgt in MATCH_FIELDS}
    for f in ['LOT_NUMBER', 'FLAT_NUMBER', 'STREET_TYPE_CODE', 'NUMBER_FIRST', 'NUMBER_FIRST_SUFFIX']:
        projection[f] = 1
    for f in GNAF_FIELDS:
        projection[f] = 1

    results = []
    tz = pytz.timezone("Australia/Sydney")

    for doc in batch:
        out = doc.copy()
        out['matched_at'] = datetime.now(tz).isoformat()
        out['agency_id'] = doc.get('Field2')
        out['source_address'] = doc.get('Full_address')
        for f in GNAF_FIELDS:
            out[f] = None

        try:
            query = build_gnaf_query(doc)
            if not query:
                out['match_status'] = 'No GNAF query built'
                out['confidence_score'] = 0
                out['gnaf_candidates_found_count'] = 0
            else:
                candidates = list(col.find(query, projection))
                out['gnaf_candidates_found_count'] = len(candidates)

                best_score = -1
                best = None
                for c in candidates:
                    score = calculate_confidence_score(doc, c)
                    if score > best_score:
                        best_score = score
                        best = c

                if best:
                    for f in GNAF_FIELDS:
                        out[f] = best.get(f)
                out['confidence_score'] = best_score if best_score >= 0 else 0
                out['match_status'] = 'Matched' if best_score > 0 else 'No suitable GNAF candidate after scoring'
                # Generate custom ADDRESS_DETAIL_PID only for zero-confidence matches
                if out.get("confidence_score", 0) == 0:
                    out["ADDRESS_DETAIL_PID"] = generate_custom_address_detail_pid(doc)
        except Exception as e:
            out['match_status'] = f'Error: {str(e)[:200]}'
            out['confidence_score'] = 0
            out['gnaf_candidates_found_count'] = 0

        results.append(out)
    return results


def read_csv(path):
    with open(path, newline='', encoding='utf-8') as f:
        docs = list(csv.DictReader(f))
    
    for doc in docs:
        if 'flat_number' in doc and doc['flat_number']:
            doc['flat_number'] = re.sub(r'\.0$', '', str(doc['flat_number']))
            
    return docs


def write_csv(path, rows):
    if not rows:
        print("No rows to write.")
        return
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--delete-input', action='store_true')
    args = parser.parse_args()

    docs = read_csv(args.input)
    results = []

    print(f"Starting GNAF matching for {len(docs)} records...")

    with tqdm(total=len(docs), desc='Matching Records', unit='record', ncols=120) as pbar:
        with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
            futures = {executor.submit(process_batch, docs[i:i + BATCH_SIZE]): i for i in range(0, len(docs), BATCH_SIZE)}
            for future in as_completed(futures):
                try:
                    batch_results = future.result()
                    results.extend(batch_results)
                    pbar.update(len(batch_results))
                except Exception as e:
                    print(f"Error in batch: {e}")
                    # Find the size of the batch that failed to update the progress bar correctly
                    start_index = futures[future]
                    failed_batch_size = len(docs[start_index:start_index + BATCH_SIZE])
                    pbar.update(failed_batch_size)
    
    # --- MODIFIED: Calculate summary statistics ---
    total_records = len(results)
    total_matched = 0
    perfect_score_count = 0
    zero_score_count = 0

    if total_records > 0:
        for res in results:
            score = res.get('confidence_score', 0)
            if res.get('match_status') == 'Matched':
                total_matched += 1
            if score == 100:
                perfect_score_count += 1
            elif score == 0:
                zero_score_count += 1
        
        write_csv(args.output, results)
        print(f"\nSuccessfully wrote {len(results)} records to {args.output}")

    # --- MODIFIED: Print final summary ---
    print('\n' + '='*40)
    print('--- GNAF MATCHING SUMMARY ---')

    if total_records > 0:
        perfect_match_percentage = (perfect_score_count / total_records) * 100 if total_records > 0 else 0
        
        print(f"Total Records Processed:      {total_records}")
        print(f"Total Records Matched:        {total_matched} ({perfect_match_percentage:.2f}%)")
        print(f"0 Score Records:              {zero_score_count}")
    else:
        print("No records were processed.")

    _update_airflow_summary({
        "successfully_matched": f"{total_matched}({perfect_match_percentage:.2f}%)",
        "failed_to_match": zero_score_count,
        "matching_output_file": args.output
        }) 

  
    print('='*40 + '\n')


    if args.delete_input:
        try:
            os.remove(args.input)
            print(f"Successfully deleted input file: {args.input}")
        except Exception as e:
            print(f"Could not delete input file: {e}")


if __name__ == '__main__':
    main()
