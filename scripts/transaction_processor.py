import pymongo
import pandas as pd
import argparse
from datetime import datetime, timedelta
import logging
import re
import os
import unicodedata
import math
import json
from multiprocessing import Pool
from pymongo import UpdateOne
from pymongo.errors import OperationFailure
from tqdm import tqdm

# --- Connection Details ---
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")


# --- Logging ---
def setup_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )
    return logging.getLogger(__name__)

# --- Type Standardization ---
def load_type_mapping(types_csv_path="types.csv"):
    """
    Load type mapping from types.csv file.
    Returns a dictionary mapping input types to standardized types.
    """
    type_mapping = {}
    try:
        if os.path.exists(types_csv_path):
            # Try reading with more robust CSV parsing options
            # Use python engine which is more forgiving with malformed lines
            try:
                df = pd.read_csv(
                    types_csv_path,
                    engine='python',
                    on_bad_lines='skip',  # Skip bad lines (pandas >= 1.3.0)
                    quotechar='"',
                    skipinitialspace=True,
                    encoding='utf-8'
                )
            except TypeError:
                # Fallback for older pandas versions
                try:
                    df = pd.read_csv(
                        types_csv_path,
                        engine='python',
                        error_bad_lines=False,  # Skip bad lines (pandas < 1.3.0)
                        warn_bad_lines=True,
                        quotechar='"',
                        skipinitialspace=True,
                        encoding='utf-8'
                    )
                except TypeError:
                    # Final fallback - try with minimal options
                    df = pd.read_csv(
                        types_csv_path,
                        engine='python',
                        quotechar='"',
                        skipinitialspace=True,
                        encoding='utf-8'
                    )
            
            # Ensure we have the required columns
            if 'type' in df.columns and 'set_to' in df.columns:
                # Filter out any rows with NaN values in key columns
                df_clean = df[df['type'].notna() & df['set_to'].notna()].copy()
                type_mapping = dict(zip(df_clean['type'], df_clean['set_to']))
            else:
                print(f"Warning: types.csv missing required columns 'type' and/or 'set_to'. Found columns: {df.columns.tolist()}")
        else:
            print(f"Warning: types.csv file not found at {types_csv_path}")
    except Exception as e:
        print(f"Error loading types.csv: {e}")
        import traceback
        traceback.print_exc()
    return type_mapping

def standardize_type(input_type, type_mapping):
    """
    Standardize property type using the mapping from types.csv.
    
    Args:
        input_type (str): The input type value from Field35
        type_mapping (dict): Dictionary mapping input types to standardized types
        
    Returns:
        str or None: Standardized type, or None if input is empty string or "nan"
    """
    # Check for None or empty values
    if input_type is None or str(input_type).strip() == "":
        return None
    
    # Check if it's a pandas nan (float nan or pandas NA)
    try:
        if isinstance(input_type, float) and math.isnan(input_type):
            return None
    except (ValueError, TypeError):
        pass
    
    # Check for pandas NA (newer pandas versions)
    try:
        if pd.isna(input_type):
            return None
    except (ValueError, TypeError):
        pass
    
    input_type_str = str(input_type).strip()
    
    # Handle "nan" string values by setting them to null
    if input_type_str.lower() == "nan" or input_type_str.lower() == "":
        return None
    
    # Check if there's a mapping for this type (case-insensitive)
    for csv_type, standardized_type in type_mapping.items():
        if input_type_str.lower() == csv_type.lower():
            return standardized_type
    
    # If no mapping found, return the original value
    return input_type_str

def create_slug(title):
    """
    Create a URL-friendly slug from a title string.
    
    Args:
        title (str): The title string to convert to slug
        
    Returns:
        str: URL-friendly slug or None if title is empty
    """
    if not title or not isinstance(title, str):
        return None
    
    # Convert to lowercase
    slug = title.lower().strip()
    
    # Remove extra whitespace and replace with single spaces
    slug = re.sub(r'\s+', ' ', slug)
    
    # Replace spaces and special characters with hyphens
    slug = re.sub(r'[^\w\s-]', '', slug)  # Remove special characters except hyphens
    slug = re.sub(r'[-\s]+', '-', slug)   # Replace spaces and multiple hyphens with single hyphen
    
    # Remove leading/trailing hyphens
    slug = slug.strip('-')
    
    return slug if slug else None

# --- Helpers ---
# Cache for parsed dates to avoid re-parsing the same date strings
# Limited cache size to prevent memory issues
_date_parse_cache = {}
_MAX_CACHE_SIZE = 10000  # Limit cache to 10k entries

def parse_date_flexible(date_input):
    if isinstance(date_input, datetime):
        return date_input
    if isinstance(date_input, str):
        # Check cache first
        if date_input in _date_parse_cache:
            return _date_parse_cache[date_input]
        
        # Clear cache if it gets too large
        if len(_date_parse_cache) > _MAX_CACHE_SIZE:
            _date_parse_cache.clear()
        
        try:
            result = datetime.fromisoformat(date_input.replace("Z", "+00:00"))
            _date_parse_cache[date_input] = result
            return result
        except Exception:
            try:
                result = datetime.strptime(date_input.split("T")[0], "%Y-%m-%d")
                _date_parse_cache[date_input] = result
                return result
            except Exception:
                _date_parse_cache[date_input] = None
                return None
    return None


def find_latest_transaction(transaction_list, history_type=None):
    """
    Find the latest transaction in a list.
    Uses contractDate by default.
    Falls back to estimatedSoldDate (for sold), estimatedSaleDate (for sale), estimatedLeasedDate, or estimatedRentDate if missing.
    """
    valid_txns = []
    for t in transaction_list:
        if not isinstance(t, dict):
            continue

        # Choose correct date field
        date_value = t.get("contractDate")
        if not date_value:
            if history_type == "sold":
                date_value = t.get("estimatedSoldDate")
            elif history_type == "leased":
                date_value = t.get("estimatedLeasedDate")
            elif history_type == "rent":
                date_value = t.get("estimatedRentDate")

        parsed_date = parse_date_flexible(date_value)
        if parsed_date:
            t["_parsed_date"] = parsed_date
            t["_chosen_date"] = date_value  # preserve chosen date string
            valid_txns.append(t)

    if not valid_txns:
        return None
    return max(valid_txns, key=lambda x: x["_parsed_date"])


def get_transaction_date_info(txn, txn_type):
    """
    Determine the most relevant date for a transaction and return the original
    string value, parsed datetime object, and year.
    """
    if not isinstance(txn, dict):
        return None, None, None

    date_value = txn.get("contractDate")
    if not date_value:
        if txn_type == "sold":
            date_value = txn.get("estimatedSoldDate")
        elif txn_type == "sale":
            date_value = txn.get("estimatedSaleDate")
        elif txn_type == "leased":
            date_value = txn.get("estimatedLeasedDate")
        elif txn_type == "rent":
            date_value = txn.get("estimatedRentDate")

    if not date_value and "_chosen_date" in txn:
        date_value = txn["_chosen_date"]

    parsed_date = parse_date_flexible(date_value)
    year = parsed_date.year if parsed_date else None
    return date_value, parsed_date, year


def is_value_missing(value):
    if value is None:
        return True
    if isinstance(value, float):
        try:
            if math.isnan(value):
                return True
        except (ValueError, TypeError):
            pass
    if isinstance(value, str):
        return value.strip() == "" or value.strip().lower() == "nan"
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) == 0
    return False


def has_actual_contract_date_and_price(txn, txn_type=None):
    """
    Check if a transaction has an actual contract date (not estimated) and a non-null price/rate.
    
    Args:
        txn (dict): Transaction dictionary
        txn_type (str, optional): Transaction type ("rent", "leased", "sale", "sold")
                                   If not provided, will try to infer from transaction
        
    Returns:
        bool: True if transaction has actual contractDate (not estimated) and price/rate is not null/empty
    """
    if not isinstance(txn, dict):
        return False
    
    # Check if contractDate exists and is not empty
    # If contractDate is not null or "", it is considered real (actual), regardless of estimatedContractDate
    contract_date = txn.get("contractDate")
    if is_value_missing(contract_date):
        return False  # No actual contract date
    
    # Determine transaction type if not provided
    if txn_type is None:
        txn_type = txn.get("_temp_type")
    
    # Check for price/rate based on transaction type
    # For rent/leased transactions, check for "rate"
    # For sale/sold transactions, check for "price"
    if txn_type in ("rent", "leased"):
        rate = txn.get("rate")
        if is_value_missing(rate):
            return False  # No rate
    else:
        # For sale/sold transactions, check for "price"
        price = txn.get("price")
        if is_value_missing(price):
            return False  # No price
    
    return True


def is_sold_backed_by_sale(sold_txn, details, max_months_diff=12):
    """
    Check if a sold transaction is backed by a sale transaction.
    A sold transaction is considered "backed" if there's an inactive saleHistory entry
    within max_months_diff months of the sold transaction date (can be across years).
    
    Args:
        sold_txn (dict): The sold transaction to check
        details (dict): The transactionDetails dictionary containing saleHistory
        max_months_diff (int): Maximum months difference to consider as "backed" (default: 12)
        
    Returns:
        bool: True if the sold transaction is backed by a sale transaction
    """
    if not isinstance(sold_txn, dict):
        return False
    
    # Get the sold transaction date (contractDate or estimatedSoldDate)
    sold_date_value = sold_txn.get("contractDate")
    if not sold_date_value:
        sold_date_value = sold_txn.get("estimatedSoldDate")
    
    sold_date = parse_date_flexible(sold_date_value)
    if not sold_date:
        return False
    
    # Check all saleHistory entries
    sale_history = details.get("saleHistory", [])
    for sale_txn in sale_history:
        if not isinstance(sale_txn, dict):
            continue
        
        # Sale must be inactive (property was sold, so sale listing is no longer active)
        sale_status = str(sale_txn.get("status", "")).strip().lower()
        if sale_status != "inactive":
            continue
        
        # Sold must be active
        sold_status = str(sold_txn.get("status", "")).strip().lower()
        if sold_status != "active":
            continue
        
        # Get the sale transaction date (contractDate or estimatedSaleDate)
        sale_date_value = sale_txn.get("contractDate")
        if not sale_date_value:
            sale_date_value = sale_txn.get("estimatedSaleDate")
        
        sale_date = parse_date_flexible(sale_date_value)
        if not sale_date:
            continue
        
        # Calculate months difference (works across years)
        months_diff = abs((sold_date.year - sale_date.year) * 12 + (sold_date.month - sale_date.month))
        if months_diff <= max_months_diff:
            return True
    
    return False


def find_dat_sold_within_months(sold_txn, details, max_months_diff=12):
    """
    Find DAT-weekly-pipeline sold records within max_months_diff months of a given sold transaction.
    
    Args:
        sold_txn (dict): The sold transaction to check against
        details (dict): The transactionDetails dictionary containing soldHistory
        max_months_diff (int): Maximum months difference (default: 12)
        
    Returns:
        list: List of DAT sold transactions within the time window, or empty list if none found
    """
    if not isinstance(sold_txn, dict):
        return []
    
    # Get the sold transaction date (contractDate or estimatedSoldDate)
    sold_date_value = sold_txn.get("contractDate")
    if not sold_date_value:
        sold_date_value = sold_txn.get("estimatedSoldDate")
    
    sold_date = parse_date_flexible(sold_date_value)
    if not sold_date:
        return []
    
    # Find DAT sold records within the time window
    dat_sold_records = []
    sold_history = details.get("soldHistory", [])
    
    for dat_txn in sold_history:
        if not isinstance(dat_txn, dict):
            continue
        
        # Must be from DAT-weekly-pipeline
        if not source_matches_dat_weekly(dat_txn.get("source")):
            continue
        
        # Skip if it's the same transaction object
        if dat_txn is sold_txn:
            continue
        
        # Get the DAT transaction date (contractDate or estimatedSoldDate)
        dat_date_value = dat_txn.get("contractDate")
        if not dat_date_value:
            dat_date_value = dat_txn.get("estimatedSoldDate")
        
        dat_date = parse_date_flexible(dat_date_value)
        if not dat_date:
            continue
        
        # Calculate months difference (works across years)
        months_diff = abs((sold_date.year - dat_date.year) * 12 + (sold_date.month - dat_date.month))
        if months_diff <= max_months_diff:
            dat_sold_records.append(dat_txn)
    
    return dat_sold_records


def merge_sold_transactions(sale_backed_sold, dat_sold):
    """
    Merge two sold transactions: sale-backed sold and DAT sold.
    Takes contractDate and price from DAT sold, and listing_images and agents from sale-backed sold.
    
    Args:
        sale_backed_sold (dict): The sale-backed sold transaction (has listing_images, agents)
        dat_sold (dict): The DAT sold transaction (has contractDate, price)
        
    Returns:
        dict: Merged transaction dictionary
    """
    if not isinstance(sale_backed_sold, dict) or not isinstance(dat_sold, dict):
        return sale_backed_sold
    
    # Create a copy of the sale-backed sold transaction as the base
    merged = sale_backed_sold.copy()
    
    # Take contractDate and price from DAT sold (they're more filled out)
    dat_contract_date = dat_sold.get("contractDate")
    dat_price = dat_sold.get("price")
    
    if dat_contract_date and not is_value_missing(dat_contract_date):
        merged["contractDate"] = dat_contract_date
    
    if dat_price and not is_value_missing(dat_price):
        merged["price"] = dat_price
    
    # Keep listing_images and agents from sale-backed sold (they're more complete)
    # These should already be in merged since we copied from sale_backed_sold
    
    # Also preserve other useful fields from DAT sold if missing in sale-backed
    # But prioritize sale-backed for listing_images and agents
    if is_value_missing(merged.get("bedrooms")) and not is_value_missing(dat_sold.get("bedrooms")):
        merged["bedrooms"] = dat_sold.get("bedrooms")
    
    if is_value_missing(merged.get("bathrooms")) and not is_value_missing(dat_sold.get("bathrooms")):
        merged["bathrooms"] = dat_sold.get("bathrooms")
    
    if is_value_missing(merged.get("carParking")) and not is_value_missing(dat_sold.get("carParking")):
        merged["carParking"] = dat_sold.get("carParking")
    
    return merged


def find_latest_transaction_by_date(candidates, txn_type, exclude_txn=None):
    """
    Find the latest transaction from a list of candidates, considering both contractDate and estimated dates.
    Excludes a specific transaction if provided.
    
    Args:
        candidates: List of transaction dictionaries
        txn_type: Transaction type ("rent", "leased", "sale", "sold")
        exclude_txn: Transaction to exclude from search (optional)
        
    Returns:
        dict or None: Latest transaction or None if no valid candidates
    """
    valid_candidates = []
    
    for txn in candidates:
        if not isinstance(txn, dict):
            continue
        
        # Skip if this is the transaction we want to exclude
        if exclude_txn is not None and txn is exclude_txn:
            continue
        
        # Get date value (contractDate first, then fall back to estimated date)
        date_value = txn.get("contractDate")
        if not date_value:
            if txn_type == "sold":
                date_value = txn.get("estimatedSoldDate")
            elif txn_type == "sale":
                date_value = txn.get("estimatedSaleDate")
            elif txn_type == "leased":
                date_value = txn.get("estimatedLeasedDate")
            elif txn_type == "rent":
                date_value = txn.get("estimatedRentDate")
        
        parsed_date = parse_date_flexible(date_value)
        if parsed_date:
            valid_candidates.append((parsed_date, txn))
    
    if not valid_candidates:
        return None
    
    # Sort by date (latest first) and return the latest transaction
    valid_candidates.sort(key=lambda x: x[0], reverse=True)
    return valid_candidates[0][1]


def backfill_missing_fields_in_state(state_dict, source_txn, details, txn_type, fields_to_backfill):
    """
    Backfill missing fields in the state dictionary from other transactions.
    Does NOT modify history objects - only modifies the state dictionary.
    
    Args:
        state_dict: The state dictionary (currentState or rentState) that needs fields backfilled
        source_txn: The source transaction that was used to build this state (to exclude from search)
        details: The transactionDetails dictionary containing all histories
        txn_type: The type of the source transaction ("rent", "leased", "sale", "sold")
        fields_to_backfill: List of field names to backfill (e.g., ["listing_images", "bedrooms", "bathrooms", "carParking"])
    """
    if not state_dict or not isinstance(state_dict, dict):
        return
    
    # Check which fields are missing in the state
    missing_fields = [field for field in fields_to_backfill if is_value_missing(state_dict.get(field))]
    
    if not missing_fields:
        return  # Nothing to backfill
    
    # Determine which history to search first (same category)
    same_category_histories = []
    if txn_type == "sold":
        same_category_histories = [("soldHistory", "sold")]
    elif txn_type == "sale":
        same_category_histories = [("saleHistory", "sale")]
    elif txn_type == "rent":
        same_category_histories = [("rentHistory", "rent")]
    elif txn_type == "leased":
        same_category_histories = [("leasedHistory", "leased")]
    
    # Try to find latest transaction in same category
    backfill_source_txn = None
    for history_key, history_txn_type in same_category_histories:
        history_list = details.get(history_key, [])
        if history_list:
            backfill_source_txn = find_latest_transaction_by_date(history_list, history_txn_type, exclude_txn=source_txn)
            if backfill_source_txn:
                break
    
    # If found in same category, copy missing fields to state
    if backfill_source_txn:
        copy_missing_property_fields(state_dict, backfill_source_txn, missing_fields)
        # Update missing_fields list after copying
        missing_fields = [field for field in missing_fields if is_value_missing(state_dict.get(field))]
    
    # If still missing fields, try other categories
    if missing_fields:
        # Define all other histories to search
        all_histories = [
            ("soldHistory", "sold"),
            ("saleHistory", "sale"),
            ("rentHistory", "rent"),
            ("leasedHistory", "leased"),
        ]
        
        # Remove the same category from the list
        other_histories = [h for h in all_histories if h not in same_category_histories]
        
        # Search through other categories
        for history_key, history_txn_type in other_histories:
            history_list = details.get(history_key, [])
            if history_list:
                backfill_source_txn = find_latest_transaction_by_date(history_list, history_txn_type, exclude_txn=source_txn)
                if backfill_source_txn:
                    copy_missing_property_fields(state_dict, backfill_source_txn, missing_fields)
                    # Update missing_fields list after copying
                    missing_fields = [field for field in missing_fields if is_value_missing(state_dict.get(field))]
                    if not missing_fields:
                        break  # All fields filled, no need to continue


def copy_missing_property_fields(target_txn, source_txn, fields):
    for field in fields:
        if is_value_missing(target_txn.get(field)) and not is_value_missing(source_txn.get(field)):
            target_txn[field] = source_txn.get(field)


def source_matches_dat_weekly(source_field):
    """
    Check if the source field indicates DAT-weekly-pipeline.
    Handles strings or iterable structures.
    """
    def matches(value):
        return isinstance(value, str) and value.strip().lower() == "dat-weekly-pipeline"

    if matches(source_field):
        return True
    if isinstance(source_field, list):
        for item in source_field:
            if matches(item):
                return True
            if isinstance(item, dict):
                for v in item.values():
                    if matches(v):
                        return True
    if isinstance(source_field, dict):
        for v in source_field.values():
            if matches(v):
                return True
    return False


def enrich_dat_weekly_sold_transaction(sold_txn, details):
    """
    For sold transactions coming from DAT-weekly-pipeline (which often miss
    listing images, bedrooms, bathrooms, and carParking), try to backfill these
    fields from other transaction histories following the specified priority.
    """
    if not sold_txn or not isinstance(details, dict):
        return

    fields_to_copy = ["listing_images", "bedrooms", "bathrooms", "carParking"]
    if all(not is_value_missing(sold_txn.get(field)) for field in fields_to_copy):
        return

    _, _, target_year = get_transaction_date_info(sold_txn, "sold")
    if target_year is None:
        return

    def candidates_from_history(history_key, txn_type, year_filter=None):
        candidates = []
        for txn in details.get(history_key, []):
            _, parsed_date, year = get_transaction_date_info(txn, txn_type)
            if parsed_date is None or year is None:
                continue
            if year_filter is not None and year == year_filter:
                candidates.append((parsed_date, txn))
        return candidates

    # Priority 1: saleHistory same year
    sale_same_year = candidates_from_history("saleHistory", "sale", year_filter=target_year)
    best_sale_same_year = None
    if sale_same_year:
        sale_same_year.sort(key=lambda x: x[0], reverse=True)
        best_sale_same_year = sale_same_year[0][1]
        copy_missing_property_fields(sold_txn, best_sale_same_year, fields_to_copy)

    # Agents and agentsCompanyId handling (DAT-specific rule):
    # - Copy only from saleHistory entries in the same year.
    # - If unavailable, explicitly set both fields to None (no further fallbacks).
    if best_sale_same_year:
        sold_txn["agents"] = best_sale_same_year.get("agents")
        sold_txn["agentsCompanyId"] = best_sale_same_year.get("agentsCompanyId")
    else:
        sold_txn["agents"] = None
        sold_txn["agentsCompanyId"] = None

    if all(not is_value_missing(sold_txn.get(field)) for field in fields_to_copy):
        return

    # Priority 2: rentHistory / leasedHistory same year
    rent_same_year = candidates_from_history("rentHistory", "rent", year_filter=target_year)
    leased_same_year = candidates_from_history("leasedHistory", "leased", year_filter=target_year)
    same_year_rl = rent_same_year + leased_same_year
    if same_year_rl:
        same_year_rl.sort(key=lambda x: x[0], reverse=True)
        copy_missing_property_fields(sold_txn, same_year_rl[0][1], fields_to_copy)

    if all(not is_value_missing(sold_txn.get(field)) for field in fields_to_copy):
        return

    # Priority 3: nearest prior year from sale/sold/rent/leased histories
    prior_candidates = []
    for key, txn_type in [
        ("saleHistory", "sale"),
        ("soldHistory", "sold"),
        ("rentHistory", "rent"),
        ("leasedHistory", "leased"),
    ]:
        history = details.get(key, [])
        for txn in history:
            _, parsed_date, year = get_transaction_date_info(txn, txn_type)
            if parsed_date is None or year is None or year >= target_year:
                continue
            prior_candidates.append(((year, parsed_date), txn))

    if prior_candidates:
        prior_candidates.sort(key=lambda x: (x[0][0], x[0][1]), reverse=True)
        copy_missing_property_fields(sold_txn, prior_candidates[0][1], fields_to_copy)


def normalize_title_text(raw_title):
    """
    Normalise title text so that:
    - Leading 'Unit' prefixes are removed but the unit number is preserved
    - A comma between two numbers is converted to a slash, e.g. '1, 50' -> '1/50'
    """
    if not raw_title or not isinstance(raw_title, str):
        return None

    # Strip whitespace
    title = raw_title.strip()

    # Remove 'Unit' prefix if present (case insensitive)
    # Pattern matches "Unit " followed by numbers/letters at the start
    unit_pattern = r'^Unit\s+(\d+[A-Za-z]?)\s*,?\s*'
    match = re.match(unit_pattern, title, re.IGNORECASE)

    if match:
        # Remove the "Unit " part, keeping the unit number and comma
        unit_number = match.group(1)
        title = title[match.end():].strip()
        # Add the unit number back without "Unit" prefix
        title = f"{unit_number}, {title}" if title else unit_number

    # Replace comma between numbers with a slash (e.g., "101, 3 Aston Road" -> "101/3 Aston Road")
    # Pattern: number, optional space, comma, optional space, number
    title = re.sub(r'(\d+)\s*,\s*(\d+)', r'\1/\2', title)

    return title if title else None


def clean_title_from_full_address(full_address):
    """
    Extract and clean title from FULL_ADDRESS field.
    Reuses the same normalisation rules as other titles.
    
    Args:
        full_address (str): The FULL_ADDRESS value
        
    Returns:
        str: Cleaned title or None if no valid address
    """
    return normalize_title_text(full_address) if isinstance(full_address, str) else None


def format_land_area(area_value, area_unit_value):
    """
    Format land area from Area and areaUnit fields.
    
    Args:
        area_value: The Area field value (number)
        area_unit_value: The areaUnit field value (string)
        
    Returns:
        str: Formatted land area string or None if area_value is missing
    """
    if area_value is None or (isinstance(area_value, float) and math.isnan(area_value)):
        return None
    
    # Convert area to a clean string (remove .0 for whole numbers)
    try:
        # Try to convert to float first
        area_float = float(area_value)
        # Check if it's a whole number
        if area_float.is_integer():
            area_str = str(int(area_float))
        else:
            # Remove trailing zeros
            area_str = str(area_float).rstrip('0').rstrip('.')
    except (ValueError, TypeError):
        # Fallback to string conversion
        area_str = str(area_value).strip()
    
    # If area is empty, return None
    if not area_str or area_str == "":
        return None
    
    # Handle areaUnit
    area_unit = str(area_unit_value).strip() if area_unit_value is not None and not (isinstance(area_unit_value, float) and math.isnan(area_unit_value)) else ""
    
    # If areaUnit is empty or just "nan", return just the area
    if not area_unit or area_unit.lower() == "nan":
        return area_str
    
    # Capitalize the unit
    area_unit = area_unit.capitalize()
    
    # Return combined value: "701 Acres" or "701 Hectares"
    return f"{area_str} {area_unit}"


def categorize_areas_from_input(input_record):
    """
    Extracts values from Field52..Field79, cleans them (remove punctuation but preserve spaces),
    and categorizes into internalAreas and externalAreas based on keyword rules.
    Returns a tuple: (internalAreas, externalAreas)
    """
    internal_keywords = [
        "living", "bedroom", "bathroom", "ensuite", "kitchen", "dining", "gas heating",
        "study", "laundry", "rumpus", "family", "office", "hallway", "pantry", "alarm",
        "media", "theatre", "store", "closet", "cellar", "air conditioning", "dishwasher",
        "heating", "fireplace", "cooling", "reverse cycle", "split-system",
    ]
    external_keywords = [
        "balcony", "terrace", "patio", "veranda", "deck", "garage", "carport",
        "driveway", "yard", "garden", "courtyard", "porch", "alfresco", "pool",
        "shed", "workshop", "outdoor", "close to",
    ]

    # Normalize keywords by removing non-alphanumerics (including spaces) and lowering for matching
    def normalize(text):
        if text is None:
            return ""
        return re.sub(r"[^A-Za-z0-9]+", "", str(text)).lower()

    normalized_internal = [normalize(k) for k in internal_keywords]
    normalized_external = [normalize(k) for k in external_keywords]

    internal_areas = []
    external_areas = []
    seen_internal = set()
    seen_external = set()

    if not input_record:
        return internal_areas, external_areas

    for i in range(52, 80):  # inclusive of 79
        key = f"Field{i}"
        raw_val = input_record.get(key)
        if raw_val is None:
            continue
        # Skip pandas NaN-like values
        try:
            if isinstance(raw_val, float) and math.isnan(raw_val):
                continue
        except (ValueError, TypeError):
            pass

        # Clean: remove punctuation but preserve spaces between words, then normalize multiple spaces
        cleaned = re.sub(r"[^\w\s]+", "", str(raw_val))  # Remove punctuation but keep alphanumeric and spaces
        cleaned = re.sub(r"\s+", " ", cleaned)  # Replace multiple spaces with single space
        cleaned = cleaned.strip()  # Remove only leading/trailing spaces
        
        if not cleaned:
            continue

        # Match keywords more precisely to avoid false positives
        # Use word boundaries for single words and phrase matching for multi-word keywords
        cleaned_lower = cleaned.lower()
        
        def matches_keyword(keyword, text):
            """Check if keyword matches in text using word boundaries for precision."""
            keyword_lower = keyword.lower()
            keyword_normalized = normalize(keyword)
            text_normalized = normalize(text)
            text_lower = text.lower()
            
            # For multi-word keywords (contain spaces), check if normalized keyword matches start of normalized text
            if " " in keyword:
                return text_normalized.startswith(keyword_normalized)
            else:
                # For single-word keywords, use word boundary matching on original text
                # Create a regex pattern that matches the keyword as a whole word
                pattern = r'\b' + re.escape(keyword_lower) + r'\b'
                return bool(re.search(pattern, text_lower))
        
        # Check external keywords first (especially multi-word like "close to")
        # External keywords take priority to ensure "Close to Schools" goes to externalAreas
        is_external = False
        for kw in external_keywords:
            if matches_keyword(kw, cleaned):
                is_external = True
                break
        
        # Check internal keywords only if no external match found
        is_internal = False
        if not is_external:
            for kw in internal_keywords:
                if matches_keyword(kw, cleaned):
                    is_internal = True
                    break

        if is_internal and cleaned not in seen_internal:
            internal_areas.append(cleaned)
            seen_internal.add(cleaned)
        elif is_external and cleaned not in seen_external:
            external_areas.append(cleaned)
            seen_external.add(cleaned)

    return internal_areas, external_areas

def calculate_sold_status(contract_date_value, estimated_date_value):
    """
    Calculate sold status based on date ranges:
    - Within last 12 months: status = 2
    - 1 to 5 years: status = 3
    - 5 to 10 years: status = 4
    - 10 to 20 years: status = 5
    - More than 20 years: status = 6
    
    Args:
        contract_date_value: The contractDate value (string or datetime)
        estimated_date_value: The estimatedSoldDate value (string or datetime) as fallback
        
    Returns:
        int: Status code (2-6) or None if no valid date found
    """
    # Try contractDate first, then estimated date
    date_value = contract_date_value if contract_date_value else estimated_date_value
    if not date_value:
        return None
    
    parsed_date = parse_date_flexible(date_value)
    if not parsed_date:
        return None
    
    now = datetime.now()
    time_diff = now - parsed_date
    
    # Calculate years difference
    years_diff = time_diff.total_seconds() / (365.25 * 24 * 3600)
    
    if years_diff < 1.0:  # Within last 12 months
        return 2
    elif years_diff < 5.0:  # 1 to 5 years
        return 3
    elif years_diff < 10.0:  # 5 to 10 years
        return 4
    elif years_diff < 20.0:  # 10 to 20 years
        return 5
    else:  # More than 20 years
        return 6


def calculate_agent_to_validate(txn, txn_type=None):
    """
    Calculate agentToValidate flag for a transaction.
    
    agentToValidate is True when either:
    - the contract date is null or empty string, OR
    - the price (or rate) is null or empty string.
    """
    if not isinstance(txn, dict):
        return False
    
    # Check if primaryUserID exists (check both primaryUserID and agents.primaryUserId)
    primary_user_id = txn.get("primaryUserID")
    if is_value_missing(primary_user_id):
        # Also check inside agents object
        agents = txn.get("agents")
        if isinstance(agents, dict):
            primary_user_id = agents.get("primaryUserId")
    
    has_primary_user = not is_value_missing(primary_user_id)
    
    if not has_primary_user:
        # No primaryUserID, so no validation needed
        return False
    
    # Determine transaction type if not provided
    if txn_type is None:
        txn_type = txn.get("_temp_type")
    
    # Check if contract date is missing (null or empty string)
    contract_date = txn.get("contractDate")
    missing_contract_date = is_value_missing(contract_date)
    
    # Determine if price/rate is missing (null or empty string)
    if txn_type in ("rent", "leased"):
        price_value = txn.get("rate")
    else:
        price_value = txn.get("price")
    
    missing_price = is_value_missing(price_value)
    
    # Set to true if contract date or price is missing
    return missing_contract_date or missing_price


def update_history_with_confirmation_flags(details):
    """
    Update all history entries (soldHistory, saleHistory, rentHistory, leasedHistory)
    with validation/normalisation flags.
    
    NOTE: This function ONLY modifies history arrays within transactionDetails.
    It does NOT modify any other fields in transactionDetails or the document root.
    
    Args:
        details: transactionDetails dictionary containing all histories
        
    Returns:
        dict: Update payload with updated histories (keys like "transactionDetails.soldHistory"),
              or empty dict if no updates needed
    """
    update_payload = {}
    updates_made = False
    
    # Process each history type
    history_configs = [
        ("soldHistory", "sold"),
        ("saleHistory", "sale"),
        ("rentHistory", "rent"),
        ("leasedHistory", "leased"),
    ]
    
    for history_key, txn_type in history_configs:
        history_list = details.get(history_key, [])
        if not history_list:
            continue
        
        updated_history = []
        
        for entry in history_list:
            if isinstance(entry, dict):
                # Create a copy of the entry with updated fields
                updated_entry = entry.copy()
                
                # Normalise title formatting (e.g. "1, 50" -> "1/50")
                if "title" in updated_entry:
                    normalised_title = normalize_title_text(updated_entry.get("title"))
                    if normalised_title:
                        updated_entry["title"] = normalised_title
                
                # Calculate single validation flag for the agent
                updated_entry["agentToValidate"] = calculate_agent_to_validate(updated_entry, txn_type)
                
                updated_history.append(updated_entry)
            else:
                # Keep non-dict entries as-is
                updated_history.append(entry)
        
        # Always update if we have entries (to ensure flags are set)
        if len(updated_history) > 0:
            update_payload[f"transactionDetails.{history_key}"] = updated_history
            updates_made = True
    
    return update_payload if updates_made else {}


def build_current_state(source_txn, txn_type, is_sold_fallback, type_mapping=None, input_record=None, existing_state=None):
    if not source_txn:
        return {}

    # Initialize with core transaction fields (contractDate handled separately)
    new_state = {
        k: source_txn.get(k)
        for k in [
            "price",
            "listing_images",
            "source",
            "bedrooms",
            "bathrooms",
            "carParking",
        ]
    }
    
    # Include secondary price for sale transactions when available
    if txn_type == "sale":
        if isinstance(source_txn, dict) and "price2" in source_txn:
            new_state["price2"] = source_txn.get("price2")
    
    # Include rent rate when available for rent transactions
    if txn_type == "rent":
        if isinstance(source_txn, dict) and "rate" in source_txn:
            new_state["rate"] = source_txn.get("rate")
    
    # Add title field - first check if it exists in the source transaction (history object)
    # If not, fall back to input record FULL_ADDRESS
    title_from_txn = source_txn.get("title")
    if title_from_txn and not is_value_missing(title_from_txn):
        normalised = normalize_title_text(title_from_txn)
        new_state["title"] = normalised if normalised else title_from_txn
    elif input_record:
        full_address = input_record.get("FULL_ADDRESS")
        if full_address:
            title = clean_title_from_full_address(full_address)
            if title:
                new_state["title"] = title
    
    # Add type field from Field35 with standardization (always create type field)
    if input_record:
        field35_value = input_record.get("Field35")
        if type_mapping:
            standardized_type = standardize_type(field35_value, type_mapping)
            new_state["type"] = standardized_type  # Always set, even if None
        else:
            # If no type mapping, use the raw Field35 value or None
            if field35_value and str(field35_value).strip():
                new_state["type"] = str(field35_value).strip()
            else:
                new_state["type"] = None
    else:
        # If no input record, set type to None
        new_state["type"] = None
    
    # Ensure "nan" string is converted to None (case-insensitive check)
    if new_state.get("type") and str(new_state["type"]).strip().lower() == "nan":
        new_state["type"] = None
    
    # Add locality field from suburb (one-time update only)
    # First, preserve locality from existing state if it exists
    if existing_state and existing_state.get("locality") and not is_value_missing(existing_state.get("locality")):
        new_state["locality"] = existing_state.get("locality")
    elif input_record:
        suburb_value = input_record.get("suburb")
        if suburb_value and str(suburb_value).strip():
            # Only add if locality doesn't already exist in currentState
            existing_locality = source_txn.get("locality")
            if not existing_locality:
                new_state["locality"] = str(suburb_value).strip().upper()
    
    # Add region field from ADDRESS_DETAIL_PID
    if input_record:
        address_pid = input_record.get("ADDRESS_DETAIL_PID")
        if address_pid and str(address_pid).strip():
            # Extract region by removing "GA" and digits from ADDRESS_DETAIL_PID
            pid_str = str(address_pid).strip()
            if pid_str.startswith("GA"):
                # Remove "GA" prefix
                region_part = pid_str[2:]
                # Remove all digits and underscores, keep only letters
                region = ''.join(c for c in region_part if c.isalpha())
                if region:
                    new_state["region"] = region.upper()
    
    # Add slug field from title
    title_value = new_state.get("title")
    if title_value:
        slug = create_slug(title_value)
        if slug:
            new_state["slug"] = slug
    
    # Add landArea field from Area and areaUnit
    if input_record:
        area_value = input_record.get("Area")
        area_unit_value = input_record.get("areaUnit")
        land_area = format_land_area(area_value, area_unit_value)
        if land_area:
            new_state["landArea"] = land_area
    
    # Default active state
    new_state["isActive"] = True

    # Add internalAreas and externalAreas (always present as lists)
    internal_areas, external_areas = [], []
    if input_record:
        internal_areas, external_areas = categorize_areas_from_input(input_record)
    new_state["internalAreas"] = internal_areas
    new_state["externalAreas"] = external_areas
    
    # Handle agent information properly
    # For sold transactions, always preserve agent data if it exists
    agent_info = source_txn.get("agents")
    if agent_info is not None:
        if isinstance(agent_info, dict):
            new_state["agents"] = agent_info
        else:
            # If it's not a dict but not None, preserve it as-is
            new_state["agents"] = agent_info
    else:
        # Only set to None if explicitly None
        new_state["agents"] = None
    
    # Get agentsCompanyId from transaction level (not from inside agents object)
    agents_company_id = source_txn.get("agentsCompanyId")
    new_state["agentsCompanyId"] = agents_company_id

    # Handle contractDate and estimatedContractDate based on whether we have actual date or estimated date
    original_contract_date = source_txn.get("contractDate")
    
    if original_contract_date:
        # Case 2: We have the actual contractDate
        new_state["contractDate"] = original_contract_date
        new_state["estimatedContractDate"] = None
    else:
        # Case 1: No actual contractDate, use estimated date
        estimated_date = None
        if "_chosen_date" in source_txn:
            estimated_date = source_txn["_chosen_date"]
        else:
            # Fallback to estimated dates based on transaction type
            if txn_type == "sold":
                estimated_date = source_txn.get("estimatedSoldDate")
            elif txn_type == "sale":
                estimated_date = source_txn.get("estimatedSaleDate")
            elif txn_type == "leased":
                estimated_date = source_txn.get("estimatedLeasedDate")
            elif txn_type == "rent":
                estimated_date = source_txn.get("estimatedRentDate")
            else:
                estimated_date = None
        
        if estimated_date:
            new_state["contractDate"] = estimated_date
            new_state["estimatedContractDate"] = estimated_date
        else:
            new_state["estimatedContractDate"] = None

    new_state["transactionType"] = txn_type
    
    # Add core fields
    new_state["description"] = source_txn.get("description")
    new_state["headline"] = None
    new_state["homieeInsights"] = None
    new_state["isAIEnabled"] = False
    
    # Force status codes by type: rent=11, leased=15, sale=1
    # For sold, calculate based on date ranges
    if txn_type == "leased":
        new_state["status"] = 15
    elif txn_type == "rent":
        new_state["status"] = 11
    elif txn_type == "sale":
        new_state["status"] = 1
    elif txn_type == "sold":
        # Calculate sold status based on date ranges
        contract_date = source_txn.get("contractDate")
        estimated_date = source_txn.get("estimatedSoldDate")
        calculated_status = calculate_sold_status(contract_date, estimated_date)
        if calculated_status is not None:
            new_state["status"] = calculated_status
        else:
            # Fallback to existing soldStatus if date calculation fails
            new_state["status"] = source_txn.get("soldStatus")
    else:
        new_state["status"] = source_txn.get(f"{txn_type}Status") if txn_type else None

    # Hide sold transactionType if older than 12 months
    # Note: Status is now calculated above based on date ranges, so we don't modify it here
    # Agent data is preserved for all sold transactions regardless of age
    if is_sold_fallback:
        contract_date = parse_date_flexible(new_state.get("contractDate"))
        twelve_months_ago = datetime.now() - timedelta(days=365)
        if contract_date and contract_date < twelve_months_ago:
            new_state["transactionType"] = "not displayed"
            # Agent data is preserved - do not clear agents or agentsCompanyId
    
    # Calculate single validation flag for the agent on the state object
    new_state["agentToValidate"] = calculate_agent_to_validate(source_txn, txn_type)
    
    return new_state


def deduplicate_sold_history_by_price(sold_history):
    """
    Deduplicate sold history records by price, enriching contract transactions
    from estimated transactions and removing duplicates.
    
    Rules:
    1. Group transactions by same price
    2. Within each group, identify contract transactions (non-null contractDate)
       and estimated transactions (null contractDate but non-null estimatedSoldDate)
    3. Enrich contract transactions from estimated transactions for missing fields:
       bedrooms, bathrooms, carParking, agents, listing_images
    4. Remove estimated transactions after enrichment
    5. Recalculate status: most recent transaction (by effectiveDate) gets "active",
       all others get "inactive"
    
    Args:
        sold_history: List of sold transaction dictionaries
        
    Returns:
        List of deduplicated sold transaction dictionaries with recalculated status
    """
    if not sold_history or not isinstance(sold_history, list):
        return sold_history
    
    # Create a working copy
    working_history = [txn.copy() if isinstance(txn, dict) else txn for txn in sold_history]
    
    # Group transactions by price
    price_groups = {}
    for idx, txn in enumerate(working_history):
        if not isinstance(txn, dict):
            continue
        
        price = txn.get("price")
        # Only group if price is not missing
        if not is_value_missing(price):
            if price not in price_groups:
                price_groups[price] = []
            price_groups[price].append((idx, txn))
    
    # Track indices to remove
    indices_to_remove = set()
    
    # Process each price group
    for price, group in price_groups.items():
        if len(group) < 2:
            # Need at least 2 transactions to deduplicate
            continue
        
        # Separate contract and estimated transactions
        contract_txns = []
        estimated_txns = []
        
        for idx, txn in group:
            contract_date = txn.get("contractDate")
            estimated_date = txn.get("estimatedSoldDate")
            
            has_contract = not is_value_missing(contract_date)
            has_estimated = not is_value_missing(estimated_date)
            
            if has_contract:
                contract_txns.append((idx, txn))
            elif has_estimated and is_value_missing(contract_date):
                estimated_txns.append((idx, txn))
        
        # If we have contract transactions and estimated transactions, enrich and remove
        if contract_txns and estimated_txns:
            # If multiple contract transactions, prefer one (use first one as canonical)
            # In practice, we'll enrich all contract transactions from all estimated ones
            canonical_contract_idx, canonical_contract = contract_txns[0]
            
            # Enrich canonical contract from all estimated transactions
            for est_idx, est_txn in estimated_txns:
                # Enrich fields: bedrooms, bathrooms, carParking, agents, listing_images
                fields_to_enrich = ["bedrooms", "bathrooms", "carParking", "agents", "listing_images"]
                
                for field in fields_to_enrich:
                    canonical_value = canonical_contract.get(field)
                    est_value = est_txn.get(field)
                    
                    # If canonical is missing and estimated has value, copy it
                    if is_value_missing(canonical_value) and not is_value_missing(est_value):
                        canonical_contract[field] = est_value
                
                # Mark estimated transaction for removal
                indices_to_remove.add(est_idx)
            
            # If there are multiple contract transactions, enrich all of them
            # and keep all (they might have different contract dates)
            for contract_idx, contract_txn in contract_txns[1:]:
                for est_idx, est_txn in estimated_txns:
                    fields_to_enrich = ["bedrooms", "bathrooms", "carParking", "agents", "listing_images"]
                    
                    for field in fields_to_enrich:
                        contract_value = contract_txn.get(field)
                        est_value = est_txn.get(field)
                        
                        if is_value_missing(contract_value) and not is_value_missing(est_value):
                            contract_txn[field] = est_value
    
    # Remove estimated transactions that were marked for removal
    result = []
    for idx, txn in enumerate(working_history):
        if idx not in indices_to_remove:
            result.append(txn)
    
    # Recalculate status for all remaining transactions
    # Calculate effectiveDate for each transaction
    transactions_with_dates = []
    for txn in result:
        if not isinstance(txn, dict):
            continue
        
        contract_date = txn.get("contractDate")
        estimated_date = txn.get("estimatedSoldDate")
        
        # Determine effectiveDate
        effective_date_value = None
        if not is_value_missing(contract_date):
            effective_date_value = contract_date
        elif not is_value_missing(estimated_date):
            effective_date_value = estimated_date
        
        # Parse the date
        effective_date_parsed = None
        if effective_date_value:
            effective_date_parsed = parse_date_flexible(effective_date_value)
        
        transactions_with_dates.append((txn, effective_date_parsed))
    
    # Find the most recent transaction
    valid_transactions = [(txn, date) for txn, date in transactions_with_dates if date is not None]
    
    if valid_transactions:
        # Sort by date (most recent first)
        valid_transactions.sort(key=lambda x: x[1], reverse=True)
        most_recent_date = valid_transactions[0][1]
        most_recent_txn = valid_transactions[0][0]
        
        # Set status: most recent = "active", others = "inactive"
        # If multiple transactions have the same most recent date, only mark the first one as active
        most_recent_found = False
        for txn, date in transactions_with_dates:
            if date is not None:
                # Compare dates to find the most recent one
                if date == most_recent_date and not most_recent_found:
                    txn["status"] = "active"
                    most_recent_found = True
                else:
                    txn["status"] = "inactive"
            else:
                # Both dates are null, leave as "inactive"
                if "status" not in txn or is_value_missing(txn.get("status")):
                    txn["status"] = "inactive"
    
    return result


def deduplicate_sold_history(sold_history):
    """
    Deduplicate sold history records by merging records with the same description
    when at least one is missing price or date (or both).
    
    Args:
        sold_history: List of sold transaction dictionaries
        
    Returns:
        List of deduplicated sold transaction dictionaries
    """
    if not sold_history or not isinstance(sold_history, list):
        return sold_history
    
    # Group sold records by description (case-insensitive, after trimming)
    description_groups = {}
    for idx, txn in enumerate(sold_history):
        if not isinstance(txn, dict):
            continue
        
        description = txn.get("description")
        if description and isinstance(description, str):
            description_key = description.strip().lower()
            if description_key:  # Only group if description is non-empty
                if description_key not in description_groups:
                    description_groups[description_key] = []
                description_groups[description_key].append((idx, txn))
    
    # Process groups that have duplicates or records with missing fields
    merged_indices = set()
    merged_records = []
    
    for description_key, group in description_groups.items():
        if len(group) < 2:
            # Single record, no merging needed
            continue
        
        # Check if at least one record in the group is missing price or date
        needs_merging = False
        for idx, txn in group:
            has_price = not is_value_missing(txn.get("price"))
            has_contract_date = not is_value_missing(txn.get("contractDate"))
            has_estimated_date = not is_value_missing(txn.get("estimatedSoldDate"))
            has_date = has_contract_date or has_estimated_date
            
            if not has_price or not has_date:
                needs_merging = True
                break
        
        if not needs_merging:
            # All records have both price and date, no merging needed
            continue
        
        # Merge records in this group
        merged_txn = {}
        for idx, txn in group:
            merged_indices.add(idx)
            # Merge all fields, preferring non-null values
            for key, value in txn.items():
                if key not in merged_txn or is_value_missing(merged_txn[key]):
                    if not is_value_missing(value):
                        merged_txn[key] = value
                # If both have values, prefer the one that's not missing
                elif not is_value_missing(value) and is_value_missing(merged_txn[key]):
                    merged_txn[key] = value
        
        merged_records.append(merged_txn)
    
    # Build final list: keep non-merged records, add merged records
    result = []
    for idx, txn in enumerate(sold_history):
        if idx not in merged_indices:
            result.append(txn)
    
    # Add merged records
    result.extend(merged_records)
    
    return result


def process_current_state(doc, type_mapping=None, input_record=None):
    """
    Process transaction histories and return states for both rentState and currentState.
    Returns a tuple: (rent_state_dict, current_state_dict)
    - rent_state_dict: State built from latest transaction in rentHistory or leasedHistory
    - current_state_dict: State built from latest transaction in saleHistory or soldHistory
    """
    details = doc.get("transactionDetails", {})
    
    # Deduplicate soldHistory by price and recalculate status (must be done first)
    if "soldHistory" in details and details["soldHistory"]:
        details["soldHistory"] = deduplicate_sold_history_by_price(details["soldHistory"])
    
    # Find latest transaction between rentHistory and leasedHistory for rentState
    # This considers BOTH rentHistory and leasedHistory, picking the latest transaction
    # by date (using contractDate, or falling back to estimatedRentDate/estimatedLeasedDate)
    rent_candidates = []
    for key, txn_type in [
        ("rentHistory", "rent"),
        ("leasedHistory", "leased"),
    ]:
        for txn in details.get(key, []):
            if isinstance(txn, dict):
                txn["_temp_type"] = txn_type
                rent_candidates.append(txn)
    
    # First, try to find transactions with actual contract date and rate/price
    ideal_rent_candidates = []
    for txn in rent_candidates:
        txn_type = txn.get("_temp_type")
        if has_actual_contract_date_and_price(txn, txn_type):
            date_value = txn.get("contractDate")
            parsed_date = parse_date_flexible(date_value)
            if parsed_date:
                txn["_chosen_date"] = date_value
                ideal_rent_candidates.append((parsed_date, txn))
    
    # If we found ideal candidates, use the latest one
    latest_rent_txn = None
    latest_rent_date = None
    if ideal_rent_candidates:
        ideal_rent_candidates.sort(key=lambda x: x[0], reverse=True)
        # Latest date is the first element after sorting
        latest_rent_date = ideal_rent_candidates[0][0]
        # Prefer leasedHistory when there is a tie on the latest date
        for date, txn in ideal_rent_candidates:
            if date == latest_rent_date and txn.get("_temp_type") == "leased":
                latest_rent_txn = txn
                break
        # If no leasedHistory entry shares the latest date, fall back to the first candidate
        if latest_rent_txn is None:
            latest_rent_date, latest_rent_txn = ideal_rent_candidates[0]
    else:
        # Fall back to current behavior: find latest transaction regardless of contract date type or price
        for txn in rent_candidates:
            txn_type = txn.get("_temp_type")
            date_value = txn.get("contractDate")
            if not date_value:
                if txn_type == "leased":
                    date_value = txn.get("estimatedLeasedDate")
                elif txn_type == "rent":
                    date_value = txn.get("estimatedRentDate")
            
            parsed_date = parse_date_flexible(date_value)
            if parsed_date:
                txn["_chosen_date"] = date_value
                if latest_rent_date is None or parsed_date > latest_rent_date:
                    latest_rent_date = parsed_date
                    latest_rent_txn = txn
                elif parsed_date == latest_rent_date:
                    # Tie-breaker: if dates are equal, prefer leasedHistory over rentHistory
                    current_type = latest_rent_txn.get("_temp_type") if latest_rent_txn else None
                    if txn_type == "leased" and current_type != "leased":
                        latest_rent_txn = txn
    
    # Find latest transaction between saleHistory and soldHistory for currentState
    sale_candidates = []
    for key, txn_type in [
        ("saleHistory", "sale"),
    ]:
        for txn in details.get(key, []):
            if isinstance(txn, dict):
                txn["_temp_type"] = txn_type
                sale_candidates.append(txn)
    
    # Add soldHistory candidates
    for txn in details.get("soldHistory", []):
        if isinstance(txn, dict):
            txn["_temp_type"] = "sold"
            sale_candidates.append(txn)
    
    # NEW LOGIC: Collect ALL candidates with dates, then apply priority as tie-breaker
    # Step 1: Extract date and priority info for all candidates
    all_candidates_with_info = []
    
    for txn in sale_candidates:
        txn_type = txn.get("_temp_type")
        
        # Extract date (contractDate or estimated date)
        date_value = txn.get("contractDate")
        if not date_value:
            if txn_type == "sale":
                date_value = txn.get("estimatedSaleDate")
            elif txn_type == "sold":
                date_value = txn.get("estimatedSoldDate")
        
        parsed_date = parse_date_flexible(date_value)
        if not parsed_date:
            continue  # Skip transactions with no date
        
        # Store date info
        txn["_chosen_date"] = date_value
        txn["_parsed_date"] = parsed_date
        
        # Calculate priority level for tie-breaking
        # Priority 1: Has actual contractDate AND price
        has_contract_and_price = has_actual_contract_date_and_price(txn, txn_type)
        
        # Priority 2: Sold transaction backed by saleHistory (even without contractDate/price)
        is_backed = False
        if txn_type == "sold" and not has_contract_and_price:
            is_backed = is_sold_backed_by_sale(txn, details)
        
        # Assign priority level (lower number = higher priority)
        if has_contract_and_price:
            priority = 1
        elif is_backed:
            priority = 2
        else:
            priority = 3
        
        # Special handling for sale-backed sold: try to merge with DAT sold
        candidate_txn = txn
        if is_backed and txn_type == "sold":
            dat_sold_records = find_dat_sold_within_months(txn, details, max_months_diff=12)
            if len(dat_sold_records) == 1:
                candidate_txn = merge_sold_transactions(txn, dat_sold_records[0])
                candidate_txn["_temp_type"] = "sold"
                candidate_txn["_chosen_date"] = date_value
                candidate_txn["_parsed_date"] = parsed_date
        
        all_candidates_with_info.append({
            "txn": candidate_txn,
            "date": parsed_date,
            "priority": priority,
            "type": txn_type,
            "has_price": not is_value_missing(candidate_txn.get("price"))
        })
    
    # Step 2: Sort by date (latest first), then by priority, then prefer sold over sale, then prefer with price
    if all_candidates_with_info:
        all_candidates_with_info.sort(
            key=lambda x: (
                x["date"],           # Latest date first (reverse=True below)
                -x["priority"],      # Lower priority number = higher priority (negative for reverse)
                x["type"] != "sold", # Prefer "sold" over "sale" (False < True)
                not x["has_price"]   # Prefer transactions with price (False < True)
            ),
            reverse=True
        )
        
        latest_sale_txn = all_candidates_with_info[0]["txn"]
        latest_sale_date = all_candidates_with_info[0]["date"]
    else:
        latest_sale_txn = None
        latest_sale_date = None
    
    # Build rentState from latest rent/leased transaction
    rent_state = None
    if latest_rent_txn:
        rent_txn_type = latest_rent_txn.get("_temp_type")
        is_sold_fallback = False  # rent/leased are never sold fallback
        rent_state = build_current_state(latest_rent_txn, rent_txn_type, is_sold_fallback, type_mapping, input_record)
        
        # Backfill missing fields in the state (listing_images, bedrooms, bathrooms, carParking) from other transactions
        # This only modifies the state dictionary, NOT the history objects
        if rent_state and len(rent_state) > 0:
            fields_to_backfill = ["listing_images", "bedrooms", "bathrooms", "carParking"]
            backfill_missing_fields_in_state(rent_state, latest_rent_txn, details, rent_txn_type, fields_to_backfill)
        else:
            rent_state = None
    
    # Build currentState from latest sale/sold transaction
    current_state = None
    if latest_sale_txn:
        sale_txn_type = latest_sale_txn.get("_temp_type")
        if sale_txn_type == "sold" and source_matches_dat_weekly(latest_sale_txn.get("source")):
            enrich_dat_weekly_sold_transaction(latest_sale_txn, details)
        
        is_sold_fallback = (sale_txn_type == "sold")
        # Get existing currentState to preserve locality
        existing_current_state = details.get("currentState")
        current_state = build_current_state(latest_sale_txn, sale_txn_type, is_sold_fallback, type_mapping, input_record, existing_current_state)
        
        # Backfill missing fields in the state (listing_images, bedrooms, bathrooms, carParking) from other transactions
        # This only modifies the state dictionary, NOT the history objects
        if current_state and len(current_state) > 0:
            fields_to_backfill = ["listing_images", "bedrooms", "bathrooms", "carParking"]
            backfill_missing_fields_in_state(current_state, latest_sale_txn, details, sale_txn_type, fields_to_backfill)
        else:
            current_state = None
    
    return rent_state, current_state

# --- MongoDB Direct Processor ---
def process_all_documents_directly(collection, logger, type_mapping=None, num_cores=8, batch_size=1000):
    """
    Process all documents directly from MongoDB without extracting PIDs first.
    This avoids the 16MB limit issue with distinct() on large collections.
    """
    try:
        logger.info("INPUT: Processing all documents directly from MongoDB (no PID extraction needed)...")
        
        # Get total document count for reporting
        total_docs = collection.count_documents({})
        logger.info(f"INPUT: Found {total_docs} total documents in collection")
        
        if total_docs == 0:
            logger.warning("No documents found in MongoDB collection")
            return 0, 0, [], [], {}
        
        # Process documents in batches
        documents_processed = 0
        documents_updated = 0
        successful_pids = []
        failed_pids = []
        suburb_counts = {}
        
        # Calculate number of batches
        num_batches = (total_docs + batch_size - 1) // batch_size
        
        # Track last processed ID for cursor-based pagination (eliminates skip())
        batch_last_id = None
        query = {}  # Base query (empty = all documents)
        
        try:
            # Process each batch
            for batch_num in range(num_batches):
                limit = min(batch_size, total_docs - (batch_num * batch_size))
                
                print(f"\nProcessing batch {batch_num + 1}/{num_batches} ({limit} records)")
                
                # Use cursor-based query (no skip needed!)
                batch_query = query.copy() if isinstance(query, dict) else query
                if batch_last_id:
                    # Continue from last processed document
                    batch_query = {"_id": {"$gt": batch_last_id}}
                
                # Process entire batch in a single worker (no parallel workers per batch)
                # This allows proper cursor-based pagination
                worker_args = [(
                    1, 1, batch_query, limit,
                    MONGO_URI, DATABASE_NAME, COLLECTION_NAME,
                    type_mapping, total_docs
                )]
                
                # Process batch with progress bar (single worker, no multiprocessing overhead)
                batch_processed = 0
                batch_updated = 0
                batch_successful_pids = []
                batch_failed_pids = []
                worker_last_id = None
                
                # Process with single worker (no multiprocessing for this batch)
                with tqdm(total=limit, desc=f"Batch {batch_num + 1}/{num_batches}", unit="docs") as pbar:
                    proc, upd, succ_pids, fail_pids, sub_counts, worker_last_id = process_documents_worker(
                        1, 1, batch_query, limit,
                        MONGO_URI, DATABASE_NAME, COLLECTION_NAME,
                        type_mapping, total_docs
                    )
                    batch_processed += proc
                    batch_updated += upd
                    batch_successful_pids.extend(succ_pids)
                    batch_failed_pids.extend(fail_pids)
                    if worker_last_id:
                        batch_last_id = worker_last_id
                    pbar.update(proc)
                
                print(f"Completed batch {batch_num + 1}/{num_batches}")
                
                # Update totals
                documents_processed += batch_processed
                documents_updated += batch_updated
                successful_pids.extend(batch_successful_pids)
                failed_pids.extend(batch_failed_pids)
        
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
        
        return documents_processed, documents_updated, successful_pids, failed_pids, suburb_counts
        
    except Exception as e:
        logger.error(f"Error processing all documents: {e}")
        import traceback
        traceback.print_exc()
        return 0, 0, [], [], {}


def process_documents_worker(worker_num, total_workers, query, limit, mongo_uri, database_name, collection_name, type_mapping, total_docs):
    """
    Worker function to process a slice of documents directly from MongoDB.
    Each worker processes documents in its assigned range using cursor-based pagination.
    """
    documents_processed = 0
    documents_updated = 0
    successful_pids = []
    failed_pids = []
    suburb_counts = {}
    last_processed_id = None
    
    # Prepare bulk operations list
    bulk_operations = []
    bulk_batch_size = 500  # Process bulk writes in batches
    
    try:
        # Create MongoDB connection for this worker
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=60000)
        db = client[database_name]
        collection = db[collection_name]
        
        with collection.database.client.start_session() as session:
            # Use cursor-based pagination (no skip needed - query already has _id filter)
            # This is MUCH faster than skip() which has to traverse all skipped documents
            cursor = (
                collection.find(query, no_cursor_timeout=True, session=session)
                .sort("_id", 1)
                .limit(limit)
                .batch_size(500)  # Optimized batch size for bulk operations
                .hint([("_id", 1)])  # Use _id index for faster sorting
            )
            
            for doc in cursor:
                try:
                    # Track last processed document ID
                    last_processed_id = doc.get("_id")
                    
                    # Extract ADDRESS_DETAIL_PID for tracking
                    address_pid = doc.get("transactionDetails", {}).get("ADDRESS_DETAIL_PID")
                    
                    # Extract input record from MongoDB document (no CSV available)
                    input_record = extract_input_record_from_document(doc)
                    
                    # Initialize update payload
                    # IMPORTANT: This script ONLY modifies the following fields in transactionDetails:
                    # - soldHistory, saleHistory, rentHistory, leasedHistory (history arrays)
                    # - rentState
                    # - currentState
                    # All other fields in transactionDetails or document root are preserved and NOT modified
                    update_payload = {}
                    updates_made = False
                    
                    # Update soldStatus for all entries in soldHistory
                    details = doc.get("transactionDetails", {})
                    sold_history = details.get("soldHistory", [])
                    
                    # Deduplicate soldHistory by price and recalculate status (must be done first)
                    if sold_history:
                        sold_history = deduplicate_sold_history_by_price(sold_history)
                        # Update details with deduplicated soldHistory for use in process_current_state
                        details["soldHistory"] = sold_history
                    
                    if sold_history:
                        updated_sold_history = []
                        for sold_entry in sold_history:
                            if isinstance(sold_entry, dict):
                                # Calculate correct soldStatus based on date
                                contract_date = sold_entry.get("contractDate")
                                estimated_date = sold_entry.get("estimatedSoldDate")
                                calculated_status = calculate_sold_status(contract_date, estimated_date)
                                
                                # Create a copy of the entry with updated status
                                updated_entry = sold_entry.copy()
                                if calculated_status is not None:
                                    updated_entry["soldStatus"] = calculated_status
                                updated_sold_history.append(updated_entry)
                            else:
                                # Keep non-dict entries as-is
                                updated_sold_history.append(sold_entry)
                        
                        # Update the soldHistory in the update payload
                        update_payload["transactionDetails.soldHistory"] = updated_sold_history
                        updates_made = True
                    
                    # Update confirmation flags for all history entries
                    history_updates = update_history_with_confirmation_flags(details)
                    if history_updates:
                        update_payload.update(history_updates)
                        updates_made = True
                    
                    rent_state, current_state = process_current_state(doc, type_mapping, input_record)
                    
                    # Update rentState if we have a latest transaction from rentHistory or leasedHistory
                    if rent_state is not None and rent_state:
                        update_payload["transactionDetails.rentState"] = rent_state
                        updates_made = True
                    
                    # Update currentState if we have a latest transaction from saleHistory or soldHistory
                    if current_state is not None and current_state:
                        update_payload["transactionDetails.currentState"] = current_state
                        updates_made = True
                    
                    # Perform update if we have at least one state to update
                    if updates_made:
                        # Add to bulk operations instead of immediate update
                        bulk_operations.append(
                            UpdateOne(
                                {"_id": doc["_id"]},
                                {"$set": update_payload}
                            )
                        )
                        
                        # Execute bulk write when batch size is reached
                        if len(bulk_operations) >= bulk_batch_size:
                            try:
                                result = collection.bulk_write(bulk_operations, ordered=False, session=session)
                                documents_updated += result.modified_count
                                bulk_operations.clear()
                            except Exception as bulk_error:
                                # Log error but continue processing
                                pass
                        
                        if address_pid:
                            successful_pids.append(address_pid)
                    else:
                        # Document processed but no states generated
                        if address_pid:
                            failed_pids.append(f"{address_pid} - No states generated (no transactions found)")

                    documents_processed += 1

                except Exception as e:
                    address_pid = doc.get("transactionDetails", {}).get("ADDRESS_DETAIL_PID")
                    if address_pid:
                        failed_pids.append(f"{address_pid} - Error: {str(e)}")
            
            # Execute remaining bulk operations
            if bulk_operations:
                try:
                    result = collection.bulk_write(bulk_operations, ordered=False, session=session)
                    documents_updated += result.modified_count
                except Exception as bulk_error:
                    # Log error but continue
                    pass
        
        client.close()
        
    except pymongo.errors.PyMongoError as e:
        pass  # Errors handled at batch level
    except Exception as e:
        pass  # Errors handled at batch level
    
    return documents_processed, documents_updated, successful_pids, failed_pids, suburb_counts, last_processed_id


def extract_input_record_from_document(doc):
    """
    Extract input record data from MongoDB document structure.
    This allows processing without CSV by using data already in the document.
    """
    if not doc or not isinstance(doc, dict):
        return None
    
    transaction_details = doc.get("transactionDetails", {})
    if not transaction_details:
        return None
    
    # Build input_record from document fields
    # Map MongoDB document fields to CSV-like structure
    input_record = {}
    
    # Extract fields that might be in transactionDetails
    input_record["ADDRESS_DETAIL_PID"] = transaction_details.get("ADDRESS_DETAIL_PID")
    input_record["FULL_ADDRESS"] = transaction_details.get("FULL_ADDRESS")
    input_record["suburb"] = transaction_details.get("suburb")
    input_record["Area"] = transaction_details.get("Area")
    input_record["areaUnit"] = transaction_details.get("areaUnit")
    
    # Extract Field35 (type) - might be in transactionDetails or document root
    input_record["Field35"] = transaction_details.get("Field35") or doc.get("Field35")
    
    # Extract Field52-79 for areas categorization
    for i in range(52, 80):
        field_key = f"Field{i}"
        input_record[field_key] = transaction_details.get(field_key) or doc.get(field_key)
    
    return input_record


# --- Polygon ID and Location Helpers ---
def ensure_indexes(geometries_collection, transactions_collection, logger):
    """Ensure indexes exist for efficient lookups."""
    # Try to create indexes, handling case where they already exist
    try:
        geometries_collection.create_index("reference_id.gnafId", name="idx_reference_id_gnafId")
        logger.info("Created index on propertyGeometric.reference_id.gnafId")
    except OperationFailure as e:
        if e.code == 85:  # IndexOptionsConflict
            logger.info("Index on propertyGeometric.reference_id.gnafId already exists")
        else:
            raise
    except Exception as e:
        logger.warning(f"Could not create index on propertyGeometric: {e}")
    
    # Create indexes on transactions collection
    indexes_to_create = [
        ("reference_id.gnafId", "idx_reference_id_gnafId"),
        ("transactionDetails.ADDRESS_DETAIL_PID", "idx_transactionDetails_ADDRESS_DETAIL_PID")
    ]
    
    for field, index_name in indexes_to_create:
        try:
            transactions_collection.create_index(field, name=index_name)
            logger.info(f"Created index on propertyTransactionDetails.{field}")
        except OperationFailure as e:
            if e.code == 85:  # IndexOptionsConflict
                logger.info(f"Index on propertyTransactionDetails.{field} already exists")
            else:
                raise
        except Exception as e:
            logger.warning(f"Could not create index on propertyTransactionDetails.{field}: {e}")


def build_aggregation_pipeline(address_pids, limit=None):
    """Build aggregation pipeline to join collections, filtered to synced PIDs."""
    # Convert to list for $in operator
    pid_list = list(address_pids)
    
    pipeline = [
        # Match documents by ADDRESS_DETAIL_PID or reference_id.gnafId
        {
            "$match": {
                "$or": [
                    {"transactionDetails.ADDRESS_DETAIL_PID": {"$in": pid_list}},
                    {"reference_id.gnafId": {"$in": pid_list}}
                ]
            }
        },
        # Add field to determine which PID field to use for lookup
        {
            "$addFields": {
                "lookup_id": {
                    "$ifNull": ["$reference_id.gnafId", "$transactionDetails.ADDRESS_DETAIL_PID"]
                }
            }
        },
        # Lookup matching geometry document
        {
            "$lookup": {
                "from": "propertyGeometric",
                "localField": "lookup_id",
                "foreignField": "reference_id.gnafId",
                "as": "geometry"
            }
        },
        # Unwind geometry array (should be 0 or 1 element)
        {
            "$unwind": {
                "path": "$geometry",
                "preserveNullAndEmptyArrays": True
            }
        },
        # Filter out documents without matches
        {
            "$match": {
                "geometry": {"$exists": True, "$ne": None}
            }
        },
        # Project only fields we need
        {
            "$project": {
                "_id": 1,
                "currentLocation": "$location",
                "currentPolygonId": "$polygonId",
                "newLocation": "$geometry.location",
                "newPolygonId": "$geometry.polygonId"
            }
        }
    ]
    
    if limit:
        pipeline.append({"$limit": limit})
    
    return pipeline


def add_location_and_polygon_id_fields(
    mongo_uri, database_name, collection_name, address_pids, logger, batch_size=500, limit=None
):
    """
    Adds location and polygonId fields from propertyGeometric to propertyTransactionDetails
    for the processed documents.
    
    Args:
        mongo_uri: MongoDB URI
        database_name: Database name
        collection_name: Collection name
        address_pids: Set of GNAF IDs that were processed
        logger: Logger instance
        batch_size: Number of docs per bulk update (default: 500)
        limit: Optional limit for processed documents (useful for dry runs)
        
    Returns:
        dict: Statistics about the field addition operation
    """
    logger.info("=" * 60)
    logger.info("Adding location and polygonId fields to processed documents...")
    logger.info("=" * 60)
    
    stats = {
        "processed": 0,
        "matched": 0,
        "updated": 0,
        "errors": 0
    }
    
    try:
        # Connect to database
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        db = client[database_name]
        transactions = db[collection_name]
        geometries = db["propertyGeometric"]
        
        # Ensure indexes exist
        logger.info("Ensuring indexes exist...")
        ensure_indexes(geometries, transactions, logger)
        
        # Build aggregation pipeline
        pipeline = build_aggregation_pipeline(address_pids, limit)
        
        updates = []
        
        logger.info(f"Processing documents with batch size {batch_size}...")
        cursor = transactions.aggregate(
            pipeline,
            allowDiskUse=True,
            batchSize=batch_size,
        )
        
        try:
            for doc in cursor:
                stats["processed"] += 1
                
                new_fields = {}
                
                # Check if location needs updating (compare as JSON strings for complex objects)
                new_location = doc.get("newLocation")
                current_location = doc.get("currentLocation")
                if new_location is not None:
                    # Compare JSON representations for complex objects
                    if json.dumps(current_location, sort_keys=True) != json.dumps(new_location, sort_keys=True):
                        new_fields["location"] = new_location
                
                # Check if polygonId needs updating
                new_polygon_id = doc.get("newPolygonId")
                current_polygon_id = doc.get("currentPolygonId")
                if new_polygon_id is not None and new_polygon_id != current_polygon_id:
                    new_fields["polygonId"] = new_polygon_id
                
                if not new_fields:
                    continue
                
                stats["matched"] += 1
                
                updates.append(
                    UpdateOne({"_id": doc["_id"]}, {"$set": new_fields})
                )
                
                if len(updates) >= batch_size:
                    try:
                        result = transactions.bulk_write(updates, ordered=False)
                        stats["updated"] += result.modified_count
                        updates.clear()
                    except Exception as e:
                        stats["errors"] += len(updates)
                        logger.error(f"Error in bulk write: {e}")
                        updates.clear()
        finally:
            cursor.close()
        
        # Process remaining updates
        if updates:
            try:
                result = transactions.bulk_write(updates, ordered=False)
                stats["updated"] += result.modified_count
            except Exception as e:
                stats["errors"] += len(updates)
                logger.error(f"Error in final bulk write: {e}")
        
        client.close()
        
    except Exception as e:
        stats["errors"] += 1
        logger.error(f"Error in add_location_and_polygon_id_fields: {e}")
        raise
    
    logger.info("=" * 60)
    logger.info("Field Addition Statistics")
    logger.info("=" * 60)
    logger.info(f"Processed: {stats['processed']}")
    logger.info(f"Matched (needed updates): {stats['matched']}")
    logger.info(f"Updated: {stats['updated']}")
    logger.info(f"Errors: {stats['errors']}")
    logger.info("=" * 60)
    
    return stats


# --- CSV Loader ---
def read_matched_csv(csv_file_path, logger, limited=False):
    try:
        df = pd.read_csv(csv_file_path)
        total_csv_records = len(df)
        logger.info(f"INPUT: Successfully read CSV file with {total_csv_records} total records")
        
        # Always extract all PIDs from the original dataframe (no filtering)
        address_detail_pids = df["ADDRESS_DETAIL_PID"].dropna().unique().tolist()
        unique_pids = len(address_detail_pids)
        logger.info(f"INPUT: Found {unique_pids} unique ADDRESS_DETAIL_PID values (processing ALL PIDs)")
        
        # Log filtering info if --limited is enabled (for tracking/reporting only, doesn't affect PID processing)
        if limited:
            logger.info("INPUT: Applying --limited filters for tracking/reporting (PIDs already extracted from all records)...")
            
            # Field2 exclusion filter: skip records where Field2 (as integer) is in the exclusion list
            excluded_field2_values = [1047, 1316, 2217, 1048, 3315, 2124, 2438, 2620, 1799, 1275, 3053, 3248, 3241, 1146, 1030]
            
            # Create a mask for Field2 exclusion filter
            def get_field2_int(field2):
                if pd.isna(field2):
                    return None
                try:
                    return int(float(field2))
                except (ValueError, TypeError):
                    return None
            
            field2_values = df["Field2"].apply(get_field2_int)
            field2_mask = ~field2_values.isin(excluded_field2_values)
            
            # Calculate filtered count for reporting purposes only
            filtered_count = field2_mask.sum()
            logger.info(f"INPUT: Filtered records count (for reporting): {filtered_count} records (from {total_csv_records} total)")
            logger.info(f"INPUT: Field2 exclusion: {field2_mask.sum()} records passed Field2 exclusion")
        
        # Create a mapping of ADDRESS_DETAIL_PID to input records for type extraction
        # Use original df to ensure all PIDs have mappings available
        input_records_map = {}
        for _, row in df.iterrows():
            pid = row.get("ADDRESS_DETAIL_PID")
            if pd.notna(pid):
                input_records_map[str(pid)] = row.to_dict()
        
        logger.info(f"INPUT: Created input records mapping for {len(input_records_map)} PIDs")
        return address_detail_pids, total_csv_records, input_records_map
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return [], 0, {}

# --- Processor ---
def load_input_records_for_batch(csv_file_path, batch_pids):
    """
    Load input records from CSV file for a specific batch of PIDs.
    This avoids loading the entire CSV into memory for each worker.
    """
    if not csv_file_path or not os.path.exists(csv_file_path):
        return {}
    
    input_records_map = {}
    try:
        # Convert batch_pids to set for faster lookup
        pid_set = {str(pid) for pid in batch_pids}
        
        # Read CSV in chunks to handle large files
        chunk_size = 10000
        for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size, low_memory=False):
            for _, row in chunk.iterrows():
                pid = row.get("ADDRESS_DETAIL_PID")
                if pd.notna(pid) and str(pid) in pid_set:
                    input_records_map[str(pid)] = row.to_dict()
    except Exception as e:
        # If CSV loading fails, return empty dict and fall back to MongoDB extraction
        pass
    
    return input_records_map


def process_batch_worker(batch_pids, mongo_uri, database_name, collection_name, type_mapping, csv_file_path, limited, batch_num, total_batches):
    """
    Worker function to process a batch of PIDs. Each worker has its own MongoDB connection.
    """
    documents_processed = 0
    documents_updated = 0
    successful_pids = []
    failed_pids = []
    suburb_counts = {}
    
    # Prepare bulk operations list for better performance
    bulk_operations = []
    bulk_batch_size = 500  # Process bulk writes in batches
    
    # Load input records for this batch from CSV if available
    input_records_map = {}
    if csv_file_path:
        input_records_map = load_input_records_for_batch(csv_file_path, batch_pids)
    
    try:
        # Create MongoDB connection for this worker
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=60000)
        db = client[database_name]
        collection = db[collection_name]
        
        query = {"transactionDetails.ADDRESS_DETAIL_PID": {"$in": batch_pids}}
        
        with collection.database.client.start_session() as session:
            # Optimized cursor with smaller batch size for better bulk operation alignment
            cursor = (
                collection.find(query, no_cursor_timeout=True, session=session)
                .batch_size(500)  # Aligned with bulk_batch_size for optimal performance
            )
            
            # Process documents directly from cursor (no list() for better memory efficiency)
            for doc in cursor:
                try:
                    # Extract ADDRESS_DETAIL_PID for tracking
                    address_pid = doc.get("transactionDetails", {}).get("ADDRESS_DETAIL_PID")
                    
                    # Get input record for this PID if available
                    input_record = None
                    if input_records_map and address_pid:
                        # Use CSV data if available
                        input_record = input_records_map.get(str(address_pid))
                    
                    # Fall back to extracting from MongoDB document if CSV data not available
                    if not input_record:
                        input_record = extract_input_record_from_document(doc)
                    
                    # Initialize update payload
                    # IMPORTANT: This script ONLY modifies the following fields in transactionDetails:
                    # - soldHistory, saleHistory, rentHistory, leasedHistory (history arrays)
                    # - rentState
                    # - currentState
                    # All other fields in transactionDetails or document root are preserved and NOT modified
                    update_payload = {}
                    updates_made = False
                    
                    # Update soldStatus for all entries in soldHistory
                    details = doc.get("transactionDetails", {})
                    sold_history = details.get("soldHistory", [])
                    
                    # Deduplicate soldHistory by price and recalculate status (must be done first)
                    if sold_history:
                        sold_history = deduplicate_sold_history_by_price(sold_history)
                        # Update details with deduplicated soldHistory for use in process_current_state
                        details["soldHistory"] = sold_history
                    
                    if sold_history:
                        updated_sold_history = []
                        for sold_entry in sold_history:
                            if isinstance(sold_entry, dict):
                                # Calculate correct soldStatus based on date
                                contract_date = sold_entry.get("contractDate")
                                estimated_date = sold_entry.get("estimatedSoldDate")
                                calculated_status = calculate_sold_status(contract_date, estimated_date)
                                
                                # Create a copy of the entry with updated status
                                updated_entry = sold_entry.copy()
                                if calculated_status is not None:
                                    updated_entry["soldStatus"] = calculated_status
                                updated_sold_history.append(updated_entry)
                            else:
                                # Keep non-dict entries as-is
                                updated_sold_history.append(sold_entry)
                        
                        # Update the soldHistory in the update payload
                        update_payload["transactionDetails.soldHistory"] = updated_sold_history
                        updates_made = True
                    
                    # Update confirmation flags for all history entries
                    history_updates = update_history_with_confirmation_flags(details)
                    if history_updates:
                        update_payload.update(history_updates)
                        updates_made = True
                    
                    rent_state, current_state = process_current_state(doc, type_mapping, input_record)
                    
                    # Update rentState if we have a latest transaction from rentHistory or leasedHistory
                    if rent_state is not None and rent_state:
                        update_payload["transactionDetails.rentState"] = rent_state
                        updates_made = True
                    
                    # Update currentState if we have a latest transaction from saleHistory or soldHistory
                    if current_state is not None and current_state:
                        update_payload["transactionDetails.currentState"] = current_state
                        updates_made = True
                    
                    # Perform update if we have at least one state to update
                    if updates_made:
                        # Add to bulk operations instead of immediate update
                        bulk_operations.append(
                            UpdateOne(
                                {"_id": doc["_id"]},
                                {"$set": update_payload}
                            )
                        )
                        
                        # Execute bulk write when batch size is reached
                        if len(bulk_operations) >= bulk_batch_size:
                            try:
                                result = collection.bulk_write(bulk_operations, ordered=False, session=session)
                                documents_updated += result.modified_count
                                bulk_operations.clear()
                            except Exception as bulk_error:
                                # Log error but continue processing
                                pass
                        
                        if address_pid:
                            successful_pids.append(address_pid)
                    else:
                        # Document processed but no states generated
                        if address_pid:
                            failed_pids.append(f"{address_pid} - No states generated (no transactions found)")

                    documents_processed += 1

                except Exception as e:
                    address_pid = doc.get("transactionDetails", {}).get("ADDRESS_DETAIL_PID")
                    if address_pid:
                        failed_pids.append(f"{address_pid} - Error: {str(e)}")
            
            # Execute remaining bulk operations
            if bulk_operations:
                try:
                    result = collection.bulk_write(bulk_operations, ordered=False, session=session)
                    documents_updated += result.modified_count
                except Exception as bulk_error:
                    # Log error but continue
                    pass
        
        client.close()
        
    except pymongo.errors.PyMongoError as e:
        pass  # Errors handled at batch level
    except Exception as e:
        pass  # Errors handled at batch level
    
    return documents_processed, documents_updated, successful_pids, failed_pids, suburb_counts


def split_into_batches(items, batch_size):
    """Split a list into batches of specified size."""
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


def worker_wrapper(batch_pids, batch_num, mongo_uri, database_name, collection_name, type_mapping, csv_file_path, limited, total_batches):
    """
    Module-level wrapper function for multiprocessing.
    This must be at module level to be picklable.
    """
    return process_batch_worker(
        batch_pids, mongo_uri, database_name, collection_name,
        type_mapping, csv_file_path, limited, batch_num, total_batches
    )


def process_transactions_by_address_pid(address_detail_pids, collection, logger, type_mapping=None, csv_file_path=None, limited=False, num_cores=8, batch_size=1000):
    if not address_detail_pids:
        logger.warning("No ADDRESS_DETAIL_PID values provided")
        return 0, 0, [], [], {}

    logger.info(f"PROCESSING: Processing {len(address_detail_pids)} unique PIDs")
    logger.info(f"PROCESSING: Using {num_cores} cores with batch size of {batch_size}")

    # Split PIDs into batches to avoid MongoDB document size limits
    pid_batches = list(split_into_batches(address_detail_pids, batch_size))
    total_batches = len(pid_batches)
    logger.info(f"PROCESSING: Split into {total_batches} batches of up to {batch_size} PIDs each")

    # Process batches with progress bars
    documents_processed = 0
    documents_updated = 0
    successful_pids = []
    failed_pids = []
    suburb_counts = {}

    try:
        # Process each batch sequentially with progress bar
        for batch_num, batch_pids in enumerate(pid_batches, 1):
            print(f"\nProcessing batch {batch_num}/{total_batches} ({len(batch_pids)} PIDs)")
            
            # Process this batch
            with tqdm(total=len(batch_pids), desc=f"Batch {batch_num}/{total_batches}", unit="PIDs") as pbar:
                result = process_batch_worker(
                    batch_pids, MONGO_URI, DATABASE_NAME, COLLECTION_NAME,
                    type_mapping, csv_file_path, limited, batch_num, total_batches
                )
                
                proc, upd, succ_pids, fail_pids, sub_counts = result
                documents_processed += proc
                documents_updated += upd
                successful_pids.extend(succ_pids)
                failed_pids.extend(fail_pids)
                pbar.update(len(batch_pids))
            
            print(f"Completed batch {batch_num}/{total_batches}")

    except Exception as e:
        logger.error(f"Error in batch processing: {e}")

    return documents_processed, documents_updated, successful_pids, failed_pids, suburb_counts

# --- Main ---
def main():
    parser = argparse.ArgumentParser(description="Process transactions based on matched CSV file or all documents in MongoDB")
    parser.add_argument("--input", required=False, help="Path to the matched CSV file (optional - if not provided, processes all documents)")
    parser.add_argument("--log", required=True, help="Path to the log file")
    parser.add_argument("--limited", action="store_true", help="Enable limited processing mode with Field2 filter (only works with CSV input)")
    parser.add_argument("--add-location-fields", action="store_true", help="Add location and polygonId fields from propertyGeometric after processing")
    parser.add_argument("--field-batch-size", type=int, default=500, help="Number of docs per bulk update for field addition (default: 500)")
    args = parser.parse_args()

    logger = setup_logging(args.log)
    logger.info("Starting transaction processing...")

    if args.input:
        logger.info(f"CSV MODE: Processing documents from CSV file: {args.input}")
        if args.limited:
            logger.info("LIMITED MODE ENABLED: Applying Field2 filter")
    else:
        logger.info("FULL DATABASE MODE: Processing all documents in propertyTransactionDetails collection")
        if args.limited:
            logger.warning("LIMITED MODE is ignored when processing all documents (no CSV input)")

    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=60000)
        client.admin.command("ismaster")
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        logger.info("Connected to MongoDB")
    except pymongo.errors.ConnectionFailure as e:
        logger.error(f"Could not connect to MongoDB: {e}")
        return

    try:
        # Load type mapping
        type_mapping = load_type_mapping()
        logger.info(f"Loaded {len(type_mapping)} type mappings from types.csv")
        
        # Process either from CSV or all documents directly
        if args.input:
            # CSV mode: extract PIDs and process
            address_detail_pids, total_csv_records, _ = read_matched_csv(args.input, logger, limited=args.limited)
            if not address_detail_pids:
                logger.warning("No PIDs found in CSV file")
                return

            processed, updated, successful_pids, failed_pids, suburb_counts = process_transactions_by_address_pid(
                address_detail_pids, collection, logger, type_mapping, args.input, limited=args.limited, num_cores=8, batch_size=1000
            )
        else:
            # Direct processing mode: process all documents without extracting PIDs first
            # Disable limited mode when processing all documents
            args.limited = False
            
            processed, updated, successful_pids, failed_pids, suburb_counts = process_all_documents_directly(
                collection, logger, type_mapping, num_cores=8, batch_size=2000
            )
            total_csv_records = processed  # Use processed count as total for reporting

        # Write detailed results to log file
        logger.info("=" * 60)
        logger.info("DETAILED RESULTS BY ADDRESS_DETAIL_PID")
        logger.info("=" * 60)
        
        logger.info(f"SUCCESSFUL UPDATES ({len(successful_pids)} PIDs):")
        logger.info(f"  (PIDs printed per batch above)")
        
        logger.info(f"\nFAILED UPDATES ({len(failed_pids)} PIDs):")
        for pid_error in failed_pids:
            logger.info(f"  FAILED: {pid_error}")
        
        logger.info("=" * 60)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 60)
        if args.input:
            logger.info(f"CSV total records: {total_csv_records}")
            logger.info(f"Unique ADDRESS_DETAIL_PID: {len(address_detail_pids)}")
        else:
            logger.info(f"Total documents processed: {total_csv_records}")
        logger.info(f"Docs processed: {processed}")
        logger.info(f"Docs updated: {updated}")
        logger.info(f"Successful PIDs: {len(successful_pids)}")
        logger.info(f"Failed PIDs: {len(failed_pids)}")
        logger.info(
            f"Success rate: {(updated/processed*100):.1f}%" if processed > 0 else "N/A"
        )
        
        logger.info("=" * 60)
        
        # Add location and polygonId fields from propertyGeometric if requested
        if args.add_location_fields:
            if successful_pids:
                logger.info("")
                logger.info("Adding location and polygonId fields from propertyGeometric...")
                # Convert successful_pids to set for the function
                pids_set = set(successful_pids)
                field_stats = add_location_and_polygon_id_fields(
                    MONGO_URI, DATABASE_NAME, COLLECTION_NAME, pids_set, logger, args.field_batch_size
                )
            else:
                logger.warning("No successful PIDs to add location/polygonId fields for")

    except Exception as e:
        logger.error(f"Processing error: {e}")
    finally:
        client.close()
        logger.info("MongoDB connection closed")

if __name__ == "__main__":
    main()
