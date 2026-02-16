import json
import re
import os
import csv
import sys
import gc
from typing import Any
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from bson import ObjectId
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import argparse

# --- Limited-mode configuration ---
EXCLUDED_FIELD2_IDS = {
    1465, 1047, 1316, 2217, 1048, 2124, 1799, 1275, 3053, 3248, 3241, 1146, 1030
}

# --- Logging Functions ---
def log_new_entry(log_file, data):
    """Appends a new entry to a specified log file."""
    try:
        with open(log_file, 'a', encoding='utf-8') as f:
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "added_data": data
            }
            f.write(json.dumps(log_entry, ensure_ascii=False, default=str) + '\n')
    except IOError as e:
        print(f"Warning: Could not write to log file {log_file}. Error: {e}")

def log_property_update(log_file, gnaf_id, update_time, db_label, status="success", reason=None, record_id=None, address=None):
    """Logs property updates (both successful and failed) to a unified log file."""
    try:
        with open(log_file, 'a', encoding='utf-8') as f:
            log_entry = {
                "gnaf_id": gnaf_id,
                "database": db_label,
                "update_timestamp": update_time.isoformat(),
                "status": status,
                "record_id": record_id,
                "address": address,
                "reason": reason
            }
            f.write(json.dumps(log_entry, ensure_ascii=False, default=str) + '\n')
    except IOError as e:
        print(f"Warning: Could not write to property update log file {log_file}. Error: {e}")


def log_failure_reason(log_file, record_id, reason, address, gnaf_pid=None):
    """Logs the reason for a failed upsert to a dedicated log file."""
    try:
        with open(log_file, 'a', encoding='utf-8') as f:
            log_entry = {
                "id": record_id,
                "gnaf_pid": gnaf_pid,
                "reason": reason,
                "address": address
            }
            f.write(json.dumps(log_entry, ensure_ascii=False, default=str) + '\n')
    except IOError as e:
        print(f"Warning: Could not write to failure log file {log_file}. Error: {e}")


# --- Helper Functions ---
def load_csv(filepath):
    """Loads a CSV file into a list of dictionaries."""
    if not os.path.exists(filepath):
        print(f"Error: Input file not found at {filepath}")
        return []
    with open(filepath, mode='r', encoding='utf-8') as infile:
        return list(csv.DictReader(infile))


def create_agent_lookup(agents):
    return {agent['homieeId']: agent for agent in agents if 'homieeId' in agent}

def create_company_lookup(companies):
    return {str(company['homieeCompanyId']): company for company in companies if 'homieeCompanyId' in company}

def find_company_by_name(companies_data, company_name):
    """
    Find company by name using case-insensitive matching.
    
    Args:
        companies_data: List of all companies from companiesV3_test
        company_name: Company name to search for
        
    Returns:
        Company object if found, None otherwise
    """
    if not company_name or not isinstance(company_name, str):
        return None
    
    company_name_normalized = company_name.strip().lower()
    if not company_name_normalized:
        return None
    
    for company in companies_data:
        if not isinstance(company, dict):
            continue
        company_name_in_db = company.get('companyName')
        if company_name_in_db and isinstance(company_name_in_db, str):
            if company_name_in_db.strip().lower() == company_name_normalized:
                return company
    
    return None

def create_brand_relationship_lookup(brand_relationships):
    """
    Creates a lookup structure for brand relationships.
    Returns a dictionary mapping brandID to a list of relationships.
    """
    lookup = {}
    for row in brand_relationships:
        brand_id = str(row.get('brandID', '')).strip()
        if brand_id:
            if brand_id not in lookup:
                lookup[brand_id] = []
            lookup[brand_id].append(row)
    return lookup

def find_company_by_backup_process(matched_agent, brand_id, companies_data, company_lookup, employment_history_collection):
    """
    Backup process to find company when primary matching fails.
    
    Args:
        matched_agent: The matched agent object from agentsV3_test
        brand_id: The brandId/source from Field2 (can be string or int)
        companies_data: List of all companies from companiesV3_test
        company_lookup: Lookup dict keyed by homieeCompanyId
        employment_history_collection: MongoDB collection for Agents_employment_history
        
    Returns:
        Company object if found, None otherwise
    """
    if not matched_agent:
        return None
    
    # Normalize brand_id for comparison (handle string/int)
    brand_id_normalized = None
    if brand_id:
        try:
            brand_id_normalized = int(float(str(brand_id).strip()))
        except (ValueError, TypeError):
            brand_id_str = str(brand_id).strip()
            if brand_id_str:
                brand_id_normalized = brand_id_str
    
    if not brand_id_normalized:
        return None
    
    # Step 1: Check if primaryCompany.brandId matches Field2
    primary_company = matched_agent.get('primaryCompany', {})
    agent_brand_id = primary_company.get('brandId')
    
    # Normalize agent brandId for comparison
    agent_brand_id_normalized = None
    if agent_brand_id is not None:
        try:
            agent_brand_id_normalized = int(float(str(agent_brand_id).strip()))
        except (ValueError, TypeError):
            agent_brand_id_str = str(agent_brand_id).strip()
            if agent_brand_id_str:
                agent_brand_id_normalized = agent_brand_id_str
    
    # Compare brandIds (handle both string and int)
    brand_ids_match = False
    if agent_brand_id_normalized is not None and brand_id_normalized is not None:
        # Try int comparison first
        try:
            brand_ids_match = (int(agent_brand_id_normalized) == int(brand_id_normalized))
        except (ValueError, TypeError):
            # Fall back to string comparison
            brand_ids_match = (str(agent_brand_id_normalized).strip() == str(brand_id_normalized).strip())
    
    if brand_ids_match:
        # Use primaryCompany.companyId to find company
        company_id = primary_company.get('companyId')
        if company_id:
            # companyId should match homieeCompanyId in the lookup
            company = company_lookup.get(str(company_id).strip())
            if company:
                return company
    
    # Step 2: Use masterAgentId to find employment history
    master_agent_id = matched_agent.get('masterAgentId')
    if not master_agent_id or employment_history_collection is None:
        return None
    
    try:
        from bson import ObjectId
        # Find employment history document
        employment_doc = employment_history_collection.find_one({"master_agent_id": ObjectId(master_agent_id)})
        
        if not employment_doc:
            return None
        
        # Get history array
        history = employment_doc.get('history', [])
        if not isinstance(history, list):
            return None
        
        # Find the last entry in history where brandID matches
        matching_entry = None
        for entry in reversed(history):  # Start from the end (most recent)
            if not isinstance(entry, dict):
                continue
            
            entry_brand_id = entry.get('brandID')
            if entry_brand_id is None:
                continue
            
            # Normalize entry brandID for comparison
            entry_brand_id_normalized = None
            try:
                entry_brand_id_normalized = int(float(str(entry_brand_id).strip()))
            except (ValueError, TypeError):
                entry_brand_id_str = str(entry_brand_id).strip()
                if entry_brand_id_str:
                    entry_brand_id_normalized = entry_brand_id_str
            
            # Compare brandIds
            if entry_brand_id_normalized is not None and brand_id_normalized is not None:
                try:
                    if int(entry_brand_id_normalized) == int(brand_id_normalized):
                        matching_entry = entry
                        break
                except (ValueError, TypeError):
                    if str(entry_brand_id_normalized).strip() == str(brand_id_normalized).strip():
                        matching_entry = entry
                        break
        
        if matching_entry:
            homiee_company_id = matching_entry.get('homieeCompanyId')
            if homiee_company_id:
                # Match to companiesV3 using homieeCompanyId
                company = company_lookup.get(str(homiee_company_id).strip())
                if company:
                    return company
    except Exception as e:
        # Silently fail on errors (don't break the main process)
        pass
    
    return None

def get_company_logo(brand_relationship_lookup, field4, matched_company):
    """
    Gets the company logo from brand_relationship.csv based on Field4 (brandID) and company name.
    Also checks the company document's logo field as a fallback.
    
    Args:
        brand_relationship_lookup: Lookup structure from create_brand_relationship_lookup
        field4: The brandID value from the input record (Field4)
        matched_company: The matched company object from companiesV3_test
        
    Returns:
        str: The logo URL or None if not found
    """
    # First, check if company document has a logo field
    if matched_company and isinstance(matched_company, dict):
        company_logo = matched_company.get('logo')
        if company_logo and str(company_logo).strip():
            return str(company_logo).strip()
    
    # Fall back to brand_relationship lookup
    if not brand_relationship_lookup or not field4:
        return None
    
    brand_id = str(field4).strip()
    if brand_id not in brand_relationship_lookup:
        return None
    
    relationships = brand_relationship_lookup[brand_id]
    
    # Get company name from matched_company if available
    company_name = None
    if matched_company and isinstance(matched_company, dict):
        company_name = matched_company.get('companyName', '')
    
    # Try to find exact match first (brandID + companyName)
    if company_name:
        company_name_lower = company_name.lower().strip()
        for rel in relationships:
            rel_company_name = str(rel.get('companyName', '')).strip().lower()
            if rel_company_name == company_name_lower:
                logo = rel.get('logo', '').strip()
                if logo:
                    return logo
    
    # If no exact match, return any logo associated with this brandID
    for rel in relationships:
        logo = rel.get('logo', '').strip()
        if logo:
            return logo
    
    return None

# Agent creation helper functions removed - only use existing agents

def find_agent_by_name_and_company(agents, first_name, last_name, company_name):
    if not all(isinstance(arg, str) and arg.strip() for arg in [first_name, last_name, company_name]): return None
    fn_lower, ln_lower, cn_lower = first_name.lower(), last_name.lower(), company_name.lower()
    for agent in agents:
        if ((agent.get('basicInfo', {}).get('firstName') or '').lower() == fn_lower and
            (agent.get('basicInfo', {}).get('lastName') or '').lower() == ln_lower and
            (agent.get('primaryCompany', {}).get('companyName') or '').lower() == cn_lower):
            return agent
    return None

def find_agent_by_name_and_email(agents, first_name, last_name, email):
    if not all(isinstance(arg, str) and arg.strip() for arg in [first_name, last_name, email]): return None
    email = email[7:].strip() if email.lower().startswith('mailto:') else email.strip()
    fn_lower, ln_lower, email_lower = first_name.lower(), last_name.lower(), email.lower()
    for agent in agents:
        if ((agent.get('basicInfo', {}).get('firstName') or '').lower() == fn_lower and
            (agent.get('basicInfo', {}).get('lastName') or '').lower() == ln_lower and
            (agent.get('primaryEmail') or '').lower() == email_lower):
            return agent
    return None

def extract_listing_image_urls(field13_value):
    """
    Extract listing image URLs from Field13, handling both comma-separated and back-to-back URLs.
    
    Args:
        field13_value: The Field13 value (string) containing image URLs
        
    Returns:
        List of image objects with structure {"type": "property", "alt": "building", "src": url}
    """
    if not field13_value or not isinstance(field13_value, str):
        return []
    
    field13_str = field13_value.strip()
    if not field13_str:
        return []
    
    # Use regex to find all URLs containing "wasabi" regardless of separators
    # Pattern matches http/https URLs that contain "wasabi" in the domain or path
    url_pattern = r'https?://[^\s,]+wasabi[^\s,]*'
    urls = re.findall(url_pattern, field13_str, re.IGNORECASE)
    
    # Also try comma-separated splitting as fallback (in case regex misses some)
    if not urls:
        # Split by comma and check each part
        parts = field13_str.split(',')
        for part in parts:
            part = part.strip()
            if part and "wasabi" in part.lower():
                # Try to extract URL from this part
                url_match = re.search(r'https?://[^\s]+', part)
                if url_match:
                    url = url_match.group(0)
                    if url not in urls:
                        urls.append(url)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_urls = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)
    
    # Create image objects
    listing_images_list = [
        {"type": "property", "alt": "building", "src": url.strip()}
        for url in unique_urls
        if url.strip()
    ]
    
    return listing_images_list

def normalize_mobile(mobile: Any) -> str:
    """
    Normalize mobile phone number to Australian format.
    Based on the same logic as csv_agents.py normalize_mobile function.
    
    Args:
        mobile: The mobile phone number to normalize
        
    Returns:
        str: Normalized mobile number or None if invalid
    """
    if not isinstance(mobile, str):
        return None
    
    # First, remove ONLY spaces and accept already-normalized form +61XXXXXXXXX
    no_spaces = mobile.replace(" ", "")
    if re.match(r"^\+61\d{9}$", no_spaces):
        return no_spaces
    
    # Otherwise, strip all non-digits and normalize
    digits = re.sub(r"\D", "", mobile)
    if len(digits) < 9:
        return None
    if len(digits) == 9:
        # Assume already without leading 0 and without country code
        return "+61" + digits
    if len(digits) == 10 and digits.startswith("0"):
        # Only add +61 if it starts with 04 (mobile), otherwise return as-is
        if digits.startswith("04"):
            return "+61" + digits[1:]
        else:
            return digits
    # If already has country code 61 (e.g. 614xxxxxxxx) or other 10+ formats
    if digits.startswith("61") and len(digits) == 11:
        return "+" + digits
    # Fallback: try to coerce Australian format if possible
    if len(digits) >= 11 and digits.startswith("0"):
        return "+61" + digits[1:10]
    return None

def to_title_case(text):
    """
    Converts text to Title Case (capitalizes first letter of each word).
    
    Args:
        text: The text to convert
        
    Returns:
        str: Title-cased text
    """
    if not text or not isinstance(text, str):
        return ""
    # Split by spaces, capitalize each word, then join
    words = text.split()
    return " ".join(word.capitalize() if word else "" for word in words)

def slugify(text):
    """
    Converts text to a URL-friendly slug.
    - Trim whitespace
    - Convert to lowercase
    - Replace spaces with hyphens
    - Remove or normalize punctuation
    - Collapse duplicate hyphens
    - Keep digits
    
    Args:
        text: The text to slugify
        
    Returns:
        str: Slugified text
    """
    if not text or not isinstance(text, str):
        return ""
    
    # Trim whitespace
    text = text.strip()
    if not text:
        return ""
    
    # Convert to lowercase
    text = text.lower()
    
    # Replace spaces with hyphens
    text = re.sub(r'\s+', '-', text)
    
    # Remove or normalize punctuation (keep alphanumeric, hyphens, and digits)
    text = re.sub(r'[^a-z0-9\-]', '', text)
    
    # Collapse duplicate hyphens
    text = re.sub(r'-+', '-', text)
    
    # Remove leading/trailing hyphens
    text = text.strip('-')
    
    return text

def clean_and_split_name(full_name_field):
    """
    Cleans and splits a full name field into first, middle, and last names.
    Removes non-letter characters and handles middle names.
    
    Args:
        full_name_field: The raw name field value
        
    Returns:
        tuple: (first_name, middle_name, last_name) - any can be None if not found
    """
    if not full_name_field or not isinstance(full_name_field, str):
        return None, None, None
    
    # Clean the name - remove non-letter characters except spaces
    import re
    cleaned_name = re.sub(r'[^a-zA-Z\s]', '', full_name_field).strip()
    
    if not cleaned_name:
        return None, None, None
    
    # Split by spaces and filter out empty strings
    name_parts = [part.strip() for part in cleaned_name.split() if part.strip()]
    
    if len(name_parts) == 0:
        return None, None, None
    elif len(name_parts) == 1:
        return name_parts[0], None, None
    elif len(name_parts) == 2:
        return name_parts[0], None, name_parts[1]
    else:  # 3 or more parts
        return name_parts[0], name_parts[1], name_parts[-1]

# New: generate homieeId from initials (first and last token) + last 6 digits of mobile
def generate_homiee_id(full_name_field, mobile):
    if not isinstance(full_name_field, str) or not full_name_field.strip() or not mobile:
        return None
    # Clean name, preserve tokens split by whitespace/hyphen
    name_clean = re.sub(r'[^a-zA-Z\-\s]', '', full_name_field).strip()
    if not name_clean:
        return None
    tokens = [t for t in re.split(r'[\s]+', name_clean) if t]
    if not tokens:
        return None
    first_token = tokens[0]
    last_token = tokens[-1]
    first_initial = first_token[0].upper() if first_token else ''
    last_initial = last_token[0].upper() if last_token else ''
    digits = re.sub(r'\D', '', str(mobile))
    if len(digits) < 6:
        return None
    last6 = digits[-6:]
    return f"{first_initial}{last_initial}{last6}"

# New: find by fullName + primaryEmail (agentsV3 fields)
def find_agent_by_fullname_and_email(agents, full_name, email):
    if not isinstance(full_name, str) or not full_name.strip() or not isinstance(email, str) or not email.strip():
        return None
    email_norm = email[7:].strip().lower() if email.lower().startswith('mailto:') else email.strip().lower()
    full_name_norm = re.sub(r'\s+', ' ', full_name.strip()).lower()
    for agent in agents:
        agent_full = re.sub(r'\s+', ' ', (agent.get('basicInfo', {}).get('fullName') or '').strip()).lower()
        agent_email = (agent.get('primaryEmail') or '').strip().lower()
        if agent_full == full_name_norm and agent_email == email_norm:
            return agent
    return None

# New: find by unique fullName only (if exactly one match)
def find_agent_by_fullname_unique(agents, full_name):
    if not isinstance(full_name, str) or not full_name.strip():
        return None
    full_name_norm = re.sub(r'\s+', ' ', full_name.strip()).lower()
    matches = []
    for agent in agents:
        agent_full = re.sub(r'\s+', ' ', (agent.get('basicInfo', {}).get('fullName') or '').strip()).lower()
        if agent_full == full_name_norm:
            matches.append(agent)
    if len(matches) == 1:
        return matches[0]
    return None

def find_secondary_agent_by_name_and_company(agents, full_name_field, email, company_name):
    """
    Finds a secondary agent by name and company, handling middle names.
    First tries first+last name, then middle+last name if no match.
    
    Args:
        agents: List of agent records
        full_name_field: Full name from Field23
        email: Email from Field25
        company_name: Company name from Field4
        
    Returns:
        Agent record if found, None otherwise
    """
    if not all(isinstance(arg, str) and arg.strip() for arg in [full_name_field, email, company_name]):
        return None
    
    first_name, middle_name, last_name = clean_and_split_name(full_name_field)
    
    if not first_name or not last_name:
        return None
    
    cn_lower = company_name.lower()
    
    # Try first + last name
    fn_lower, ln_lower = first_name.lower(), last_name.lower()
    for agent in agents:
        if ((agent.get('basicInfo', {}).get('firstName') or '').lower() == fn_lower and
            (agent.get('basicInfo', {}).get('lastName') or '').lower() == ln_lower and
            (agent.get('primaryCompany', {}).get('companyName') or '').lower() == cn_lower):
            return agent
    
    # If no match and we have a middle name, try middle + last name
    if middle_name:
        mn_lower = middle_name.lower()
        for agent in agents:
            if ((agent.get('basicInfo', {}).get('firstName') or '').lower() == mn_lower and
                (agent.get('basicInfo', {}).get('lastName') or '').lower() == ln_lower and
                (agent.get('primaryCompany', {}).get('companyName') or '').lower() == cn_lower):
                return agent
    
    return None

def find_secondary_agent_by_name_and_email(agents, full_name_field, email):
    """
    Finds a secondary agent by name and email, handling middle names.
    First tries first+last name, then middle+last name if no match.
    
    Args:
        agents: List of agent records
        full_name_field: Full name from Field23
        email: Email from Field25
        
    Returns:
        Agent record if found, None otherwise
    """
    if not all(isinstance(arg, str) and arg.strip() for arg in [full_name_field, email]):
        return None
    
    first_name, middle_name, last_name = clean_and_split_name(full_name_field)
    
    if not first_name or not last_name:
        return None
    
    email = email[7:].strip() if email.lower().startswith('mailto:') else email.strip()
    email_lower = email.lower()
    
    # Try first + last name
    fn_lower, ln_lower = first_name.lower(), last_name.lower()
    for agent in agents:
        if ((agent.get('basicInfo', {}).get('firstName') or '').lower() == fn_lower and
            (agent.get('basicInfo', {}).get('lastName') or '').lower() == ln_lower and
            (agent.get('primaryEmail') or '').lower() == email_lower):
            return agent
    
    # If no match and we have a middle name, try middle + last name
    if middle_name:
        mn_lower = middle_name.lower()
        for agent in agents:
            if ((agent.get('basicInfo', {}).get('firstName') or '').lower() == mn_lower and
                (agent.get('basicInfo', {}).get('lastName') or '').lower() == ln_lower and
                (agent.get('primaryEmail') or '').lower() == email_lower):
                return agent
    
    return None

# New: secondary agent matching by name and mobile (handles normalization and hyphenated names)
def find_secondary_agent_by_name_and_mobile(agents, full_name_field, mobile):
    """
    Finds a secondary agent by name and mobile.
    Handles names with hyphens/spaces and normalizes Australian mobiles.
    """
    if not isinstance(full_name_field, str) or not full_name_field.strip() or not mobile:
        return None
    first_name, middle_name, last_name = clean_and_split_name(full_name_field)
    if not first_name or not last_name:
        return None
    normalized_mobile = normalize_mobile(mobile)
    if not normalized_mobile:
        return None
    # Try first + last name, then middle + last name
    fn_lower, ln_lower = first_name.lower(), last_name.lower()
    for agent in agents:
        if ((agent.get('basicInfo', {}).get('firstName') or '').lower() == fn_lower and
            (agent.get('basicInfo', {}).get('lastName') or '').lower() == ln_lower and
            normalize_mobile(agent.get('mobileNo')) == normalized_mobile):
            return agent
    if middle_name:
        mn_lower = middle_name.lower()
        for agent in agents:
            if ((agent.get('basicInfo', {}).get('firstName') or '').lower() == mn_lower and
                (agent.get('basicInfo', {}).get('lastName') or '').lower() == ln_lower and
                normalize_mobile(agent.get('mobileNo')) == normalized_mobile):
                return agent
    return None

# New: attempt to resolve a secondary agent via homieeId provided in CSV (if present)
def find_agent_by_homiee_id(agents, homiee_id_candidate):
    if not isinstance(homiee_id_candidate, str) or not homiee_id_candidate.strip():
        return None
    candidate = homiee_id_candidate.strip()
    for agent in agents:
        if str(agent.get('homieeId', '')).strip().lower() == candidate.lower():
            return agent
    return None

# --- Core Data Processing Functions ---
def create_property_document_for_custom_pid(record, prop_collection_primary):
    """
    Creates a new propertyTransactionDetails document for records with confidence_score == 0
    and existing ADDRESS_DETAIL_PID.
    
    Eligibility Rules:
    1. confidence_score == 0
    2. ADDRESS_DETAIL_PID is present and non-empty
    3. No existing document already exists with that ADDRESS_DETAIL_PID
    
    Args:
        record: Input record dictionary
        prop_collection_primary: MongoDB collection for propertyTransactionDetails
        
    Returns:
        tuple: (success: bool, property_doc: dict or None, error_message: str or None)
        - success: True if document was created or already exists, False if error
        - property_doc: The created or existing document
        - error_message: Error message if creation failed
    """
    # Check eligibility
    confidence_score = record.get("confidence_score")
    gnaf_pid = record.get("ADDRESS_DETAIL_PID")
    
    # Rule 1: confidence_score must be 0
    try:
        confidence_int = int(float(str(confidence_score).strip())) if confidence_score is not None else None
    except (ValueError, TypeError):
        confidence_int = None
    
    if confidence_int != 0:
        return False, None, f"confidence_score is not 0 (got: {confidence_score})"
    
    # Rule 2: ADDRESS_DETAIL_PID must be present and non-empty
    if not gnaf_pid or str(gnaf_pid).strip() == "":
        return False, None, "ADDRESS_DETAIL_PID is missing or empty"
    
    gnaf_pid = str(gnaf_pid).strip()
    
    # Use atomic upsert to prevent race conditions
    # This ensures only one document is created even if multiple threads try simultaneously
    try:
        # Generate ObjectId for the document
        property_id = ObjectId()
        
        # Get field values
        field5 = str(record.get("Field5", "")).strip()
        suburb = str(record.get("suburb", "")).strip()
        region_value = record.get("state")
        
        # Normalize fields
        title = to_title_case(field5) if field5 else ""
        locality = suburb.upper() if suburb else ""
        slug = slugify(field5) if field5 else ""
        
        # Process region - only add if not empty or null
        region = None
        if region_value is not None and str(region_value).strip() != "":
            region = str(region_value).strip()
        
        # Get current datetime (MongoDB will convert to ISODate)
        now = datetime.now()
        
        # Build currentState dictionary
        current_state = {
            "externalAreas": [],
            "internalAreas": [],
            "locality": locality,
            "title": title,
            "slug": slug
        }
        
        # Add region conditionally
        if region:
            current_state["region"] = region
        
        # Create document structure for $setOnInsert (only set on insert, not on update)
        new_document = {
            "_id": property_id,
            "propertyId": property_id,  # Same as _id
            "transactionDetails": {
                "soldHistory": [],
                "saleHistory": [],
                "rentHistory": [],
                "leasedHistory": [],
                "dateCreated": now,  # ISODate (MongoDB converts datetime to ISODate)
                "lastModified": now,  # ISODate (MongoDB converts datetime to ISODate)
                "ADDRESS_DETAIL_PID": gnaf_pid,
                "currentState": current_state,
                "custom_ADDRESS_DETAIL_PID": "true"
            }
        }
        
        # Use atomic upsert: only insert if document doesn't exist
        # $setOnInsert ensures fields are only set on insert, not on update
        filter_query = {"transactionDetails.ADDRESS_DETAIL_PID": gnaf_pid}
        update_operation = {"$setOnInsert": new_document}
        
        result = prop_collection_primary.update_one(filter_query, update_operation, upsert=True)
        
        # Fetch the document (either newly created or existing)
        existing_doc = prop_collection_primary.find_one({"transactionDetails.ADDRESS_DETAIL_PID": gnaf_pid})
        
        if existing_doc:
            return True, existing_doc, None
        else:
            # This should never happen, but handle it gracefully
            return False, None, "Document was not created or found after upsert"
        
    except Exception as e:
        return False, None, f"Failed to create document: {str(e)}"

def parse_date_flexible(date_input):
    if isinstance(date_input, dict) and "$date" in date_input:
        date_input = date_input["$date"]
    if isinstance(date_input, datetime):
        return date_input
    if isinstance(date_input, str):
        # Clean whitespace
        date_str = date_input.strip()
        if not date_str:
            return None
        
        # Remove ISO-8601 time components indicated by T/Z before further parsing
        # This handles formats like "2024-08-13T00:00:00" or "2024-08-13T00:00:00.000Z"
        for sep in ("T", "t", "Z", "z"):
            if sep in date_str:
                date_str = date_str.split(sep)[0]
                break
        
        # Split by space to separate date from time (if present)
        # But keep the full date part (could be space-separated like "12 Aug 25")
        date_parts = date_str.split()
        if not date_parts:
            return None
        
        # Try to identify if there's a time component (contains colon)
        # If so, only use the date part before the time
        date_only_str = date_str
        for i, part in enumerate(date_parts):
            if ':' in part:  # Found time component
                date_only_str = ' '.join(date_parts[:i])
                break
        
        # Clean the date string
        date_only_str = date_only_str.strip()
        if not date_only_str:
            return None
        
        # Try standard formats first (prioritize y-m-d format which is most common)
        # y-m-d formats: %Y-%m-%d (e.g., "2024-08-13")
        # d/m/y formats: %d/%m/%Y (e.g., "13/08/2024")
        # m/d/y formats: %m/%d/%Y (e.g., "08/13/2024")
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%b-%y", "%d-%b-%Y", "%m/%d/%y"):
             try: 
                 parsed_date = datetime.strptime(date_only_str, fmt)
                 # Handle 2-digit years properly - ensure they're in 2000s
                 if fmt in ("%d-%b-%y", "%m/%d/%y"):
                     # For 2-digit years, treat 00-99 as 2000-2099
                     if parsed_date.year < 2000:
                         parsed_date = parsed_date.replace(year=parsed_date.year + 100)
                 return parsed_date
             except (ValueError, TypeError): pass
        
        # Try space-separated formats (e.g., "12 Aug 25", "12 Aug 2025", "12 August 25")
        # First try with abbreviated month names (%b)
        for fmt in ("%d %b %y", "%d %b %Y", "%d %b, %Y"):
            try:
                parsed_date = datetime.strptime(date_only_str, fmt)
                # Handle 2-digit years properly - ensure they're in 2000s
                if fmt == "%d %b %y":
                    if parsed_date.year < 2000:
                        parsed_date = parsed_date.replace(year=parsed_date.year + 100)
                return parsed_date
            except (ValueError, TypeError): pass
        
        # Try with full month names (%B)
        for fmt in ("%d %B %y", "%d %B %Y", "%d %B, %Y"):
            try:
                parsed_date = datetime.strptime(date_only_str, fmt)
                # Handle 2-digit years properly - ensure they're in 2000s
                if fmt == "%d %B %y":
                    if parsed_date.year < 2000:
                        parsed_date = parsed_date.replace(year=parsed_date.year + 100)
                return parsed_date
            except (ValueError, TypeError): pass
        
        # Try case-insensitive parsing for hyphen-separated formats like '27-Aug-25' or '27-aug-25'
        try:
            # Normalize the date string: capitalize first letter of month
            parts = date_only_str.split('-')
            if len(parts) == 3:
                day, month, year = parts
                # Capitalize month properly (e.g., 'aug' -> 'Aug', 'AUG' -> 'Aug')
                month_capitalized = month.capitalize()
                normalized_date = f"{day}-{month_capitalized}-{year}"
                parsed_date = datetime.strptime(normalized_date, "%d-%b-%y")
                # Handle 2-digit years - ensure they're in 2000s
                if parsed_date.year < 2000:
                    parsed_date = parsed_date.replace(year=parsed_date.year + 100)
                return parsed_date
        except (ValueError, TypeError, IndexError):
            pass
        
        # Try case-insensitive parsing for space-separated formats like '12 Aug 25' or '12 aug 25'
        try:
            parts = date_only_str.split()
            if len(parts) == 3:
                day, month, year = parts
                # Capitalize month properly (e.g., 'aug' -> 'Aug', 'AUG' -> 'Aug')
                month_capitalized = month.capitalize()
                normalized_date = f"{day} {month_capitalized} {year}"
                # Try abbreviated month first
                try:
                    parsed_date = datetime.strptime(normalized_date, "%d %b %y")
                    if parsed_date.year < 2000:
                        parsed_date = parsed_date.replace(year=parsed_date.year + 100)
                    return parsed_date
                except (ValueError, TypeError):
                    # Try full month name
                    parsed_date = datetime.strptime(normalized_date, "%d %B %y")
                    if parsed_date.year < 2000:
                        parsed_date = parsed_date.replace(year=parsed_date.year + 100)
                    return parsed_date
        except (ValueError, TypeError, IndexError):
            pass
    return None

def calculate_status_code(sold_history):
    valid_history_items = []
    for item in sold_history:
        # Skip non-dictionary items (like soldStatus integer)
        if not isinstance(item, dict):
            continue
        date_obj = parse_date_flexible(item.get("contractDate"))
        if not date_obj:
            date_obj = parse_date_flexible(item.get("estimatedSoldDate"))
        if date_obj:
            valid_history_items.append({"item": item, "date_obj": date_obj})
    if not valid_history_items: return 0
    
    sorted_history = sorted(valid_history_items, key=lambda x: x["date_obj"], reverse=True)
    most_recent_date = sorted_history[0]["date_obj"]
    diff = datetime.now().year - most_recent_date.year
    
    if diff <= 1: return 2
    if diff <= 5: return 3
    if diff <= 10: return 4
    if diff <= 20: return 5
    return 6



def process_record(record, prop_collection_primary, prop_collection_secondary, agents_data, companies_data, agent_lookup, company_lookup, processed_records, lock, batch_id, property_update_log_file, failures_log_file, sold_onhold_collection=None, batch_processed_transactions=None, brand_relationship_lookup=None, employment_history_collection=None):
    """
    Processes a single record from the input CSV.
    """
    gnaf_pid = record.get("ADDRESS_DETAIL_PID")
    record_id = record.get("id", "N/A")
    address = record.get("Field5", "N/A")

    if not gnaf_pid:
        reason = "Skipped: No ADDRESS_DETAIL_PID in record"
        log_failure_reason(failures_log_file, record_id, reason, address)
        log_property_update(property_update_log_file, None, datetime.now(), "primary", "failed", reason, record_id, address)
        with lock:
            record['failure_reason'] = reason
            record['action'] = f"SKIPPED - {reason}"
            record['primary_result'] = f"skipped: {reason}"
            processed_records.append(record)
        return

    gnaf_pid = str(gnaf_pid).strip()
    
    # Check if this is a confidence_score == 0 record that needs document creation
    confidence_score = record.get("confidence_score")
    try:
        confidence_int = int(float(str(confidence_score).strip())) if confidence_score is not None else None
    except (ValueError, TypeError):
        confidence_int = None
    
    # If confidence_score == 0 and ADDRESS_DETAIL_PID exists, try to create/get document
    if confidence_int == 0:
        success, matched_property_doc, error_msg = create_property_document_for_custom_pid(record, prop_collection_primary)
        if not success:
            reason = f"Failed to create/get document for confidence_score=0: {error_msg}"
            log_failure_reason(failures_log_file, record_id, reason, address, gnaf_pid)
            log_property_update(property_update_log_file, gnaf_pid, datetime.now(), "primary", "failed", reason, record_id, address)
            with lock:
                record['failure_reason'] = reason
                record['action'] = f"SKIPPED - {reason}"
                record['primary_result'] = f"skipped: {reason}"
                processed_records.append(record)
            return
        # Document created or found, continue with normal processing
    else:
        # Normal flow: find existing document
        matched_property_doc = prop_collection_primary.find_one({"transactionDetails.ADDRESS_DETAIL_PID": gnaf_pid})
        if not matched_property_doc:
            reason = "PropertyTransactionDetails document not found in Primary DB"
            log_failure_reason(failures_log_file, record_id, reason, address, gnaf_pid)
            log_property_update(property_update_log_file, gnaf_pid, datetime.now(), "primary", "failed", reason, record_id, address)
            with lock:
                record['failure_reason'] = reason
                record['action'] = f"SKIPPED - {reason}"
                record['primary_result'] = f"skipped: {reason}"
                processed_records.append(record)
            return

    property_v3_id = matched_property_doc.get('_id')
    original_sold_history = matched_property_doc.get("transactionDetails", {}).get("soldHistory", []) or []
    # Normalize to list of dicts (support legacy dict-of-objects)
    if isinstance(original_sold_history, dict):
        original_sold_history_list = [v for k, v in original_sold_history.items() if isinstance(v, dict)]
    elif isinstance(original_sold_history, list):
        original_sold_history_list = [v for v in original_sold_history if isinstance(v, dict)]
    else:
        original_sold_history_list = []
    
    # Get saleHistory for cross-checking with sold transactions
    original_sale_history = matched_property_doc.get("transactionDetails", {}).get("saleHistory", []) or []
    # Normalize to list of dicts (support legacy dict-of-objects)
    if isinstance(original_sale_history, dict):
        original_sale_history_list = [v for k, v in original_sale_history.items() if isinstance(v, dict)]
    elif isinstance(original_sale_history, list):
        original_sale_history_list = [v for v in original_sale_history if isinstance(v, dict)]
    else:
        original_sale_history_list = []
    
    # --- Scenario helper functions (DAT vs SOLD merge) ---
    def to_int_or_none(val):
        if val is None or str(val).strip().upper() == "N/A" or str(val).strip() == "": return None
        try: 
            result = int(float(val))
            return None if result == 0 else result
        except (ValueError, TypeError): return None

    def is_dat_like(item: dict) -> bool:
        if not isinstance(item, dict): return False
        # Treat as DAT-like if the fields exist, even when values are null
        if "area" in item and "areaType" in item:
            return True
        # Fallback: source string includes 'DAT'
        src = item.get("source")
        if isinstance(src, str) and "dat" in src.lower():
            return True
        return False

    def pick_most_recent(items):
        dated = []
        for it in items or []:
            if not isinstance(it, dict):
                continue
            d = parse_date_flexible(it.get("contractDate"))
            if not d:
                d = parse_date_flexible(it.get("estimatedSoldDate"))
            if d:
                dated.append((d, it))
        if not dated:
            return None
        dated.sort(key=lambda x: x[0], reverse=True)
        return dated[0][1]

    def scenario_1_matches(dat_item: dict, sold_item: dict) -> bool:
        dat_price = to_int_or_none(dat_item.get("price"))
        sold_price = to_int_or_none(sold_item.get("price"))
        dat_dt = parse_date_flexible(dat_item.get("contractDate"))
        sold_dt = parse_date_flexible(sold_item.get("contractDate"))
        return (dat_price is not None and sold_price is not None and dat_price == sold_price and dat_dt and sold_dt and dat_dt == sold_dt)

    def scenario_2_matches(dat_item: dict, sold_item: dict) -> bool:
        dat_price = to_int_or_none(dat_item.get("price"))
        sold_price = to_int_or_none(sold_item.get("price"))
        dat_dt = parse_date_flexible(dat_item.get("contractDate"))
        sold_dt = parse_date_flexible(sold_item.get("contractDate"))
        return (dat_price is not None and sold_price is not None and dat_price == sold_price and dat_dt is not None and sold_dt is None)

    def scenario_3_matches(dat_item: dict, sold_item: dict) -> bool:
        dat_price = to_int_or_none(dat_item.get("price"))
        sold_price = to_int_or_none(sold_item.get("price"))
        dat_dt = parse_date_flexible(dat_item.get("contractDate"))
        sold_dt = parse_date_flexible(sold_item.get("contractDate"))
        if dat_price is None: return False
        if sold_price is not None: return False
        if not dat_dt or not sold_dt: return False
        return dat_dt.year == sold_dt.year and dat_dt != sold_dt

    def build_merged_entry(dat_item: dict, sold_item: dict) -> dict:
        title = (sold_item.get("title") or dat_item.get("title") or "").strip()
        description = sold_item.get("description", "")
        bedrooms = to_int_or_none(sold_item.get("bedrooms"))
        bathrooms = to_int_or_none(sold_item.get("bathrooms"))
        car_parking = to_int_or_none(sold_item.get("carParking"))
        listing_images = sold_item.get("listing_images") or []
        agents = sold_item.get("agents") or {}
        dat_dt = parse_date_flexible(dat_item.get("contractDate"))
        dat_price = to_int_or_none(dat_item.get("price"))
        return {
            "createdDate": datetime.now().strftime("%Y-%m-%d"),
            "contractDate": dat_dt.strftime("%Y-%m-%d") if dat_dt else None,
            "estimatedSoldDate": sold_item.get("estimatedSoldDate") or "",
            "title": title,
            "description": description,
            "price": dat_price,
            "listing_images": listing_images,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "carParking": car_parking,
            "agents": agents,
            "agentsCompanyId": matched_company.get('_id') if matched_company else None,
            "status": "active",
            "source": sold_item.get("source"),
            "sourceDocumentId": sold_item.get("sourceDocumentId") or str(record.get('_id', ''))
        }
    
    # *** MODIFIED: New de-duplication logic with 365-day threshold ***
    def is_duplicate(price_to_check, date_to_check, history_list, batch_transactions=None):
        """
        Checks if a sold history entry with the same price exists within a 365-day window.
        Returns (True, matching_transaction) if a duplicate is found, (False, None) otherwise.
        Now also checks against batch transactions being processed.
        """
        # If both price and date are missing, allow the transaction to proceed
        if (price_to_check is None or str(price_to_check).strip() == "") and date_to_check is None:
            return (False, None)  # Allow transactions missing both price and date
        
        # If only price is missing but date exists, allow it (handled as valid missing-price scenario)
        if price_to_check is None or str(price_to_check).strip() == "":
            return (False, None)

        try:
            price_to_check_clean = int(float(price_to_check))
        except (ValueError, TypeError):
            return (True, None)  # If the new price is not a valid number, skip it

        # Check against existing database history
        for transaction in history_list or []:
            if not isinstance(transaction, dict):
                continue
            
            existing_price = transaction.get("price")
            if existing_price is None:
                continue
            
            try:
                if int(float(existing_price)) == price_to_check_clean:
                    # Prices match, now check the date threshold
                    existing_date = parse_date_flexible(transaction.get("contractDate"))
                    if not existing_date:
                        existing_date = parse_date_flexible(transaction.get("estimatedSoldDate"))
                    
                    if date_to_check and existing_date:
                        # If both dates are valid, check the difference
                        if abs((date_to_check - existing_date).days) < 365:
                            return (True, transaction)  # Found a duplicate within the 365-day window
                    else:
                        # If one of the dates is missing, but prices match, treat as duplicate to be safe
                        return (True, transaction)
            except (ValueError, TypeError):
                continue # Ignore malformed price data in existing history
        
        # Check against batch transactions being processed (if provided)
        if batch_transactions:
            for batch_txn in batch_transactions:
                if not isinstance(batch_txn, dict):
                    continue
                
                batch_price = batch_txn.get("price")
                if batch_price is None:
                    continue
                
                try:
                    if int(float(batch_price)) == price_to_check_clean:
                        # Prices match, now check the date threshold
                        batch_date = parse_date_flexible(batch_txn.get("contractDate"))
                        if not batch_date:
                            batch_date = parse_date_flexible(batch_txn.get("estimatedSoldDate"))
                        
                        if date_to_check and batch_date:
                            # If both dates are valid, check the difference
                            if abs((date_to_check - batch_date).days) < 365:
                                return (True, None)  # Found a duplicate in batch, but can't update it
                        else:
                            # If one of the dates is missing, but prices match, treat as duplicate to be safe
                            return (True, None)
                except (ValueError, TypeError):
                    continue # Ignore malformed price data in batch transactions
        
        return (False, None) # No duplicate found

    def is_date_duplicate(date_to_check, history_list, batch_transactions=None):
        """
        Returns True if there exists any transaction whose date is within 365 days of
        the provided date_to_check (including the same date). Checks both existing
        DB history and batch transactions.
        """
        if not date_to_check:
            return False
        # Check existing history
        for transaction in history_list or []:
            if not isinstance(transaction, dict):
                continue
            existing_date = parse_date_flexible(transaction.get("contractDate"))
            if not existing_date:
                existing_date = parse_date_flexible(transaction.get("estimatedSoldDate"))
            if not existing_date:
                continue
            try:
                if abs((date_to_check - existing_date).days) < 365:
                    return True
            except Exception:
                continue
        # Check batch transactions
        if batch_transactions:
            for batch_txn in batch_transactions:
                if not isinstance(batch_txn, dict):
                    continue
                batch_date = parse_date_flexible(batch_txn.get("contractDate"))
                if not batch_date:
                    batch_date = parse_date_flexible(batch_txn.get("estimatedSoldDate"))
                if not batch_date:
                    continue
                try:
                    if abs((date_to_check - batch_date).days) < 365:
                        return True
                except Exception:
                    continue
        return False

    # --- Post-merge exact duplicate cleanup (same date AND same price) ---
    def has_non_null_agents(agents_value):
        if not isinstance(agents_value, dict):
            return False
        for key in ["primaryUserId", "secondaryUserId", "name", "mobile", "email"]:
            value = agents_value.get(key)
            if value is not None and str(value).strip() != "":
                return True
        return False

    def normalize_price_to_int(price_value):
        if price_value is None or str(price_value).strip() == "":
            return None
        try:
            result = int(float(price_value))
            return None if result == 0 else result
        except (ValueError, TypeError):
            return None

    def get_quality_score(transaction_item):
        images = transaction_item.get("listing_images")
        images_present = isinstance(images, list) and len(images) > 0
        agents_present = has_non_null_agents(transaction_item.get("agents"))
        score = 0
        if images_present:
            score += 1
        if agents_present:
            score += 1
        return score

    def merge_same_price_with_missing_date(transactions_list):
        """
        Merges transactions with the same price where one has contractDate and the other doesn't.
        The transaction with contractDate becomes the base, and is enriched with data from the one without.
        """
        if not isinstance(transactions_list, list):
            return transactions_list
        
        # Early exit if list is small (no point merging if there's only one or two items)
        if len(transactions_list) < 2:
            return transactions_list
        
        # Group transactions by normalized price
        price_groups = {}
        for idx, txn in enumerate(transactions_list):
            if not isinstance(txn, dict):
                continue
            norm_price = normalize_price_to_int(txn.get("price"))
            if norm_price is None:
                continue
            if norm_price not in price_groups:
                price_groups[norm_price] = []
            price_groups[norm_price].append(idx)
        
        indices_to_remove = set()
        
        # Process each price group
        for price, indices in price_groups.items():
            if len(indices) < 2:
                continue
            
            # Separate transactions: those with contractDate and those without
            with_date = []
            without_date = []
            
            for idx in indices:
                txn = transactions_list[idx]
                contract_date = parse_date_flexible(txn.get("contractDate"))
                estimated_date = parse_date_flexible(txn.get("estimatedSoldDate"))
                
                if contract_date:
                    with_date.append(idx)
                elif estimated_date:
                    without_date.append(idx)
            
            # For each transaction with contractDate, try to merge with one without
            for base_idx in with_date:
                base_txn = transactions_list[base_idx]
                
                # Find a transaction without contractDate to merge with
                for merge_idx in without_date:
                    if merge_idx in indices_to_remove:
                        continue
                    
                    merge_txn = transactions_list[merge_idx]
                    
                    # Enrich base transaction with fields from merge transaction
                    # Only copy if base doesn't have the field or it's empty/null
                    fields_to_enrich = [
                        "description", "listing_images", "bedrooms", 
                        "bathrooms", "carParking", "agents", "agentsCompanyId"
                    ]
                    
                    enriched = False
                    for field in fields_to_enrich:
                        base_value = base_txn.get(field)
                        merge_value = merge_txn.get(field)
                        
                        # Skip if merge doesn't have a value to copy
                        if not merge_value:
                            continue
                        
                        # Check if base needs enrichment
                        if field == "listing_images":
                            # For listing_images, check if it's empty or None
                            if not base_value or (isinstance(base_value, list) and len(base_value) == 0):
                                if isinstance(merge_value, list) and len(merge_value) > 0:
                                    base_txn[field] = merge_value
                                    enriched = True
                        elif field == "agents":
                            # For agents, check if it's empty dict or missing meaningful data
                            if not base_value or not isinstance(base_value, dict):
                                # Base has no agents, copy entire agents object
                                base_txn[field] = merge_value.copy() if isinstance(merge_value, dict) else merge_value
                                enriched = True
                            elif isinstance(merge_value, dict):
                                # Base has agents but may be incomplete, merge in missing fields
                                agents_updated = False
                                for k, v in merge_value.items():
                                    if v and not base_txn[field].get(k):
                                        base_txn[field][k] = v
                                        agents_updated = True
                                if agents_updated:
                                    enriched = True
                        elif field == "agentsCompanyId":
                            # For agentsCompanyId, copy if base doesn't have it
                            if not base_value:
                                base_txn[field] = merge_value
                                enriched = True
                        elif field in ["bedrooms", "bathrooms", "carParking"]:
                            # For numeric fields, copy if base is None or 0
                            if base_value is None or base_value == 0:
                                if merge_value is not None and merge_value != 0:
                                    base_txn[field] = merge_value
                                    enriched = True
                        else:
                            # For other fields (like description), copy if base is None/empty
                            if not base_value or base_value == "":
                                base_txn[field] = merge_value
                                enriched = True
                    
                    # If we enriched, mark the merge transaction for removal
                    if enriched:
                        indices_to_remove.add(merge_idx)
                        break  # Only merge one transaction per base
        
        if not indices_to_remove:
            return transactions_list
        
        # Build a new list excluding merged transactions
        merged = []
        for idx, txn in enumerate(transactions_list):
            if idx in indices_to_remove:
                continue
            merged.append(txn)
        return merged

    def dedupe_same_price_and_date(transactions_list):
        if not isinstance(transactions_list, list):
            return transactions_list
        # Group by exact contractDate string and normalized price int
        groups = {}
        for idx, txn in enumerate(transactions_list):
            if not isinstance(txn, dict):
                continue
            contract_date_str = txn.get("contractDate")
            if not isinstance(contract_date_str, str) or contract_date_str.strip() == "":
                continue
            norm_price = normalize_price_to_int(txn.get("price"))
            if norm_price is None:
                continue
            key = (contract_date_str.strip(), norm_price)
            groups.setdefault(key, []).append(idx)

        # Decide which indices to remove per group of duplicates
        indices_to_remove = set()
        for key, indices in groups.items():
            if len(indices) <= 1:
                continue
            # Choose the best (highest quality score) to keep
            best_index = None
            best_score = -1
            for idx in indices:
                txn = transactions_list[idx]
                score = get_quality_score(txn)
                if score > best_score:
                    best_score = score
                    best_index = idx
            # If tie on score, best_index stays as the first with that score (stable keep)
            for idx in indices:
                if idx != best_index:
                    indices_to_remove.add(idx)

        if not indices_to_remove:
            return transactions_list

        # Build a new list preserving order while excluding duplicates chosen for removal
        cleaned = []
        for idx, txn in enumerate(transactions_list):
            if idx in indices_to_remove:
                continue
            cleaned.append(txn)
        return cleaned

    # --- Agent Matching Logic ---
    agent_first_name = str(record.get('Field15') or '').strip()
    agent_last_name = str(record.get('Field16') or '').strip()
    agent_phone = str(record.get('Field20') or '').strip()
    agent_email = str(record.get('Field19') or '').strip()
    # Use Field17 as backup if Field15 and Field16 are not available
    if not agent_first_name and not agent_last_name:
        primary_full_name = str(record.get('Field17') or '').strip()
    else:
        primary_full_name = (f"{agent_first_name} {agent_last_name}" ).strip()
    company_name = str(record.get('Field4') or '').strip()
    brand_id = str(record.get('Field2') or '').strip()
    
    matched_agent = None
    matched_company = None
    
    # Primary agent matching priority:
    # 1) homieeId from initials + last 6 of Field20
    homiee_from_primary = generate_homiee_id(primary_full_name, agent_phone)
    if homiee_from_primary:
        matched_agent = find_agent_by_homiee_id(agents_data, homiee_from_primary)
    # 2) fullName + email
    if not matched_agent:
        matched_agent = find_agent_by_fullname_and_email(agents_data, primary_full_name, agent_email)
    # 3) unique fullName
    if not matched_agent:
        matched_agent = find_agent_by_fullname_unique(agents_data, primary_full_name)
    # 4) legacy fallbacks (name+company, name+email) for safety
    if not matched_agent:
        matched_agent = find_agent_by_name_and_company(agents_data, agent_first_name, agent_last_name, company_name)
    if not matched_agent and (agent_first_name or agent_last_name) and agent_email:
        matched_agent = find_agent_by_name_and_email(agents_data, agent_first_name, agent_last_name, agent_email)
    if matched_agent:
        company_str_id = matched_agent.get('primaryCompany', {}).get('companyId')
        if company_str_id:
            matched_company = company_lookup.get(company_str_id)
    
    # Try to find company by name (case-insensitive) if not found yet
    if not matched_company and company_name:
        matched_company = find_company_by_name(companies_data, company_name)
    
    # Backup process: Try to find company using brandId and employment history
    if not matched_company and matched_agent:
        matched_company = find_company_by_backup_process(
            matched_agent, brand_id, companies_data, company_lookup, 
            employment_history_collection
        )

    # Agent and company creation removed - only use existing agents and companies

    # --- Secondary Agent Matching Logic ---
    secondary_agent_full_name = str(record.get('Field23') or '').strip()
    secondary_agent_email = str(record.get('Field25') or '').strip()
    secondary_agent_mobile = str(record.get('Field26') or '').strip()
    # Compute secondary homieeId from name + Field26 (mobile) when possible
    secondary_agent_homiee_id = generate_homiee_id(secondary_agent_full_name, secondary_agent_mobile) or str(record.get('Field24') or record.get('secondaryHomieeId') or '').strip()
    
    matched_secondary_agent = None
    
    # 1) Try homieeId if provided in CSV
    if secondary_agent_homiee_id:
        matched_secondary_agent = find_agent_by_homiee_id(agents_data, secondary_agent_homiee_id)
    
    # 2) Try fullName + email
    if not matched_secondary_agent:
        matched_secondary_agent = find_agent_by_fullname_and_email(agents_data, secondary_agent_full_name, secondary_agent_email)
    # 3) Try by unique fullName
    if not matched_secondary_agent:
        matched_secondary_agent = find_agent_by_fullname_unique(agents_data, secondary_agent_full_name)
    # 4) Try legacy: name+company
    if not matched_secondary_agent:
        matched_secondary_agent = find_secondary_agent_by_name_and_company(agents_data, secondary_agent_full_name, secondary_agent_email, company_name)
    # 5) Try by name and mobile
    if not matched_secondary_agent:
        matched_secondary_agent = find_secondary_agent_by_name_and_mobile(agents_data, secondary_agent_full_name, secondary_agent_mobile)

    agent_homiee_id = matched_agent.get('homieeId') if matched_agent else None
    agents_company_id = matched_agent.get('primaryCompany', {}).get('companyId') if matched_agent else None
    agent_mobile_int = int(agent_phone) if agent_phone and agent_phone.isdigit() else None
    source_id = int(brand_id) if brand_id and brand_id.isdigit() else brand_id

    # --- Process transactions ---
    new_transactions_to_add = []
    all_known_transactions = list(original_sold_history_list)
    merged_transactions = []  # Track merged transactions for action display

    # Helper function to check if price is a word that should be treated as null
    def is_price_word(val):
        """Checks if a price value is a word like 'SOLD' that should be treated as null."""
        if val is None:
            return True
        val_str = str(val).strip().upper()
        if val_str == "" or val_str == "N/A":
            return True
        # Check if it's a common word that indicates no numeric price
        price_words = {"SOLD", "N/A", "NA", "NULL", "NONE", ""}
        if val_str in price_words:
            return True
        # Check if it contains only letters (no digits) - likely a word
        if val_str and not any(char.isdigit() for char in val_str):
            return True
        return False
    
    # Helper function for cleaning and converting values
    def clean_and_convert_to_int(val):
        if val is None or str(val).strip().upper() == "N/A" or str(val).strip() == "": return None
        # Check if it's a word like "SOLD" - treat as null
        if is_price_word(val):
            return None
        try: 
            result = int(float(val))
            return None if result == 0 else result
        except (ValueError, TypeError): return None
    
    # Prepare listing images list (new structure)
    # Use robust URL extraction that handles both comma-separated and back-to-back URLs
    listing_images_list = extract_listing_image_urls(record.get("Field13"))

    # 1. Process Primary Transaction
    dat_purchase_price_raw = record.get("price")
    # Normalize price: treat words like "SOLD" as None
    if is_price_word(dat_purchase_price_raw):
        dat_purchase_price = None
    else:
        dat_purchase_price = dat_purchase_price_raw
    
    # Contract date MUST come from Field11 only - never from Field3
    dat_contract_date = parse_date_flexible(record.get("Field11"))
    is_estimated = False
    
    # Estimated date MUST come from Field3 only - never used as contractDate
    estimated_date_str = record.get("Field3")
    estimated_date = parse_date_flexible(estimated_date_str) if estimated_date_str else None
    
    # If Field11 is missing, contractDate is None (do NOT use Field3 as contractDate)
    # Field3 is only for estimated dates, never for contractDate
    
    # Determine if this is a missing price case
    original_price_missing = dat_purchase_price is None
    original_date_missing = not parse_date_flexible(record.get("Field11")) and not record.get("Field3")
    
    # Try DAT+SOLD merge using scenarios (if a DAT entry exists in history)
    # Optimize: Only check for DAT candidates if history list is not too large
    merged_applied = False
    merge_attempted = False  # Track if we attempted merge (regardless of success)
    dat_candidate = None
    # Early exit optimization: if history is very large, skip DAT merge check to save time
    # This optimization helps when properties have many historical transactions
    if len(original_sold_history_list) < 500:  # Only check if history is reasonably sized
        dat_candidates = [h for h in original_sold_history_list if is_dat_like(h)]
        dat_candidate = pick_most_recent(dat_candidates)

    if dat_candidate:
        # Backfill primary agent contact if CSV missing
        primary_email_merge = agent_email or (matched_agent.get('primaryEmail') if matched_agent else None)
        primary_mobile_merge = normalize_mobile(agent_phone) if agent_phone else None
        # Backfill mobile from agent if still missing
        if not primary_mobile_merge and matched_agent and matched_agent.get('mobileNo'):
            primary_mobile_merge = normalize_mobile(matched_agent.get('mobileNo'))
        
        incoming_sold_like = {
            "price": clean_and_convert_to_int(record.get("price")),
            "contractDate": (parse_date_flexible(record.get("Field11")) or None),
            "estimatedSoldDate": (estimated_date.strftime("%Y-%m-%d") if estimated_date else None),
            "title": address,
            "description": record.get('Field12', ''),
            "bedrooms": clean_and_convert_to_int(record.get("Field6")),
            "bathrooms": clean_and_convert_to_int(record.get("Field7")),
            "carParking": clean_and_convert_to_int(record.get("Field8")),
            "listing_images": listing_images_list,
            "agents": {
                "primaryUserId": matched_agent.get('_id') if matched_agent else None,
                "secondaryUserId": matched_secondary_agent.get('_id') if matched_secondary_agent else None,
                "fullName": primary_full_name or None,
                "companyName": company_name or None,
                "photoUrl": (matched_agent.get('photoUrl') if matched_agent and matched_agent.get('photoUrl') else (str(record.get('Field22') or '').strip() or None)),
                "companyLogo": get_company_logo(brand_relationship_lookup, brand_id, matched_company),
                "mobile": primary_mobile_merge,
                "email": primary_email_merge
            }
        }
        merge_attempted = True  # We attempted merge (regardless of whether scenarios matched)
        if scenario_1_matches(dat_candidate, incoming_sold_like) or \
           scenario_2_matches(dat_candidate, incoming_sold_like) or \
           scenario_3_matches(dat_candidate, incoming_sold_like):
            merged_entry = build_merged_entry(dat_candidate, incoming_sold_like)
            try:
                merged_entry["soldStatus"] = calculate_status_code([merged_entry])
            except Exception:
                merged_entry["soldStatus"] = 0
            new_transactions_to_add.append(merged_entry)
            all_known_transactions.append(merged_entry)
            merged_transactions.append(merged_entry)  # Track for action display
            if batch_processed_transactions is not None:
                with lock:
                    batch_processed_transactions.append(merged_entry)
            merged_applied = True
    
    # Scenario 1: Missing price but has date -> date-only dedupe, then add into soldHistory
    if original_price_missing and not original_date_missing:
        # If soldHistory is empty, only check batch duplicates (not database history)
        # If soldHistory has entries, check both database and batch duplicates
        if len(original_sold_history_list) == 0:
            # Only check batch duplicates when database history is empty
            # Filter batch transactions by ADDRESS_DETAIL_PID first
            filtered_batch = [txn for txn in (batch_processed_transactions or []) if isinstance(txn, dict) and txn.get("_gnaf_pid") == gnaf_pid]
            if filtered_batch and is_date_duplicate(dat_contract_date, [], filtered_batch):
                reason = "Skipped date-only duplicate: Transaction within 365 days of existing entry in batch."
                log_failure_reason(failures_log_file, record_id, reason, address, gnaf_pid)
                log_property_update(property_update_log_file, gnaf_pid, datetime.now(), "primary", "failed", reason, record_id, address)
                with lock:
                    record['failure_reason'] = reason
                    record['action'] = f"SKIPPED - {reason}"
                    record['primary_result'] = f"skipped: {reason}"
                    processed_records.append(record)
                return
            # No duplicates found, proceed with adding the transaction
        else:
            # Filter batch transactions by ADDRESS_DETAIL_PID first
            filtered_batch = [txn for txn in (batch_processed_transactions or []) if isinstance(txn, dict) and txn.get("_gnaf_pid") == gnaf_pid]
            if is_date_duplicate(dat_contract_date, original_sold_history_list, filtered_batch):
                reason = "Skipped date-only duplicate: Transaction within 365 days of existing entry."
                log_failure_reason(failures_log_file, record_id, reason, address, gnaf_pid)
                log_property_update(property_update_log_file, gnaf_pid, datetime.now(), "primary", "failed", reason, record_id, address)
                with lock:
                    record['failure_reason'] = reason
                    record['action'] = f"SKIPPED - {reason}"
                    record['primary_result'] = f"skipped: {reason}"
                    processed_records.append(record)
                return
    elif original_price_missing and original_date_missing:
        # Both price and date missing -> create a normal soldHistory entry (no sold_onhold)
        _estimated_dt = estimated_date
        # Don't default to today's date - if no date in scraped data, leave as None
        _estimated_str = _estimated_dt.strftime("%Y-%m-%d") if _estimated_dt else None
        
        # Backfill primary agent contact if CSV missing
        primary_email_out = agent_email or (matched_agent.get('primaryEmail') if matched_agent else None)
        primary_mobile_out = normalize_mobile(agent_phone) if agent_phone else None
        # Backfill mobile from agent if still missing
        if not primary_mobile_out and matched_agent and matched_agent.get('mobileNo'):
            primary_mobile_out = normalize_mobile(matched_agent.get('mobileNo'))
        
        primary_sold_entry_missing = {
            "createdDate": datetime.now().strftime("%Y-%m-%d"),
            "contractDate": None,
            "estimatedSoldDate": _estimated_str,
            "title": record.get('FULL_ADDRESS'),
            "description": record.get('Field12', ''),
            "price": None,
            "listing_images": listing_images_list,
            "bedrooms": clean_and_convert_to_int(record.get("Field6")),
            "bathrooms": clean_and_convert_to_int(record.get("Field7")),
            "carParking": clean_and_convert_to_int(record.get("Field8")),
            "agents": {
                "primaryUserId": matched_agent.get('_id') if matched_agent else None,
                "secondaryUserId": matched_secondary_agent.get('_id') if matched_secondary_agent else None,
                "fullName": primary_full_name or None,
                "companyName": company_name or None,
                "photoUrl": (matched_agent.get('photoUrl') if matched_agent and matched_agent.get('photoUrl') else (str(record.get('Field22') or '').strip() or None)),
                "companyLogo": get_company_logo(brand_relationship_lookup, brand_id, matched_company),
                "mobile": primary_mobile_out,
                "email": primary_email_out
            },
            "agentsCompanyId": matched_company.get('_id') if matched_company else None,
            "sourceDocId": record.get('_id', ''),
            "source": record.get('Field2', ''),
            "status": "active"
        }
        try:
            primary_sold_entry_missing["soldStatus"] = calculate_status_code([primary_sold_entry_missing])
        except Exception:
            primary_sold_entry_missing["soldStatus"] = 0
        new_transactions_to_add.append(primary_sold_entry_missing)
        all_known_transactions.append(primary_sold_entry_missing)
        if batch_processed_transactions is not None:
            with lock:
                # Create a copy for batch tracking with _gnaf_pid (don't add to actual transaction)
                batch_tracking_entry = primary_sold_entry_missing.copy()
                batch_tracking_entry["_gnaf_pid"] = gnaf_pid
                batch_processed_transactions.append(batch_tracking_entry)
    
    # *** MODIFIED: Check for duplicates atomically to ensure only first occurrence in batch is inserted ***
    # First check database history (no lock needed)
    is_dup, matching_transaction = is_duplicate(dat_purchase_price, dat_contract_date, original_sold_history_list, None)
    
    # Then check batch transactions atomically and reserve slot if no duplicate
    batch_duplicate_found = False
    placeholder_index = -1
    # Initialize price_clean outside the conditional blocks so it's always available
    price_clean = None
    if dat_purchase_price:
        try:
            price_clean = int(float(dat_purchase_price))
            if price_clean == 0:
                price_clean = None
        except (ValueError, TypeError):
            pass
    
    if not merged_applied and not is_dup and batch_processed_transactions is not None:
        with lock:
            # Check batch transactions while holding lock to prevent race conditions
            # This ensures only the first occurrence in the batch gets inserted
            if batch_processed_transactions:
                # Check for duplicates in batch
                # Must match ADDRESS_DETAIL_PID, price, and date to be considered a duplicate
                for batch_txn in batch_processed_transactions:
                    if not isinstance(batch_txn, dict):
                        continue
                    # Skip placeholders when checking
                    if batch_txn.get("_reserved"):
                        continue
                    # First check ADDRESS_DETAIL_PID - must match for it to be a duplicate
                    batch_gnaf_pid = batch_txn.get("_gnaf_pid")
                    # Skip transactions without _gnaf_pid (old format) or different property
                    if not batch_gnaf_pid or str(batch_gnaf_pid).strip() != str(gnaf_pid).strip():
                        continue  # Different property or missing _gnaf_pid, not a duplicate
                    batch_price = batch_txn.get("price")
                    if batch_price is None:
                        continue
                    try:
                        if price_clean and int(float(batch_price)) == price_clean:
                            batch_date = parse_date_flexible(batch_txn.get("contractDate"))
                            if not batch_date:
                                batch_date = parse_date_flexible(batch_txn.get("estimatedSoldDate"))
                            if dat_contract_date and batch_date:
                                if abs((dat_contract_date - batch_date).days) < 365:
                                    batch_duplicate_found = True
                                    break
                            elif price_clean:
                                # Prices match but dates missing, treat as duplicate
                                batch_duplicate_found = True
                                break
                    except (ValueError, TypeError):
                        continue
            
            # If no batch duplicate found, immediately add a placeholder to reserve the slot
            # This ensures only the first occurrence gets inserted
            if not batch_duplicate_found:
                # Create a minimal placeholder to reserve the slot
                placeholder = {
                    "_gnaf_pid": gnaf_pid,  # Store ADDRESS_DETAIL_PID for comparison
                    "price": price_clean if price_clean else None,
                    "contractDate": dat_contract_date.strftime("%Y-%m-%d") if dat_contract_date else None,
                    "estimatedSoldDate": estimated_date.strftime("%Y-%m-%d") if estimated_date else None,
                    "_reserved": True,  # Mark as placeholder
                    "_record_id": record_id  # Store record ID for matching
                }
                batch_processed_transactions.append(placeholder)
                placeholder_index = len(batch_processed_transactions) - 1
    
    if batch_duplicate_found:
        is_dup = True
        matching_transaction = None
    
    if not merged_applied and not is_dup:
        # Handle estimatedSoldDate properly - it should come from Field3 and be formatted as yyyy-mm-dd
        # Field3 is ONLY for estimated dates, never for contractDate
        estimated_sold_date = None
        if estimated_date:
            # Use Field3 for estimatedSoldDate (never use it as contractDate)
            estimated_sold_date = estimated_date.strftime("%Y-%m-%d")
        
        # Backfill primary agent contact if CSV missing
        primary_email_out2 = agent_email or (matched_agent.get('primaryEmail') if matched_agent else None)
        primary_mobile_out2 = normalize_mobile(agent_phone) if agent_phone else None
        # Backfill mobile from agent if still missing
        if not primary_mobile_out2 and matched_agent and matched_agent.get('mobileNo'):
            primary_mobile_out2 = normalize_mobile(matched_agent.get('mobileNo'))
        
        primary_sold_entry = {
            "createdDate": datetime.now().strftime("%Y-%m-%d"),
            "contractDate": dat_contract_date.strftime("%Y-%m-%d") if dat_contract_date and not is_estimated else None,
            "estimatedSoldDate": estimated_sold_date,
            "title": address,
            "description": record.get('Field12', ''),
            "price": None if original_price_missing else clean_and_convert_to_int(record.get("price")),
            "listing_images": listing_images_list,
            "bedrooms": clean_and_convert_to_int(record.get("Field6")),
            "bathrooms": clean_and_convert_to_int(record.get("Field7")),
            "carParking": clean_and_convert_to_int(record.get("Field8")),
            "agents": {
                "primaryUserId": matched_agent.get('_id') if matched_agent else None,
                "secondaryUserId": matched_secondary_agent.get('_id') if matched_secondary_agent else None,
                "fullName": primary_full_name or None,
                "companyName": company_name or None,
                "photoUrl": (matched_agent.get('photoUrl') if matched_agent and matched_agent.get('photoUrl') else (str(record.get('Field22') or '').strip() or None)),
                "companyLogo": get_company_logo(brand_relationship_lookup, brand_id, matched_company),
                "mobile": primary_mobile_out2,
                "email": primary_email_out2
            },
            "agentsCompanyId": matched_company.get('_id') if matched_company else None,
            "status": "active",
            "sourceDocId": record.get('_id', ''),
            "source": record.get('Field2', '')
        }
        # Embed soldStatus only inside the primary sold entry
        try:
            primary_sold_entry["soldStatus"] = calculate_status_code([primary_sold_entry])
        except Exception:
            primary_sold_entry["soldStatus"] = 0
        
        new_transactions_to_add.append(primary_sold_entry)
        all_known_transactions.append(primary_sold_entry)
        
        # Replace placeholder with actual transaction in batch tracking
        if batch_processed_transactions is not None and placeholder_index >= 0:
            with lock:
                # Replace the placeholder with a copy that includes _gnaf_pid for batch tracking
                # Don't add _gnaf_pid to the actual transaction that goes to the database
                batch_tracking_entry = primary_sold_entry.copy()
                batch_tracking_entry["_gnaf_pid"] = gnaf_pid
                if placeholder_index < len(batch_processed_transactions):
                    batch_processed_transactions[placeholder_index] = batch_tracking_entry
                else:
                    # Placeholder not found (shouldn't happen), just append
                    batch_processed_transactions.append(batch_tracking_entry)
    elif not merged_applied:
        # Duplicate found - check if we need to update agent/company info
        transaction_updated = False
        if matching_transaction:
            # Check if existing transaction needs agent/company info
            existing_agents = matching_transaction.get("agents", {})
            needs_update = False
            
            # Check if agent info is missing
            if not existing_agents.get("primaryUserId") and matched_agent:
                needs_update = True
            if not existing_agents.get("secondaryUserId") and matched_secondary_agent:
                needs_update = True
            if not existing_agents.get("fullName") and primary_full_name:
                needs_update = True
            if not existing_agents.get("companyName") and company_name:
                needs_update = True
            if not existing_agents.get("mobile") and agent_phone:
                needs_update = True
            if not existing_agents.get("email") and agent_email:
                needs_update = True
            if not matching_transaction.get("agentsCompanyId") and matched_company:
                needs_update = True
            
            # Update the existing transaction if needed
            if needs_update:
                # Backfill primary agent contact if CSV missing
                primary_email_merge = agent_email or (matched_agent.get('primaryEmail') if matched_agent else None)
                primary_mobile_merge = normalize_mobile(agent_phone) if agent_phone else None
                # Backfill mobile from agent if still missing
                if not primary_mobile_merge and matched_agent and matched_agent.get('mobileNo'):
                    primary_mobile_merge = normalize_mobile(matched_agent.get('mobileNo'))
                
                # Update agent info (only fill in missing fields)
                if not existing_agents.get("primaryUserId") and matched_agent:
                    existing_agents["primaryUserId"] = matched_agent.get('_id')
                if not existing_agents.get("secondaryUserId") and matched_secondary_agent:
                    existing_agents["secondaryUserId"] = matched_secondary_agent.get('_id')
                if not existing_agents.get("fullName") and primary_full_name:
                    existing_agents["fullName"] = primary_full_name
                if not existing_agents.get("companyName") and company_name:
                    existing_agents["companyName"] = company_name
                if not existing_agents.get("photoUrl") and matched_agent and matched_agent.get('photoUrl'):
                    existing_agents["photoUrl"] = matched_agent.get('photoUrl')
                elif not existing_agents.get("photoUrl") and record.get('Field22'):
                    existing_agents["photoUrl"] = str(record.get('Field22') or '').strip() or None
                if not existing_agents.get("companyLogo") and brand_id:
                    logo = get_company_logo(brand_relationship_lookup, brand_id, matched_company)
                    if logo:
                        existing_agents["companyLogo"] = logo
                if not existing_agents.get("mobile") and primary_mobile_merge:
                    existing_agents["mobile"] = primary_mobile_merge
                if not existing_agents.get("email") and primary_email_merge:
                    existing_agents["email"] = primary_email_merge
                
                matching_transaction["agents"] = existing_agents
                
                # Update company ID if missing
                if not matching_transaction.get("agentsCompanyId") and matched_company:
                    matching_transaction["agentsCompanyId"] = matched_company.get('_id')
                
                transaction_updated = True
        
        # Check if we attempted merge but it was a duplicate
        if merge_attempted:
            # We tried to merge but scenarios didn't match, and it's a duplicate
            price_display = dat_purchase_price_raw if dat_purchase_price_raw else "missing"
            if transaction_updated:
                reason = f"Duplicate transaction (merge attempted): Updated existing transaction with agent/company info. A sale with price '{price_display}' was found within the 365-day threshold."
            else:
                reason = f"Skipped duplicate transaction (merge attempted): A sale with price '{price_display}' was found within the 365-day threshold but merge scenarios did not match."
            log_failure_reason(failures_log_file, record_id, reason, address, gnaf_pid)
            log_property_update(property_update_log_file, gnaf_pid, datetime.now(), "primary", "failed", reason, record_id, address)
            with lock:
                record['failure_reason'] = reason
                record['action'] = "updated (duplicate)" if transaction_updated else "skipped (merged)"
                record['primary_result'] = f"updated: {reason}" if transaction_updated else f"skipped: {reason}"
                processed_records.append(record)
        else:
            # Regular duplicate (no merge attempt)
            price_display = dat_purchase_price_raw if dat_purchase_price_raw else "missing"
            if transaction_updated:
                reason = f"Duplicate transaction: Updated existing transaction with agent/company info. A sale with price '{price_display}' was found within the 365-day threshold."
            else:
                reason = f"Skipped duplicate transaction: A sale with price '{price_display}' was found within the 365-day threshold."
            log_failure_reason(failures_log_file, record_id, reason, address, gnaf_pid)
            log_property_update(property_update_log_file, gnaf_pid, datetime.now(), "primary", "failed", reason, record_id, address)
            with lock:
                record['failure_reason'] = reason
                record['action'] = "updated (duplicate)" if transaction_updated else f"SKIPPED - {reason}"
                record['primary_result'] = f"updated: {reason}" if transaction_updated else f"skipped: {reason}"
                processed_records.append(record)
        
        # If transaction was updated, save it to the database
        if transaction_updated:
            # Check and update saleHistory when duplicate sold transaction is updated
            # Get years from all active sold transactions
            active_sold_years = set()
            for transaction in original_sold_history_list:
                if not isinstance(transaction, dict):
                    continue
                if transaction.get("status") == "active":
                    date_obj = parse_date_flexible(transaction.get("contractDate"))
                    if not date_obj:
                        date_obj = parse_date_flexible(transaction.get("estimatedSoldDate"))
                    if date_obj:
                        active_sold_years.add(date_obj.year)
            
            # Get the most recent year from all sold transactions
            max_sold_year = None
            for transaction in original_sold_history_list:
                if not isinstance(transaction, dict):
                    continue
                date_obj = parse_date_flexible(transaction.get("contractDate"))
                if not date_obj:
                    date_obj = parse_date_flexible(transaction.get("estimatedSoldDate"))
                if date_obj:
                    if max_sold_year is None or date_obj.year > max_sold_year:
                        max_sold_year = date_obj.year
            
            # Update saleHistory: deactivate active sale transactions that match the year or are older
            updated_sale_history_list = list(original_sale_history_list)
            sale_history_updated = False
            for sale_transaction in updated_sale_history_list:
                if not isinstance(sale_transaction, dict):
                    continue
                if sale_transaction.get("status") != "active":
                    continue
                
                sale_date_obj = parse_date_flexible(sale_transaction.get("contractDate"))
                if not sale_date_obj:
                    sale_date_obj = parse_date_flexible(sale_transaction.get("estimatedSaleDate"))
                
                if sale_date_obj:
                    sale_year = sale_date_obj.year
                    if (sale_year in active_sold_years) or (max_sold_year is not None and sale_year < max_sold_year):
                        sale_transaction["status"] = "inactive"
                        sale_history_updated = True
            
            # Update the history list in the database
            update_payload = {'$set': {
                'transactionDetails.soldHistory': original_sold_history_list,
                'transactionDetails.lastModified': datetime.now().strftime("%Y-%m-%d")
            }}
            
            # Include saleHistory in update if it was modified
            if sale_history_updated:
                update_payload['$set']['transactionDetails.saleHistory'] = updated_sale_history_list
            
            try:
                result = prop_collection_primary.update_one({'_id': property_v3_id}, update_payload)
                if result.matched_count > 0:
                    log_property_update(property_update_log_file, gnaf_pid, datetime.now(), "primary")
                    if prop_collection_secondary is not None:
                        try:
                            sec_result = prop_collection_secondary.update_one({'transactionDetails.ADDRESS_DETAIL_PID': gnaf_pid}, update_payload)
                            if sec_result.matched_count > 0:
                                log_property_update(property_update_log_file, gnaf_pid, datetime.now(), "secondary")
                        except OperationFailure:
                            pass
            except OperationFailure as e:
                print(f"Warning: Failed to update duplicate transaction with agent info: {e}")
        
        return


    # 2. Update DB if new transactions exist
    if not new_transactions_to_add: 
        # No new transactions to add, mark as skipped
        with lock:
            record['action'] = "SKIPPED - No new transactions to add"
            record['primary_result'] = "skipped: No new transactions to add"
            processed_records.append(record)
        return
    
    # Start with original list and append new ones (initially marked as inactive)
    updated_sold_history_list = list(original_sold_history_list)
    for txn in new_transactions_to_add:
        txn["status"] = "inactive"
        updated_sold_history_list.append(txn)

    # Merge transactions with same price where one has contractDate and the other doesn't
    # This enriches transactions with contractDate using data from those without
    updated_sold_history_list = merge_same_price_with_missing_date(updated_sold_history_list)

    # Remove exact duplicates introduced by merging: same contractDate and same price.
    # Preference: keep entries that have listing_images and/or meaningful agents data.
    updated_sold_history_list = dedupe_same_price_and_date(updated_sold_history_list)

    # Final deduplication: remove duplicates based on (contractDate, price, primaryUserId)
    def remove_duplicates_final(history_list):
        """
        Removes duplicates based on multiple rules:
        1. Standard: (contractDate, price, primaryUserId) combination
        2. Rule 1: If contractDate and estimatedSoldDate are both null/empty, price is null/empty, 
           and primaryUserId matches → keep only one
        3. Rule 2: If price is null/empty, contractDate or estimatedSoldDate are in the same year, 
           and primaryUserId matches → keep only one
        """
        def is_empty_value(val):
            """Check if value is None, empty string, or empty after stripping."""
            return val is None or (isinstance(val, str) and val.strip() == "")
        
        def get_year_from_date(date_val):
            """Extract year from date string or return None."""
            if is_empty_value(date_val):
                return None
            date_obj = parse_date_flexible(date_val)
            return date_obj.year if date_obj else None
        
        def get_primary_user_id(item):
            """Extract primaryUserId from item (check both top level and agents object)."""
            primary_user_id = item.get("primaryUserId")
            if primary_user_id is None:
                agents = item.get("agents", {})
                if isinstance(agents, dict):
                    primary_user_id = agents.get("primaryUserId")
            return primary_user_id
        
        seen_standard = set()
        seen_rule1 = set()  # For null dates + null price + same primaryUserId
        seen_rule2 = set()  # For null price + same year + same primaryUserId
        unique = []
        
        for item in history_list:
            if not isinstance(item, dict):
                unique.append(item)
                continue
            
            primary_user_id = get_primary_user_id(item)
            contract_date = item.get("contractDate")
            estimated_date = item.get("estimatedSoldDate")
            price = item.get("price")
            
            # Standard deduplication: (contractDate, price, primaryUserId)
            key_standard = (contract_date, price, primary_user_id)
            if key_standard in seen_standard:
                continue
            seen_standard.add(key_standard)
            
            # Rule 1: Both dates null/empty + price null/empty + same primaryUserId
            contract_empty = is_empty_value(contract_date)
            estimated_empty = is_empty_value(estimated_date)
            price_empty = is_empty_value(price)
            
            if contract_empty and estimated_empty and price_empty:
                key_rule1 = (None, None, primary_user_id)  # (contractDate, estimatedDate, primaryUserId)
                if key_rule1 in seen_rule1:
                    continue
                seen_rule1.add(key_rule1)
            
            # Rule 2: Price null/empty + same year + same primaryUserId
            if price_empty:
                contract_year = get_year_from_date(contract_date)
                estimated_year = get_year_from_date(estimated_date)
                # Use contract year if available, otherwise estimated year
                year = contract_year if contract_year is not None else estimated_year
                if year is not None:
                    key_rule2 = (year, primary_user_id)  # (year, primaryUserId)
                    if key_rule2 in seen_rule2:
                        continue
                    seen_rule2.add(key_rule2)
            
            unique.append(item)
        return unique
    
    updated_sold_history_list = remove_duplicates_final(updated_sold_history_list)

    # Calculate status code from all transactions (including new ones) for downstream consumers (no longer stored in soldHistory)
    new_status_code = calculate_status_code(updated_sold_history_list)
    
    # Find the most recent transaction date to determine which should be active
    all_transactions = []
    for transaction in updated_sold_history_list:
        if not isinstance(transaction, dict):
            continue
        date_obj = parse_date_flexible(transaction.get("contractDate"))
        if not date_obj:
            date_obj = parse_date_flexible(transaction.get("estimatedSoldDate"))
        if date_obj:
            all_transactions.append({"transaction": transaction, "date_obj": date_obj})
    
    if all_transactions:
        # Sort by date, most recent first
        sorted_transactions = sorted(all_transactions, key=lambda x: x["date_obj"], reverse=True)
        most_recent_date = sorted_transactions[0]["date_obj"]
        
        # Mark all transactions as inactive first
        for transaction in updated_sold_history_list:
            if isinstance(transaction, dict):
                transaction["status"] = "inactive"
        
        # Mark transactions with the most recent date as active
        for item in sorted_transactions:
            if item["date_obj"] == most_recent_date:
                item["transaction"]["status"] = "active"

    # --- Deactivate matching saleHistory transactions ---
    # Get years from all active sold transactions (including newly added ones)
    active_sold_years = set()
    for transaction in updated_sold_history_list:
        if not isinstance(transaction, dict):
            continue
        # Only check active sold transactions
        if transaction.get("status") == "active":
            date_obj = parse_date_flexible(transaction.get("contractDate"))
            if not date_obj:
                date_obj = parse_date_flexible(transaction.get("estimatedSoldDate"))
            if date_obj:
                active_sold_years.add(date_obj.year)
    
    # Also check newly added transactions (they might not be marked active yet if they're the most recent)
    # Get the most recent year from all sold transactions
    max_sold_year = None
    for transaction in updated_sold_history_list:
        if not isinstance(transaction, dict):
            continue
        date_obj = parse_date_flexible(transaction.get("contractDate"))
        if not date_obj:
            date_obj = parse_date_flexible(transaction.get("estimatedSoldDate"))
        if date_obj:
            if max_sold_year is None or date_obj.year > max_sold_year:
                max_sold_year = date_obj.year
    
    # Update saleHistory: deactivate active sale transactions that match the year or are older
    updated_sale_history_list = list(original_sale_history_list)
    sale_history_updated = False
    for sale_transaction in updated_sale_history_list:
        if not isinstance(sale_transaction, dict):
            continue
        # Only process active sale transactions
        if sale_transaction.get("status") != "active":
            continue
        
        # Get the year from the sale transaction
        sale_date_obj = parse_date_flexible(sale_transaction.get("contractDate"))
        if not sale_date_obj:
            sale_date_obj = parse_date_flexible(sale_transaction.get("estimatedSaleDate"))
        
        if sale_date_obj:
            sale_year = sale_date_obj.year
            # Deactivate if:
            # 1. The sale year matches any active sold transaction year, OR
            # 2. The sale year is older than the most recent sold transaction year
            if (sale_year in active_sold_years) or (max_sold_year is not None and sale_year < max_sold_year):
                sale_transaction["status"] = "inactive"
                sale_history_updated = True

    update_payload = {'$set': {
        'transactionDetails.soldHistory': updated_sold_history_list,
        'transactionDetails.lastModified': datetime.now().strftime("%Y-%m-%d")
    }}
    
    # Include saleHistory in update if it was modified
    if sale_history_updated:
        update_payload['$set']['transactionDetails.saleHistory'] = updated_sale_history_list
    

    
    try:
        result = prop_collection_primary.update_one({'_id': property_v3_id}, update_payload)
        if result.matched_count > 0:
            log_property_update(property_update_log_file, gnaf_pid, datetime.now(), "primary")
            primary_result = f"inserted: Successfully added {len(new_transactions_to_add)} transaction(s)"
            
            # Update secondary DB if available (but don't track result)
            if prop_collection_secondary is not None:
                try:
                    sec_result = prop_collection_secondary.update_one({'transactionDetails.ADDRESS_DETAIL_PID': gnaf_pid}, update_payload)
                    if sec_result.matched_count > 0:
                        log_property_update(property_update_log_file, gnaf_pid, datetime.now(), "secondary")
                    else:
                        print(f"Warning: ADDRESS_DETAIL_PID {gnaf_pid} updated in primary but not found in secondary.")
                except OperationFailure as e_sec:
                    print(f"Warning: Failed to update ADDRESS_DETAIL_PID {gnaf_pid} in secondary DB: {e_sec}")
            
            # Determine overall action based on primary result
            if "inserted" in primary_result:
                overall_action = "updated"
            else:
                overall_action = "skipped"
            
            # Mark as processed
            with lock:
                # Include merged info in action value
                if merged_transactions:
                    record['action'] = f"{overall_action} (merged)"
                else:
                    record['action'] = overall_action
                record['primary_result'] = primary_result
                processed_records.append(record)
        else:
            raise OperationFailure("Matched count was 0 on update in Primary DB")
            
    except OperationFailure as e:
        reason = f"DB update failed in primary: {e}"
        log_failure_reason(failures_log_file, record_id, reason, address, gnaf_pid)
        log_property_update(property_update_log_file, gnaf_pid, datetime.now(), "primary", "failed", reason, record_id, address)
        with lock:
            record['failure_reason'] = reason
            record['action'] = f"SKIPPED - {reason}"
            record['primary_result'] = f"skipped: {reason}"
            processed_records.append(record)


def main():
    """
    Main execution function. Connects to DB, loads data from CSV, and processes records concurrently.
    """
    parser = argparse.ArgumentParser(description="Upsert sold property history from a CSV file to two databases.")
    parser.add_argument('--input', required=True, help="Path to the input CSV file.")
    parser.add_argument('--timestamp', required=True, help="Timestamp for log file naming, e.g., YYYYMMDD_HHMM")
    parser.add_argument('--failure-log', required=True, help="Path for the failure log file.")
    parser.add_argument('--limited', action='store_true', help="Enable limited mode: exclude specific Field2 values.")
    args = parser.parse_args()

    # --- Logging File Configuration ---
    # Use the same directory as the input file for logs
    input_dir = os.path.dirname(args.input)
    LOG_DIRECTORY = os.path.join(input_dir, "run_logs", "sold")
    os.makedirs(LOG_DIRECTORY, exist_ok=True)
    ts = args.timestamp
    failures_csv_file = os.path.join(LOG_DIRECTORY, f'failed_sold_upsert_{ts}.csv')
    failures_log_file = args.failure_log
    # Agent and company creation logging removed
    property_update_log_file = os.path.join(LOG_DIRECTORY, f'property_updates_{ts}.log')


    client_primary = None
    try:
        # --- Database Connection ---
        MONGO_URI_PRIMARY = os.environ.get("MONGO_URI")
        DATABASE_NAME = os.environ.get("MONGO_DB_NAME", "DB")
        PROPERTY_TRANSACTION_COLLECTION = os.environ.get("MONGO_PROPERTY_COLLECTION")
        AGENTS_COLLECTION_NAME = os.environ.get("MONGO_AGENTS_COLLECTION")
        COMPANIES_COLLECTION_NAME = os.environ.get("MONGO_COMPANIES_COLLECTION")
        CORE_COUNT = 8
        BATCH_SIZE = 1000

        print("Connecting to MongoDB instances...")
        client_primary = MongoClient(MONGO_URI_PRIMARY, serverSelectionTimeoutMS=10000)
        client_primary.admin.command('ping')
        db_primary = client_primary[DATABASE_NAME]
        print("Primary MongoDB connection successful.")

        # Force using only DigitalOcean (Primary) for all operations
        print("INFO: Using only DigitalOcean (Primary) DB for reads and updates. No secondary DB will be used.")

        # --- Collection Assignments ---
        agents_collection_primary = db_primary[AGENTS_COLLECTION_NAME]
        companies_collection_primary = db_primary[COMPANIES_COLLECTION_NAME]
        prop_collection_primary = db_primary[PROPERTY_TRANSACTION_COLLECTION]
        employment_history_collection = db_primary.get_collection("Agents_employment_history")# changes here
        
        prop_collection_secondary = None
        sold_onhold_collection = None
            

        # --- Pre-load Data (from Primary) ---
        print("Loading all agents and companies from Primary DB...")
        agents_data = list(agents_collection_primary.find({}))
        companies_data = list(companies_collection_primary.find({}))
        agent_lookup = create_agent_lookup(agents_data)
        company_lookup = create_company_lookup(companies_data)
        print(f"Loaded {len(agents_data)} agents and {len(companies_data)} companies.")
        
        # --- Load Brand Relationship CSV ---
        print("Loading brand relationship data...")
        brand_relationship_path = os.path.join(os.path.dirname(__file__), "brand_relationship.csv")
        brand_relationship_data = load_csv(brand_relationship_path)
        brand_relationship_lookup = create_brand_relationship_lookup(brand_relationship_data)
        print(f"Loaded {len(brand_relationship_data)} brand relationships.")

        # --- Load Input CSV ---
        print(f"Loading records from {args.input}...")
        input_records = load_csv(args.input)
        if not input_records:
            print("No records to process. Exiting.")
            return

        # Apply limited-mode filters if enabled
        if args.limited:
            print("Limited mode enabled: applying Field2 filter...")
            filtered = []
            skipped_field2 = 0
            for rec in input_records:
                # Field2 exclusion (convert to int if possible)
                field2_val = rec.get("Field2")
                field2_int = None
                if field2_val is not None and str(field2_val).strip() != "":
                    try:
                        field2_int = int(float(str(field2_val).strip()))
                    except Exception:
                        field2_int = None
                if field2_int is not None and field2_int in EXCLUDED_FIELD2_IDS:
                    skipped_field2 += 1
                    continue

                filtered.append(rec)

            print(f"Filtered input records: {len(filtered)} (skipped by Field2: {skipped_field2})")
            input_records = filtered

        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        print(f"Starting data upsert for {len(input_records)} records. Batch ID: {batch_id}")
        print(f"Using {CORE_COUNT} worker threads with batch size of {BATCH_SIZE} records per batch")
        
        # --- Batch Processing ---
        processed_records = []
        batch_processed_transactions = []  # Track transactions being processed in this batch
        lock = Lock()
        
        # Track which records have been processed to ensure all input records appear in output
        processed_record_ids = set()
        
        # Split records into batches
        total_batches = (len(input_records) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"Processing {len(input_records)} records in {total_batches} batch(es)...")
        
        for batch_num in range(total_batches):
            start_idx = batch_num * BATCH_SIZE
            end_idx = min(start_idx + BATCH_SIZE, len(input_records))
            batch_records = input_records[start_idx:end_idx]
            
            print(f"\nProcessing batch {batch_num + 1}/{total_batches} ({len(batch_records)} records)...")
            
            # Process each batch with ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=CORE_COUNT) as executor:
                # Create a mapping of future to record for exception handling
                future_to_record = {}
                futures = []
                
                for record in batch_records:
                    future = executor.submit(
                        process_record, record, prop_collection_primary, prop_collection_secondary,
                        agents_data, companies_data, agent_lookup, company_lookup,
                        processed_records, lock, batch_id,
                        property_update_log_file,
                        failures_log_file, sold_onhold_collection, batch_processed_transactions,
                        brand_relationship_lookup, employment_history_collection
                    )
                    future_to_record[future] = record
                    futures.append(future)

                # Wait for all futures in this batch to complete
                for future in tqdm(as_completed(futures), total=len(futures), desc=f"Batch {batch_num + 1}/{total_batches}"):
                    try:
                        future.result()
                    except Exception as exc:
                        record = future_to_record[future]
                        record_id = record.get('id', 'N/A')
                        print(f'A record generated an unhandled exception: {exc}')

                        # Ensure the record is still added to processed_records even if there was an exception
                        with lock:
                            if record_id not in processed_record_ids:
                                record['failure_reason'] = f"Unhandled exception: {str(exc)}"
                                record['action'] = f"ERROR - {str(exc)}"
                                record['primary_result'] = f"error: {str(exc)}"
                                processed_records.append(record)
                                processed_record_ids.add(record_id)
            
            # Clear batch_processed_transactions every 20 batches to free memory
            # This balances memory usage with duplicate detection across batches
            if (batch_num + 1) % 20 == 0:
                with lock:
                    batch_processed_transactions.clear()
                gc.collect()  # Force garbage collection
                print(f"Memory cleared after batch {batch_num + 1}/{total_batches}")
            
            print(f"Completed batch {batch_num + 1}/{total_batches}")

        # Count successful updates
        updated_primary = sum(1 for record in processed_records if "inserted" in record.get('primary_result', ''))
        
        # --- Final Summary and File/DB Writing ---
        print(f"\nProcessing complete.")
        print(f"SUMMARY: Input records: {len(input_records)}")
        print(f"SUMMARY: Processed records: {len(processed_records)}")
        print(f"SUMMARY: Updated {updated_primary} properties in primary database.")
        
        # Validate record counts match
        if len(processed_records) != len(input_records):
            print(f"WARNING: Record count mismatch! Input: {len(input_records)}, Output: {len(processed_records)}")
            print("This indicates some records may have been lost during processing.")
            
            # Find missing records and add them as error records
            processed_ids = {record.get('id', 'N/A') for record in processed_records}
            missing_records = [record for record in input_records if record.get('id', 'N/A') not in processed_ids]
            
            if missing_records:
                print(f"Adding {len(missing_records)} missing records as error records...")
                for record in missing_records:
                    record['failure_reason'] = "Record was lost during processing (no result recorded)"
                    record['action'] = "ERROR - Record lost during processing"
                    record['primary_result'] = "error: Record lost during processing"
                    processed_records.append(record)
                
                print(f" Added {len(missing_records)} missing records. New total: {len(processed_records)}")
        else:
            print("Record count validation passed: Input and output record counts match.")
        
        # Create output file path (similar to leased script)
        output_file_path = args.input.replace('.csv', '_with_actions.csv')
        
        if processed_records:
            # Get original CSV fieldnames by reading the input file (like leased script)
            try:
                with open(args.input, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    original_fieldnames = reader.fieldnames
            except Exception as e:
                print(f"Warning: Could not read original CSV fieldnames: {e}")
                # Fallback: get fieldnames from processed records
                original_fieldnames = []
                for record in processed_records:
                    for key in record.keys():
                        if key not in ['action', 'primary_result', 'failure_reason']:
                            if key not in original_fieldnames:
                                original_fieldnames.append(key)
            
            # Order fieldnames: original columns first, then action columns
            ordered_fieldnames = original_fieldnames + ['action', 'primary_result']
            if any('failure_reason' in record for record in processed_records):
                ordered_fieldnames.append('failure_reason')

            try:
                # Write to the main output file with action columns (like leased script)
                with open(output_file_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=ordered_fieldnames, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(processed_records)
                print(f"SUMMARY: Output file saved as {output_file_path}")
                
                # Also write to the original failure file for backward compatibility
                with open(failures_csv_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=ordered_fieldnames, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(processed_records)
                print(f"Wrote {len(processed_records)} records to {failures_csv_file}")
            except IOError as e:
                print(f"Error: Could not write output CSV: {e}")

        # Agent and company creation removed - no new agents or companies will be created

    except ConnectionFailure as e:
        print(f"MongoDB Connection Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unhandled error occurred: {e}")
        sys.exit(1)
    finally:
        if client_primary:
            client_primary.close()
            print("Primary MongoDB connection closed.")

if __name__ == "__main__":
    main()
