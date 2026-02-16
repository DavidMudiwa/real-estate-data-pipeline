"""
Sold Data Cleaning Script
Cleans and standardizes sold property data fields including:
- Dates
- Prices
- Area values
- Property type
- Feature fields
- Text descriptions
Outputs a cleaned CSV for downstream processing.
"""

import re
import csv
import os
import sys
import argparse
import pandas as pd
from datetime import datetime


# =============================================================================
# Utility Functions
# =============================================================================

def clean_date(raw_date):
    if not raw_date or pd.isna(raw_date):
        return None

    if isinstance(raw_date, str) and re.match(r'^\d{4}-\d{2}-\d{2}$', raw_date.strip()):
        return raw_date.strip()

    try:
        date_obj = pd.to_datetime(raw_date)
        return date_obj.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None


def parse_price_value(value_str):
    if not isinstance(value_str, str):
        return None

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
    if not isinstance(raw_price, str) or not raw_price.strip():
        return None, None

    cleaned_check = raw_price.strip().replace(',', '').replace('$', '').replace(' ', '')
    if re.match(r'^\d+\.?\d*$', cleaned_check):
        try:
            return int(float(cleaned_check)), None
        except (ValueError, TypeError):
            pass

    s = raw_price.replace(',', '').replace('$', '').replace(' ', '')
    s = re.sub(r'(?i)(offersover|plusgstifappliable|buyerguide|private(sale)?|eoi|guide|auction|\(.*\)|[|+]|to)', '-', s)

    price_parts = re.findall(r'(\d+\.?\d*k|\d+\.?\d*m|\d+\.?\d*mil|\d+\.?\d*)', s, re.IGNORECASE)

    numeric_prices = [parse_price_value(p) for p in price_parts if parse_price_value(p) is not None]

    if not numeric_prices:
        return None, None

    price1 = numeric_prices[0]
    price2 = numeric_prices[1] if len(numeric_prices) > 1 else None

    return price1, price2


def clean_area(raw_area):
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
            return float(match.group(1)), area_unit
        except (ValueError, TypeError):
            return None, None

    return None, None


# =============================================================================
# Main Processing
# =============================================================================

def main(args):

    try:
        with open(args.input_file, 'r', encoding='utf-8') as f_in:
            reader = csv.DictReader(f_in)
            data = list(reader)

            if not data:
                print("Input file is empty.")
                return

            original_headers = reader.fieldnames

    except FileNotFoundError:
        print("Input file not found.")
        sys.exit(1)

    processed_data = []

    for row in data:
        new_row = {k: v for k, v in row.items() if v != ''}

        price1, price2 = clean_price(new_row.get('Field9'))
        new_row['price'] = price1
        new_row['price2'] = price2

        if new_row.get('price') is not None and new_row['price'] < 10000:
            new_row['price'] = None

        area, area_unit = clean_area(new_row.get('Field36'))
        new_row['Area'] = area
        new_row['areaUnit'] = area_unit

        new_row['Field11'] = clean_date(new_row.get('Field11'))

        processed_data.append(new_row)

    final_headers = original_headers + ['price', 'price2', 'Area', 'areaUnit']

    try:
        with open(args.output_file, 'w', newline='', encoding='utf-8') as f_out:
            writer = csv.DictWriter(f_out, fieldnames=final_headers, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(processed_data)

        print(f"Successfully processed {len(processed_data)} records.")

    except IOError as e:
        print(f"Error writing output file: {e}")


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clean and standardize sold property CSV data."
    )

    parser.add_argument('--input', dest='input_file', required=True)
    parser.add_argument('--output', dest='output_file', required=True)
    parser.add_argument('--delete-input', action='store_true')

    args = parser.parse_args()
    main(args)
