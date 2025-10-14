"""
Data transformation functions for processing CSV data
"""
import csv
import logging
from typing import List, Dict
from io import StringIO
from CSV.utils.csv_helper import clean_csv_row
from common.central_validator import CentralValidator

logger = logging.getLogger(__name__)


def parse_csv_content(csv_content: str) -> List[Dict]:
    """
    Parses CSV content into list of dictionaries.
    
    Args:
        csv_content: CSV file content as string
        
    Returns:
        List of row dictionaries
    """
    rows = []
    csv_file = StringIO(csv_content)
    reader = csv.DictReader(csv_file)
    
    for row in reader:
        cleaned_row = clean_csv_row(row)
        rows.append(cleaned_row)
    
    logger.info(f"Parsed {len(rows)} rows from CSV")
    return rows


def apply_default_values(rows: List[Dict], table_type: str, schema_path: str) -> List[Dict]:
    """
    Applies default values from schema to all rows.
    Forces fields with default_value to that value, regardless of existing data.
    
    Args:
        rows: List of row dictionaries
        table_type: Either 'balance' or 'transactions'
        schema_path: Path to schema JSON file
        
    Returns:
        List of rows with default values applied
    """
    validator = CentralValidator(schema_path)
    schema = validator._get_schema_for_table(table_type)
    
    # Build a map of field names to their default values
    default_values_map = {}
    for field_def in schema:
        field_name = field_def["name"]
        if "default_value" in field_def:
            default_values_map[field_name] = field_def["default_value"]
    
    if not default_values_map:
        logger.info(f"No default values defined for {table_type}")
        return rows
    
    # Apply defaults to all rows
    processed_rows = []
    for row in rows:
        processed_row = row.copy()
        for field_name, default_value in default_values_map.items():
            # Always override with default value
            processed_row[field_name] = default_value
        processed_rows.append(processed_row)
    
    logger.info(f"Applied default values to {len(processed_rows)} rows. Fields: {list(default_values_map.keys())}")
    return processed_rows


def transform_for_bigquery(rows: List[Dict], table_type: str, schema_path: str, org_id: str, div_id: str) -> List[Dict]:
    """
    Transforms validated rows for BigQuery loading.
    Adds organisation_biz_id and division_biz_id from GCS bucket labels.
    
    Args:
        rows: List of validated row dictionaries
        table_type: Either 'balance' or 'transactions' (table ID constant)
        schema_path: Path to schema JSON file
        org_id: Organisation business ID from bucket labels
        div_id: Division business ID from bucket labels
        
    Returns:
        List of transformed rows ready for BigQuery
    """
    validator = CentralValidator(schema_path)
    
    # Map table_type to schema type string
    schema_table_type = "balance" if "balance" in table_type.lower() else "transactions"
    schema = validator._get_schema_for_table(schema_table_type)
    
    transformed_rows = []
    
    for row in rows:
        # Add target table identifier
        transformed_row = row.copy()
        transformed_row["_target_table"] = table_type
        
        # Add org_id and div_id from bucket labels (overriding any values from CSV)
        transformed_row["organisation_biz_id"] = org_id
        transformed_row["division_biz_id"] = div_id
        
        # Extract only fields defined in schema
        field_names = [field["name"] for field in schema]
        
        # Filter row to include only schema fields
        filtered_row = {
            k: v for k, v in transformed_row.items() 
            if k in field_names or k == "_target_table"
        }
        
        transformed_rows.append(filtered_row)
    
    logger.info(f"Transformed {len(transformed_rows)} rows for BigQuery with org_id='{org_id}', div_id='{div_id}'")
    return transformed_rows


def prepare_rows_for_encryption(rows: List[Dict], table_type: str, schema_path: str) -> List[Dict]:
    """
    Prepares rows for encryption.
    Note: organisation_biz_id is already in the row and will be used for key lookup.
    
    Args:
        rows: List of row dictionaries
        table_type: Either 'balance' or 'transactions'
        schema_path: Path to schema JSON file
        
    Returns:
        List of rows ready for encryption (no modifications needed)
    """
    validator = CentralValidator(schema_path)
    sensitive_fields = validator.get_sensitive_fields(table_type)
    
    if not sensitive_fields:
        logger.info(f"No sensitive fields found for {table_type}")
        # No encryption needed, return rows as-is
        return rows
    
    # Verify organisation_biz_id exists in all rows
    for idx, row in enumerate(rows):
        if "organisation_biz_id" not in row or not row["organisation_biz_id"]:
            raise ValueError(f"Row {idx} missing required 'organisation_biz_id' for encryption key lookup")
    
    logger.info(f"Prepared {len(rows)} rows for encryption using organisation_biz_id")
    return rows