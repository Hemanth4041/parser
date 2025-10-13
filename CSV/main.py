"""
CSV Processor - Extracts IDs from GCS bucket labels
"""
import logging
from pathlib import Path
from CSV.transformer import (
    parse_csv_content,
    apply_default_values,
    transform_for_bigquery,
    prepare_rows_for_encryption
)
from common.central_validator import CentralValidator  # CHANGED
from CSV.config import settings as csv_settings
from common.settings import BALANCE_TABLE_ID, TRANSACTIONS_TABLE_ID
from gcp_services.gcs_service import (
    read_file_from_gcs,
    extract_ids_from_gcs_path
)
from gcp_services.cmek_service import KmsEncryptor
from gcp_services.bq_loader import load_rows_to_bq

logger = logging.getLogger(__name__)


def _determine_table_type_from_filename(filename: str) -> str:
    """
    Determine table type from CSV filename.
    Expected patterns: *balance*.csv or *transaction*.csv
    
    Args:
        filename: CSV filename (case-insensitive)
        
    Returns:
        Table ID constant (BALANCE_TABLE_ID or TRANSACTIONS_TABLE_ID)
        
    Raises:
        ValueError: If table type cannot be determined
    """
    filename_lower = filename.lower()
    
    if "balance" in filename_lower:
        return BALANCE_TABLE_ID
    elif "transaction" in filename_lower:
        return TRANSACTIONS_TABLE_ID
    else:
        raise ValueError(
            f"Cannot determine table type from filename: {filename}. "
            f"Expected filename to contain 'balance' or 'transaction'"
        )


def process_csv_file(gcs_path: str):
    """
    Process CSV file from GCS using bucket labels for org/div/customer IDs.
    
    Args:
        gcs_path: GCS path (bucket_name/blob_name)
        
    Returns:
        Processing result dictionary
    """
    logger.info(f"Starting CSV processing for: {gcs_path}")
    
    # Extract org_id, div_id, customer_id from bucket labels
    bucket_name, org_id, div_id, customer_id = extract_ids_from_gcs_path(gcs_path)
    logger.info(f"From bucket labels - Org: '{org_id}', Div: '{div_id}', Customer: '{customer_id}'")
    
    # Extract filename and determine table type
    filename = Path(gcs_path).name
    table_type = _determine_table_type_from_filename(filename)
    logger.info(f"Processing as {table_type} table (from filename: {filename})")
    
    # Determine schema table type string
    schema_table_type = "balance" if table_type == BALANCE_TABLE_ID else "transactions"
    
    # Read and parse
    csv_content = read_file_from_gcs(gcs_path)
    rows = parse_csv_content(csv_content)
    logger.info(f"Parsed {len(rows)} rows")
    
    # Apply defaults
    rows = apply_default_values(rows, schema_table_type, csv_settings.SCHEMA_PATH)
    
    # Validate using centralized validator - CHANGED
    validator = CentralValidator(csv_settings.SCHEMA_PATH)
    valid_rows, errors, warnings = validator.validate_rows_batch(rows, schema_table_type)
    
    # Log warnings but continue
    if warnings:
        logger.warning(f"Validation warnings ({len(warnings)}):")
        for warning in warnings[:10]:
            logger.warning(f"  {warning}")
    
    # Stop if critical errors
    if errors:
        logger.error(f"Validation failed with {len(errors)} error(s)")
        raise ValueError(f"Validation failed: {errors[0]}")
    
    if not valid_rows:
        raise ValueError("No valid rows to process")
    
    # Transform for BigQuery
    transformed_rows = transform_for_bigquery(
        valid_rows, 
        table_type, 
        csv_settings.SCHEMA_PATH,
        org_id,
        div_id
    )
    
    # Prepare for encryption
    rows_with_customer = prepare_rows_for_encryption(
        transformed_rows, 
        schema_table_type, 
        csv_settings.SCHEMA_PATH, 
        customer_id
    )
    
    # Encrypt sensitive fields
    sensitive_fields = validator.get_sensitive_fields(schema_table_type)
    
    if sensitive_fields:
        encryptor = KmsEncryptor()
        encrypted_rows = [
            encryptor.encrypt_row(row, sensitive_fields) 
            for row in rows_with_customer
        ]
    else:
        # No encryption needed, remove customer_id before loading
        encrypted_rows = [
            {k: v for k, v in row.items() if k != "customer_id"} 
            for row in rows_with_customer
        ]
    
    # Load to BigQuery
    rows_loaded = load_rows_to_bq(encrypted_rows)
    
    logger.info(f"CSV processing complete: {rows_loaded} rows loaded")
    return {"rows_processed": rows_loaded}