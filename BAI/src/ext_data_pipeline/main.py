"""
BAI Processor - Extracts IDs from GCS bucket labels
"""
import logging
import os
from BAI.src.bai2_core import parse_from_string
from common.settings import BALANCE_TABLE_ID, TRANSACTIONS_TABLE_ID
from BAI.src.ext_data_pipeline import transformer
from BAI.src.ext_data_pipeline.config import settings as bai_settings
from BAI.src.ext_data_pipeline.config.config_loader import (
    load_config, 
    get_all_sensitive_fields,
    get_bank_mappings
)
from common.central_validator import CentralValidator
from gcp_services.gcs_service import (
    read_file_from_gcs, 
    extract_ids_from_gcs_path
)
from gcp_services.cmek_service import KmsEncryptor
from gcp_services.bq_loader import load_rows_to_bq

logger = logging.getLogger(__name__)


def _extract_bank_id_from_filename(filename: str) -> str:
    """
    Extract bank_id from BAI filename.
    Expected format: '<bank_id>_<anything>.bai' or just filename with bank_id embedded
    
    Args:
        filename: BAI filename
        
    Returns:
        Bank ID string
    """
    base = os.path.basename(filename).split(".")[0]
    parts = base.split("_")
    
    # If filename format is: bank_id_timestamp.bai, take first part
    if len(parts) >= 1:
        bank_id = parts[0].strip()
        if bank_id:
            return bank_id
    
    raise ValueError(f"Cannot extract bank_id from filename: {filename}")


def process_bai_file(gcs_path: str):
    """
    Process BAI file from GCS using bucket labels for org/div IDs.
    Bank ID is still extracted from filename for bank-specific mappings.
    
    Args:
        gcs_path: GCS path (bucket_name/blob_name)
        
    Returns:
        Processing result dictionary
    """
    logger.info(f"Starting BAI processing for: {gcs_path}")
    
    # Extract org_id and div_id from bucket labels (no customer_id)
    bucket_name, org_id, div_id = extract_ids_from_gcs_path(gcs_path)
    logger.info(f"From bucket labels - Org: '{org_id}', Div: '{div_id}'")
    
    # Extract bank_id from filename (needed for bank-specific mappings)
    filename = gcs_path.split("/", 1)[1] if "/" in gcs_path else gcs_path
    bank_id = _extract_bank_id_from_filename(filename)
    logger.info(f"Bank ID from filename: '{bank_id}'")
    
    # Load config
    config = load_config(bai_settings.MAPPING_CONFIG_PATH)
    
    # Read and parse
    bai_text = read_file_from_gcs(gcs_path)
    bai_file = parse_from_string(bai_text, check_integrity=True)
    
    # Transform (removed customer_id parameter)
    bank_mappings = get_bank_mappings(config, bank_id)
    transformed_rows = transformer.transform_bai_to_rows(
        bai_file, org_id, div_id, config, bank_mappings
    )
    
    if not transformed_rows:
        logger.warning("No rows to process")
        return {"rows_processed": 0}
    
    # Validate using centralized validator
    validator = CentralValidator(bai_settings.MAPPING_CONFIG_PATH)

    # Separate balance and transaction rows for validation
    balance_rows = [r for r in transformed_rows if r.get("_target_table") == BALANCE_TABLE_ID]
    tx_rows = [r for r in transformed_rows if r.get("_target_table") == TRANSACTIONS_TABLE_ID]
    
    valid_rows = []
    all_errors = []
    all_warnings = []
    
    if balance_rows:
        bal_valid, bal_errors, bal_warnings = validator.validate_rows_batch(balance_rows, "balance")
        valid_rows.extend(bal_valid)
        all_errors.extend(bal_errors)
        all_warnings.extend(bal_warnings)
    
    if tx_rows:
        tx_valid, tx_errors, tx_warnings = validator.validate_rows_batch(tx_rows, "transactions")
        valid_rows.extend(tx_valid)
        all_errors.extend(tx_errors)
        all_warnings.extend(tx_warnings)
    
    # Log warnings but continue processing
    if all_warnings:
        logger.warning(f"Validation warnings ({len(all_warnings)}):")
        for warning in all_warnings[:10]:
            logger.warning(f"  {warning}")
    
    # Stop if there are critical errors
    if all_errors:
        logger.error(f"Validation failed with {len(all_errors)} error(s)")
        raise ValueError(f"Validation failed: {all_errors[0]}")  # Raise first error
    
    # Encrypt (using organisation_biz_id from rows)
    sensitive_fields = get_all_sensitive_fields(config)
    encryptor = KmsEncryptor()
    encrypted_rows = [
        encryptor.encrypt_row(row, sensitive_fields) 
        for row in valid_rows
    ]
    
    # Load to BigQuery
    rows_loaded = load_rows_to_bq(encrypted_rows)
    
    logger.info(f"BAI processing complete: {rows_loaded} rows loaded")
    return {"rows_processed": rows_loaded}