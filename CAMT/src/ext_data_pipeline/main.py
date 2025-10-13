"""
CAMT Processor - Extracts IDs from GCS bucket labels
"""
import logging
from CAMT.src.camt_core.camt_parse import CAMT053Parser
from CAMT.src.ext_data_pipeline.transformer import Transformer
from common.central_validator import CentralValidator, ValidationError # CHANGED
from CAMT.src.ext_data_pipeline.config import settings as camt_settings
from common.settings import BALANCE_TABLE_ID, TRANSACTIONS_TABLE_ID
from gcp_services.gcs_service import (
    read_file_from_gcs,
    extract_ids_from_gcs_path
)
from gcp_services.cmek_service import KmsEncryptor
from gcp_services.bq_loader import load_rows_to_bq

logger = logging.getLogger(__name__)


def process_camt_file(gcs_path: str):
    """
    CAMT processor with schema version validation
    """
    logger.info(f"Starting CAMT processing for: {gcs_path}")
    
    # Extract IDs
    bucket_name, org_id, div_id, cust_id = extract_ids_from_gcs_path(gcs_path)
    
    # Read file
    xml_content = read_file_from_gcs(gcs_path)
    
    # Initialize validator
    validator = CentralValidator(camt_settings.SCHEMA_PATH)
    
    # ---- NEW: Extract CAMT Version from XML ----
    # Option 1: Extract from XML namespace
    import re
    namespace_match = re.search(r'xmlns="[^"]*camt\.053\.001\.(\d+)"', xml_content)
    if namespace_match:
        version_number = namespace_match.group(1)
        camt_version = f'camt.053.001.{version_number.zfill(2)}'
    else:
        # Option 2: Use default or extract from parsed document
        camt_version = 'camt.053.001.02'  # Default
    
    supported_versions = camt_settings.CAMT_SUPPORTED_VERSIONS  
    # From config: ['camt.053.001.02', 'camt.053.001.04', 'camt.053.001.08']
    
    try:
        validator.validate_schema_version('CAMT', camt_version, supported_versions)
    except ValidationError as e:
        logger.error(f"CAMT schema version validation failed: {e}")
        raise  # Stop processing
    
    # Parse after version validation
    parser = CAMT053Parser(logger=logger)
    document = parser.parse_string(xml_content)
    
    # Continue with existing code...
    transformer = Transformer(org_id, div_id, cust_id, BALANCE_TABLE_ID, TRANSACTIONS_TABLE_ID)
    transformed_rows = transformer.transform(document)
    
    if not transformed_rows:
        logger.warning("No rows to process")
        return {"rows_processed": 0}
    
    # Apply defaults - CHANGED (pass validator instance)
    transformed_rows = transformer.apply_default_values(transformed_rows, validator)
    
    # Validate using centralized validator - CHANGED
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
    
    # Log warnings but continue
    if all_warnings:
        logger.warning(f"Validation warnings ({len(all_warnings)}):")
        for warning in all_warnings[:10]:
            logger.warning(f"  {warning}")
    
    # Stop if critical errors
    if all_errors:
        logger.error(f"Validation failed with {len(all_errors)} error(s)")
        raise ValueError(f"Validation failed: {all_errors[0]}")
    
    # Encrypt
    sensitive_fields = validator.get_sensitive_fields("balance") + validator.get_sensitive_fields("transactions")
    encryptor = KmsEncryptor()
    encrypted_rows = [
        encryptor.encrypt_row(row, list(set(sensitive_fields))) 
        for row in valid_rows
    ]
    
    # Load to BigQuery
    rows_loaded = load_rows_to_bq(encrypted_rows)
    
    logger.info(f"CAMT processing complete: {rows_loaded} rows loaded")
    return {"rows_processed": rows_loaded}