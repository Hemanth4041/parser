"""
Base Parser - Central orchestrator for all file format parsers
Handles the common pipeline: parse -> transform -> validate -> encrypt -> load
"""
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from pathlib import Path

from common.config_loader.config_loader import ConfigLoader
from common.validator.central_validator import CentralValidator
from common.env_variables.settings import BALANCE_TABLE_ID, TRANSACTIONS_TABLE_ID
from gcp_services.gcs_service import read_file_from_gcs, extract_ids_from_gcs_path
from gcp_services.cmek_service import KmsEncryptor
from gcp_services.bq_loader import load_rows_to_bq

logger = logging.getLogger(__name__)


class BaseParser(ABC):
    """
    Abstract base class for all file format parsers.
    Provides common pipeline orchestration while allowing format-specific implementations.
    """
    
    def __init__(self, schema_path: str = None):
        """
        Initialize base parser with common components.
        
        Args:
            schema_path: Path to schema JSON file (defaults to common schema)
        """
        # Use common schema if not specified
        if schema_path is None:
            schema_path = Path(__file__).parent / "schema" / "target_bq_schema.json"
        
        self.schema_path = str(schema_path)
        self.config_loader = ConfigLoader(self.schema_path)
        self.validator = CentralValidator(self.schema_path)
        self.encryptor = KmsEncryptor()
        
        logger.info(f"{self.__class__.__name__} initialized with schema: {self.schema_path}")
    
    @abstractmethod
    def parse_file_content(self, file_content: str, **kwargs) -> Any:
        """
        Parse file content into format-specific object.
        Must be implemented by each parser.
        
        Args:
            file_content: Raw file content as string
            **kwargs: Additional parser-specific arguments
            
        Returns:
            Parsed file object (format-specific)
        """
        pass
    
    @abstractmethod
    def get_transformer(self, org_id: str, div_id: str, **kwargs):
        """
        Get transformer instance for this parser type.
        Must be implemented by each parser.
        
        Args:
            org_id: Organisation business ID
            div_id: Division business ID
            **kwargs: Additional transformer-specific arguments
            
        Returns:
            Transformer instance (subclass of BaseTransformer)
        """
        pass
    
    @abstractmethod
    def get_table_type_from_filename(self, filename: str) -> Optional[str]:
        """
        Determine table type from filename (for formats that need it like CSV).
        
        Args:
            filename: File name
            
        Returns:
            Table type identifier or None if not applicable
        """
        pass
    
    def process_file(self, gcs_path: str, **parser_kwargs) -> Dict:
        """
        Main processing pipeline: parse -> transform -> validate -> encrypt -> load.
        This is the central orchestration method that all parsers use.
        
        Args:
            gcs_path: GCS path (bucket_name/blob_name)
            **parser_kwargs: Additional parser-specific arguments
            
        Returns:
            Processing result dictionary
        """
        logger.info(f"Starting {self.__class__.__name__} processing for: {gcs_path}")
        
        # Step 1: Extract metadata from GCS bucket labels
        bucket_name, org_id, div_id = extract_ids_from_gcs_path(gcs_path)
        logger.info(f"Extracted from bucket labels - Org: '{org_id}', Div: '{div_id}'")
        
        # Step 2: Read file content
        file_content = read_file_from_gcs(gcs_path)
        logger.info(f"Read file content: {len(file_content)} bytes")
        
        # Step 3: Parse file (format-specific)
        parsed_object = self.parse_file_content(file_content, **parser_kwargs)
        logger.info("File parsed successfully")
        
        # Step 4: Get transformer instance
        filename = Path(gcs_path).name
        table_type = self.get_table_type_from_filename(filename)
        
        # Pass any additional kwargs to transformer (e.g., bank_id for BAI)
        transformer = self.get_transformer(org_id, div_id, **parser_kwargs)
        
        # Step 5: Transform to BigQuery rows
        transformed_rows = transformer.transform(parsed_object, table_type)
        logger.info(f"Transformed {len(transformed_rows)} rows")
        
        if not transformed_rows:
            logger.warning("No rows to process")
            return {"rows_processed": 0}
        
        # Step 6: Validate rows
        valid_rows = self._validate_rows(transformed_rows)
        logger.info(f"Validated {len(valid_rows)} rows")
        
        # Step 7: Encrypt sensitive fields
        encrypted_rows = self._encrypt_rows(valid_rows)
        logger.info("Encrypted sensitive fields")
        
        # Step 8: Load to BigQuery
        rows_loaded = load_rows_to_bq(encrypted_rows)
        logger.info(f"Loaded {rows_loaded} rows to BigQuery")
        
        logger.info(f"{self.__class__.__name__} processing complete")
        return {"rows_processed": rows_loaded}
    
    def _validate_rows(self, rows: List[Dict]) -> List[Dict]:
        """
        Validate rows using central validator.
        
        Args:
            rows: List of row dictionaries
            
        Returns:
            List of valid rows
            
        Raises:
            ValueError: If validation fails
        """
        # Separate by table type
        balance_rows = [r for r in rows if r.get("_target_table") == BALANCE_TABLE_ID]
        tx_rows = [r for r in rows if r.get("_target_table") == TRANSACTIONS_TABLE_ID]
        
        valid_rows = []
        all_errors = []
        all_warnings = []
        
        # Validate balance rows
        if balance_rows:
            bal_valid, bal_errors, bal_warnings = self.validator.validate_rows_batch(
                balance_rows, "balance"
            )
            valid_rows.extend(bal_valid)
            all_errors.extend(bal_errors)
            all_warnings.extend(bal_warnings)
        
        # Validate transaction rows
        if tx_rows:
            tx_valid, tx_errors, tx_warnings = self.validator.validate_rows_batch(
                tx_rows, "transactions"
            )
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
        
        return valid_rows
    
    def _encrypt_rows(self, rows: List[Dict]) -> List[Dict]:
        """
        Encrypt sensitive fields in rows.
        
        Args:
            rows: List of row dictionaries
            
        Returns:
            List of encrypted rows
        """
        encrypted_rows = []
        
        for row in rows:
            target_table = row.get("_target_table")
            table_type = self._get_table_type_string(target_table)
            
            # Get sensitive fields from config loader
            sensitive_fields = self.config_loader.get_sensitive_fields(table_type)
            
            # Encrypt row
            if sensitive_fields:
                encrypted_row = self.encryptor.encrypt_row(row, sensitive_fields)
            else:
                encrypted_row = row
            
            encrypted_rows.append(encrypted_row)
        
        return encrypted_rows
    
    def _get_table_type_string(self, target_table: str) -> str:
        """
        Convert target table ID to schema table type string.
        
        Args:
            target_table: Target table ID constant
            
        Returns:
            Schema table type string ('balance' or 'transactions')
        """
        if target_table == BALANCE_TABLE_ID:
            return "balance"
        elif target_table == TRANSACTIONS_TABLE_ID:
            return "transactions"
        else:
            raise ValueError(f"Unknown target table: {target_table}")