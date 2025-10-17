"""
BAI Parser - Handles BAI2 file format parsing
Inherits from BaseParser and implements BAI-specific logic
"""
import logging
import os
from pathlib import Path
from typing import Any, Optional

from common.base_parser import BaseParser
from BAI.src.bai2_core import parse_from_string
from BAI.src.ext_data_pipeline.transformer import BAITransformer

logger = logging.getLogger(__name__)


class BAIParser(BaseParser):
    """BAI2 file format parser implementation."""
    
    def parse_file_content(self, file_content: str, **kwargs) -> Any:
        """
        Parse BAI2 file content.
        
        Args:
            file_content: Raw BAI2 file content
            **kwargs: Additional arguments (e.g., check_integrity)
            
        Returns:
            Parsed Bai2File object
        """
        check_integrity = kwargs.get("check_integrity", True)
        
        logger.info("Parsing BAI2 file...")
        bai_file = parse_from_string(file_content, check_integrity=check_integrity)
        logger.info(f"Parsed BAI2 file with {len(bai_file.children)} groups")
        
        return bai_file
    
    def get_transformer(self, org_id: str, div_id: str, **kwargs):
        """
        Get BAI transformer instance with bank-specific configuration.
        
        Args:
            org_id: Organisation business ID
            div_id: Division business ID
            **kwargs: Additional arguments (expects 'bank_id')
            
        Returns:
            BAITransformer instance
        """
        bank_id = kwargs.get("bank_id")
        return BAITransformer(org_id, div_id, self.config_loader, bank_id=bank_id)
    
    def get_table_type_from_filename(self, filename: str) -> Optional[str]:
        """
        BAI files don't need table type from filename.
        Returns None as BAI format contains both balance and transaction data.
        
        Args:
            filename: BAI filename
            
        Returns:
            None (not applicable for BAI)
        """
        return None
    
    def get_bank_id_from_filename(self, filename: str) -> str:
        """
        Extract bank_id from BAI filename.
        Expected format: '<bank_id>_<other>.bai'
        
        Args:
            filename: BAI filename
            
        Returns:
            Bank ID string
            
        Raises:
            ValueError: If bank_id cannot be extracted
        """
        base = os.path.basename(filename).split(".")[0]
        parts = base.split("_")
        
        if len(parts) >= 1:
            bank_id = parts[0].strip()
            if bank_id:
                logger.info(f"Extracted bank_id: {bank_id}")
                return bank_id
        
        raise ValueError(f"Cannot extract bank_id from filename: {filename}")
    
    def process_file(self, gcs_path: str, **parser_kwargs) -> dict:
        """
        Process BAI file with bank-specific mappings.
        Overrides base process_file to extract bank_id before transformation.
        
        Args:
            gcs_path: GCS path to BAI file
            **parser_kwargs: Additional parser arguments
            
        Returns:
            Processing result dictionary
        """
        # Extract bank_id from filename for bank-specific mappings
        filename = Path(gcs_path).name
        
        try:
            bank_id = self.get_bank_id_from_filename(filename)
            # Add bank_id to parser kwargs so transformer can access it
            parser_kwargs["bank_id"] = bank_id
        except ValueError as e:
            logger.warning(f"Could not extract bank_id: {e}. Using default mappings.")
            parser_kwargs["bank_id"] = None
        
        # Call base processing pipeline
        return super().process_file(gcs_path, **parser_kwargs)


def process_bai_file(gcs_path: str) -> dict:
    """
    Entry point for BAI file processing.
    
    Args:
        gcs_path: GCS path (bucket_name/blob_name)
        
    Returns:
        Processing result dictionary
    """
    parser = BAIParser()
    return parser.process_file(gcs_path)