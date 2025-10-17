"""
CAMT Parser - Handles CAMT.053 XML file format parsing
Inherits from BaseParser and implements CAMT-specific logic
"""
import logging
import re
from typing import Any, Optional

from common.base_parser import BaseParser
from common.validator.central_validator import ValidationError
from CAMT.src.camt_core.camt_parse import CAMT053Parser
from CAMT.src.ext_data_pipeline.transformer import CAMTTransformer
from CAMT.src.ext_data_pipeline.config import settings as camt_settings

logger = logging.getLogger(__name__)


class CAMTParser(BaseParser):
    """CAMT.053 XML file format parser implementation."""
    
    def parse_file_content(self, file_content: str, **kwargs) -> Any:
        """
        Parse CAMT.053 XML file content with version validation.
        
        Args:
            file_content: Raw XML file content
            **kwargs: Additional arguments
            
        Returns:
            Parsed BankToCustomerStatement object
            
        Raises:
            ValidationError: If CAMT version is not supported
        """
        # Extract CAMT version from XML namespace
        camt_version = self._extract_camt_version(file_content)
        logger.info(f"Detected CAMT version: {camt_version}")
        
        # Validate schema version
        supported_versions = camt_settings.CAMT_SUPPORTED_VERSIONS
        try:
            self.validator.validate_schema_version('CAMT', camt_version, supported_versions)
        except ValidationError as e:
            logger.error(f"CAMT schema version validation failed: {e}")
            raise
        
        # Parse XML
        logger.info("Parsing CAMT.053 XML...")
        parser = CAMT053Parser(logger=logger)
        document = parser.parse_string(file_content)
        logger.info(f"Parsed CAMT document with {len(document.statements)} statements")
        
        return document
    
    def get_transformer(self, org_id: str, div_id: str, **kwargs):
        """
        Get CAMT transformer instance.
        
        Args:
            org_id: Organisation business ID
            div_id: Division business ID
            **kwargs: Additional arguments (unused for CAMT)
            
        Returns:
            CAMTTransformer instance
        """
        return CAMTTransformer(org_id, div_id, self.config_loader)
    
    def get_table_type_from_filename(self, filename: str) -> Optional[str]:
        """
        CAMT files don't need table type from filename.
        Returns None as CAMT format contains both balance and transaction data.
        
        Args:
            filename: CAMT filename
            
        Returns:
            None (not applicable for CAMT)
        """
        return None
    
    def _extract_camt_version(self, xml_content: str) -> str:
        """
        Extract CAMT version from XML namespace.
        
        Args:
            xml_content: Raw XML content
            
        Returns:
            CAMT version string (e.g., 'camt.053.001.02')
        """
        namespace_match = re.search(r'xmlns="[^"]*camt\.053\.001\.(\d+)"', xml_content)
        if namespace_match:
            version_number = namespace_match.group(1)
            return f'camt.053.001.{version_number.zfill(2)}'
        else:
            # Default to version 02 if not found
            logger.warning("Could not extract CAMT version from XML, using default: camt.053.001.02")
            return 'camt.053.001.02'


def process_camt_file(gcs_path: str) -> dict:
    """
    Entry point for CAMT file processing.
    
    Args:
        gcs_path: GCS path (bucket_name/blob_name)
        
    Returns:
        Processing result dictionary
    """
    parser = CAMTParser()
    return parser.process_file(gcs_path)