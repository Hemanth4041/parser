"""
CSV Parser - Handles CSV file format parsing
Inherits from BaseParser and implements CSV-specific logic
"""
import logging
import csv
from io import StringIO
from typing import Any, List, Dict

from common.base_parser import BaseParser
from common.env_variables.settings import BALANCE_TABLE_ID, TRANSACTIONS_TABLE_ID
from CSV.transformer import CSVTransformer
from CSV.utils.csv_helper import clean_csv_row

logger = logging.getLogger(__name__)


class CSVParser(BaseParser):
    """CSV file format parser implementation."""
    
    def parse_file_content(self, file_content: str, **kwargs) -> List[Dict]:
        """
        Parse CSV file content.
        
        Args:
            file_content: Raw CSV file content
            **kwargs: Additional arguments
            
        Returns:
            List of parsed row dictionaries
        """
        logger.info("Parsing CSV file...")
        
        rows = []
        csv_file = StringIO(file_content)
        reader = csv.DictReader(csv_file)
        
        for row in reader:
            cleaned_row = clean_csv_row(row)
            rows.append(cleaned_row)
        
        logger.info(f"Parsed {len(rows)} rows from CSV")
        return rows
    
    def get_transformer(self, org_id: str, div_id: str, **kwargs):
        """
        Get CSV transformer instance.
        
        Args:
            org_id: Organisation business ID
            div_id: Division business ID
            **kwargs: Additional arguments (unused for CSV)
            
        Returns:
            CSVTransformer instance
        """
        return CSVTransformer(org_id, div_id, self.config_loader)
    
    def get_table_type_from_filename(self, filename: str) -> str:
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


def process_csv_file(gcs_path: str) -> dict:
    """
    Entry point for CSV file processing.
    
    Args:
        gcs_path: GCS path (bucket_name/blob_name)
        
    Returns:
        Processing result dictionary
    """
    parser = CSVParser()
    return parser.process_file(gcs_path)