"""
Base Transformer - Abstract base for all transformers
Provides common interface and default value application
"""
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any

from common.config_loader.config_loader import ConfigLoader
from common.env_variables.settings import BALANCE_TABLE_ID, TRANSACTIONS_TABLE_ID

logger = logging.getLogger(__name__)


class BaseTransformer(ABC):
    """
    Abstract base class for all data transformers.
    Handles common transformation logic and default value application.
    """
    
    def __init__(self, org_id: str, div_id: str, config_loader: ConfigLoader):
        """
        Initialize transformer.
        
        Args:
            org_id: Organisation business ID
            div_id: Division business ID
            config_loader: ConfigLoader instance
        """
        self.org_id = org_id
        self.div_id = div_id
        self.config_loader = config_loader
        logger.info(f"{self.__class__.__name__} initialized for org='{org_id}', div='{div_id}'")
    
    @abstractmethod
    def transform(self, parsed_data: Any, table_type: str = None) -> List[Dict]:
        """
        Transform parsed data into BigQuery-ready rows.
        Must be implemented by each transformer.
        
        Args:
            parsed_data: Parsed data object (format-specific)
            table_type: Table type identifier (optional, used by CSV)
            
        Returns:
            List of transformed row dictionaries
        """
        pass
    
    def apply_default_values(self, rows: List[Dict]) -> List[Dict]:
        """
        Apply default values from schema to rows based on their target table.
        
        Args:
            rows: List of row dictionaries with _target_table field
            
        Returns:
            List of rows with default values applied
        """
        processed_rows = []
        
        for row in rows:
            target_table = row.get("_target_table")
            
            # Determine table type
            if target_table == BALANCE_TABLE_ID:
                table_type = "balance"
            elif target_table == TRANSACTIONS_TABLE_ID:
                table_type = "transactions"
            else:
                logger.warning(f"Unknown target table: {target_table}, skipping defaults")
                processed_rows.append(row)
                continue
            
            # Get defaults and apply
            defaults = self.config_loader.get_default_values(table_type)
            processed_row = row.copy()
            
            for field_name, default_value in defaults.items():
                processed_row[field_name] = default_value
            
            processed_rows.append(processed_row)
        
        logger.info(f"Applied default values to {len(processed_rows)} rows")
        return processed_rows
    
    def _get_common_fields(self) -> Dict[str, str]:
        """
        Get common fields that appear in all rows.
        
        Returns:
            Dictionary with org_id, div_id, and source_system
        """
        return {
            "organisation_biz_id": self.org_id,
            "division_biz_id": self.div_id,
            "source_system": "external"
        }