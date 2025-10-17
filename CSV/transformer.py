"""
CSV Transformer - Transforms parsed CSV data to BigQuery rows
Inherits from BaseTransformer
"""
import logging
from typing import List, Dict, Any

from common.base_transformer import BaseTransformer

logger = logging.getLogger(__name__)


class CSVTransformer(BaseTransformer):
    """Transforms CSV rows into schema-compliant BigQuery rows."""
    
    def transform(self, parsed_data: List[Dict], table_type: str = None) -> List[Dict]:
        """
        Transform CSV rows into BigQuery-ready rows.
        
        Args:
            parsed_data: List of parsed CSV row dictionaries
            table_type: Target table ID (BALANCE_TABLE_ID or TRANSACTIONS_TABLE_ID)
            
        Returns:
            List of transformed rows with common fields and _target_table
        """
        if table_type is None:
            raise ValueError("CSV transformer requires table_type parameter")
        
        logger.info(f"Transforming {len(parsed_data)} CSV rows for table: {table_type}")
        
        # Get common fields
        common_fields = self._get_common_fields()
        
        transformed_rows = []
        for row in parsed_data:
            # Start with common fields
            transformed_row = common_fields.copy()
            
            # Add all CSV data
            transformed_row.update(row)
            
            # Add target table identifier
            transformed_row["_target_table"] = table_type
            
            # Force org_id and div_id from bucket labels (override CSV values)
            transformed_row["organisation_biz_id"] = self.org_id
            transformed_row["division_biz_id"] = self.div_id
            
            transformed_rows.append(transformed_row)
        
        logger.info(f"Transformed {len(transformed_rows)} rows for table '{table_type}'")
        
        # Apply default values before returning
        return self.apply_default_values(transformed_rows)