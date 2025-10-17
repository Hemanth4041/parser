"""
Config Loader - Centralized configuration and schema loading
Provides utilities to extract default values, sensitive fields, and mappings
"""
import json
import logging
from typing import Dict, List, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class ConfigLoader:
    """
    Centralized configuration loader for all parsers.
    Loads and provides access to schema definitions, defaults, and mappings.
    """
    
    def __init__(self, schema_path: str):
        """
        Initialize config loader with schema file.
        
        Args:
            schema_path: Path to target_bq_schema.json
        """
        self.schema_path = schema_path
        self.config = self._load_config()
        logger.info(f"ConfigLoader initialized with schema: {schema_path}")
    
    def _load_config(self) -> Dict:
        """Load configuration from JSON file."""
        try:
            with open(self.schema_path, 'r') as f:
                config = json.load(f)
            logger.info(f"Loaded configuration from {self.schema_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load config from {self.schema_path}: {e}")
            raise
    
    def get_common_schema(self) -> List[Dict]:
        """
        Get common fields schema.
        
        Returns:
            List of common field definitions
        """
        return self.config.get("common_fields_schema", [])
    
    def get_table_schema(self, table_type: str) -> List[Dict]:
        """
        Get schema for specific table type.
        
        Args:
            table_type: 'balance' or 'transactions'
            
        Returns:
            List of field definitions for the table
        """
        if table_type == "balance":
            return self.config.get("balance_table_schema", [])
        elif table_type == "transactions":
            return self.config.get("transactions_table_schema", [])
        else:
            raise ValueError(f"Unknown table type: {table_type}")
    
    def get_full_schema(self, table_type: str) -> List[Dict]:
        """
        Get full schema (common + table-specific fields).
        
        Args:
            table_type: 'balance' or 'transactions'
            
        Returns:
            Combined list of field definitions
        """
        common = self.get_common_schema()
        specific = self.get_table_schema(table_type)
        return common + specific
    
    def get_default_values(self, table_type: str) -> Dict[str, Any]:
        """
        Extract default values from schema.
        
        Args:
            table_type: 'balance' or 'transactions'
            
        Returns:
            Dictionary mapping field names to default values
        """
        defaults = {}
        
        # Get defaults from common fields
        for field in self.get_common_schema():
            if "default_value" in field:
                defaults[field["name"]] = field["default_value"]
        
        # Get defaults from table-specific fields
        for field in self.get_table_schema(table_type):
            if "default_value" in field:
                defaults[field["name"]] = field["default_value"]
        
        logger.debug(f"Extracted {len(defaults)} default values for {table_type}")
        return defaults
    
    def get_sensitive_fields(self, table_type: str) -> List[str]:
        """
        Extract sensitive field names from schema.
        
        Args:
            table_type: 'balance' or 'transactions'
            
        Returns:
            List of sensitive field names
        """
        sensitive = []
        
        # Check common fields
        for field in self.get_common_schema():
            if field.get("sensitive", False):
                sensitive.append(field["name"])
        
        # Check table-specific fields
        for field in self.get_table_schema(table_type):
            if field.get("sensitive", False):
                sensitive.append(field["name"])
        
        logger.debug(f"Found {len(sensitive)} sensitive fields for {table_type}")
        return sensitive
    
    def get_required_fields(self, table_type: str) -> List[str]:
        """
        Extract required field names from schema.
        
        Args:
            table_type: 'balance' or 'transactions'
            
        Returns:
            List of required field names
        """
        required = []
        
        # Check common fields
        for field in self.get_common_schema():
            if field.get("required", False):
                required.append(field["name"])
        
        # Check table-specific fields
        for field in self.get_table_schema(table_type):
            if field.get("required", False):
                required.append(field["name"])
        
        return required
    
    def get_nullable_fields(self, table_type: str) -> List[str]:
        """
        Extract nullable field names from schema.
        
        Args:
            table_type: 'balance' or 'transactions'
            
        Returns:
            List of nullable field names
        """
        nullable = []
        
        for field in self.get_full_schema(table_type):
            if field.get("nullable", False):
                nullable.append(field["name"])
        
        return nullable