"""
Central Data Validator for BAI, CAMT, and CSV formats
Implements comprehensive validation rules with fail/warning severity levels
"""
import logging
import re
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime
from decimal import Decimal, InvalidOperation
import json

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Critical validation error that prevents file loading"""
    pass


class ValidationWarning(Exception):
    """Non-critical validation warning that allows file loading"""
    pass


class CentralValidator:
    """Centralized validator for all data formats"""
    
    # ISO 4217 Currency Codes (subset - extend as needed)
    VALID_CURRENCIES = {
        'AUD', 'USD', 'EUR', 'GBP', 'JPY', 'CNY', 'NZD', 'CAD', 'CHF', 
        'HKD', 'SGD', 'INR', 'KRW', 'BRL', 'ZAR', 'MXN', 'THB', 'MYR'
    }
    
    # Date format pattern
    DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    
    def __init__(self, schema_path: str):
        """
        Initialize validator with schema configuration
        
        Args:
            schema_path: Path to JSON schema file
        """
        self.schema = self._load_schema(schema_path)
        self.validation_errors = []
        self.validation_warnings = []
    
    def _load_schema(self, schema_path: str) -> Dict:
        """Load and parse schema JSON"""
        try:
            with open(schema_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            raise ValidationError(f"Failed to load schema from {schema_path}: {str(e)}")
    
    def reset_messages(self):
        """Clear accumulated validation messages"""
        self.validation_errors = []
        self.validation_warnings = []
    
    def _get_schema_for_table(self, table_type: str) -> List[Dict]:
        """
        Get combined schema (common + table-specific fields)
        
        Args:
            table_type: 'balance' or 'transactions'
            
        Returns:
            List of field definitions
        """
        common_fields = self.schema.get("common_fields_schema", [])
        
        if table_type == "balance":
            table_fields = self.schema.get("balance_table_schema", [])
        elif table_type == "transactions":
            table_fields = self.schema.get("transactions_table_schema", [])
        else:
            raise ValueError(f"Unknown table type: {table_type}")
        
        return common_fields + table_fields
    
    # ============================================================================
    # FIELD-LEVEL VALIDATIONS
    # ============================================================================
    
    def _validate_date_format(self, value: Any, field_name: str) -> Optional[str]:
        """
        Validate date is in YYYY-MM-DD format
        
        Returns:
            Error message if invalid, None if valid
        """
        if value is None:
            return None
        
        str_value = str(value)
        if not self.DATE_PATTERN.match(str_value):
            return f"Invalid date format in '{field_name}': '{str_value}'. Expected YYYY-MM-DD."
        
        # Check if valid date
        try:
            datetime.strptime(str_value, '%Y-%m-%d')
        except ValueError:
            return f"Invalid date value in '{field_name}': '{str_value}'."
        
        return None
    
    def _validate_currency(self, value: Any, field_name: str) -> Optional[str]:
        """
        Validate currency is a valid 3-letter ISO code
        
        Returns:
            Error message if invalid, None if valid
        """
        if value is None:
            return f"Currency field '{field_name}' must not be null."
        
        str_value = str(value).strip().upper()
        
        if len(str_value) != 3:
            return f"Invalid currency in '{field_name}': '{value}'. Must be 3-letter ISO code."
        
        if str_value not in self.VALID_CURRENCIES:
            return f"Unrecognized currency code in '{field_name}': '{str_value}'."
        
        return None
    
    def _validate_required_field(self, row: Dict, field_def: Dict) -> Optional[str]:
        """
        Validate required field is present and not null
        
        Returns:
            Error message if invalid, None if valid
        """
        field_name = field_def["name"]
        
        if field_def.get("required", False):
            if field_name not in row:
                return f"Required field missing: '{field_name}'."
            
            if field_def.get("nullable", True) is False and row[field_name] is None:
                return f"Required field '{field_name}' cannot be null."
        
        return None
    
    def _validate_data_type(self, value: Any, field_def: Dict) -> Optional[str]:
        """
        Validate field data type matches schema
        
        Returns:
            Error message if invalid, None if valid
        """
        if value is None:
            return None
        
        field_name = field_def["name"]
        expected_type = field_def.get("type", "STRING")
        
        if expected_type == "STRING":
            # Accept strings, numbers (int, float), and convert them to string
            if not isinstance(value, (str, int, float, bool)):
                return f"Invalid data type in field '{field_name}': expected STRING, got {type(value).__name__}."
        
        elif expected_type == "DATE":
            return self._validate_date_format(value, field_name)
        
        return None
    
    def _validate_at_least_one_of(self, row: Dict, field_def: Dict) -> Optional[str]:
        """
        Validate at least one field from a group is present
        CF-BAL-EX-011: At least one of closing_balance or opening_balance must exist
        
        Returns:
            Error message if invalid, None if valid
        """
        if "at_least_one_of" not in field_def:
            return None
        
        field_group = field_def["at_least_one_of"]
        field_name = field_def["name"]
        
        # Check if at least one field in the group has a value
        has_value = False
        for group_field in field_group:
            if row.get(group_field) is not None:
                has_value = True
                break
        
        if not has_value:
            return f"Validation failed: At least one of {field_group} must be provided (CF-BAL-EX-011)."
        
        return None
    
    # ============================================================================
    # ROW-LEVEL VALIDATIONS
    # ============================================================================
    
    def _validate_balance_integrity(self, row: Dict) -> Tuple[Optional[str], Optional[str]]:
        """
        Validate balance calculation integrity
        CF-BAL-EX-012: closing_balance = opening_balance + credits - debits
        
        Returns:
            Tuple of (error_message, warning_message)
        """
        # Only validate for balance table rows
        if row.get("_target_table") != "balance":
            return None, None
        
        opening = row.get("opening_balance")
        closing = row.get("closing_balance")
        
        # Step 1: Check for closing balance
        if closing is None:
            warning = "Closing balance missing. Opening balance will be used if available."
            
            # Step 2: Check for opening balance
            if opening is None:
                # Step 3: Both missing - reject
                error = "Validation failed: Both opening_balance and closing_balance are missing. File rejected (CF-BAL-EX-011)."
                return error, None
            else:
                # Opening exists, allow load with warning
                return None, warning
        
        # If both exist, validate calculation (implementation depends on having transaction data)
        # For now, just check they are valid numbers
        try:
            if opening is not None:
                Decimal(str(opening))
            Decimal(str(closing))
        except (InvalidOperation, ValueError):
            return "Invalid balance values: must be numeric.", None
        
        return None, None
    
    def validate_row(self, row: Dict, table_type: str) -> Tuple[bool, List[str], List[str]]:
        """
        Validate a single row against schema
        
        Args:
            row: Row dictionary
            table_type: 'balance' or 'transactions'
            
        Returns:
            Tuple of (is_valid, error_messages, warning_messages)
        """
        errors = []
        warnings = []
        
        schema = self._get_schema_for_table(table_type)
        
        # Track at_least_one_of groups to validate only once
        validated_groups = set()
        
        # Field-level validations
        for field_def in schema:
            field_name = field_def["name"]
            value = row.get(field_name)
            
            # Required field check
            error = self._validate_required_field(row, field_def)
            if error:
                errors.append(error)
                continue
            
            # Skip validation if field not present and not required
            if field_name not in row:
                continue
            
            # Data type validation
            error = self._validate_data_type(value, field_def)
            if error:
                errors.append(error)
            
            # Date format validation for date fields
            if field_name.endswith("_date") and value is not None:
                error = self._validate_date_format(value, field_name)
                if error:
                    errors.append(error)
            
            # Currency validation
            if field_name == "currency":
                error = self._validate_currency(value, field_name)
                if error:
                    errors.append(error)
            
            # At least one of validation (check only once per group)
            if "at_least_one_of" in field_def:
                group_key = tuple(sorted(field_def["at_least_one_of"]))
                if group_key not in validated_groups:
                    validated_groups.add(group_key)
                    error = self._validate_at_least_one_of(row, field_def)
                    if error:
                        errors.append(error)
        
        # Row-level validations
        bal_error, bal_warning = self._validate_balance_integrity(row)
        if bal_error:
            errors.append(bal_error)
        if bal_warning:
            warnings.append(bal_warning)
        
        # Check for extra fields not in schema
        schema_fields = {f["name"] for f in schema}
        extra_fields = set(row.keys()) - schema_fields - {"_target_table", "customer_id"}
        if extra_fields:
            warnings.append(f"Extra columns detected: {', '.join(extra_fields)}. Ignored during ingestion.")
        
        is_valid = len(errors) == 0
        return is_valid, errors, warnings
    
    # ============================================================================
    # FILE-LEVEL VALIDATIONS
    # ============================================================================
    
    def validate_file_structure(self, file_format: str, parsed_data: Any) -> None:
        """
        Validate file structure is correct for format
        
        Args:
            file_format: 'BAI', 'CAMT', or 'CSV'
            parsed_data: Parsed file object
            
        Raises:
            ValidationError: If structure is invalid
        """
        if file_format == "BAI":
            if not hasattr(parsed_data, 'header') or not hasattr(parsed_data, 'trailer'):
                raise ValidationError("Invalid BAI2 structure: missing header/trailer records.")
            
            if not hasattr(parsed_data, 'children') or len(parsed_data.children) == 0:
                raise ValidationError("Invalid BAI2 structure: no groups found.")
        
        elif file_format == "CAMT":
            if not hasattr(parsed_data, 'statements') or len(parsed_data.statements) == 0:
                raise ValidationError("Invalid CAMT structure: no statements found.")
        
        elif file_format == "CSV":
            if not isinstance(parsed_data, list) or len(parsed_data) == 0:
                raise ValidationError("CSV structure invalid: no data rows found.")
            
            # Check first row has headers
            if not isinstance(parsed_data[0], dict):
                raise ValidationError("CSV structure invalid: missing headers or inconsistent rows.")
    
    def validate_source_system(self, source_system: Optional[str], file_format: str, 
                               requires_parsing_logic: bool = False) -> None:
        """
        Validate source system information
        
        Args:
            source_system: Source system identifier
            file_format: File format type
            requires_parsing_logic: If True, source system is critical for parsing
            
        Raises:
            ValidationError: If source system is critical and missing
        """
        if not source_system or source_system.strip() == "":
            if requires_parsing_logic:
                raise ValidationError(
                    "Missing source system info required for format-specific parsing."
                )
            else:
                self.validation_warnings.append(
                    "Source system info missing. Tagged as 'unknown'."
                )
    
    def validate_rows_batch(self, rows: List[Dict], table_type: str) -> Tuple[List[Dict], List[str], List[str]]:
        """
        Validate a batch of rows
        
        Args:
            rows: List of row dictionaries
            table_type: 'balance' or 'transactions'
            
        Returns:
            Tuple of (valid_rows, all_errors, all_warnings)
        """
        valid_rows = []
        all_errors = []
        all_warnings = []
        
        for idx, row in enumerate(rows, 1):
            is_valid, errors, warnings = self.validate_row(row, table_type)
            
            if errors:
                for error in errors:
                    all_errors.append(f"Row {idx}: {error}")
            
            if warnings:
                for warning in warnings:
                    all_warnings.append(f"Row {idx}: {warning}")
            
            if is_valid:
                valid_rows.append(row)
        
        logger.info(f"Validation complete: {len(valid_rows)}/{len(rows)} rows valid")
        
        if all_errors:
            logger.error(f"Validation errors: {len(all_errors)}")
            for error in all_errors[:10]:  # Log first 10
                logger.error(f"  {error}")
        
        if all_warnings:
            logger.warning(f"Validation warnings: {len(all_warnings)}")
            for warning in all_warnings[:10]:  # Log first 10
                logger.warning(f"  {warning}")
        
        return valid_rows, all_errors, all_warnings
    
    def get_sensitive_fields(self, table_type: str) -> List[str]:
        """
        Get list of sensitive fields for encryption
        
        Args:
            table_type: 'balance' or 'transactions'
            
        Returns:
            List of sensitive field names
        """
        schema = self._get_schema_for_table(table_type)
        return [
            field["name"] for field in schema 
            if field.get("sensitive", False)
        ]
    def validate_schema_version(self, file_format: str, detected_version: str, 
                            supported_versions: List[str]) -> None:
        """
        Validate schema version is supported
        FAIL validation - rejects file if version not supported
        
        Args:
            file_format: 'BAI', 'CAMT', or 'CSV'
            detected_version: Version detected from file (e.g., '2', 'camt.053.001.02')
            supported_versions: List of supported version strings
            
        Raises:
            ValidationError: If version not supported
            
        Example:
            >>> validator.validate_schema_version('BAI', '2', ['2'])  # OK
            >>> validator.validate_schema_version('CAMT', 'camt.053.001.09', ['camt.053.001.02'])
            ValidationError: Unsupported CAMT schema version: camt.053.001.09
        """
        if not detected_version:
            raise ValidationError(
                f"Cannot determine {file_format} schema version. File rejected from ingestion."
            )
        
        # Normalize versions for comparison
        detected_normalized = detected_version.strip().lower()
        supported_normalized = [v.strip().lower() for v in supported_versions]
        
        if detected_normalized not in supported_normalized:
            raise ValidationError(
                f"Unsupported {file_format} schema version: {detected_version}. "
                f"Supported versions: {', '.join(supported_versions)}. "
                f"File rejected from ingestion."
            )
        
        logger.info(f"{file_format} schema version validated: {detected_version}")