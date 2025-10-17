"""
Central Data Validator for BAI, CAMT, and CSV formats
Implements comprehensive validation rules with fail/warning severity levels
Handles separate CSV files for balances and transactions
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
    
    # Date format pattern
    DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    
    # Currency pattern: 3 letters A-Z
    CURRENCY_PATTERN = re.compile(r'^[A-Z]{3}$')
    
    # Valid transaction types
    VALID_TRANSACTION_TYPES = {'CREDIT', 'DEBIT', 'C', 'D', 'CRDT', 'DBIT'}
    
    def __init__(self, schema_path: str):
        """
        Initialize validator with schema configuration
        
        Args:
            schema_path: Path to JSON schema file
        """
        self.schema = self._load_schema(schema_path)
        self.validation_errors = []
        self.validation_warnings = []
        logger.info(f"CentralValidator initialized with schema: {schema_path}")
    
    def _load_schema(self, schema_path: str) -> Dict:
        """Load and parse schema JSON"""
        try:
            with open(schema_path, 'r') as f:
                schema = json.load(f)
            logger.info(f"Schema loaded successfully from {schema_path}")
            return schema
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

    # FIELD-LEVEL VALIDATIONS
    def _validate_date_format(self, value: Any, field_name: str, is_required: bool = False) -> Optional[str]:
        """
        Validate date is in YYYY-MM-DD format
        MANDATORY CHECK - FAIL if invalid
        
        Args:
            value: Date value to validate
            field_name: Name of the field being validated
            is_required: Whether the field is required
        
        Returns:
            Error message if invalid, None if valid
        """
        if value is None:
            if is_required:
                return f"CRITICAL: Date field '{field_name}' cannot be null. File load rejected."
            return None
        
        str_value = str(value).strip()
        
        if not self.DATE_PATTERN.match(str_value):
            return f"CRITICAL: Invalid date format in '{field_name}': '{str_value}'. Expected YYYY-MM-DD. File load rejected."
        
        # Check if valid date
        try:
            datetime.strptime(str_value, '%Y-%m-%d')
        except ValueError:
            return f"CRITICAL: Invalid date value in '{field_name}': '{str_value}'. File load rejected."
        
        return None
    
    def _validate_currency(self, value: Any, field_name: str) -> Optional[str]:
        """
        Validate currency is not null and is 3 letters [A-Z]
        MANDATORY CHECK - FAIL if invalid or null
        
        Args:
            value: Currency value to validate
            field_name: Name of the field being validated
        
        Returns:
            Error message if invalid, None if valid
        """
        if value is None or str(value).strip() == "":
            return f"CRITICAL: Currency field '{field_name}' cannot be null or empty. File load rejected."
        
        str_value = str(value).strip().upper()
        
        if not self.CURRENCY_PATTERN.match(str_value):
            return f"CRITICAL: Invalid currency format in '{field_name}': '{value}'. Must be 3 uppercase letters [A-Z]. File load rejected."
        
        return None
    
    def _validate_required_field(self, row: Dict, field_def: Dict) -> Optional[str]:
        """
        Validate required field is present and not null
        MANDATORY CHECK - FAIL if missing or null based on schema definition
        Uses 'required' and 'nullable' properties from JSON schema
        
        Args:
            row: Row dictionary
            field_def: Field definition from schema
        
        Returns:
            Error message if invalid, None if valid
        """
        field_name = field_def["name"]
        
        # Check if field is marked as required in schema
        if field_def.get("required", False):
            # Field must be present
            if field_name not in row:
                return f"CRITICAL: Required field missing: '{field_name}'. File load rejected."
            
            value = row[field_name]
            
            # Check nullable property from schema
            if field_def.get("nullable", True) is False:
                # Field cannot be null or empty
                if value is None or (isinstance(value, str) and value.strip() == ""):
                    return f"CRITICAL: Required field '{field_name}' cannot be null or empty. File load rejected."
        
        return None
    
    def _validate_data_type(self, value: Any, field_def: Dict) -> Optional[str]:
        """
        Validate field data type matches schema
        
        Args:
            value: Value to validate
            field_def: Field definition from schema
        
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
            is_required = field_def.get("required", False) and not field_def.get("nullable", True)
            return self._validate_date_format(value, field_name, is_required)
        
        return None
    
    def _validate_at_least_one_of(self, row: Dict, field_def: Dict) -> Optional[str]:
        """
        Validate at least one field from a group is present
        CF-BAL-EX-011: At least one of closing_balance or opening_balance must exist
        MANDATORY CHECK - FAIL if both are null
        
        Args:
            row: Row dictionary
            field_def: Field definition from schema
        
        Returns:
            Error message if invalid, None if valid
        """
        if "at_least_one_of" not in field_def:
            return None
        
        field_group = field_def["at_least_one_of"]
        
        # Check if at least one field in the group has a non-null value
        has_value = False
        for group_field in field_group:
            value = row.get(group_field)
            if value is not None and (not isinstance(value, str) or value.strip() != ""):
                has_value = True
                break
        
        if not has_value:
            return f"CRITICAL: At least one of {field_group} must have a value. File load rejected (CF-BAL-EX-011)."
        
        return None
    
    def _validate_transaction_amount(self, value: Any, field_name: str) -> Optional[str]:
        """
        Validate transaction amount is not null and not negative
        MANDATORY CHECK - FAIL if null or negative
        
        Args:
            value: Amount value to validate
            field_name: Name of the field being validated
        
        Returns:
            Error message if invalid, None if valid
        """
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return f"CRITICAL: Transaction amount '{field_name}' cannot be null or empty. File load rejected."
        
        try:
            amount = Decimal(str(value))
            if amount < 0:
                return f"CRITICAL: Transaction amount '{field_name}' cannot be negative. Value: {value}. File load rejected."
        except (InvalidOperation, ValueError):
            return f"CRITICAL: Invalid transaction amount in '{field_name}': '{value}'. Must be a valid number. File load rejected."
        
        return None
    
    def _validate_transaction_type(self, value: Any, field_name: str) -> Optional[str]:
        """
        Validate transaction type is not null and is one of: CREDIT, DEBIT, C, D, CRDT, DBIT
        MANDATORY CHECK - FAIL if invalid
        
        Args:
            value: Transaction type value to validate
            field_name: Name of the field being validated
        
        Returns:
            Error message if invalid, None if valid
        """
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return f"CRITICAL: Transaction type '{field_name}' cannot be null or empty. File load rejected."
        
        str_value = str(value).strip().upper()
        
        if str_value not in self.VALID_TRANSACTION_TYPES:
            return f"CRITICAL: Invalid transaction type in '{field_name}': '{value}'. Must be one of: {', '.join(sorted(self.VALID_TRANSACTION_TYPES))}. File load rejected."
        
        return None
    
  
    # ROW-LEVEL VALIDATIONS
   
    
    def _validate_balance_integrity(self, row: Dict, all_rows: List[Dict] = None) -> Tuple[Optional[str], Optional[str]]:
        """
        Validate balance calculation integrity
        CF-BAL-EX-012: closing_balance = opening_balance + credits - debits
        MANDATORY CHECK - FAIL if both balances are missing, otherwise warn if mismatch
        
        NOTE: Balance calculation validation only works when transactions are available
        in the same processing context (BAI, CAMT). For CSV, transactions come in 
        separate files, so calculation validation is skipped.
        
        Args:
            row: Current balance row
            all_rows: All rows (for transaction lookup)
        
        Returns:
            Tuple of (error_message, warning_message)
        """
        # Only validate for balance table rows
        if row.get("_target_table") != "balance":
            return None, None
        
        opening = row.get("opening_balance")
        closing = row.get("closing_balance")
        
        # Check if opening balance is null or empty
        opening_is_null = opening is None or (isinstance(opening, str) and opening.strip() == "")
        
        # Check if closing balance is null or empty
        closing_is_null = closing is None or (isinstance(closing, str) and closing.strip() == "")
        
        #  CRITICAL CHECK - At least one balance must exist (CF-BAL-EX-011)
        
        if opening_is_null and closing_is_null:
            error = "CRITICAL: Both opening_balance and closing_balance are missing. File load rejected (CF-BAL-EX-011)."
            return error, None
        
      # If only one balance exists, issue warning but allow processing
       
        if opening_is_null:
            warning = "Opening balance missing. Closing balance will be used. Balance integrity check skipped."
            return None, warning
        
        if closing_is_null:
            warning = "Closing balance missing. Opening balance will be used. Balance integrity check skipped."
            return None, warning

        # Both balances exist - Check if we can perform calculation validation
        
        # Check 3a: Are rows provided?
        if all_rows is None or len(all_rows) == 0:
            logger.debug("No rows provided for balance integrity check. Skipping calculation validation.")
            return None, None
        
        # Check 3b: Are there any transactions in the dataset?
        has_any_transactions = any(r.get("_target_table") == "transactions" for r in all_rows)
        
        if not has_any_transactions:
            # Scenario: CSV balance file processed alone (no transactions available)
            logger.debug("No transactions available in current dataset. Skipping balance calculation validation.")
            return None, None
        
        # Check 3c: Are there transactions for THIS specific account and date?
        org_id = row.get("organisation_biz_id")
        account_num = row.get("account_number")
        balance_date = row.get("balance_date")
        
        # Filter transactions for this specific org_id, account_number, and date
        account_transactions = [
            r for r in all_rows 
            if r.get("_target_table") == "transactions" 
            and r.get("organisation_biz_id") == org_id
            and r.get("account_number") == account_num
            and r.get("transaction_posting_date") == balance_date
        ]
        
        if len(account_transactions) == 0:
            # Scenario: Balance exists but no transactions for this account on this date
            # This is valid (account might have had no activity)
            logger.debug(f"No transactions found for account {account_num} on {balance_date}. Balance integrity check skipped.")
            return None, None
        
        # Perform balance calculation validation
        # We have: both balances + transactions for this account
        
        try:
            opening_dec = Decimal(str(opening))
            closing_dec = Decimal(str(closing))
            
            # Calculate total credits and debits
            credits = Decimal('0')
            debits = Decimal('0')
            
            for txn in account_transactions:
                txn_type = str(txn.get("transaction_type", "")).strip().upper()
                amount_str = txn.get("transaction_amount", "0")
                
                try:
                    amount = Decimal(str(amount_str))
                    
                    # Match all credit variants: CREDIT, C, CRDT
                    if txn_type in ['CREDIT', 'C', 'CRDT']:
                        credits += amount
                    # Match all debit variants: DEBIT, D, DBIT
                    elif txn_type in ['DEBIT', 'D', 'DBIT']:
                        debits += amount
                except (InvalidOperation, ValueError):
                    # Skip invalid transaction amounts
                    logger.warning(f"Invalid transaction amount in calculation: {amount_str}")
                    continue
            
            # Verify: closing_balance = opening_balance + credits - debits
            calculated_closing = opening_dec + credits - debits
            
            # Allow small rounding differences (2 decimal places)
            if abs(calculated_closing - closing_dec) > Decimal('0.01'):
                warning = (
                    f"WARNING: Balance calculation mismatch for account {account_num}. "
                    f"Opening: {opening_dec}, Credits: {credits}, Debits: {debits}, "
                    f"Calculated Closing: {calculated_closing}, Actual Closing: {closing_dec}. "
                    f"Difference: {abs(calculated_closing - closing_dec)}. "
                    f"Please review (CF-BAL-EX-012)."
                )
                return None, warning
            
            # Balance calculation is correct
            logger.debug(f"Balance integrity validated for account {account_num}: "
                        f"Opening={opening_dec} + Credits={credits} - Debits={debits} = Closing={closing_dec}")
            
        except (InvalidOperation, ValueError) as e:
            return f"CRITICAL: Invalid balance values: {str(e)}. File load rejected.", None
        
        return None, None
    
    def validate_row(self, row: Dict, table_type: str, all_rows: List[Dict] = None) -> Tuple[bool, List[str], List[str]]:
        """
        Validate a single row against schema
        
        Args:
            row: Row dictionary
            table_type: 'balance' or 'transactions'
            all_rows: All rows for cross-row validation (optional)
            
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
            
            # Required field check (MANDATORY)
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
            
            # Date format validation for date fields (MANDATORY for required dates)
            if field_def.get("type") == "DATE":
                is_required = field_def.get("required", False) and not field_def.get("nullable", True)
                error = self._validate_date_format(value, field_name, is_required)
                if error:
                    errors.append(error)
            
            # Currency validation (MANDATORY)
            if field_name == "currency":
                error = self._validate_currency(value, field_name)
                if error:
                    errors.append(error)
            
            # Transaction amount validation (MANDATORY for transactions)
            if field_name == "transaction_amount" and table_type == "transactions":
                error = self._validate_transaction_amount(value, field_name)
                if error:
                    errors.append(error)
            
            # Transaction type validation (MANDATORY for transactions)
            if field_name == "transaction_type" and table_type == "transactions":
                error = self._validate_transaction_type(value, field_name)
                if error:
                    errors.append(error)
            
            # At least one of validation (check only once per group) (MANDATORY)
            if "at_least_one_of" in field_def:
                group_key = tuple(sorted(field_def["at_least_one_of"]))
                if group_key not in validated_groups:
                    validated_groups.add(group_key)
                    error = self._validate_at_least_one_of(row, field_def)
                    if error:
                        errors.append(error)
        
        # Row-level validations
        bal_error, bal_warning = self._validate_balance_integrity(row, all_rows)
        if bal_error:
            errors.append(bal_error)
        if bal_warning:
            warnings.append(bal_warning)
        
        # Check for extra fields not in schema
        schema_fields = {f["name"] for f in schema}
        extra_fields = set(row.keys()) - schema_fields - {"_target_table", "customer_id"}
        if extra_fields:
            warnings.append(f"Extra columns detected: {', '.join(sorted(extra_fields))}. Ignored during ingestion.")
        
        is_valid = len(errors) == 0
        return is_valid, errors, warnings
    
    # FILE-LEVEL VALIDATIONS
   
    
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
                    f"Missing source system info required for {file_format} format-specific parsing."
                )
            else:
                self.validation_warnings.append(
                    f"Source system info missing for {file_format}. Tagged as 'unknown'."
                )
    
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
            is_valid, errors, warnings = self.validate_row(row, table_type, rows)
            
            if errors:
                for error in errors:
                    all_errors.append(f"Row {idx}: {error}")
            
            if warnings:
                for warning in warnings:
                    all_warnings.append(f"Row {idx}: {warning}")
            
            if is_valid:
                valid_rows.append(row)
        
        logger.info(f"Validation complete: {len(valid_rows)}/{len(rows)} rows valid for table '{table_type}'")
        
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
        sensitive = [
            field["name"] for field in schema 
            if field.get("sensitive", False)
        ]
        logger.debug(f"Found {len(sensitive)} sensitive fields for table '{table_type}': {sensitive}")
        return sensitive