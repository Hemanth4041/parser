import logging
from typing import List, Dict
from CAMT.src.camt_core.models.camt_model import Statement, BankToCustomerStatement
from decimal import Decimal

logger = logging.getLogger(__name__)


class Transformer:
    """Transforms parsed CAMT objects into schema-compliant rows with BigQuery target tables."""

    def __init__(self, org_id: str, div_id: str, cust_id: str, 
                 balance_table: str, transactions_table: str):
        self.org_id = org_id
        self.div_id = div_id
        self.cust_id = cust_id
        self.balance_table = balance_table
        self.transactions_table = transactions_table

    def transform(self, document: BankToCustomerStatement) -> List[Dict]:
        """
        Transform entire CAMT.053 document into BigQuery-ready rows.
        
        This is the main transformation method that processes all statements
        in the document and returns a flat list of rows ready for validation
        and loading into BigQuery.
        
        Args:
            document: Parsed CAMT.053 BankToCustomerStatement object containing statements
            
        Returns:
            List of dictionaries representing rows for BigQuery tables
            (includes both balance rows and transaction rows)
        """
        transformed_rows: List[Dict] = []
        
        logger.info(f"Transforming {len(document.statements)} statement(s)...")
        
        for idx, statement in enumerate(document.statements, start=1):
            logger.debug(f"Processing statement {idx}/{len(document.statements)}")
            
            # Transform balance data for this statement
            balance_row = self.transform_balances(statement)
            transformed_rows.append(balance_row)
            
            # Transform transaction entries for this statement
            transaction_rows = self.transform_entries(statement)
            transformed_rows.extend(transaction_rows)
            
            logger.debug(f"Statement {idx}: 1 balance + {len(transaction_rows)} transactions")
        
        logger.info(f"Transformation complete: {len(transformed_rows)} total rows")
        return transformed_rows

    def _extract_bsb(self, account_id: str) -> str:
        """Extract BSB from account ID"""
        if not account_id:
            return None
            
        if "-" in account_id:
            # Format: "032-999999994"
            return account_id.split("-")[0]
        elif len(account_id) >= 6 and account_id[:6].isdigit():
            # Format: "032999999994" - extract first 3 digits as BSB
            return account_id[:6]
        
        return None

    def _get_common_fields(self, statement: Statement) -> Dict[str, str]:
        """Get common fields to be added to all rows (balance and transaction tables)"""
        account_id = statement.account.id
        bsb = self._extract_bsb(account_id)
        
        return {
            "customer_id": self.cust_id,  # For KMS encryption lookup
            "organisation_biz_id": self.org_id,
            "division_biz_id": self.div_id,
            "source_system": "external",
            "account_name": statement.account.name if hasattr(statement.account, 'name') else None,
            "account_number": account_id,
            "bsb": bsb,
            "financial_institute": statement.account.servicer.bic
        }

    def transform_balances(self, statement: Statement) -> Dict[str, str]:
        """
        Transform statement balances -> balance table row with common fields
        
        Args:
            statement: Parsed CAMT Statement object
            
        Returns:
            Dictionary with all fields (common + balance-specific) for balance table
        """
        closing_bal = statement.get_closing_balance()
        opening_bal = statement.get_opening_balance()
        
        # Start with common fields
        row = self._get_common_fields(statement)
        
        # Add balance-specific fields ONLY
        row.update({
            "_target_table": self.balance_table,
            "balance_date": statement.creation_datetime.date().isoformat(),
            "currency": statement.account.currency,
            "closing_balance": int(closing_bal.amount * 100) if closing_bal else 0,
            "opening_balance": int(opening_bal.amount * 100) if opening_bal else 0,
            "overdraft_limit": None
        })
        
        return row

    def transform_entries(self, statement: Statement) -> List[Dict[str, str]]:
        """
        Transform entries -> transaction table rows with counterparty account details
        
        Args:
            statement: Parsed CAMT Statement object
            
        Returns:
            List of dictionaries with all fields (common + transaction-specific) for transactions table
        """
        rows = []
        common_fields = self._get_common_fields(statement)
        
        for entry in statement.entries:
            for detail in entry.transaction_details:
                is_credit = entry.credit_debit_indicator.value == "CRDT"
                
                # Get the counterparty (opposite party)
                # If it's a credit, the debtor is the counterparty
                # If it's a debit, the creditor is the counterparty
                counterparty = detail.debtor if is_credit else detail.creditor
                
                # Extract counterparty account details
                counterparty_account = None
                counterparty_bsb = None
                counterparty_fi = None
                counterparty_name = None
                
                if counterparty:
                    # Get counterparty name
                    counterparty_name = counterparty.name if hasattr(counterparty, 'name') else None
                    
                    # Get account details if available
                    if counterparty.account:
                        counterparty_account = counterparty.account.account_id
                        counterparty_bsb = counterparty.account.bsb
                    
                    # Get financial institution BIC
                    counterparty_fi = counterparty.agent_bic
                
                # Start with common fields
                row = common_fields.copy()
                
                # Add transaction-specific fields ONLY (including counterparty fields)
                row.update({
                    "_target_table": self.transactions_table,
                    "counterparty_name": counterparty_name,
                    "counterparty_account_number": counterparty_account,
                    "counterparty_account_bsb": counterparty_bsb,
                    "counterparty_financial_institute": counterparty_fi,
                    "transaction_posting_date": entry.booking_date.isoformat(),
                    "transaction_value_date": entry.value_date.isoformat(),
                    "currency": statement.account.currency,
                    "transaction_amount": int(detail.amount * 100),
                    "transaction_type": entry.credit_debit_indicator.value,
                    "swift_transaction_code": self._format_swift_code(detail.bank_transaction_code)
                })
                
                rows.append(row)
        
        return rows
    
    def _format_swift_code(self, btc) -> str:
        """Format bank transaction code as SWIFT code string"""
        if not btc:
            return None
        return f"{btc.domain_code}-{btc.family_code}-{btc.sub_family_code}"
    
    def apply_default_values(self, rows: List[Dict], validator) -> List[Dict]:
        """
        Applies default values from schema to all rows.
        Forces fields with default_value to that value, regardless of existing data.
        
        Args:
            rows: List of row dictionaries
            validator: SchemaValidator instance with loaded schema
            
        Returns:
            List of rows with default values applied
        """
        # Collect default values separately for balance and transaction schemas
        balance_defaults = {}
        transaction_defaults = {}
        
        # Get defaults from balance_table_schema (excluding common fields to avoid duplicates)
        for field_def in validator.schema.get("balance_table_schema", []):
            field_name = field_def["name"]
            if "default_value" in field_def:
                balance_defaults[field_name] = field_def["default_value"]
        
        # Get defaults from transactions_table_schema (excluding common fields to avoid duplicates)
        for field_def in validator.schema.get("transactions_table_schema", []):
            field_name = field_def["name"]
            if "default_value" in field_def:
                transaction_defaults[field_name] = field_def["default_value"]
        
        if not balance_defaults and not transaction_defaults:
            logger.info("No default values defined in schema")
            return rows
        
        # Apply defaults to rows based on their target table
        processed_rows = []
        for row in rows:
            processed_row = row.copy()
            target_table = row.get("_target_table")
            
            # Apply appropriate defaults based on target table
            if target_table == self.balance_table and balance_defaults:
                for field_name, default_value in balance_defaults.items():
                    processed_row[field_name] = default_value
            elif target_table == self.transactions_table and transaction_defaults:
                for field_name, default_value in transaction_defaults.items():
                    processed_row[field_name] = default_value
            
            processed_rows.append(processed_row)
        
        logger.info(f"Applied default values to {len(processed_rows)} rows")
        return processed_rows