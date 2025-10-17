"""
CAMT Transformer - Transforms CAMT.053 statements to BigQuery rows
Inherits from BaseTransformer
"""
import logging
from typing import List, Dict, Any
from decimal import Decimal

from common.base_transformer import BaseTransformer
from common.env_variables.settings import BALANCE_TABLE_ID, TRANSACTIONS_TABLE_ID
from CAMT.src.camt_core.models.camt_model import Statement, BankToCustomerStatement

logger = logging.getLogger(__name__)


class CAMTTransformer(BaseTransformer):
    """Transforms CAMT.053 documents into schema-compliant BigQuery rows."""
    
    def transform(self, parsed_data: BankToCustomerStatement, table_type: str = None) -> List[Dict]:
        """
        Transform CAMT.053 document into BigQuery-ready rows.
        
        Args:
            parsed_data: Parsed BankToCustomerStatement object
            table_type: Not used for CAMT (format contains both balance and transactions)
            
        Returns:
            List of transformed rows (both balance and transaction rows)
        """
        logger.info(f"Transforming {len(parsed_data.statements)} CAMT statement(s)")
        
        transformed_rows = []
        
        for idx, statement in enumerate(parsed_data.statements, start=1):
            logger.debug(f"Processing statement {idx}/{len(parsed_data.statements)}")
            
            # Transform balance data
            balance_row = self._transform_balance(statement)
            transformed_rows.append(balance_row)
            
            # Transform transaction entries
            transaction_rows = self._transform_transactions(statement)
            transformed_rows.extend(transaction_rows)
            
            logger.debug(f"Statement {idx}: 1 balance + {len(transaction_rows)} transactions")
        
        logger.info(f"Transformed {len(transformed_rows)} total rows")
        
        # Apply default values before returning
        return self.apply_default_values(transformed_rows)
    
    def _transform_balance(self, statement: Statement) -> Dict:
        """Transform statement balances into balance table row."""
        closing_bal = statement.get_closing_balance()
        opening_bal = statement.get_opening_balance()
        
        account_id = statement.account.id
        bsb = self._extract_bsb(account_id)
        
        row = self._get_common_fields()
        row.update({
            "_target_table": BALANCE_TABLE_ID,
            "account_name": statement.account.name if hasattr(statement.account, 'name') else None,
            "account_number": account_id,
            "bsb": bsb,
            "financial_institute": statement.account.servicer.bic,
            "balance_date": statement.creation_datetime.date().isoformat(),
            "currency": statement.account.currency,
            "closing_balance": int(closing_bal.amount * 100) if closing_bal else 0,
            "opening_balance": int(opening_bal.amount * 100) if opening_bal else 0,
            "overdraft_limit": None
        })
        
        return row
    
    def _transform_transactions(self, statement: Statement) -> List[Dict]:
        """Transform statement entries into transaction table rows."""
        rows = []
        
        account_id = statement.account.id
        bsb = self._extract_bsb(account_id)
        
        for entry in statement.entries:
            for detail in entry.transaction_details:
                is_credit = entry.credit_debit_indicator.value == "CRDT"
                
                # Get counterparty (opposite party)
                counterparty = detail.debtor if is_credit else detail.creditor
                
                # Extract counterparty details
                counterparty_name = None
                counterparty_account = None
                counterparty_bsb = None
                counterparty_fi = None
                
                if counterparty:
                    counterparty_name = counterparty.name if hasattr(counterparty, 'name') else None
                    
                    if counterparty.account:
                        counterparty_account = counterparty.account.account_id
                        counterparty_bsb = counterparty.account.bsb
                    
                    counterparty_fi = counterparty.agent_bic
                
                # Build transaction row
                row = self._get_common_fields()
                row.update({
                    "_target_table": TRANSACTIONS_TABLE_ID,
                    "account_name": statement.account.name if hasattr(statement.account, 'name') else None,
                    "account_number": account_id,
                    "bsb": bsb,
                    "financial_institute": statement.account.servicer.bic,
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
    
    def _extract_bsb(self, account_id: str) -> str:
        """Extract BSB from account ID."""
        if not account_id:
            return None
        
        if "-" in account_id:
            return account_id.split("-")[0]
        elif len(account_id) >= 6 and account_id[:6].isdigit():
            return account_id[:6]
        
        return None
    
    def _format_swift_code(self, btc) -> str:
        """Format bank transaction code as SWIFT code string."""
        if not btc:
            return None
        return f"{btc.domain_code}-{btc.family_code}-{btc.sub_family_code}"