"""
BAI Transformer - Transforms BAI2 data to BigQuery rows
Inherits from BaseTransformer
"""
import logging
import re
from typing import List, Dict, Any, Optional, Tuple

from common.base_transformer import BaseTransformer
from common.env_variables.settings import BALANCE_TABLE_ID, TRANSACTIONS_TABLE_ID
from BAI.src.bai2_core.models.bai2_model import Bai2File

logger = logging.getLogger(__name__)


class BAITransformer(BaseTransformer):
    """Transforms BAI2 files into schema-compliant BigQuery rows."""
    
    def __init__(self, org_id: str, div_id: str, config_loader, bank_id: str = None):
        """
        Initialize BAI transformer with bank-specific configuration.
        
        Args:
            org_id: Organisation business ID
            div_id: Division business ID
            config_loader: ConfigLoader instance
            bank_id: Bank identifier for mapping lookups (optional)
        """
        super().__init__(org_id, div_id, config_loader)
        self.bank_id = bank_id
        
        # Load bank-specific configuration
        self.config = config_loader.config
        self.bai_to_swift_map = self.config.get("BAI_TO_SWIFT_MAP", {})
        self.swift_code_patterns = self.config.get("SWIFT_CODE_PATTERNS", {})
        self.code_map = self._load_code_mappings()
        
        logger.info(f"BAITransformer initialized with bank_id: {bank_id}")
    
    def transform(self, parsed_data: Bai2File, table_type: str = None) -> List[Dict]:
        """
        Transform BAI2 file into BigQuery-ready rows.
        
        Args:
            parsed_data: Parsed Bai2File object
            table_type: Not used for BAI (format contains both balance and transactions)
            
        Returns:
            List of transformed rows (both balance and transaction rows)
        """
        logger.info("Transforming BAI2 file")
        
        all_rows = []
        
        # Extract SWIFT code at file level
        financial_institute_swift = self._extract_financial_institute_swift(parsed_data)
        logger.info(f"Financial institute SWIFT: {financial_institute_swift}")
        
        # Process each group
        for group in parsed_data.children:
            group_date = group.header.as_of_date
            if not group_date:
                logger.warning("Skipping group without as_of_date")
                continue
            
            # Process each account
            for account in group.children:
                account_number = account.header.customer_account_number
                bsb = self._extract_bsb_from_account(
                    account.header, group, financial_institute_swift
                )
                
                logger.debug(f"Processing account: {account_number}, BSB: {bsb}")
                
                # Create balance row
                balance_row = self._create_balance_row(
                    account, group_date, bsb, account_number, financial_institute_swift
                )
                all_rows.append(balance_row)
                
                # Create transaction rows
                transaction_rows = self._create_transaction_rows(
                    account, group_date, bsb, account_number, financial_institute_swift
                )
                all_rows.extend(transaction_rows)
        
        balance_count = sum(1 for r in all_rows if r["_target_table"] == BALANCE_TABLE_ID)
        tx_count = sum(1 for r in all_rows if r["_target_table"] == TRANSACTIONS_TABLE_ID)
        logger.info(f"Transformed {len(all_rows)} rows: {balance_count} balances, {tx_count} transactions")
        
        # Apply default values before returning
        return self.apply_default_values(all_rows)
    
    def _load_code_mappings(self) -> Dict:
        """Load bank-specific code mappings or default mappings."""
        # Try to find bank-specific mappings
        if self.bank_id:
            for mapping in self.config.get("mappings", []):
                if mapping.get("bank_id") == self.bank_id:
                    return {m["bai_code"]: m for m in mapping["mappings"]}
        
        # Fallback to default mappings
        default_mappings = self.config.get("bank_id_default_typecodes", [])
        return {m["bai_code"]: m for m in default_mappings}
    
    def _create_balance_row(self, account, group_date, bsb, account_number, fi_swift) -> Dict:
        """Create balance table row from account summary."""
        row = self._get_common_fields()
        row.update({
            "_target_table": BALANCE_TABLE_ID,
            "account_number": account_number,
            "bsb": bsb,
            "financial_institute": fi_swift,
            "balance_date": group_date.isoformat(),
            "currency": account.header.currency or " "
        })
        
        # Map summary items to balance fields
        for summary in account.header.summary_items or []:
            code = summary.type_code.code if summary.type_code else None
            if code and code in self.code_map:
                rule = self.code_map[code]
                if rule["table"] == "balance":
                    row[rule["bq_column"]] = getattr(summary, rule["bai_field"], None)
        
        return row
    
    def _create_transaction_rows(self, account, group_date, bsb, account_number, fi_swift) -> List[Dict]:
        """Create transaction table rows from account transactions."""
        rows = []
        
        for tx in getattr(account, "children", []):
            row = self._get_common_fields()
            row.update({
                "_target_table": TRANSACTIONS_TABLE_ID,
                "account_number": account_number,
                "bsb": bsb,
                "financial_institute": fi_swift,
                "currency": account.header.currency or " ",
                "transaction_posting_date": getattr(tx, "posting_date", group_date).isoformat(),
                "transaction_value_date": getattr(tx, "value_date", group_date).isoformat()
            })
            
            # Extract type code
            tx_type_code = None
            tx_type_code_obj = None
            if hasattr(tx, 'type_code') and tx.type_code:
                tx_type_code_obj = tx.type_code
                tx_type_code = tx.type_code.code if hasattr(tx.type_code, 'code') else str(tx.type_code)
            
            # Map transaction fields
            for code, rule in self.code_map.items():
                if rule["table"] != "transactions":
                    continue
                value = getattr(tx, rule["bai_field"], None)
                if value is not None:
                    row[rule["bq_column"]] = value
                    
                    # Set transaction type
                    if rule["bq_column"] == "transaction_amount" and hasattr(tx, 'type_code'):
                        if hasattr(tx.type_code, 'transaction') and tx.type_code.transaction:
                            row["transaction_type"] = "D" if tx.type_code.transaction.value == "debit" else "C"
                        else:
                            row["transaction_type"] = "D"
            
            # Extract SWIFT code
            swift_code = self._extract_swift_code(tx_type_code, tx_type_code_obj)
            row["swift_transaction_code"] = swift_code or ""
            
            # Extract counterparty information
            counterparty_account, counterparty_bsb, counterparty_swift = self._extract_counterparty_info(tx)
            row["counterparty_account_number"] = counterparty_account
            row["counterparty_account_bsb"] = counterparty_bsb
            row["counterparty_financial_institute"] = counterparty_swift
            
            # Ensure required fields
            if "transaction_amount" not in row or row["transaction_amount"] is None:
                row["transaction_amount"] = 0
                row["transaction_type"] = "D"
            
            rows.append(row)
        
        return rows
    
    def _extract_swift_code(self, type_code: str, type_code_obj) -> Optional[str]:
        """Extract SWIFT code using two-tier strategy."""
        # Strategy 1: Direct BAI to SWIFT mapping
        if type_code and type_code in self.bai_to_swift_map:
            return self.bai_to_swift_map[type_code]
        
        # Strategy 2: Match keywords in description
        if type_code_obj and hasattr(type_code_obj, 'description'):
            description = type_code_obj.description
            if description:
                description_upper = description.upper()
                for swift_code, pattern_info in self.swift_code_patterns.items():
                    keywords = pattern_info.get("keywords", [])
                    for keyword in keywords:
                        if keyword.upper() in description_upper:
                            logger.info(f"Matched keyword '{keyword}' -> SWIFT: {swift_code}")
                            return swift_code
        
        return None
    
    def _extract_counterparty_info(self, tx) -> Tuple[str, str, str]:
        """Extract counterparty account number, BSB, and SWIFT code."""
        # Build combined text
        text_parts = []
        for attr in ['text', 'bank_reference', 'customer_reference']:
            if hasattr(tx, attr) and getattr(tx, attr):
                text_parts.append(str(getattr(tx, attr)))
        combined_text = " ".join(text_parts)
        
        # Get SWIFT mapping
        swift_to_bank_map = self.config.get("SWIFT_TO_BANK", [{}])[0] if self.config.get("SWIFT_TO_BANK") else {}
        
        # Extract account number patterns
        account_pattern_1 = r'\b(\d{2,3}-\d{4}-\d{7,12}-\d{2,3})\b'
        account_pattern_2 = r'\b(\d{6,16})\b'
        
        account_number = ""
        bsb = ""
        
        # Try dashed format first
        match = re.search(account_pattern_1, combined_text)
        if match:
            account_number = match.group(1)
            digits_only = re.sub(r'\D', '', account_number)
            if len(digits_only) >= 6:
                bsb = digits_only[:6]
        
        # Try numeric format
        if not account_number:
            matches = re.finditer(account_pattern_2, combined_text)
            for match in matches:
                potential_account = match.group(1)
                if 8 <= len(potential_account) <= 16:
                    account_number = potential_account
                    if len(account_number) >= 6:
                        bsb = account_number[:6]
                    break
        
        # Extract SWIFT code
        swift_code = self._extract_swift_from_text(combined_text, swift_to_bank_map)
        
        return account_number, bsb, swift_code
    
    def _extract_swift_from_text(self, text: str, swift_map: Dict) -> str:
        """Extract SWIFT code from text."""
        if not text:
            return ""
        
        swift_pattern = r'\b([A-Z]{6}[A-Z0-9]{2}(?:[A-Z0-9]{3})?)\b'
        match = re.search(swift_pattern, text.upper())
        
        if match:
            swift_code = match.group(1)
            for prefix in swift_map.keys():
                if swift_code.startswith(prefix):
                    return swift_code
        
        return ""
    
    def _extract_financial_institute_swift(self, bai_file: Bai2File) -> str:
        """Extract SWIFT code from BAI2 file header."""
        swift_map = self.config.get("SWIFT_TO_BANK", [{}])[0] if self.config.get("SWIFT_TO_BANK") else {}
        
        # Try receiver_id first
        if hasattr(bai_file.header, 'receiver_id') and bai_file.header.receiver_id:
            swift = self._extract_swift_from_text(bai_file.header.receiver_id, swift_map)
            if swift:
                return swift
        
        # Try sender_id
        if hasattr(bai_file.header, 'sender_id') and bai_file.header.sender_id:
            swift = self._extract_swift_from_text(bai_file.header.sender_id, swift_map)
            if swift:
                return swift
        
        return ""
    
    def _extract_bsb_from_account(self, account_header, group, fi_swift: str) -> str:
        """Extract BSB from account or group header based on bank format."""
        swift_map = self.config.get("SWIFT_TO_BANK", [{}])[0] if self.config.get("SWIFT_TO_BANK") else {}
        
        # Get bank name for logic
        bank_name = ""
        for prefix, name in swift_map.items():
            if fi_swift.startswith(prefix):
                bank_name = name
                break
        
        # Westpac format - BSB in account number
        if bank_name and "WESTPAC" in bank_name.upper():
            if hasattr(account_header, 'customer_account_number') and account_header.customer_account_number:
                account_num = str(account_header.customer_account_number)
                if len(account_num) >= 6 and account_num[:6].isdigit():
                    return account_num[:6]
        
        # NAB format - BSB in originator_id
        elif bank_name and "NAB" in bank_name.upper():
            if hasattr(group.header, 'originator_id') and group.header.originator_id:
                originator = group.header.originator_id.replace('-', '')
                if len(originator) == 6 and originator.isdigit():
                    return originator
        
        # Auto-detect fallback
        else:
            if hasattr(account_header, 'customer_account_number') and account_header.customer_account_number:
                account_num = str(account_header.customer_account_number)
                if len(account_num) >= 6 and account_num[:6].isdigit():
                    return account_num[:6]
            
            if hasattr(group.header, 'originator_id') and group.header.originator_id:
                originator = group.header.originator_id.replace('-', '')
                if len(originator) == 6 and originator.isdigit():
                    return originator
        
        return " "