"""
Transforms parsed BAI2 data into structured rows for BigQuery with SWIFT transaction codes.
Modified to store SWIFT codes instead of bank names.
"""

import logging
import re
from typing import Dict, List, Any, Optional, Tuple
from BAI.src.bai2_core.models.bai2_model import Bai2File
from BAI.src.ext_data_pipeline.config import config_loader
from common import settings

logger = logging.getLogger(__name__)


def _extract_swift_code(text_to_search: str, swift_to_bank_map: Dict) -> str:
    """
    Extracts SWIFT code from text by matching against known SWIFT code prefixes.
    
    Args:
        text_to_search: Text to search for SWIFT codes
        swift_to_bank_map: Dictionary mapping SWIFT prefixes to bank names
        
    Returns:
        SWIFT code if found, empty string otherwise
    """
    if not text_to_search:
        return ""
    
    # SWIFT codes are typically 8 or 11 characters (e.g., BKNZNZ22, MACQAU2S)
    swift_pattern = r'\b([A-Z]{6}[A-Z0-9]{2}(?:[A-Z0-9]{3})?)\b'
    swift_match = re.search(swift_pattern, text_to_search.upper())
    
    if swift_match:
        swift_code = swift_match.group(1)
        
        # Verify it's a known SWIFT code by checking prefixes
        for prefix in swift_to_bank_map.keys():
            if swift_code.startswith(prefix):
                return swift_code
    
    return ""


def _extract_counterparty_info(tx, config: Dict) -> Tuple[str, str, str]:
    """
    Extracts counterparty account number, BSB, and SWIFT code from transaction.
    
    Args:
        tx: Transaction object with text, bank_reference, customer_reference
        config: Configuration dictionary containing SWIFT_TO_BANK mapping
        
    Returns:
        Tuple of (account_number, bsb, swift_code)
    """
    # Build combined text from all transaction fields
    text_parts = []
    if hasattr(tx, 'text') and tx.text:
        text_parts.append(str(tx.text))
    if hasattr(tx, 'bank_reference') and tx.bank_reference:
        text_parts.append(str(tx.bank_reference))
    if hasattr(tx, 'customer_reference') and tx.customer_reference:
        text_parts.append(str(tx.customer_reference))
    
    combined_text = " ".join(text_parts)
    
    # Get SWIFT to bank mapping
    swift_to_bank_map = {}
    if "SWIFT_TO_BANK" in config and isinstance(config["SWIFT_TO_BANK"], list) and len(config["SWIFT_TO_BANK"]) > 0:
        swift_to_bank_map = config["SWIFT_TO_BANK"][0]
    
    # Pattern 1: Account with dashes (e.g., 03-1234-5678901-123)
    account_pattern_1 = r'\b(\d{2,3}-\d{4}-\d{7,12}-\d{2,3})\b'
    # Pattern 2: Account without dashes but with proper length (e.g., 0312345678901)
    account_pattern_2 = r'\b(\d{6,16})\b'
    
    account_number = ""
    bsb = ""
    swift_code = ""
    
    # Try to find account number with dashes first
    match = re.search(account_pattern_1, combined_text)
    if match:
        account_number = match.group(1)
        # Extract BSB from first 6 digits (remove dashes)
        digits_only = re.sub(r'\D', '', account_number)
        if len(digits_only) >= 6:
            bsb = digits_only[:6]
    
    # If no dashed account found, look for numeric account
    if not account_number:
        matches = re.finditer(account_pattern_2, combined_text)
        for match in matches:
            potential_account = match.group(1)
            # Filter out likely non-account numbers (too short/long)
            if 8 <= len(potential_account) <= 16:
                account_number = potential_account
                # Extract BSB from first 6 digits
                if len(account_number) >= 6:
                    bsb = account_number[:6]
                break
    
    # Extract SWIFT code (return actual code, not mapped name)
    swift_code = _extract_swift_code(combined_text, swift_to_bank_map)
    
    return account_number, bsb, swift_code


def _extract_swift_code_from_description(type_code_obj, swift_code_patterns: Dict) -> Optional[str]:
    """
    Extracts SWIFT code by matching TypeCode description against keywords.
    
    Args:
        type_code_obj: TypeCode object with description attribute
        swift_code_patterns: Dictionary of SWIFT codes with their keyword patterns
        
    Returns:
        SWIFT code or None if not found
    """
    if not type_code_obj or not hasattr(type_code_obj, 'description'):
        return None
    
    description = type_code_obj.description
    if not description:
        return None
    
    description_upper = description.upper()
    
    # Check for keyword patterns in the description
    for swift_code, pattern_info in swift_code_patterns.items():
        keywords = pattern_info.get("keywords", [])
        for keyword in keywords:
            if keyword.upper() in description_upper:
                logger.info(f"Matched keyword '{keyword}' in description '{description}' -> SWIFT: {swift_code}")
                return swift_code
    
    return None


def _map_bai_code_to_swift(type_code: str, bai_to_swift_map: Dict) -> Optional[str]:
    """
    Maps BAI transaction type codes to SWIFT transaction codes.
    
    Args:
        type_code: BAI transaction type code (e.g., '195', '495')
        bai_to_swift_map: Mapping dictionary from config
        
    Returns:
        SWIFT transaction code or None
    """
    if not type_code or not bai_to_swift_map:
        return None
    
    # Direct lookup
    return bai_to_swift_map.get(str(type_code))


def _apply_default_values(row: Dict, schema: List[Dict]):
    """Applies default values to a row based on its schema."""
    for col in schema:
        if "default_value" in col and row.get(col["name"]) is None:
            row[col["name"]] = col["default_value"]


def _extract_bsb_from_account(account_header, group, financial_institute_swift: str, config: Dict) -> str:
    """
    Extracts BSB from account header or group header based on bank format.
    
    Args:
        account_header: Account header object with customer_account_number
        group: Group object with header.originator_id
        financial_institute_swift: SWIFT code to determine bank format
        config: Configuration dictionary containing SWIFT_TO_BANK mapping
        
    Returns:
        BSB string in format 'XXXXXX' or ' ' if not found
    """
    # Get bank name from SWIFT code for logic comparison
    swift_to_bank_map = {}
    if "SWIFT_TO_BANK" in config and isinstance(config["SWIFT_TO_BANK"], list) and len(config["SWIFT_TO_BANK"]) > 0:
        swift_to_bank_map = config["SWIFT_TO_BANK"][0]
    
    bank_name = ""
    for prefix, name in swift_to_bank_map.items():
        if financial_institute_swift.startswith(prefix):
            bank_name = name
            break
    
    # Westpac format - BSB in customer_account_number (03 record)
    if bank_name and "WESTPAC" in bank_name.upper():
        if hasattr(account_header, 'customer_account_number') and account_header.customer_account_number:
            account_num = str(account_header.customer_account_number)
            # If account number starts with 6 digits, extract BSB
            if len(account_num) >= 6 and account_num[:6].isdigit():
                return account_num[:6]
    
    # NAB format - BSB in group originator_id (02 record)
    elif bank_name and "NAB" in bank_name.upper():
        if hasattr(group, 'header') and hasattr(group.header, 'originator_id') and group.header.originator_id:
            originator = group.header.originator_id
            # Remove hyphen if present
            if '-' in originator:
                return originator.replace('-', '')
            # If 6 digits, return as is
            if len(originator) == 6 and originator.isdigit():
                return originator
            # If other format, remove any hyphens
            return originator.replace('-', '')
    
    # Auto-detect fallback: Try both approaches
    else:
        # Try Westpac format first (account number)
        if hasattr(account_header, 'customer_account_number') and account_header.customer_account_number:
            account_num = str(account_header.customer_account_number)
            if len(account_num) >= 6 and account_num[:6].isdigit():
                bsb = account_num[:6]
                logger.info(f"Auto-detected Westpac format BSB: {bsb}")
                return bsb
        
        # Fallback to NAB format (originator_id)
        if hasattr(group, 'header') and hasattr(group.header, 'originator_id') and group.header.originator_id:
            originator = group.header.originator_id
            if '-' in originator or (len(originator) == 6 and originator.isdigit()):
                if len(originator) == 6 and originator.isdigit():
                    bsb = originator
                else:
                    bsb = originator.replace('-', '')
                logger.info(f"Auto-detected NAB format BSB: {bsb}")
                return bsb
    
    return " "


def extract_financial_institute_swift(bai_file: Bai2File, config: Dict) -> str:
    """
    Extracts SWIFT code from BAI2 file header.
    
    Args:
        bai_file: Parsed BAI2 file object
        config: Configuration dictionary containing SWIFT_TO_BANK mapping
        
    Returns:
        SWIFT code or empty string if not found
    """
    # Get SWIFT to bank mapping from config
    swift_to_bank_map = {}
    if "SWIFT_TO_BANK" in config and isinstance(config["SWIFT_TO_BANK"], list) and len(config["SWIFT_TO_BANK"]) > 0:
        swift_to_bank_map = config["SWIFT_TO_BANK"][0]
    
    # Try receiver_id first (most common location for SWIFT code)
    if hasattr(bai_file, 'header') and hasattr(bai_file.header, 'receiver_id'):
        receiver_id = bai_file.header.receiver_id
        if receiver_id:
            swift_code = _extract_swift_code(receiver_id, swift_to_bank_map)
            if swift_code:
                return swift_code
    
    # Try sender_id as fallback
    if hasattr(bai_file, 'header') and hasattr(bai_file.header, 'sender_id'):
        sender_id = bai_file.header.sender_id
        if sender_id:
            swift_code = _extract_swift_code(sender_id, swift_to_bank_map)
            if swift_code:
                return swift_code
    
    return ""


def _create_base_row(org_id: str, div_id: str, 
                    account_header: Any, group_date: Any, 
                    table_type: str, bsb: str = " ", account_number: str = "",
                    financial_institute_swift: str = "") -> Dict:
    """
    Creates a base row with common fields.
    
    Args:
        org_id: Organisation business ID
        div_id: Division business ID
        account_header: Account header object
        group_date: Group date
        table_type: Type of table ("balance" or "transactions")
        bsb: Bank State Branch code
        account_number: Account number
        financial_institute_swift: SWIFT code of financial institution
        
    Returns:
        Dictionary with base row fields
    """
    base_row = {
        "organisation_biz_id": org_id,
        "division_biz_id": div_id,
        "account_number": account_number,
        "bsb": bsb,
        "financial_institute": financial_institute_swift,  # SWIFT code instead of bank name
    }

    if table_type == "balance":
        base_row.update({
            "balance_date": group_date.isoformat(),
            "currency": account_header.currency or " ",
            "_target_table": settings.BALANCE_TABLE_ID,
        })
    elif table_type == "transactions":
        base_row.update({
            "currency": account_header.currency or " ",
            "_target_table": settings.TRANSACTIONS_TABLE_ID,
        })

    return base_row


def transform_bai_to_rows(bai_file: Bai2File, org_id: str, div_id: str, 
                         config: Dict, code_map: Dict) -> List[Dict]:
    """
    Transforms a parsed BAI2 file into rows for BigQuery tables.
    
    Args:
        bai_file: Parsed BAI2 file object
        org_id: Organisation business ID
        div_id: Division business ID
        config: Configuration dictionary
        code_map: Mapping of BAI codes to BigQuery columns
        
    Returns:
        List of dictionaries representing rows for BigQuery
    """
    all_rows = []
    balance_schema = config_loader.get_table_schema(config, "balance")
    tx_schema = config_loader.get_table_schema(config, "transactions")
    
    # Extract SWIFT code once at file level (instead of bank name)
    financial_institute_swift = extract_financial_institute_swift(bai_file, config)
    logger.info(f"Extracted financial institute SWIFT code: {financial_institute_swift}")
    
    # Load SWIFT code mappings from config
    bai_to_swift_map = config.get("BAI_TO_SWIFT_MAP", {})
    swift_code_patterns = config.get("SWIFT_CODE_PATTERNS", {})

    for group in bai_file.children:
        group_date = group.header.as_of_date
        if not group_date:
            logger.warning("Skipping a group because it is missing 'as_of_date'.")
            continue

        for account in group.children:
            # Extract BSB (using SWIFT code for logic, but storing SWIFT code in DB)
            bsb = _extract_bsb_from_account(account.header, group, financial_institute_swift, config)
            account_number = account.header.customer_account_number
            
            logger.debug(f"SWIFT Code: {financial_institute_swift}, BSB: {bsb}, Account: {account_number}")
            
            # Balance Row
            balance_row = _create_base_row(
                org_id, div_id, account.header, group_date, "balance", 
                bsb, account_number, financial_institute_swift
            )
            _apply_default_values(balance_row, balance_schema)
            for summary in account.header.summary_items or []:
                code = summary.type_code.code if summary.type_code else None
                if code and code in code_map and code_map[code]["table"] == "balance":
                    rule = code_map[code]
                    balance_row[rule["bq_column"]] = getattr(summary, rule["bai_field"], None)
            all_rows.append(balance_row)

            # Transaction Rows
            for tx in getattr(account, "children", []):
                logger.debug(f"Processing transaction object: {tx}")
                logger.debug(f"Transaction type_code: {getattr(tx, 'type_code', None)}")
                logger.debug(f"Transaction amount: {getattr(tx, 'amount', None)}")
                
                tx_row = _create_base_row(
                    org_id, div_id, account.header, group_date, "transactions", 
                    bsb, account_number, financial_institute_swift
                )
                _apply_default_values(tx_row, tx_schema)

                # Extract type_code directly from transaction object first
                tx_type_code = None
                tx_type_code_obj = None
                if hasattr(tx, 'type_code') and tx.type_code:
                    tx_type_code_obj = tx.type_code  # Keep the full object for description matching
                    if hasattr(tx.type_code, 'code'):
                        tx_type_code = tx.type_code.code
                    else:
                        tx_type_code = str(tx.type_code)
                    logger.debug(f"Extracted type_code: {tx_type_code}, description: {getattr(tx.type_code, 'description', 'N/A')}")

                # Build text for pattern matching
                tx_text_parts = []
                if hasattr(tx, 'text') and tx.text:
                    tx_text_parts.append(str(tx.text))
                if hasattr(tx, 'bank_reference') and tx.bank_reference:
                    tx_text_parts.append(str(tx.bank_reference))
                if hasattr(tx, 'customer_reference') and tx.customer_reference:
                    tx_text_parts.append(str(tx.customer_reference))
                tx_text = " ".join(tx_text_parts)
                logger.debug(f"Transaction text for pattern matching: {tx_text}")
                
                # Map fields from code_map
                for code, rule in code_map.items():
                    if rule["table"] != "transactions":
                        continue
                    value = getattr(tx, rule["bai_field"], None)
                    if value is not None:
                        tx_row[rule["bq_column"]] = value
                        logger.debug(f"Mapped {rule['bai_field']}={value} to {rule['bq_column']}")
                        
                        # Set transaction type based on amount mapping
                        if rule["bq_column"] == "transaction_amount" and hasattr(tx, 'type_code'):
                            if hasattr(tx.type_code, 'transaction') and tx.type_code.transaction:
                                tx_row["transaction_type"] = "D" if tx.type_code.transaction.value == "debit" else "C"
                            else:
                                # Default to debit if transaction type not available
                                tx_row["transaction_type"] = "D"
                
                logger.debug(f"Type code for SWIFT mapping: {tx_type_code}")
                
                # SWIFT Code Extraction - Two-tier strategy
                swift_code = None
                
                # Strategy 1: Direct BAI code to SWIFT mapping
                if tx_type_code:
                    swift_code = _map_bai_code_to_swift(tx_type_code, bai_to_swift_map)
                
                # Strategy 2: Match keywords in TypeCode description
                if not swift_code and tx_type_code_obj:
                    swift_code = _extract_swift_code_from_description(tx_type_code_obj, swift_code_patterns)
                
                # Add SWIFT code to row (empty string if not found)
                tx_row["swift_transaction_code"] = swift_code if swift_code else ""
                
                if not swift_code:
                    logger.warning(f"No SWIFT code found - BAI: {tx_type_code}, Description: {getattr(tx_type_code_obj, 'description', 'N/A')}")
                
                # Extract counterparty information (returns SWIFT code instead of bank name)
                counterparty_account, counterparty_bsb, counterparty_swift = _extract_counterparty_info(tx, config)
                tx_row["counterparty_account_number"] = counterparty_account
                tx_row["counterparty_account_bsb"] = counterparty_bsb
                tx_row["counterparty_financial_institute"] = counterparty_swift  # SWIFT code
                
                # Ensure required fields are present
                tx_row["transaction_posting_date"] = getattr(tx, "posting_date", group_date).isoformat()
                tx_row["transaction_value_date"] = getattr(tx, "value_date", group_date).isoformat()
                if "transaction_amount" not in tx_row or tx_row["transaction_amount"] is None:
                    tx_row["transaction_amount"] = 0
                    tx_row["transaction_type"] = "D"
                all_rows.append(tx_row)

    balance_count = sum(1 for r in all_rows if r["_target_table"] == settings.BALANCE_TABLE_ID)
    tx_count = sum(1 for r in all_rows if r["_target_table"] == settings.TRANSACTIONS_TABLE_ID)
    logger.info(f"Prepared {len(all_rows)} rows: {balance_count} balances, {tx_count} transactions")
    return all_rows