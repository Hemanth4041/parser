"""
Document Structure, Statement Level, Entry Parsing, and Transaction Details
Handles all core parsing logic for CAMT.053 documents
"""
import xml.etree.ElementTree as ET
import logging
from typing import List, Optional
from datetime import datetime
from decimal import Decimal

from CAMT.src.camt_core.models.camt_model import (
    BankToCustomerStatement, GroupHeader, Statement, Account,
    FinancialInstitution, Balance, BalanceType, TransactionSummary,
    Entry, TransactionDetails, BankTransactionCode, CreditDebitIndicator
)
from CAMT.src.camt_core.utils.camt_helper import HelperParsers
from CAMT.src.camt_core.utils.parser_utils import get_text, parse_datetime, parse_date


class DocumentParser:
    """Handles parsing of CAMT.053 document structure"""
    
    def __init__(self, namespace: str, logger: logging.Logger):
        self.namespace = namespace
        self.logger = logger
        self.helper_parsers = HelperParsers(namespace, logger)
    
    
    def parse_document(self, root: ET.Element) -> BankToCustomerStatement:
        """
        Main orchestrator - parses entire CAMT.053 document
        
        Args:
            root: Root XML element
            
        Returns:
            BankToCustomerStatement object
        """
        stmt_root = root.find(f'{self.namespace}BkToCstmrStmt')
        if stmt_root is None:
            raise ValueError("BkToCstmrStmt element not found")
        
        group_header = self._parse_group_header(stmt_root)
        statements = self._parse_statements(stmt_root)
        
        return BankToCustomerStatement(
            group_header=group_header,
            statements=statements
        )
    
    def _parse_group_header(self, root: ET.Element) -> GroupHeader:
        """
        Parses message-level metadata (GrpHdr element)
        
        Args:
            root: BkToCstmrStmt element
            
        Returns:
            GroupHeader object
        """
        grp_hdr = root.find(f'{self.namespace}GrpHdr')
        if grp_hdr is None:
            raise ValueError("Group Header not found")
        
        msg_id = get_text(grp_hdr, f'{self.namespace}MsgId', required=True)
        cre_dt = get_text(grp_hdr, f'{self.namespace}CreDtTm', required=True)
        add_inf = get_text(grp_hdr, f'{self.namespace}AddtlInf', required=False)
        
        return GroupHeader(
            message_id=msg_id,
            creation_datetime=parse_datetime(cre_dt),
            additional_info=add_inf
        )
    
    def _parse_statements(self, root: ET.Element) -> List[Statement]:
        """
        Parses all statement elements (Stmt)
        
        Args:
            root: BkToCstmrStmt element
            
        Returns:
            List of Statement objects
        """
        statements = []
        stmt_elements = root.findall(f'{self.namespace}Stmt')
        
        if not stmt_elements:
            raise ValueError("No statements found in document")
        
        for stmt_elem in stmt_elements:
            try:
                statement = self._parse_statement(stmt_elem)
                statements.append(statement)
            except Exception as e:
                self.logger.error(f"Error parsing statement: {str(e)}")
                raise
        
        return statements
    
  
    
    def _parse_statement(self, stmt: ET.Element) -> Statement:
        """
        Parses one complete statement
        
        Args:
            stmt: Stmt XML element
            
        Returns:
            Statement object
        """
        # Basic info
        stmt_id = get_text(stmt, f'{self.namespace}Id', required=True)
        elec_seq = get_text(stmt, f'{self.namespace}ElctrncSeqNb', required=True)
        cre_dt = get_text(stmt, f'{self.namespace}CreDtTm', required=True)
        creation_datetime = parse_datetime(cre_dt)
        
        # Date range - FrToDt is optional
        fr_to_dt = stmt.find(f'{self.namespace}FrToDt')
        if fr_to_dt is not None:
            from_dt = get_text(fr_to_dt, f'{self.namespace}FrDtTm', required=True)
            to_dt = get_text(fr_to_dt, f'{self.namespace}ToDtTm', required=True)
            from_datetime = parse_datetime(from_dt)
            to_datetime = parse_datetime(to_dt)
        else:
            # Use creation date as fallback
            from_datetime = creation_datetime
            to_datetime = creation_datetime
        
        # Account
        account = self._parse_account(stmt)
        
        # Balances
        balances = self._parse_balances(stmt)
        
        # Transaction summary - may or may not exist
        tx_summary_elem = stmt.find(f'{self.namespace}TxsSummry')
        if tx_summary_elem is not None:
            tx_summary = self._parse_transaction_summary(tx_summary_elem)
            entries = self._parse_entries(stmt)
        else:
            # Calculate from entries
            entries = self._parse_entries(stmt)
            tx_summary = self._calculate_transaction_summary(entries, balances)
        
        return Statement(
            id=stmt_id,
            electronic_sequence_number=int(elec_seq),
            creation_datetime=creation_datetime,
            from_datetime=from_datetime,
            to_datetime=to_datetime,
            account=account,
            balances=balances,
            transaction_summary=tx_summary,
            entries=entries
        )
    
    def _parse_account(self, stmt: ET.Element) -> Account:
        """
        Parses account information (Acct element)
        
        Args:
            stmt: Stmt XML element
            
        Returns:
            Account object
        """
        acct = stmt.find(f'{self.namespace}Acct')
        if acct is None:
            raise ValueError("Account element not found")
        
        # Account ID
        acct_id = get_text(
            acct, f'{self.namespace}Id/{self.namespace}Othr/{self.namespace}Id',
            required=True
        )
        
        # Currency
        ccy = get_text(acct, f'{self.namespace}Ccy', required=True)
        
        # Servicer/BIC - Try both BICFI (v12) and BIC (v02)
        bic = get_text(
            acct,
            f'{self.namespace}Svcr/{self.namespace}FinInstnId/{self.namespace}BICFI',
            required=False
        )
        if not bic:
            bic = get_text(
                acct,
                f'{self.namespace}Svcr/{self.namespace}FinInstnId/{self.namespace}BIC',
                required=False
            )
        
        if not bic:
            # If neither exists, use a default or raise error
            self.logger.warning("BIC/BICFI not found, using UNKNOWN")
            bic = "UNKNOWN"
        
        return Account(
            id=acct_id,
            currency=ccy,
            servicer=FinancialInstitution(bic=bic)
        )
    
    def _parse_balances(self, stmt: ET.Element) -> List[Balance]:
        """
        Parses all balance elements (Bal)
        
        Args:
            stmt: Stmt XML element
            
        Returns:
            List of Balance objects
        """
        from datetime import date
        
        balances = []
        bal_elements = stmt.findall(f'{self.namespace}Bal')
        
        for bal_elem in bal_elements:
            bal_type_code = get_text(
                bal_elem,
                f'{self.namespace}Tp/{self.namespace}CdOrPrtry/{self.namespace}Cd',
                required=True
            )
            
            bal_type_map = {
                'OPBD': BalanceType.OPENING,
                'CLBD': BalanceType.CLOSING,
                'CLAV': BalanceType.AVAILABLE
            }
            bal_type = bal_type_map.get(bal_type_code)
            if bal_type is None:
                self.logger.warning(f"Unknown balance type: {bal_type_code}")
                continue
            
            # Amount
            amt_elem = bal_elem.find(f'{self.namespace}Amt')
            if amt_elem is None or not amt_elem.text:
                raise ValueError(f"Balance amount not found for type {bal_type_code}")
            amount = Decimal(amt_elem.text)
            
            # Credit/Debit indicator
            cdt_dbt = get_text(bal_elem, f'{self.namespace}CdtDbtInd', required=True)
            cdt_dbt_ind = CreditDebitIndicator.CREDIT if cdt_dbt == 'CRDT' else CreditDebitIndicator.DEBIT
            
            # Date
            bal_date = get_text(
                bal_elem, f'{self.namespace}Dt/{self.namespace}Dt',
                required=True
            )
            
            balances.append(Balance(
                type=bal_type,
                amount=amount,
                credit_debit_indicator=cdt_dbt_ind,
                date=parse_date(bal_date)
            ))
        
        # If we don't have all 3 balance types, create dummy ones
        if len(balances) < 3:
            self.logger.warning(f"Only {len(balances)} balances found, creating dummy balances")
            existing_types = {b.type for b in balances}
            
            # Get a reference balance for dummy creation
            ref_balance = balances[0] if balances else None
            
            for bal_type in [BalanceType.OPENING, BalanceType.CLOSING, BalanceType.AVAILABLE]:
                if bal_type not in existing_types:
                    balances.append(Balance(
                        type=bal_type,
                        amount=ref_balance.amount if ref_balance else Decimal('0'),
                        credit_debit_indicator=ref_balance.credit_debit_indicator if ref_balance else CreditDebitIndicator.CREDIT,
                        date=ref_balance.date if ref_balance else date.today()
                    ))
        
        return balances
    
    def _parse_transaction_summary(self, summary: ET.Element) -> TransactionSummary:
        """
        Parse transaction summary from TxsSummry element
        
        Args:
            summary: TxsSummry XML element
            
        Returns:
            TransactionSummary object
        """
        # Total entries
        ttl_ntries = summary.find(f'{self.namespace}TtlNtries')
        if ttl_ntries is None:
            raise ValueError("Total Entries not found")
        
        total_count = int(get_text(ttl_ntries, f'{self.namespace}NbOfNtries', required=True))
        total_sum = Decimal(get_text(ttl_ntries, f'{self.namespace}Sum', required=True))
        total_net = Decimal(get_text(ttl_ntries, f'{self.namespace}TtlNetNtryAmt', required=True))
        net_ind = get_text(ttl_ntries, f'{self.namespace}CdtDbtInd', required=True)
        
        # Credit entries
        cdt_ntries = summary.find(f'{self.namespace}TtlCdtNtries')
        if cdt_ntries is None:
            raise ValueError("Total Credit Entries not found")
        
        cdt_count = int(get_text(cdt_ntries, f'{self.namespace}NbOfNtries', required=True))
        cdt_sum = Decimal(get_text(cdt_ntries, f'{self.namespace}Sum', required=True))
        
        # Debit entries
        dbt_ntries = summary.find(f'{self.namespace}TtlDbtNtries')
        if dbt_ntries is None:
            raise ValueError("Total Debit Entries not found")
        
        dbt_count = int(get_text(dbt_ntries, f'{self.namespace}NbOfNtries', required=True))
        dbt_sum = Decimal(get_text(dbt_ntries, f'{self.namespace}Sum', required=True))
        
        return TransactionSummary(
            total_entries_count=total_count,
            total_entries_sum=total_sum,
            total_net_amount=total_net,
            net_credit_debit_indicator=CreditDebitIndicator.CREDIT if net_ind == 'CRDT' else CreditDebitIndicator.DEBIT,
            total_credit_entries_count=cdt_count,
            total_credit_entries_sum=cdt_sum,
            total_debit_entries_count=dbt_count,
            total_debit_entries_sum=dbt_sum
        )
    
    def _calculate_transaction_summary(self, entries: List[Entry], balances: List[Balance]) -> TransactionSummary:
        """
        Calculate transaction summary from entries since TxsSummry is not in XML
        
        Args:
            entries: List of Entry objects
            balances: List of Balance objects (unused but kept for compatibility)
            
        Returns:
            Computed TransactionSummary object
        """
        total_credit = Decimal('0')
        total_debit = Decimal('0')
        credit_count = 0
        debit_count = 0
        
        for entry in entries:
            if entry.credit_debit_indicator == CreditDebitIndicator.CREDIT:
                total_credit += entry.amount
                credit_count += 1
            else:
                total_debit += entry.amount
                debit_count += 1
        
        total_count = credit_count + debit_count
        total_sum = total_credit + total_debit
        net_amount = total_credit - total_debit
        net_indicator = CreditDebitIndicator.CREDIT if net_amount >= 0 else CreditDebitIndicator.DEBIT
        
        return TransactionSummary(
            total_entries_count=total_count,
            total_entries_sum=total_sum,
            total_net_amount=abs(net_amount),
            net_credit_debit_indicator=net_indicator,
            total_credit_entries_count=credit_count,
            total_credit_entries_sum=total_credit,
            total_debit_entries_count=debit_count,
            total_debit_entries_sum=total_debit
        )
    
  
    def _parse_entries(self, stmt: ET.Element) -> List[Entry]:
        """
        Parses all transaction entries (Ntry elements)
        
        Args:
            stmt: Stmt XML element
            
        Returns:
            List of Entry objects
        """
        entries = []
        entry_elements = stmt.findall(f'{self.namespace}Ntry')
        
        for idx, entry_elem in enumerate(entry_elements, 1):
            try:
                entry = self._parse_entry(entry_elem, idx)
                entries.append(entry)
            except Exception as e:
                self.logger.error(f"Error parsing entry {idx}: {str(e)}")
                raise
        
        return entries
    
    def _parse_entry(self, entry: ET.Element, entry_num: int) -> Entry:
        """
        Parses one transaction entry
        
        Args:
            entry: Ntry XML element
            entry_num: Entry number for default reference generation
            
        Returns:
            Entry object
        """
        # Entry reference - may not exist
        ntry_ref = get_text(entry, f'{self.namespace}NtryRef', required=False)
        if not ntry_ref:
            ntry_ref = f"ENTRY-{entry_num}"
        
        # Amount
        amt_elem = entry.find(f'{self.namespace}Amt')
        if amt_elem is None or not amt_elem.text:
            raise ValueError("Entry amount not found")
        amount = Decimal(amt_elem.text)
        
        # Credit/Debit indicator
        cdt_dbt = get_text(entry, f'{self.namespace}CdtDbtInd', required=True)
        cdt_dbt_ind = CreditDebitIndicator.CREDIT if cdt_dbt == 'CRDT' else CreditDebitIndicator.DEBIT
        
        # Status - may not exist
        status = get_text(entry, f'{self.namespace}Sts', required=False)
        if not status:
            status = "BOOK"  # Default to booked
        
        # Dates
        booking_date = get_text(
            entry, f'{self.namespace}BookgDt/{self.namespace}Dt',
            required=True
        )
        value_date = get_text(
            entry, f'{self.namespace}ValDt/{self.namespace}Dt',
            required=True
        )
        
        # Bank transaction code
        bk_tx_cd = self._parse_bank_transaction_code(
            entry.find(f'{self.namespace}BkTxCd')
        )
        
        # Transaction details
        tx_details = self._parse_transaction_details(entry)
        
        return Entry(
            entry_reference=ntry_ref,
            amount=amount,
            credit_debit_indicator=cdt_dbt_ind,
            status=status,
            booking_date=parse_date(booking_date),
            value_date=parse_date(value_date),
            bank_transaction_code=bk_tx_cd,
            transaction_details=tx_details
        )
    
    def _parse_bank_transaction_code(self, bk_tx_cd: Optional[ET.Element]) -> BankTransactionCode:
        """
        Parse bank transaction code - handles both full and simplified structures
        
        Args:
            bk_tx_cd: BkTxCd XML element
            
        Returns:
            BankTransactionCode object
        """
        if bk_tx_cd is None:
            raise ValueError("Bank Transaction Code not found")
        
        # Try full structure first (v02 format with Domain/Family)
        domn = bk_tx_cd.find(f'{self.namespace}Domn')
        if domn is not None:
            # Full structure exists
            domain_code = get_text(domn, f'{self.namespace}Cd', required=True)
            
            fmly = domn.find(f'{self.namespace}Fmly')
            if fmly is None:
                raise ValueError("Family not found in BkTxCd")
            
            family_code = get_text(fmly, f'{self.namespace}Cd', required=True)
            sub_family_code = get_text(fmly, f'{self.namespace}SubFmlyCd', required=True)
            
            # Proprietary
            prtry = bk_tx_cd.find(f'{self.namespace}Prtry')
            if prtry is not None:
                prop_code = get_text(prtry, f'{self.namespace}Cd', required=True)
                issuer = get_text(prtry, f'{self.namespace}Issr', required=True)
            else:
                prop_code = f"{domain_code}-{family_code}-{sub_family_code}"
                issuer = "BANK"
            
            return BankTransactionCode(
                domain_code=domain_code,
                family_code=family_code,
                sub_family_code=sub_family_code,
                proprietary_code=prop_code,
                issuer=issuer
            )
        else:
            # Simplified structure - only Prtry field (v12 format)
            prop_code = get_text(bk_tx_cd, f'{self.namespace}Prtry', required=True)
            
            # Parse the proprietary code (e.g., "PMNT-CRDT" or "CARD-PURCH")
            parts = prop_code.split('-')
            domain_code = parts[0] if len(parts) > 0 else prop_code
            family_code = parts[1] if len(parts) > 1 else "UNKNOWN"
            
            return BankTransactionCode(
                domain_code=domain_code,
                family_code=family_code,
                sub_family_code="UNKNOWN",
                proprietary_code=prop_code,
                issuer="BANK"
            )
    

    
    def _parse_transaction_details(self, entry: ET.Element) -> List[TransactionDetails]:
        """
        Parses entry details (NtryDtls element)
        
        Args:
            entry: Ntry XML element
            
        Returns:
            List of TransactionDetails objects
        """
        details = []
        ntry_dtls = entry.find(f'{self.namespace}NtryDtls')
        
        if ntry_dtls is None:
            self.logger.warning("Entry Details not found, creating minimal detail")
            return [self._create_minimal_transaction_detail(entry)]
        
        tx_dtls_elements = ntry_dtls.findall(f'{self.namespace}TxDtls')
        
        if not tx_dtls_elements:
            self.logger.warning("No Transaction Details found, creating minimal detail")
            return [self._create_minimal_transaction_detail(entry)]
        
        for tx_dtls in tx_dtls_elements:
            try:
                detail = self._parse_single_transaction_detail(tx_dtls, entry)
                details.append(detail)
            except Exception as e:
                self.logger.error(f"Error parsing transaction detail: {str(e)}")
                raise
        
        return details
    
    def _create_minimal_transaction_detail(self, entry: ET.Element) -> TransactionDetails:
        """
        Create minimal transaction detail when full details not available
        
        Args:
            entry: Ntry XML element
            
        Returns:
            Minimal TransactionDetails object
        """
        from camt_core.models import TransactionReferences
        
        amt_elem = entry.find(f'{self.namespace}Amt')
        amount = Decimal(amt_elem.text) if amt_elem is not None and amt_elem.text else Decimal('0')
        
        bk_tx_cd = self._parse_bank_transaction_code(entry.find(f'{self.namespace}BkTxCd'))
        
        return TransactionDetails(
            references=TransactionReferences(),
            amount=amount,
            bank_transaction_code=bk_tx_cd,
            creditor=None,
            debtor=None,
            remittance_info=None,
            return_info=None,
            additional_info=None,
            transaction_datetime=None
        )
    
    def _parse_single_transaction_detail(self, tx_dtls: ET.Element, entry: ET.Element) -> TransactionDetails:
        """
        Parses one complete transaction detail (TxDtls element) with account information
        
        Args:
            tx_dtls: TxDtls XML element
            entry: Parent Ntry element (for fallback data)
            
        Returns:
            TransactionDetails object
        """
        # References
        refs = self.helper_parsers.parse_references(tx_dtls.find(f'{self.namespace}Refs'))
        
        # Amount - use entry amount since AmtDtls may not exist
        amt_elem = entry.find(f'{self.namespace}Amt')
        amount = Decimal(amt_elem.text) if amt_elem is not None and amt_elem.text else Decimal('0')
        
        # Bank transaction code - from entry level
        bk_tx_cd = self._parse_bank_transaction_code(entry.find(f'{self.namespace}BkTxCd'))
        
        # Related parties with account details
        rltd_parties = tx_dtls.find(f'{self.namespace}RltdPties')
        creditor = None
        debtor = None
        
        if rltd_parties is not None:
            # Parse creditor with account and agent
            creditor = self.helper_parsers.parse_related_party(
                party=rltd_parties.find(f'{self.namespace}Cdtr'),
                party_account=rltd_parties.find(f'{self.namespace}CdtrAcct'),
                party_agent=rltd_parties.find(f'{self.namespace}CdtrAgt')
            )
            
            # Parse debtor with account and agent
            debtor = self.helper_parsers.parse_related_party(
                party=rltd_parties.find(f'{self.namespace}Dbtr'),
                party_account=rltd_parties.find(f'{self.namespace}DbtrAcct'),
                party_agent=rltd_parties.find(f'{self.namespace}DbtrAgt')
            )
        
        # Related agents (if not in RltdPties)
        rltd_agts = tx_dtls.find(f'{self.namespace}RltdAgts')
        if rltd_agts is not None and creditor:
            # Update creditor agent BIC if not already set
            if not creditor.agent_bic:
                creditor.agent_bic = self.helper_parsers._parse_agent_bic(
                    rltd_agts.find(f'{self.namespace}CdtrAgt')
                )
        
        if rltd_agts is not None and debtor:
            # Update debtor agent BIC if not already set
            if not debtor.agent_bic:
                debtor.agent_bic = self.helper_parsers._parse_agent_bic(
                    rltd_agts.find(f'{self.namespace}DbtrAgt')
                )
        
        # Remittance information
        rmt_inf = self.helper_parsers.parse_remittance_info(
            tx_dtls.find(f'{self.namespace}RmtInf')
        )
        
        # Return information
        rtr_inf = self.helper_parsers.parse_return_info(
            tx_dtls.find(f'{self.namespace}RtrInf')
        )
        
        # Additional info
        add_tx_inf = get_text(tx_dtls, f'{self.namespace}AddtlTxInf')
        
        return TransactionDetails(
            references=refs,
            amount=amount,
            bank_transaction_code=bk_tx_cd,
            creditor=creditor,
            debtor=debtor,
            remittance_info=rmt_inf,
            return_info=rtr_inf,
            additional_info=add_tx_inf,
            transaction_datetime=None
        )