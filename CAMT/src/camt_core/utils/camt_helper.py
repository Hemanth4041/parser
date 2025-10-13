"""
Helper Parsers - Updated to extract account details
Specialized parsers for specific data structures like references, parties, remittance info
"""
import xml.etree.ElementTree as ET
import logging
from typing import Optional, List

from CAMT.src.camt_core.models.camt_model import (
    TransactionReferences, RelatedParty, PartyAccount, 
    RemittanceInformation, ReturnInformation
)
from CAMT.src.camt_core.utils.parser_utils import get_text


class HelperParsers:
    """Collection of helper parsing methods for specific data structures"""
    
    def __init__(self, namespace: str, logger: logging.Logger):
        self.namespace = namespace
        self.logger = logger
    
    def parse_references(self, refs: Optional[ET.Element]) -> TransactionReferences:
        """
        Parses reference IDs (Refs element)
        
        Args:
            refs: Refs XML element (optional)
            
        Returns:
            TransactionReferences object with all optional IDs
        """
        if refs is None:
            return TransactionReferences()
        
        return TransactionReferences(
            instruction_id=get_text(refs, f'{self.namespace}InstrId'),
            end_to_end_id=get_text(refs, f'{self.namespace}EndToEndId'),
            transaction_id=get_text(refs, f'{self.namespace}TxId'),
            payment_info_id=get_text(refs, f'{self.namespace}PmtInfId'),
            message_id=get_text(refs, f'{self.namespace}MsgId'),
            account_servicer_ref=get_text(refs, f'{self.namespace}AcctSvcrRef')
        )
    
    def _parse_party_account(self, acct: Optional[ET.Element]) -> Optional[PartyAccount]:
        """
        Parse account information from Acct element
        
        Args:
            acct: Account XML element (CdtrAcct or DbtrAcct)
            
        Returns:
            PartyAccount object or None if no account data found
        """
        if acct is None:
            return None
        
        # Try to get IBAN
        iban = get_text(acct, f'{self.namespace}Id/{self.namespace}IBAN')
        
        # Try to get Other ID (BSB + Account format)
        other_id = get_text(acct, f'{self.namespace}Id/{self.namespace}Othr/{self.namespace}Id')
        
        account_id = iban or other_id
        
        if not account_id:
            return None
        
        # Extract BSB if present in account_id
        bsb = None
        if account_id:
            if "-" in account_id:
                # Format: "032-999999994"
                bsb = account_id.split("-")[0]
            elif len(account_id) >= 3 and account_id[:3].isdigit():
                # Format: "032999999994" - extract first 3 digits as BSB
                bsb = account_id[:3]
        
        return PartyAccount(account_id=account_id, bsb=bsb)
    
    def _parse_agent_bic(self, agt: Optional[ET.Element]) -> Optional[str]:
        """
        Parse BIC from agent element (CdtrAgt or DbtrAgt)
        
        Args:
            agt: Agent XML element
            
        Returns:
            BIC code or None
        """
        if agt is None:
            return None
        
        # Try BICFI first (v12), then BIC (v02)
        bic = get_text(agt, f'{self.namespace}FinInstnId/{self.namespace}BICFI')
        if not bic:
            bic = get_text(agt, f'{self.namespace}FinInstnId/{self.namespace}BIC')
        
        return bic
    
    def parse_related_party(self, party: Optional[ET.Element], 
                           party_account: Optional[ET.Element] = None,
                           party_agent: Optional[ET.Element] = None) -> Optional[RelatedParty]:
        """
        Parses party information with account details (Cdtr/Dbtr element)
        
        Args:
            party: Creditor or Debtor XML element (optional)
            party_account: CdtrAcct or DbtrAcct XML element (optional)
            party_agent: CdtrAgt or DbtrAgt XML element (optional)
            
        Returns:
            RelatedParty object or None if party element not found
        """
        if party is None:
            return None
        
        name = get_text(party, f'{self.namespace}Nm')
        
        contact_dtls = party.find(f'{self.namespace}CtctDtls')
        contact_info = None
        if contact_dtls is not None:
            contact_info = {
                'email': get_text(contact_dtls, f'{self.namespace}EmailAdr'),
                'other': get_text(contact_dtls, f'{self.namespace}Othr')
            }
        
        # Parse account details
        account = self._parse_party_account(party_account)
        
        # Parse agent BIC
        agent_bic = self._parse_agent_bic(party_agent)
        
        return RelatedParty(
            name=name, 
            account=account,
            agent_bic=agent_bic,
            contact_details=contact_info
        )
    
    def parse_remittance_info(self, rmt_inf: Optional[ET.Element]) -> Optional[RemittanceInformation]:
        """
        Parses remittance information (RmtInf element)
        
        Args:
            rmt_inf: RmtInf XML element (optional)
            
        Returns:
            RemittanceInformation object or None if no unstructured text found
        """
        if rmt_inf is None:
            return None
        
        unstructured = []
        ustrd_elements = rmt_inf.findall(f'{self.namespace}Ustrd')
        for ustrd in ustrd_elements:
            if ustrd.text:
                unstructured.append(ustrd.text)
        
        if not unstructured:
            return None
        
        return RemittanceInformation(unstructured=unstructured)
    
    def parse_return_info(self, rtr_inf: Optional[ET.Element]) -> Optional[ReturnInformation]:
        """
        Parses return/reversal information (RtrInf element)
        
        Args:
            rtr_inf: RtrInf XML element (optional)
            
        Returns:
            ReturnInformation object or None if no return data found
        """
        if rtr_inf is None:
            return None
        
        reason_code = get_text(
            rtr_inf, f'{self.namespace}Rsn/{self.namespace}Cd'
        )
        additional_info = get_text(rtr_inf, f'{self.namespace}AddtlInf')
        
        if not reason_code and not additional_info:
            return None
        
        return ReturnInformation(
            reason_code=reason_code,
            additional_info=additional_info
        )