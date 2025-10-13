"""
CAMT.053.001.02 Data Models - Updated with Account Details
"""
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime, date
from decimal import Decimal
from enum import Enum


class CreditDebitIndicator(Enum):
    """Credit/Debit indicator"""
    CREDIT = "CRDT"
    DEBIT = "DBIT"


class BalanceType(Enum):
    """Balance type codes"""
    OPENING = "OPBD"
    CLOSING = "CLDB"
    AVAILABLE = "CLAV"


class StatementType(Enum):
    """Statement type"""
    END_OF_DAY = "EODY"


@dataclass
class GroupHeader:
    """Group level header information"""
    message_id: str
    creation_datetime: datetime
    additional_info: Optional[str] = None 
    
    def __post_init__(self):
        if not self.message_id:
            raise ValueError("Message ID is required")


@dataclass
class FinancialInstitution:
    """Financial institution identification"""
    bic: str
    
    def __post_init__(self):
        if not self.bic:
            raise ValueError("BIC is required")


@dataclass
class Account:
    """Account information"""
    id: str  # BSB + Account number
    currency: str
    servicer: FinancialInstitution
    
    def __post_init__(self):
        if not self.id:
            raise ValueError("Account ID is required")
        if not self.currency or len(self.currency) != 3:
            raise ValueError("Currency must be a 3-character ISO code")


@dataclass
class Balance:
    """Balance information"""
    type: BalanceType
    amount: Decimal
    credit_debit_indicator: CreditDebitIndicator
    date: date
    
    def __post_init__(self):
        if self.amount is None:
            raise ValueError("Amount is required")
        if self.amount < 0:
            raise ValueError("Amount must be positive")


@dataclass
class TransactionSummary:
    """Transaction summary totals"""
    total_entries_count: int
    total_entries_sum: Decimal
    total_net_amount: Decimal
    net_credit_debit_indicator: CreditDebitIndicator
    total_credit_entries_count: int
    total_credit_entries_sum: Decimal
    total_debit_entries_count: int
    total_debit_entries_sum: Decimal
    
    def validate(self):
        """Validate summary calculations"""
        if self.total_entries_count != (self.total_credit_entries_count + self.total_debit_entries_count):
            raise ValueError("Total entries count mismatch")
        
        expected_sum = self.total_credit_entries_sum + self.total_debit_entries_sum
        if abs(self.total_entries_sum - expected_sum) > Decimal('0.01'):
            raise ValueError("Total entries sum mismatch")


@dataclass
class BankTransactionCode:
    """Bank transaction code structure"""
    domain_code: str
    family_code: str
    sub_family_code: str
    proprietary_code: str
    issuer: str


@dataclass
class TransactionReferences:
    """Transaction reference identifiers"""
    instruction_id: Optional[str] = None
    end_to_end_id: Optional[str] = None
    transaction_id: Optional[str] = None
    payment_info_id: Optional[str] = None
    message_id: Optional[str] = None
    account_servicer_ref: Optional[str] = None


@dataclass
class PartyAccount:
    """Account information for a party (creditor/debtor)"""
    account_id: Optional[str] = None  # Full account number
    bsb: Optional[str] = None  # BSB extracted from account_id or separately provided


@dataclass
class RelatedParty:
    """Related party information with account details"""
    name: Optional[str] = None
    account: Optional[PartyAccount] = None
    agent_bic: Optional[str] = None  # Financial institution BIC
    contact_details: Optional[dict] = None


@dataclass
class RemittanceInformation:
    """Remittance information"""
    unstructured: List[str] = field(default_factory=list)


@dataclass
class ReturnInformation:
    """Return/reversal information"""
    reason_code: Optional[str] = None
    additional_info: Optional[str] = None


@dataclass
class TransactionDetails:
    """Detailed transaction information"""
    references: TransactionReferences
    amount: Decimal
    bank_transaction_code: BankTransactionCode
    creditor: Optional[RelatedParty] = None
    debtor: Optional[RelatedParty] = None
    remittance_info: Optional[RemittanceInformation] = None
    return_info: Optional[ReturnInformation] = None
    additional_info: Optional[str] = None
    transaction_datetime: Optional[datetime] = None
    
    def __post_init__(self):
        if self.amount is None:
            raise ValueError("Transaction amount is required")


@dataclass
class Entry:
    """Transaction entry (header level)"""
    entry_reference: str
    amount: Decimal
    credit_debit_indicator: CreditDebitIndicator
    status: str
    booking_date: date
    value_date: date
    bank_transaction_code: BankTransactionCode
    transaction_details: List[TransactionDetails] = field(default_factory=list)
    
    def __post_init__(self):
        if not self.entry_reference:
            raise ValueError("Entry reference is required")
        if self.amount is None or self.amount < 0:
            raise ValueError("Amount must be positive")
        if not self.transaction_details:
            raise ValueError("At least one transaction detail is required")
    
    def is_debulked(self) -> bool:
        """Check if entry has multiple transaction details (debulked)"""
        return len(self.transaction_details) > 1


@dataclass
class Statement:
    """Statement level information"""
    id: str  # BSB + Account + Timestamp
    electronic_sequence_number: int
    creation_datetime: datetime
    from_datetime: datetime
    to_datetime: datetime
    account: Account
    balances: List[Balance]
    transaction_summary: TransactionSummary
    entries: List[Entry] = field(default_factory=list)
        
    def __post_init__(self):
        if not self.id:
            raise ValueError("Statement ID is required")
        if len(self.balances) < 1:
            raise ValueError("Statement must have at least 1 balance")
        if self.from_datetime > self.to_datetime:
            raise ValueError("From datetime must be before or equal to To datetime")
    
    def get_balance_by_type(self, balance_type: BalanceType) -> Optional[Balance]:
        """Get specific balance by type"""
        for bal in self.balances:
            if bal.type == balance_type:
                return bal
        return None
    
    def get_opening_balance(self) -> Optional[Balance]:
        """Get opening balance"""
        return self.get_balance_by_type(BalanceType.OPENING)
    
    def get_closing_balance(self) -> Optional[Balance]:
        """Get closing balance"""
        return self.get_balance_by_type(BalanceType.CLOSING)
    
    def get_available_balance(self) -> Optional[Balance]:
        """Get available balance"""
        return self.get_balance_by_type(BalanceType.AVAILABLE)


@dataclass
class BankToCustomerStatement:
    """Complete CAMT.053 document"""
    group_header: GroupHeader
    statements: List[Statement]
    
    def __post_init__(self):
        if not self.statements:
            raise ValueError("At least one statement is required")
    
    def get_statement_by_account(self, account_id: str) -> Optional[Statement]:
        """Get statement for specific account"""
        for stmt in self.statements:
            if stmt.account.id == account_id:
                return stmt
        return None