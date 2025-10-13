from collections import OrderedDict
from BAI.src.bai2_core.config.config_loader import IGNORED_SUMMARY_CODES
from BAI.src.bai2_core.constants import GroupStatus, AsOfDateModifier, FundsType
from BAI.src.bai2_core.exceptions.exceptions import ParsingException, NotSupportedYetException, \
    IntegrityException
from BAI.src.bai2_core.models.bai2_model import \
    Bai2File, Bai2FileHeader, Bai2FileTrailer, \
    Group, GroupHeader, GroupTrailer, \
    AccountIdentifier, AccountTrailer, Account, \
    TransactionDetail, Summary
from BAI.src.bai2_core.utils.date_utils import parse_date, parse_time, parse_type_code


"""
parse.py
---------
This module contains parser classes to read and validate BAI2 format files.
Each parser handles a different level of the hierarchical BAI2 structure:
File → Group → Account → Transactions.

Core responsibilities:
- Parsing raw records into structured models (`bai2_model`).
- Validating integrity constraints such as record counts and control totals.
- Handling availability schedules and funds types.
- Providing reusable base parser classes for consistency.

Classes:
    BaseParser              – Base for all parsers, provides shared logic.
    BaseSectionParser       – Base for multi-record sections (header, children, trailer).
    BaseSingleParser        – Base for single-record parsers.
    TransactionDetailParser – Parses transaction (16) records.
    AccountIdentifierParser – Parses account header (03) records.
    AccountTrailerParser    – Parses account trailer (49) records.
    AccountParser           – Parses full account sections.
    GroupHeaderParser       – Parses group header (02) records.
    GroupTrailerParser      – Parses group trailer (98) records.
    GroupParser             – Parses group sections.
    Bai2FileHeaderParser    – Parses file header (01) records.
    Bai2FileTrailerParser   – Parses file trailer (99) records.
    Bai2FileParser          – Parses the complete BAI2 file.
"""


class BaseParser:
    """Base class for all parsers. Handles initialization and generic parsing logic."""
    
    model = None
    child_parser_class = None

    def __init__(self, iterator, check_integrity=True):
        """
        Initialize the parser.

        Args:
            iterator: An iterator providing access to BAI2 records.
            check_integrity (bool): Whether to validate integrity constraints.
        """
        super().__init__()
        self._iter = iterator
        self.check_integrity = check_integrity
        self.child_parser = self._get_parser('child')

    def _check_record_code(self, expected_code):
        """Ensure the current record has the expected BAI2 record code."""
        if self._iter.current_record.code != expected_code:
            raise ParsingException(
                f'Expected {expected_code}, got {self._iter.current_record.code} instead'
            )

    def _get_parser(self, parser_type):
        """Return a child/header/trailer parser instance if defined."""
        name = f'{parser_type.lower()}_parser_class'
        parser_clazz = getattr(self, name)
        if parser_clazz:
            return parser_clazz(
                self._iter,
                check_integrity=self.check_integrity,
            )
        return None

    def validate(self, obj):
        """Hook for validation logic. To be overridden by subclasses."""
        pass

    def can_parse(self):
        """Return True if the current record can be parsed by this parser."""
        raise NotImplementedError()

    def parse(self):
        """Parse the current record or section."""
        raise NotImplementedError()


class BaseSectionParser(BaseParser):
    """Base class for parsing sections with header, children, and trailer."""

    header_parser_class = None
    trailer_parser_class = None

    def __init__(self, iterator, **kwargs):
        super().__init__(iterator, **kwargs)
        self.header_parser = self._get_parser('header')
        self.trailer_parser = self._get_parser('trailer')

    def _parse_header(self):
        """Parse and return section header object."""
        return self.header_parser.parse()

    def _parse_trailer(self):
        """Parse and return section trailer object."""
        return self.trailer_parser.parse()

    def _parse_children(self):
        """Parse all child records within this section."""
        if not self.child_parser:
            return []
        
        children = []
        while self.child_parser.can_parse():
            children.append(self.child_parser.parse())
        return children

    def can_parse(self):
        """Check if section (header, trailer, or children) can be parsed."""
        return (
            self.header_parser.can_parse() or
            self.trailer_parser.can_parse() or
            (self.child_parser and self.child_parser.can_parse())
        )

    def validate_number_of_records(self, obj):
        if self.check_integrity:
            # For accounts: count 03, all 16/88 records, and 49
            number_of_records = len(obj.header.rows)  # Count the 03 record
            
            for child in obj.children:
                number_of_records += len(child.rows)
            
            number_of_records += len(obj.trailer.rows)
            
            if obj.trailer.number_of_records is not None:
                if number_of_records != obj.trailer.number_of_records:
                    raise IntegrityException(
                        f'Invalid number of records for {obj.__class__.__name__}. '
                        f'expected {obj.trailer.number_of_records}, found {number_of_records}'
                    )

    def validate(self, obj):
        """Validate section integrity and record counts."""
        super().validate(obj)
        self.validate_number_of_records(obj)

    def parse(self):
        """Parse a complete section (header, children, trailer)."""
        header = self._parse_header()
        children = self._parse_children()
        trailer = self._parse_trailer()

        obj = self.build_model(header, children, trailer)
        self.validate(obj)
        return obj

    def build_model(self, header, children, trailer):
        """Build section model object."""
        return self.model(header, trailer, children)


class BaseSingleParser(BaseParser):
    """Base class for single-record parsers (e.g., headers, trailers, transactions)."""

    fields_config = {}

    def can_parse(self):
        """Check if the current record matches the expected model code."""
        try:
            self._check_record_code(self.model.code)
        except ParsingException:
            return False
        return True

    def _parse_field_from_config(self, field_config, raw_value):
        if isinstance(field_config, str):
            field_config = (field_config, lambda x: x)
        
        field_name, parser = field_config

        if raw_value is None or raw_value == '':
            return field_name, None

        # Special handling for amounts
        if parser == float or ('amount' in field_name):
            raw_value = raw_value.strip()
            # Handle trailing minus sign
            if raw_value.endswith('-'):
                field_value = -float(raw_value[:-1])
            else:
                field_value = float(raw_value)
        elif parser == int and 'total' in field_name:
            field_value = int(raw_value) if raw_value else 0
        else:
            field_value = parser(raw_value)

        return field_name, field_value

    def _parse_fields_from_config(self, values, fields_config):
        """
        Parse multiple fields based on field configuration list.
        Handles missing fields gracefully.
        """
        fields = {}
        index = 0

        for field_config in fields_config:
            if index < len(values):
                field_name, field_value = self._parse_field_from_config(
                    field_config, values[index]
                )
                fields[field_name] = field_value
                index += 1
            else:
                if isinstance(field_config, str):
                    fields[field_config] = None
                else:
                    field_name, _ = field_config
                    fields[field_name] = None
                break
        
        return fields

    def _parse_fields(self, record):
        """Parse fields of the current record into a dictionary."""
        return self._parse_fields_from_config(record.fields, self.fields_config)

    def _parse_availability(self, funds_type, rest):
        """
        Parse funds availability schedule depending on funds_type.
        Supports simple, value-dated, and distributed availability.
        """
        availability = None

        if funds_type == FundsType.distributed_availability_simple:
            availability = OrderedDict()
            for day in ['0', '1', '>1']:
                availability[day] = int(rest.pop(0)) if rest else 0

        elif funds_type == FundsType.value_dated:
            date = rest.pop(0) if rest else None
            time = rest.pop(0) if rest else None
            availability = OrderedDict()
            availability['date'] = parse_date(date) if date else None
            availability['time'] = parse_time(time) if time else None

        elif funds_type == FundsType.distributed_availability:
            if rest:
                num_distributions = int(rest.pop(0))
                availability = OrderedDict()
                for _ in range(num_distributions):
                    if len(rest) >= 2:
                        day = rest.pop(0)
                        amount = int(rest.pop(0))
                        availability[day] = amount
                    else:
                        break
        
        return availability, rest

    def parse(self):
        """Parse a single record into its model object."""
        self._check_record_code(self.model.code)

        obj = self.model(
            self._iter.current_record.rows,
            **self._parse_fields(self._iter.current_record),
        )

        self.validate(obj)

        try:
            self._iter.advance()
        except StopIteration:
            pass

        return obj


# --- Concrete Parsers ---

class TransactionDetailParser(BaseSingleParser):
    """Parses Transaction Detail records (code 16)."""

    model = TransactionDetail
    head_fields_config = [
        ('type_code', parse_type_code),
        ('amount', float),
        ('funds_type', FundsType),
    ]
    tail_fields_config = [
        'bank_reference',
        'customer_reference',
        'text',
    ]

    def _parse_fields(self, record):
        """
        Parse transaction details, handling availability fields
        and variable-length tail fields.
        """
        rest = record.fields[:]
        fields = self._parse_fields_from_config(
            rest[:len(self.head_fields_config)],
            self.head_fields_config
        )
        
        rest = rest[len(self.head_fields_config):]

        # availability
        availability, rest = self._parse_availability(
            fields.get('funds_type'), rest,
        )
        fields['availability'] = availability

        # tail fields (bank ref, customer ref, text)
        if len(rest) >= 2:
            tail_values = rest[:2] + [','.join(rest[2:]) if len(rest) > 2 else '']
        else:
            tail_values = rest + [''] * (3 - len(rest))
        
        fields.update(
            self._parse_fields_from_config(
                tail_values,
                self.tail_fields_config
            )
        )

        return fields


class AccountIdentifierParser(BaseSingleParser):
    """Parses Account Identifier records (code 03)."""

    model = AccountIdentifier

    common_fields_config = [
        'customer_account_number',
        'currency',
    ]
    summary_fields_config = [
        ('type_code', parse_type_code),
        ('amount', float),
        ('item_count', int),
        ('funds_type', FundsType),
    ]

    def _parse_fields(self, record):
        """
        Parse account identifier and any summary items present.
        Summary items may include availability data.
        """
        model_fields = self._parse_fields_from_config(
            record.fields[:len(self.common_fields_config)],
            self.common_fields_config,
        )

        summary_items = []
        rest = record.fields[len(self.common_fields_config):]
        while rest:
            if len(rest) == 1 and not rest[0]:
                break

            summary = self._parse_fields_from_config(
                rest, self.summary_fields_config
            )
            rest = rest[len(self.summary_fields_config):]
            availability, rest = self._parse_availability(
                summary['funds_type'], rest
            )
            if availability:
                summary['availability'] = availability
            summary_items.append(Summary(**summary))
        model_fields['summary_items'] = summary_items

        return model_fields


class AccountTrailerParser(BaseSingleParser):
    """Parses Account Trailer records (code 49)."""

    model = AccountTrailer
    fields_config = [
        ('account_control_total', float),
        ('number_of_records', int),
    ]


class AccountParser(BaseSectionParser):
    """Parses an Account section (03 header, 16 details, 49 trailer)."""

    model = Account
    header_parser_class = AccountIdentifierParser
    trailer_parser_class = AccountTrailerParser
    child_parser_class = TransactionDetailParser

    def validate(self, obj):
        """Validate account control total and record counts."""
        super().validate(obj)
        
        if self.check_integrity:
            transaction_sum = sum([child.amount or 0 for child in obj.children])
            
            summary_sum = 0
            if obj.header and hasattr(obj.header, 'summary_items'):
                for summary in obj.header.summary_items:
                    if summary.type_code.code not in IGNORED_SUMMARY_CODES:
                        summary_sum += summary.amount or 0
            
            total_sum = transaction_sum + summary_sum
            
            if abs(total_sum - obj.trailer.account_control_total) > 0.01:
                raise IntegrityException(
                    f'Invalid account control total for {obj.__class__.__name__}. '
                    f'expected {obj.trailer.account_control_total}, found {total_sum}'
                )


class GroupHeaderParser(BaseSingleParser):
    """Parses Group Header records (code 02)."""

    model = GroupHeader
    fields_config = [
        'ultimate_receiver_id',
        'originator_id',
        ('group_status', GroupStatus),
        ('as_of_date', parse_date),
        ('as_of_time', parse_time),
        'currency',
        ('as_of_date_modifier', AsOfDateModifier),
    ]

    def _parse_fields(self, record):
        """Parse fields and ensure default currency if missing."""
        fields = super()._parse_fields(record)
        
        if not fields.get('currency'):
            fields['currency'] = ''
        
        return fields


class GroupTrailerParser(BaseSingleParser):
    """Parses Group Trailer records (code 98)."""

    model = GroupTrailer
    fields_config = [
        ('group_control_total', float),
        ('number_of_accounts', int),
        ('number_of_records', int),
    ]


class GroupParser(BaseSectionParser):
    """Parses a Group section (02 header, accounts, 98 trailer)."""

    model = Group
    header_parser_class = GroupHeaderParser
    trailer_parser_class = GroupTrailerParser
    child_parser_class = AccountParser

    def validate(self, obj):
        """Validate group integrity: accounts count and control totals."""
        super().validate(obj)
        
        if not obj.children:
            raise ParsingException('Group without accounts not allowed')
        
        if self.check_integrity:
            if obj.trailer.number_of_accounts != len(obj.children):
                raise IntegrityException(
                    f'Invalid number of accounts for {obj.__class__.__name__}. '
                    f'expected {obj.trailer.number_of_accounts}, found {len(obj.children)}'
                )
            
            control_total = sum([
                account.trailer.account_control_total for account in obj.children
            ])
            
            if abs(control_total - obj.trailer.group_control_total) > 0.01:
                raise IntegrityException(
                    f'Invalid group control total for {obj.__class__.__name__}. '
                    f'expected {obj.trailer.group_control_total}, found {control_total}'
                )


class Bai2FileHeaderParser(BaseSingleParser):
    """Parses File Header records (code 01)."""

    model = Bai2FileHeader
    fields_config = (
        'sender_id',
        'receiver_id',
        ('creation_date', parse_date),
        ('creation_time', parse_time),
        'file_id',
        ('physical_record_length', int),
        ('block_size', int),
        ('version_number', int),
    )

    def validate(self, obj):
        """Validate that only BAI version 2 is supported."""
        super().validate(obj)
        
        # Accept files without explicit version (NAB format) or with version 2
        if obj.version_number is None:
            return  # NAB format - no explicit version field
        
        # Only validate if version is explicitly set and is not 2
        version = obj.version_number
        if isinstance(version, str):
            try:
                version = int(version)
            except (ValueError, TypeError):
                return  # Can't parse, accept it
        
        if version != 2:
            raise NotSupportedYetException(
                f'Only BAI version 2 supported, found version {version}'
            )


class Bai2FileTrailerParser(BaseSingleParser):
    """Parses File Trailer records (code 99)."""

    model = Bai2FileTrailer
    fields_config = (
        ('file_control_total', float),
        ('number_of_groups', int),
        ('number_of_records', int),
    )
    
    def _parse_fields(self, record):
        """
        Parse trailer fields.
        Some banks swap number_of_groups and number_of_records.
        This method corrects for such cases.
        """
        fields = super()._parse_fields(record)
        
        if fields.get('number_of_groups') and fields.get('number_of_records') is None:
            fields['number_of_records'] = fields['number_of_groups']
            fields['number_of_groups'] = None
        
        return fields


class Bai2FileParser(BaseSectionParser):
    """Parses the complete BAI2 File (01 header, groups, 99 trailer)."""

    model = Bai2File
    header_parser_class = Bai2FileHeaderParser
    trailer_parser_class = Bai2FileTrailerParser
    child_parser_class = GroupParser

    def validate_number_of_records(self, obj):
        """
        Validate record count for the full file.
        Unlike accounts, includes header (01) and trailer (99).
        """
        if self.check_integrity:
            number_of_records = len(obj.header.rows)  # Count 01

            for child in obj.children:
                number_of_records += len(child.rows)

            number_of_records += len(obj.trailer.rows)  # Count 99

            if obj.trailer.number_of_records is not None:
                if number_of_records != obj.trailer.number_of_records:
                    raise IntegrityException(
                        f'Invalid number of records for {obj.__class__.__name__}. '
                        f'expected {obj.trailer.number_of_records}, found {number_of_records}'
                    )

    def validate(self, obj):
        """Validate file integrity: record counts, groups count, and totals."""
        self.validate_number_of_records(obj)
        
        if not obj.children:
            raise ParsingException('File without groups not allowed')
        
        if self.check_integrity:
            if obj.trailer.number_of_groups is not None:
                if obj.trailer.number_of_groups != len(obj.children):
                    raise IntegrityException(
                        f'Invalid number of groups for {obj.__class__.__name__}. '
                        f'expected {obj.trailer.number_of_groups}, found {len(obj.children)}'
                    )
            
            control_total = sum([
                group.trailer.group_control_total for group in obj.children
            ])
            
            if abs(control_total - obj.trailer.file_control_total) > 0.01:
                raise IntegrityException(
                    f'Invalid file control total for {obj.__class__.__name__}. '
                    f'expected {obj.trailer.file_control_total}, found {control_total}'
                )
