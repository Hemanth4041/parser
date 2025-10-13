from BAI.src.bai2_core.constants import RecordCode
from BAI.src.bai2_core.models.bai2_model import Record


def _is_valid_type_code(field):
    """
    Check if a field is likely a valid BAI2 type code.
    Type codes are 3-digit strings in specific ranges (010-999).
    Common type codes: 010-040 (status), 100-499 (credits/debits), 500-699 (adjustments), 900-999 (summary).
    """
    if not field or len(field) != 3:
        return False
    
    if not field.isdigit():
        return False
    
    code = int(field)
    # Type codes typically range from 010 to 999
    # Amounts starting with 0 are rare (like 010, 015, etc. are more likely type codes)
    if code < 10:  # 000-009 are not valid type codes
        return False
    
    return True


def _build_account_identifier_record(rows):
    """
    Build account identifier record (03) handling continuation records (88)
    and normalizing missing commas in summary fields.
    
    Uses a look-ahead strategy to determine field boundaries by analyzing
    remaining fields and detecting valid type code patterns.
    """
    fields_str = ''
    
    # Concatenate all rows (including 88 continuation records)
    for index, row in enumerate(rows):
        field_str = row[1]
        
        if field_str:
            # Remove trailing slash if present
            if field_str[-1] == '/':
                field_str = field_str[:-1]
            
            fields_str += field_str

    # Split into fields and strip whitespace
    fields = [f.strip() for f in fields_str.split(',')]
    
    # Remove any empty fields at the end only
    while fields and not fields[-1]:
        fields.pop()
    
    # Normalize: account number, currency are first 2 fields
    # Then groups of 4: type_code, amount, item_count, funds_type
    if len(fields) >= 2:
        normalized = fields[:2]  # account_number, currency
        rest = fields[2:]
        
        # Process summary items
        i = 0
        while i < len(rest):
            # Type code (required)
            if i >= len(rest):
                break
                
            type_code = rest[i]
            i += 1
            
            # Amount (required but can be empty)
            if i >= len(rest):
                # Type code at end with no amount
                normalized.extend([type_code, '', '', ''])
                break
            
            amount = rest[i]
            i += 1
            
            # Now determine if we have item_count and/or funds_type
            # Strategy: look ahead to find the next valid type code
            item_count = ''
            funds_type = ''
            
            # Look ahead to find next type code position
            next_type_code_pos = None
            for j in range(i, min(i + 4, len(rest))):  # Look up to 4 fields ahead
                if _is_valid_type_code(rest[j]):
                    next_type_code_pos = j
                    break
            
            if next_type_code_pos is not None:
                # Found next type code, determine how many optional fields we have
                optional_fields_count = next_type_code_pos - i
                
                if optional_fields_count >= 2:
                    # We have both item_count and funds_type
                    item_count = rest[i] if i < len(rest) else ''
                    funds_type = rest[i + 1] if i + 1 < len(rest) else ''
                    i += 2
                elif optional_fields_count == 1:
                    # We have only item_count (funds_type is missing)
                    item_count = rest[i] if i < len(rest) else ''
                    i += 1
                # else: optional_fields_count == 0, both are missing
            else:
                # No next type code found, consume remaining fields as optional fields
                remaining = len(rest) - i
                
                if remaining >= 2:
                    # Assume we have both item_count and funds_type
                    item_count = rest[i] if i < len(rest) else ''
                    funds_type = rest[i + 1] if i + 1 < len(rest) else ''
                    i += 2
                elif remaining == 1:
                    # Only one field left, treat as item_count
                    item_count = rest[i] if i < len(rest) else ''
                    i += 1
            
            normalized.extend([type_code, amount, item_count, funds_type])
        
        fields = normalized
    
    return Record(code=rows[0][0], fields=fields, rows=rows)


def _build_generic_record(rows):
    """Build generic records by concatenating all rows including continuation records."""
    fields_str = ''
    for row in rows:
        field_str = row[1]

        if field_str:
            if field_str[-1] == '/':
                fields_str += field_str[:-1] + ','
            else:
                fields_str += field_str + ' '

    fields = fields_str[:-1].split(',')
    return Record(code=rows[0][0], fields=fields, rows=rows)


RecordBuilderFactory = {
    RecordCode.file_header: _build_generic_record,
    RecordCode.group_header: _build_generic_record,
    RecordCode.account_identifier: _build_account_identifier_record,
    RecordCode.transaction_detail: _build_generic_record,
    RecordCode.account_trailer: _build_generic_record,
    RecordCode.group_trailer: _build_generic_record,
    RecordCode.file_trailer: _build_generic_record,
}


def _build_record(rows):
    record_code = rows[0][0]
    return RecordBuilderFactory[record_code](rows)


def record_generator(lines):
    rows = iter(
        [(RecordCode(line[:2]), line[3:]) for line in lines]
    )

    records = [next(rows)]
    while True:
        try:
            row = next(rows)
        except StopIteration:
            break

        if row[0] != RecordCode.continuation:
            yield _build_record(records)
            records = [row]
        else:
            records.append(row)

    yield _build_record(records)


class IteratorHelper:
    def __init__(self, lines):
        self._generator = record_generator(lines)
        self.current_record = None
        self.advance()

    def advance(self):
        self.current_record = next(self._generator)