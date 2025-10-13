"""
Helper functions for data processing and cleaning
"""
import logging
import re
from datetime import datetime
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

DATE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y/%m/%d",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y%m%d",
    "%d.%m.%Y",
    "%Y.%m.%d"
]


def normalize_date(date_value: Any) -> str:
    """
    Converts various date formats to YYYY-MM-DD format.
    
    Args:
        date_value: Date value in various formats
        
    Returns:
        Date string in YYYY-MM-DD format
        
    Raises:
        ValueError: If date format is not recognized
    """
    if date_value is None or str(date_value).strip() == "":
        raise ValueError("Date value is empty or None")
    
    date_str = str(date_value).strip()
    
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    
    raise ValueError(f"Unable to parse date: {date_value}")


def clean_string(value: Any) -> str:
    """
    Cleans string values by removing extra whitespace.
    
    Args:
        value: Value to clean
        
    Returns:
        Cleaned string value
    """
    if value is None:
        return None
    
    cleaned = str(value).strip()
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned if cleaned else None


def clean_csv_row(row: Dict) -> Dict:
    """
    Cleans all values in a CSV row by removing whitespace.
    
    Args:
        row: Dictionary representing a CSV row
        
    Returns:
        Cleaned row dictionary
    """
    cleaned_row = {}
    for key, value in row.items():
        cleaned_key = key.strip() if isinstance(key, str) else key
        
        if value is None or (isinstance(value, str) and value.strip() == ""):
            cleaned_row[cleaned_key] = None
        else:
            cleaned_row[cleaned_key] = clean_string(value)
    
    return cleaned_row


def validate_numeric(value: Any, field_name: str) -> float:
    """
    Validates and converts numeric values.
    
    Args:
        value: Value to validate
        field_name: Name of the field (for error messages)
        
    Returns:
        Numeric value as float
        
    Raises:
        ValueError: If value is not numeric
    """
    if value is None:
        raise ValueError(f"{field_name} cannot be None")
    
    try:
        numeric_value = float(str(value).strip().replace(",", ""))
    except (ValueError, AttributeError):
        raise ValueError(f"{field_name} must be numeric, got: {value}")
    
    return numeric_value


def load_schema(schema_path: str) -> Dict:
    """
    Loads schema from JSON file.
    
    Args:
        schema_path: Path to schema JSON file
        
    Returns:
        Schema dictionary
    """
    import json
    
    try:
        with open(schema_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Schema file not found: {schema_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in schema file: {e}")
        raise