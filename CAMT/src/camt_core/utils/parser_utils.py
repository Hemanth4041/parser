"""
Utility Functions
Low-level helper functions for XML processing and data conversion
"""
import xml.etree.ElementTree as ET
from typing import Optional
from datetime import datetime, date


def extract_namespace(element: ET.Element) -> str:
    """
    Extracts XML namespace from element tag
    
    Args:
        element: XML element
        
    Returns:
        Namespace string like '{http://...}' or empty string if no namespace
        
    Example:
        >>> element.tag = '{http://www.example.com}Document'
        >>> extract_namespace(element)
        '{http://www.example.com}'
    """
    if element.tag.startswith('{'):
        return element.tag.split('}')[0] + '}'
    return ''


def get_text(element: ET.Element, path: str, required: bool = False) -> Optional[str]:
    """
    Safely extracts text from XML element
    
    Args:
        element: Parent XML element
        path: XPath to child element
        required: Whether element is required (raises error if missing)
        
    Returns:
        Stripped text content or None if not found
        
    Raises:
        ValueError: If required=True and element not found
        
    Example:
        >>> get_text(element, 'ns:Name', required=True)
        'John Doe'
    """
    elem = element.find(path)
    if elem is not None and elem.text:
        return elem.text.strip()
    
    if required:
        raise ValueError(f"Required element not found: {path}")
    
    return None


def parse_date(date_str: str) -> date:
    for fmt in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {date_str}")

def parse_datetime(dt_str: str) -> datetime:
    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y%m%d%H%M%S', '%Y%m%d'):
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported datetime format: {dt_str}")