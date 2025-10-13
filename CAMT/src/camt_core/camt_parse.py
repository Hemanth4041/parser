"""
Main entry points for CAMT.053 parsing
"""
import xml.etree.ElementTree as ET
import logging
from typing import Optional
from CAMT.src.camt_core.models.camt_model import BankToCustomerStatement
from CAMT.src.camt_core.parser_document import DocumentParser
from CAMT.src.camt_core.utils.parser_utils import extract_namespace


class CAMT053Parser:
    """Parser for simplified CAMT.053 XML files"""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.namespace = ''
        self.doc_parser = None
    
    def parse_file(self, file_path: str) -> BankToCustomerStatement:
        """
        Parse CAMT.053 XML from file path
        
        Args:
            file_path: Path to XML file
            
        Returns:
            BankToCustomerStatement object
            
        Raises:
            ValueError: If XML parsing fails
        """
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            return self._parse_document(root)
        except ET.ParseError as e:
            raise ValueError(f"XML parsing error: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error parsing file {file_path}: {str(e)}")
            raise
    
    def parse_string(self, xml_string: str) -> BankToCustomerStatement:
        """
        Parse CAMT.053 XML from string
        
        Args:
            xml_string: XML content as string
            
        Returns:
            BankToCustomerStatement object
            
        Raises:
            ValueError: If XML parsing fails
        """
        try:
            root = ET.fromstring(xml_string)
            return self._parse_document(root)
        except ET.ParseError as e:
            raise ValueError(f"XML parsing error: {str(e)}")
        except Exception as e:
            self.logger.error(f"Error parsing XML string: {str(e)}")
            raise
    
    def _parse_document(self, root: ET.Element) -> BankToCustomerStatement:
        """
        Initialize document parser and parse the document
        
        Args:
            root: Root XML element
            
        Returns:
            BankToCustomerStatement object
        """
        
        self.namespace = extract_namespace(root)
        
        # Initialize document parser with namespace and logger
        self.doc_parser = DocumentParser(self.namespace, self.logger)
        
        # Delegate to document parser
        return self.doc_parser.parse_document(root)