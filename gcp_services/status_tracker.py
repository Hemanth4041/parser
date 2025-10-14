import logging
from datetime import datetime
from typing import Optional, Dict, Any
from google.cloud import bigquery
from common.settings import PROJECT_ID
from common.settings import DATASET_ID
from common.settings import STATUS_TABLE_ID

logger = logging.getLogger(__name__)


class StatusTracker:
    """Tracks file processing status - only inserts SUCCESS or FAILED records"""
    
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_ID)
        self.table_ref = self.client.dataset(DATASET_ID).table(STATUS_TABLE_ID)
        # Use STATUS_TABLE_ID as both the manifest and status table (same table)
        self.manifest_ref = self.table_ref
        self._schema = None
        self._load_schema_from_manifest()
        self._verify_table_exists()
    
    def _load_schema_from_manifest(self):
        """Load schema from manifest table"""
        try:
            manifest_table = self.client.get_table(self.manifest_ref)
            self._schema = {field.name: field.field_type for field in manifest_table.schema}
            logger.info(f"Loaded schema from manifest table '{STATUS_TABLE_ID}': {list(self._schema.keys())}")
        except Exception as e:
            logger.error(f"Failed to load schema from manifest table '{STATUS_TABLE_ID}': {e}")
            raise RuntimeError(
                f"Manifest table '{DATASET_ID}.{STATUS_TABLE_ID}' does not exist or is not accessible. "
                "Please ensure the manifest table exists and has the correct schema."
            )
    
    def _verify_table_exists(self):
        """Verify status table exists"""
        try:
            status_table = self.client.get_table(self.table_ref)
            logger.info(f"Status table '{STATUS_TABLE_ID}' found with schema: {list(self._schema.keys())}")
        except Exception as e:
            logger.error(f"Status table '{STATUS_TABLE_ID}' not found: {e}")
            raise RuntimeError(
                f"Status table '{DATASET_ID}.{STATUS_TABLE_ID}' does not exist. "
                f"Please create it with columns: {list(self._schema.keys())}"
            )
    
    def insert_status(self, filename: str, status: str):
        """
        Insert status record for a file using schema from manifest table
        
        Args:
            filename: Full GCS path (bucket_name/blob_name)
            status: Processing status (SUCCESS, FAILED)
        """
        timestamp = datetime.utcnow()
        
        # Build row dynamically based on manifest schema
        row: Dict[str, Any] = {}
        
        for column_name, column_type in self._schema.items():
            column_lower = column_name.lower()
            if column_lower == 'filename':
                row[column_name] = filename
            elif column_lower == 'status':
                row[column_name] = status
            elif column_lower == 'source':
                row[column_name] = "external"
            elif column_lower == 'timestamp':
                row[column_name] = timestamp.isoformat()
            else:
                # For any additional columns, set to None (NULL)
                row[column_name] = None
        
        errors = self.client.insert_rows_json(self.table_ref, [row])
        if errors:
            logger.error(f"Failed to insert status record: {errors}")
            raise RuntimeError(f"Failed to insert status for {filename}: {errors}")
        
        logger.info(f"Inserted status record - File: {filename}, Status: {status}")
    
    def update_processing(self, filename: str):
        """Mark file as processing - NO DATABASE INSERT, just log"""
        logger.info(f"Processing started for: {filename}")
        # Don't insert to database
        pass
    
    def update_success(self, filename: str):
        """Mark file as successfully processed - INSERT to database"""
        self.insert_status(filename, "SUCCESS")
    
    def update_failed(self, filename: str, error_message: str = ""):
        """Mark file as failed - INSERT to database"""
        # error_message parameter kept for backwards compatibility but not used
        self.insert_status(filename, "FAILED")