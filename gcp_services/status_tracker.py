import logging
from datetime import datetime
from typing import Optional
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
        self._verify_table_exists()
    
    def _verify_table_exists(self):
        """Verify status table exists"""
        try:
            self.client.get_table(self.table_ref)
            logger.info(f"Status table '{STATUS_TABLE_ID}' found")
        except Exception as e:
            logger.error(f"Status table '{STATUS_TABLE_ID}' not found: {e}")
            raise RuntimeError(
                f"Status table '{DATASET_ID}.{STATUS_TABLE_ID}' does not exist. "
                "Please create it with columns: filename (STRING), status (STRING), "
                "source (STRING), timestamp (TIMESTAMP)"
            )
    
    def insert_status(self, filename: str, status: str):
        """
        Insert status record for a file
        
        Args:
            filename: Full GCS path (bucket_name/blob_name)
            status: Processing status (SUCCESS, FAILED)
        """
        timestamp = datetime.utcnow()
        
        row = {
            "filename": filename,
            "status": status,
            "source": "external",
            "timestamp": timestamp.isoformat()
        }
        
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
    
    def update_failed(self, filename: str, error_message: str):
        """Mark file as failed - INSERT to database"""
        # error_message parameter kept for backwards compatibility but not used
        self.insert_status(filename, "FAILED")