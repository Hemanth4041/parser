import json
import logging
import sys
from typing import Dict, Optional
from datetime import datetime
from pathlib import Path

from gcp_services.gcs_service import read_file_from_gcs
from gcp_services.gcs_service import move_file_in_gcs
from gcp_services.status_tracker import StatusTracker
from BAI.src.ext_data_pipeline.main import process_bai_file
from CAMT.src.ext_data_pipeline.main import process_camt_file
from CSV.main import process_csv_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


class FileRouter:
    """Routes files to appropriate parser and handles post-processing"""
    
    def __init__(self):
        self.status_tracker = StatusTracker()
        
        # File type routing map
        self.parsers = {
            "bai": process_bai_file,
            "text": process_bai_file,  # Alias for BAI
            "camt": process_camt_file,
            "xml": process_camt_file,  # Alias for CAMT
            "csv": process_csv_file
        }
        
    def route_and_process(self, request: Dict) -> Dict:
        """
        Main routing method
        
        Args:
            request: {
                "filename": "bucket_name/path/to/file.bai",
                "file_type": "bai|text|camt|xml|csv"
            }
            
        Returns:
            Processing result dictionary
        """
        filename = request.get("filename")
        file_type = request.get("file_type", "").lower()
        
        if not filename or not file_type:
            raise ValueError("Both 'filename' and 'file_type' are required")
        
        logger.info(f"Processing file: {filename}, type: {file_type}")
        
        # Get appropriate parser
        parser_func = self.parsers.get(file_type)
        if not parser_func:
            raise ValueError(
                f"Unsupported file type: {file_type}. "
                f"Supported types: {list(self.parsers.keys())}"
            )
        
        # Update status: PROCESSING
        self.status_tracker.update_processing(filename)
        
        try:
            # Execute parser
            result = parser_func(filename)
            
            # Move file to success folder
            success_path = self._move_to_success(filename)
            
            # Update status: SUCCESS
            self.status_tracker.update_success(filename)
            
            logger.info(f"Successfully processed {filename}")
            return {
                "status": "SUCCESS",
                "filename": filename,
                "moved_to": success_path,
                **result
            }
            
        except Exception as e:
            logger.error(f"Failed to process {filename}: {e}", exc_info=True)
            
            # Move file to failed folder
            failed_path = self._move_to_failed(filename)
            
            # Update status: FAILED with error message
            self.status_tracker.update_failed(filename, str(e))
            
            return {
                "status": "FAILED",
                "filename": filename,
                "moved_to": failed_path,
                "error": str(e)
            }
    
    def _generate_processing_id(self, filename: str) -> str:
        """Generate unique processing ID"""
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        file_hash = str(abs(hash(filename)))[:8]
        return f"PROC_{timestamp}_{file_hash}"
    
    def _move_to_success(self, source_path: str) -> str:
        """Move file to archive folder"""
        return self._move_file(source_path, "archive")
    
    def _move_to_failed(self, source_path: str) -> str:
        """Move file to error folder"""
        return self._move_file(source_path, "error")
    
    def _move_file(self, source_path: str, target_folder: str) -> str:
        """
        Move file within GCS bucket
        
        Args:
            source_path: bucket_name/path/to/file.ext
            target_folder: relative folder path (e.g., 'processed/success')
            
        Returns:
            New file path
        """
        parts = source_path.split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid GCS path format: {source_path}")
        
        bucket_name, blob_name = parts
        filename = Path(blob_name).name
        
        # Add timestamp to filename
        name_parts = filename.rsplit(".", 1)
        if len(name_parts) == 2:
            new_filename = f"{name_parts[0]}.{name_parts[1]}"
        else:
            new_filename = f"{filename}"
        
        destination_blob = f"{target_folder}/{new_filename}"
        
        move_file_in_gcs(bucket_name, blob_name, destination_blob)
        
        return f"{bucket_name}/{destination_blob}"


def main(request_json: Dict):
    """
    Main entry point
    
    Args:
        request_json: {
            "filename": "bucket_name/path/to/file.bai",
            "file_type": "bai|text|camt|xml|csv"
        }
    """
    router = FileRouter()
    result = router.route_and_process(request_json)
    return result


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="External Data Parser Router")
    
    # For DAG usage - receives JSON metadata
    parser.add_argument("--file_metadata", type=str, help="JSON string with file metadata from DAG")
    
    # For manual testing
    parser.add_argument("--filename", help="GCS path (bucket/blob)")
    parser.add_argument("--file-type", choices=["bai", "text", "camt", "xml", "csv"], help="File type")
    
    args = parser.parse_args()
    
    # Handle DAG input
    if args.file_metadata:
        metadata = json.loads(args.file_metadata)
        
        # Extract from DAG format
        # DAG sends: {"input_file_path": "gs://bucket/path/file.bai", "format": ".bai", "cmek_key_id": "..."}
        gcs_path = metadata.get("input_file_path", "")
        file_format = metadata.get("format", "")
        
        # Remove gs:// prefix
        filename = gcs_path.replace("gs://", "")
        
        # Map file extension to type
        format_map = {
            ".bai": "bai",
            ".txt": "text",
            ".xml": "camt",
            ".csv": "csv"
        }
        file_type = format_map.get(file_format, "")
        
        if not file_type:
            logger.error(f"Unsupported file format: {file_format}")
            sys.exit(1)
        
        logger.info(f"DAG Input - File: {filename}, Type: {file_type}")
        
        request = {
            "filename": filename,
            "file_type": file_type
        }
    else:
        # Manual testing mode
        if not args.filename or not args.file_type:
            parser.error("--filename and --file-type are required for manual testing")
        
        request = {
            "filename": args.filename,
            "file_type": args.file_type
        }
    
    # Process file
    result = main(request)
    print(json.dumps(result, indent=2))
    
    # Exit with proper code
    sys.exit(0 if result.get("status") == "SUCCESS" else 1)