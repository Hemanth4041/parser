"""
File Router - Routes files to appropriate parser and handles post-processing
Orchestrates the complete file processing workflow with status tracking
"""
import json
import logging
import sys
from typing import Dict, Optional
from datetime import datetime
from pathlib import Path

from gcp_services.status_tracker import StatusTracker
from gcp_services.gcs_service import move_file_in_gcs
from BAI.src.ext_data_pipeline.bai_parser import process_bai_file
from CAMT.src.ext_data_pipeline.camt_parser import process_camt_file
from CSV.csv_parser import process_csv_file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


class FileRouter:
    """
    Routes files to appropriate parser and handles post-processing.
    Manages status tracking and file movement based on processing results.
    """
    
    # Supported file types and their parser functions
    PARSERS = {
        "bai": process_bai_file,
        "text": process_bai_file,  # Alias for BAI
        "camt": process_camt_file,
        "xml": process_camt_file,  # Alias for CAMT
        "csv": process_csv_file
    }
    
    def __init__(self):
        """Initialize router with status tracker."""
        self.status_tracker = StatusTracker()
        logger.info("FileRouter initialized")
    
    def route_and_process(self, request: Dict) -> Dict:
        """
        Main routing method - validates input, routes to parser, handles results.
        
        Args:
            request: {
                "filename": "bucket_name/path/to/file.ext",
                "file_type": "bai|text|camt|xml|csv"
            }
            
        Returns:
            Processing result dictionary with status, filename, and details
        """
        # Validate input
        filename = request.get("filename")
        file_type = request.get("file_type", "").lower()
        
        if not filename:
            raise ValueError("'filename' is required in request")
        if not file_type:
            raise ValueError("'file_type' is required in request")
        
        logger.info(f"Processing file: {filename}, type: {file_type}")
        
        # Validate file type
        if file_type not in self.PARSERS:
            raise ValueError(
                f"Unsupported file type: '{file_type}'. "
                f"Supported types: {list(self.PARSERS.keys())}"
            )
        
        # Get appropriate parser function
        parser_func = self.PARSERS[file_type]
        
        # Update status: PROCESSING
        self.status_tracker.update_processing(filename)
        
        try:
            # Execute parser
            result = parser_func(filename)
            
            # Move file to success folder
            success_path = self._move_to_folder(filename, "archive")
            
            # Update status: SUCCESS
            self.status_tracker.update_success(filename)
            
            logger.info(f"Successfully processed {filename}")
            return {
                "status": "SUCCESS",
                "filename": filename,
                "file_type": file_type,
                "moved_to": success_path,
                "rows_processed": result.get("rows_processed", 0)
            }
            
        except Exception as e:
            logger.error(f"Failed to process {filename}: {e}", exc_info=True)
            
            # Move file to failed folder
            failed_path = self._move_to_folder(filename, "error")
            
            # Update status: FAILED with error message
            self.status_tracker.update_failed(filename, str(e))
            
            return {
                "status": "FAILED",
                "filename": filename,
                "file_type": file_type,
                "moved_to": failed_path,
                "error": str(e),
                "error_type": type(e).__name__
            }
    
    def _move_to_folder(self, source_path: str, target_folder: str) -> str:
        """
        Move file within GCS bucket to specified folder.
        
        Args:
            source_path: Source GCS path (bucket_name/path/to/file.ext)
            target_folder: Target folder name ('archive' or 'error')
            
        Returns:
            New file path after move
            
        Raises:
            ValueError: If source_path format is invalid
        """
        # Split bucket name and blob path
        parts = source_path.split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid GCS path format: {source_path}")
        
        bucket_name, blob_name = parts
        filename = Path(blob_name).name
        
        # Construct destination path (keeping original filename)
        destination_blob = f"{target_folder}/{filename}"
        
        # Move file in GCS
        move_file_in_gcs(bucket_name, blob_name, destination_blob)
        
        new_path = f"{bucket_name}/{destination_blob}"
        logger.info(f"Moved file from {source_path} to {new_path}")
        
        return new_path


def main(request_json: Dict) -> Dict:
    """
    Main entry point for file processing.
    
    Args:
        request_json: {
            "filename": "bucket_name/path/to/file.ext",
            "file_type": "bai|text|camt|xml|csv"
        }
        
    Returns:
        Processing result dictionary
    """
    router = FileRouter()
    result = router.route_and_process(request_json)
    return result


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="External Data Parser Router",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Manual testing
  python router.py --filename mybucket/data/file.bai --file-type bai
  
  # DAG usage (receives JSON metadata)
  python router.py --file_metadata '{"input_file_path": "gs://bucket/file.bai", "format": ".bai"}'
        """
    )
    
    # For DAG usage - receives JSON metadata
    parser.add_argument(
        "--file_metadata",
        type=str,
        help="JSON string with file metadata from DAG"
    )
    
    # For manual testing
    parser.add_argument(
        "--filename",
        help="GCS path (bucket/blob)"
    )
    parser.add_argument(
        "--file-type",
        choices=["bai", "text", "camt", "xml", "csv"],
        help="File type to process"
    )
    
    args = parser.parse_args()
    
    # Handle DAG input
    if args.file_metadata:
        try:
            metadata = json.loads(args.file_metadata)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in file_metadata: {e}")
            sys.exit(1)
        
        # Extract from DAG format
        # DAG sends: {"input_file_path": "gs://bucket/path/file.bai", "format": ".bai"}
        gcs_path = metadata.get("input_file_path", "")
        file_format = metadata.get("format", "")
        
        if not gcs_path:
            logger.error("Missing 'input_file_path' in metadata")
            sys.exit(1)
        
        # Remove gs:// prefix
        filename = gcs_path.replace("gs://", "")
        
        # Map file extension to type
        format_map = {
            ".bai": "bai",
            ".txt": "text",
            ".xml": "camt",
            ".csv": "csv"
        }
        file_type = format_map.get(file_format.lower(), "")
        
        if not file_type:
            logger.error(f"Unsupported file format: {file_format}")
            logger.error(f"Supported formats: {list(format_map.keys())}")
            sys.exit(1)
        
        logger.info(f"DAG Input - File: {filename}, Type: {file_type}")
        
        request = {
            "filename": filename,
            "file_type": file_type
        }
    
    # Handle manual testing input
    elif args.filename and args.file_type:
        request = {
            "filename": args.filename,
            "file_type": args.file_type
        }
    
    else:
        parser.error("Either --file_metadata OR both --filename and --file-type are required")
    
    # Process file
    try:
        result = main(request)
        print(json.dumps(result, indent=2))
        
        # Exit with proper code
        exit_code = 0 if result.get("status") == "SUCCESS" else 1
        sys.exit(exit_code)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        error_result = {
            "status": "FAILED",
            "filename": request.get("filename"),
            "error": str(e),
            "error_type": type(e).__name__
        }
        print(json.dumps(error_result, indent=2))
        sys.exit(1)