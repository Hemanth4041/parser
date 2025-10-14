"""
GCS utility functions for reading files and extracting bucket labels
"""
import logging
from google.cloud import storage as gcs
from google.api_core.exceptions import NotFound
from common.settings import PROJECT_ID
from typing import Tuple, Optional

logger = logging.getLogger(__name__)


def get_bucket_labels(bucket_name: str) -> dict:
    """
    Retrieves labels from a GCS bucket.
    
    Args:
        bucket_name: The name of the GCS bucket
        
    Returns:
        Dictionary of bucket labels
    """
    try:
        client = gcs.Client(project=PROJECT_ID)
        bucket = client.bucket(bucket_name)
        bucket.reload()  # Fetch latest metadata
        
        labels = bucket.labels or {}
        logger.info(f"Retrieved labels from bucket '{bucket_name}': {labels}")
        return labels
    except NotFound:
        logger.error(f"Bucket not found: {bucket_name}")
        raise ValueError(f"Bucket not found: {bucket_name}")
    except Exception as e:
        logger.error(f"Failed to get bucket labels: {e}", exc_info=True)
        raise


def extract_ids_from_bucket_labels(bucket_name: str) -> Tuple[str, str]:
    """
    Extracts organization_biz_id and division_biz_id from bucket labels.
    
    Expected label keys:
    - organization_id or org_id or organisation_id or organisation_biz_id
    - division_biz_id or div_id or division_id
    
    Args:
        bucket_name: GCS bucket name
        
    Returns:
        Tuple of (organization_biz_id, division_biz_id)
        
    Raises:
        ValueError: If required labels are missing
    """
    labels = get_bucket_labels(bucket_name)
    
    # Try multiple possible label names for organization
    org_id = (labels.get('organization_id') or 
              labels.get('org_id') or 
              labels.get('organisation_id') or
              labels.get('organisation_biz_id'))
    
    # Try multiple possible label names for division
    div_id = (labels.get('division_biz_id') or 
              labels.get('div_id') or 
              labels.get('division_id'))
    
    # Validate all required IDs are present
    missing_labels = []
    if not org_id:
        missing_labels.append('organization_id/org_id/organisation_biz_id')
    if not div_id:
        missing_labels.append('division_biz_id/div_id')
    
    if missing_labels:
        raise ValueError(
            f"Missing required bucket labels: {', '.join(missing_labels)}. "
            f"Found labels: {list(labels.keys())}"
        )
    
    logger.info(f"Extracted IDs from bucket labels - Org: '{org_id}', Div: '{div_id}'")
    return org_id, div_id


def extract_ids_from_gcs_path(gcs_path: str) -> Tuple[str, str, str]:
    """
    Extracts bucket name and IDs from GCS path using bucket labels.
    
    Args:
        gcs_path: GCS path in format 'bucket_name/blob_name'
        
    Returns:
        Tuple of (bucket_name, organization_biz_id, division_biz_id)
    """
    if "/" not in gcs_path:
        raise ValueError("Invalid GCS path format. Expected 'bucket_name/blob_name'.")
    
    bucket_name = gcs_path.split("/", 1)[0]
    org_id, div_id = extract_ids_from_bucket_labels(bucket_name)
    
    return bucket_name, org_id, div_id


def read_file_from_gcs(gcs_path: str) -> str:
    """
    Downloads a file from GCS and returns its content as a string.
    
    Args:
        gcs_path: The GCS path in the format 'bucket_name/blob_name'.
        
    Returns:
        The text content of the file.
    """
    if "/" not in gcs_path:
        raise ValueError("Invalid GCS path format. Expected 'bucket_name/blob_name'.")
    
    bucket_name, blob_name = gcs_path.split("/", 1)
    logger.info(f"Reading file from GCS: gs://{bucket_name}/{blob_name}")
    
    try:
        client = gcs.Client(project=PROJECT_ID)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_text()
    except NotFound:
        logger.error(f"File not found at GCS path: gs://{gcs_path}")
        raise FileNotFoundError(f"File not found at GCS path: gs://{gcs_path}")
    except Exception as e:
        logger.error(f"Failed to read from GCS path gs://{gcs_path}: {e}", exc_info=True)
        raise


def move_file_in_gcs(bucket_name: str, source_blob_name: str, 
                     destination_blob_name: str) -> None:
    """
    Move (copy + delete) a file within a GCS bucket.
    
    Args:
        bucket_name: GCS bucket name
        source_blob_name: Source blob path
        destination_blob_name: Destination blob path
    """
    logger.info(f"Moving gs://{bucket_name}/{source_blob_name} to "
               f"gs://{bucket_name}/{destination_blob_name}")
    
    try:
        client = gcs.Client(project=PROJECT_ID)
        bucket = client.bucket(bucket_name)
        
        source_blob = bucket.blob(source_blob_name)
        
        # Copy to destination
        bucket.copy_blob(source_blob, bucket, destination_blob_name)
        
        # Delete original
        source_blob.delete()
        
        logger.info(f"Successfully moved file to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        logger.error(f"Failed to move file: {e}", exc_info=True)
        raise


def write_file_to_gcs(bucket_name: str, blob_name: str, content: str) -> None:
    """
    Write content to a GCS file.
    
    Args:
        bucket_name: GCS bucket name
        blob_name: Blob path
        content: Text content to write
    """
    logger.info(f"Writing to gs://{bucket_name}/{blob_name}")
    
    try:
        client = gcs.Client(project=PROJECT_ID)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(content)
        
        logger.info(f"Successfully wrote to gs://{bucket_name}/{blob_name}")
    except Exception as e:
        logger.error(f"Failed to write to GCS: {e}", exc_info=True)
        raise