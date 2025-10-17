import logging
from typing import List, Dict
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from common.env_variables.settings import PROJECT_ID
from common.env_variables.settings import DATASET_ID

logger = logging.getLogger(__name__)


def load_rows_to_bq(rows: List[Dict]) -> int:
    """
    Loads a list of rows into their respective BigQuery tables.
    
    Args:
        rows: List of dictionaries, each containing a '_target_table' key.
        
    Returns:
        Number of rows loaded
    """
    if not rows:
        logger.warning("No rows to load into BigQuery.")
        return 0
    
    client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = client.dataset(DATASET_ID)
    
    tables_to_load: Dict[str, List[Dict]] = {}
    for row in rows:
        table_name = row.pop("_target_table", None)
        if not table_name:
            logger.warning(f"Skipping row with no '_target_table' key: {row}")
            continue
        tables_to_load.setdefault(table_name, []).append(row)
    
    total_loaded = 0
    for table_name, table_rows in tables_to_load.items():
        table_ref = dataset_ref.table(table_name)
        logger.info(f"Loading {len(table_rows)} rows into table '{table_name}'...")
        
        try:
            client.get_table(table_ref)
        except NotFound:
            logger.error(f"BigQuery table '{table_name}' not found in dataset '{DATASET_ID}'.")
            raise RuntimeError(f"Target table '{table_name}' does not exist.")
        
        errors = client.insert_rows_json(table_ref, table_rows)
        if errors:
            for error in errors:
                logger.error(f"BigQuery insert error for table '{table_name}': {error}")
            raise RuntimeError(f"Failed to load data into BigQuery table '{table_name}'.")
        
        total_loaded += len(table_rows)
    
    logger.info(f"Successfully loaded {total_loaded} rows into BigQuery.")
    return total_loaded