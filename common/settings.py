import os

# GCP Configuration
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
LOCATION = os.environ.get("GCP_LOCATION", "")

# BigQuery Configuration
DATASET_ID = os.environ.get("BQ_DATASET_ID", "")
BALANCE_TABLE_ID = os.environ.get("BQ_BALANCE_TABLE_ID", "")
TRANSACTIONS_TABLE_ID = os.environ.get("BQ_TRANSACTIONS_TABLE_ID", "")
STATUS_TABLE_ID = os.environ.get("BQ_STATUS_TABLE_ID", "")

# KMS Configuration
KEY_RING = os.environ.get("KMS_KEY_RING", "")