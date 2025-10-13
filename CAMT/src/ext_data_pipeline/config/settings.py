"""
CAMT-specific settings
"""
import os
from pathlib import Path

# Schema configuration path
CAMT_MODULE_DIR = Path(__file__).parent.parent.parent.parent
SCHEMA_PATH = str(CAMT_MODULE_DIR / "ddl" / "target_bq_schema.json")

CAMT_SUPPORTED_VERSIONS =['camt.053.001.02']