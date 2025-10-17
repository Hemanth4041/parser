
"""
CAMT-specific settings
"""

from pathlib import Path

# Schema configuration path
CAMT_MODULE_DIR = Path(__file__).resolve().parents[4]
SCHEMA_PATH = str(CAMT_MODULE_DIR / "common" / "schema" / "target_bq_schema.json")


CAMT_SUPPORTED_VERSIONS =['camt.053.001.02']

