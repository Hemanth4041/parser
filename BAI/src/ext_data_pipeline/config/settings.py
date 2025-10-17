"""
BAI-specific settings
"""
from pathlib import Path

# Base directory: go up 4 levels from this file
BAI_MODULE_DIR = Path(__file__).resolve().parents[4]

# Updated mapping configuration path
MAPPING_CONFIG_PATH = str(BAI_MODULE_DIR / "common" / "schema" / "target_bq_schema.json")
