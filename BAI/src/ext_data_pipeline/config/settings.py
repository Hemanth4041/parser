"""
BAI-specific settings
"""
import os
from pathlib import Path

# Mapping configuration path
BAI_MODULE_DIR = Path(__file__).parent.parent.parent.parent
MAPPING_CONFIG_PATH = str(BAI_MODULE_DIR / "ddl" / "target_bq_schema.json")
