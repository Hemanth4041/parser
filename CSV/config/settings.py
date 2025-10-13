import os
from pathlib import Path

# Schema configuration path
CSV_MODULE_DIR = Path(__file__).parent.parent
SCHEMA_PATH = os.environ.get(
    "CSV_SCHEMA_PATH",
    str(CSV_MODULE_DIR / "ddl" / "target_bq_schema.json")
)