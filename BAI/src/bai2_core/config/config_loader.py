import json
from pathlib import Path

# config_loader.py is in: BAI/src/bai2_core/config/
# We need to go to: BAI/ddl/target_bq_mappings.json
# So go up 3 levels to BAI/, then into ddl/

CONFIG_PATH = Path(__file__).resolve().parent.parent.parent.parent / "ddl" / "target_bq_schema.json"


def load_config():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found at: {CONFIG_PATH}")
    
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

CONFIG = load_config()
IGNORED_SUMMARY_CODES = set(CONFIG.get("ignored_summary_codes", []))