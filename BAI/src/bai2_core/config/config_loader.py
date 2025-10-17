from pathlib import Path
import json

# Go up 3 levels from config_loader.py to reach ext_data_parser/
CONFIG_PATH = Path(__file__).resolve().parents[4] / "common" / "schema" / "target_bq_schema.json"

def load_config():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found at: {CONFIG_PATH}")
    
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

CONFIG = load_config()
IGNORED_SUMMARY_CODES = set(CONFIG.get("ignored_summary_codes", []))
