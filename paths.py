"""Centralized filesystem paths for persistent data and configs."""

from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

DATA_DIR = BASE_DIR / "data"
LEDGER_PATH = DATA_DIR / "transactions.jsonl"
QUEUE_PATH = DATA_DIR / "manual_review_queue.jsonl"
DECISIONS_PATH = DATA_DIR / "manual_review_decisions.jsonl"
TAGS_PATH = DATA_DIR / "tags.jsonl"

CACHE_DIR = DATA_DIR / "cache"
VISION_CACHE_DIR = CACHE_DIR / "vision_text"
DEBUG_DIR = DATA_DIR / "debug"
STRUCTURED_OUTPUTS_DIR = DEBUG_DIR / "structured_outputs"

CONFIG_DIR = BASE_DIR / "config"
FARMS_CONFIG_PATH = CONFIG_DIR / "farms.json"
DYNAMIC_RULES_PATH = CONFIG_DIR / "dynamic_rules.json"

INVOICES_DIR = BASE_DIR / "invoices"


def ensure_data_dirs() -> None:
    """Ensure required data directories exist."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    VISION_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    STRUCTURED_OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
