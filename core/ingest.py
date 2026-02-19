"""Thin orchestration for the ingestion pipeline. Delegates to cli.main; no business logic here."""

from __future__ import annotations

from pathlib import Path

from paths import (
    BASE_DIR,
    DYNAMIC_RULES_PATH,
    FARMS_CONFIG_PATH,
    INVOICES_DIR,
    STRUCTURED_OUTPUTS_DIR,
    ensure_data_dirs,
)
from core.db import init_db

# Lazy imports to avoid pulling CLI deps at module load
def _get_pipeline():
    from config import load_config
    from farm_tagger import load_farms
    from core.rules import load_dynamic_rules
    from cli.main import process_batch, process_single_invoice

    ensure_data_dirs()
    init_db()
    config = load_config()
    farms_config = load_farms(FARMS_CONFIG_PATH)
    dynamic_rules = load_dynamic_rules(DYNAMIC_RULES_PATH)
    return config, farms_config, dynamic_rules, process_single_invoice, process_batch


def run_one(pdf_path: Path) -> dict:
    """Process a single PDF through OCR, parse, tag, and ledger. Returns result dict from pipeline."""
    config, farms_config, dynamic_rules, process_single_invoice, _ = _get_pipeline()
    return process_single_invoice(
        str(pdf_path),
        config,
        BASE_DIR,
        farms_config,
        STRUCTURED_OUTPUTS_DIR,
        dynamic_rules_config=dynamic_rules,
        silent=True,
        verbose=False,
    )


def run_all() -> dict:
    """Process all PDFs in invoices directory. Returns batch summary dict."""
    config, farms_config, dynamic_rules, _, process_batch = _get_pipeline()
    return process_batch(
        "invoices",
        config,
        BASE_DIR,
        farms_config,
        dynamic_rules_config=dynamic_rules,
        verbose=False,
    )


def list_invoice_pdfs() -> list[str]:
    """Return sorted list of PDF filenames in INVOICES_DIR."""
    ensure_data_dirs()
    INVOICES_DIR.mkdir(parents=True, exist_ok=True)
    return sorted(p.name for p in INVOICES_DIR.glob("*.pdf"))
