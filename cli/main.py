"""Entry point and orchestration for the invoice ingestion pipeline."""

import argparse
import copy
import datetime
import hashlib
import json
import re
import sqlite3
import sys
import traceback
import uuid
from pathlib import Path

from config import load_config
from core.db import execute, fetchone, get_connection, init_db
from farm_tagger import TagResult, load_farms, tag_document_text
from core.rules import apply_dynamic_rules, load_dynamic_rules
from llm_parser import (
    LLMParseError,
    extract_invoice_text_with_vision,
    parse_invoice_with_llm,
)
from core.validator import (
    PARSE_STATUS_INVALID_JSON,
    PARSE_STATUS_SUCCESS,
    PARSE_STATUS_VALIDATION_FAILED,
    normalize_total_to_cents,
    validate_invoice_payload,
)
from paths import (
    BASE_DIR,
    DYNAMIC_RULES_PATH,
    FARMS_CONFIG_PATH,
    INVOICES_DIR,
    STRUCTURED_OUTPUTS_DIR,
    ensure_data_dirs,
)


CANONICAL_TRANSACTION_SCHEMA = {
    "doc_id": None,
    "farm_id": None,
    "farm_name": None,
    "vendor_key": None,
    "vendor_name": None,
    "invoice_number": None,
    "invoice_date": None,
    "due_date": None,
    "total_amount": None,
    "service_address": None,
    "account_number": None,
    "line_items": [],
    "raw_text_hash": None,
    "confidence": 0.0,
    "needs_manual_review": False,
    "manual_override": False,
    "processed_at": None,
    "error": None,
    "content_fingerprint": "",
    "invoice_key": None,
    "duplicate_detected": False,
    "duplicate_reason": None,
    "duplicate_of": None,
}

REQUIRED_TRANSACTION_KEYS = set(CANONICAL_TRANSACTION_SCHEMA.keys())


def hash_text(text: str) -> str:
    """Generate SHA256 hash of text for deduplication."""
    return f"sha256:{hashlib.sha256(text.encode('utf-8')).hexdigest()[:16]}"


def normalize_for_fingerprint(text: str) -> str:
    """
    Normalize text for stable fingerprinting across OCR variations.

    Uses conservative normalization to avoid false positives.
    """
    text = (text or "").lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    text = text.strip()
    return text[:50000]


def compute_content_fingerprint(text: str) -> str:
    """
    Compute SHA-256 hash of normalized text.

    Format: "sha256:<16_hex_chars>"
    """
    normalized = normalize_for_fingerprint(text)
    hash_full = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"sha256:{hash_full[:16]}"


def norm_identifier(s: str) -> str:
    """Normalize account/invoice identifiers for stable key generation."""
    if not s:
        return ""
    normalized = s.lower().strip()
    normalized = re.sub(r"[-_/\s]", "", normalized)
    normalized = re.sub(r"[^\w]", "", normalized)
    return normalized


def norm_date(date_str: str) -> str:
    """Conservatively normalize dates for key generation."""
    if not date_str:
        return ""
    return date_str.strip()


def amount_to_cents(amount: float) -> str:
    """Convert amount to integer cents string for stable keying."""
    if amount is None:
        return "0"
    cents = int(round(amount * 100))
    return str(cents)


def norm_address(address: str) -> str:
    """Normalize service address for key generation."""
    if not address:
        return ""
    normalized = address.lower().strip()
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"[,.]", "", normalized)
    return normalized


def compute_invoice_key(
    vendor_key: str | None, parsed_invoice: dict | None
) -> str | None:
    """
    Compute deterministic invoice key from structured fields.

    Tier A: vendor + account + invoice number
    Tier B: vendor + account + date + amount
    Tier C: vendor + address + date + amount
    """
    if not vendor_key or not parsed_invoice:
        return None
    account_number = parsed_invoice.get("account_number")
    invoice_number = parsed_invoice.get("invoice_number")
    invoice_date = parsed_invoice.get("invoice_date")
    total_amount = parsed_invoice.get("total_amount")
    service_address = parsed_invoice.get("service_address")
    if account_number and invoice_number:
        return (
            f"{vendor_key}|"
            f"acct:{norm_identifier(account_number)}|"
            f"inv:{norm_identifier(invoice_number)}"
        )
    if account_number and invoice_date and total_amount is not None:
        return (
            f"{vendor_key}|"
            f"acct:{norm_identifier(account_number)}|"
            f"date:{norm_date(invoice_date)}|"
            f"amt_cents:{amount_to_cents(total_amount)}"
        )
    if service_address and invoice_date and total_amount is not None:
        return (
            f"{vendor_key}|"
            f"addr:{norm_address(service_address)}|"
            f"date:{norm_date(invoice_date)}|"
            f"amt_cents:{amount_to_cents(total_amount)}"
        )
    return None


def normalize_date_iso(value: str | None) -> str | None:
    """Normalize date strings to YYYY-MM-DD when parseable."""
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def to_cents(value: float | int | None) -> int:
    """Convert a numeric amount to integer cents."""
    if value is None:
        return 0
    try:
        return int(round(float(value) * 100))
    except (TypeError, ValueError):
        return 0


def insert_document(
    doc_id: str,
    file_name: str,
    file_path: str | None,
    content_fingerprint: str | None,
    raw_text_hash: str | None,
    raw_text: str | None = None,
) -> bool:
    """Insert document and return False on content fingerprint duplicates."""
    try:
        with get_connection() as connection:
            connection.execute(
                """
                INSERT INTO documents (
                    doc_id, file_name, file_path, raw_text_hash, raw_text, content_fingerprint, extracted_at
                )
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                (doc_id, file_name, file_path, raw_text_hash, raw_text, content_fingerprint),
            )
            connection.commit()
        return True
    except sqlite3.IntegrityError as exc:
        message = str(exc)
        if "documents.content_fingerprint" in message:
            return False
        raise


def insert_tagging_event(
    doc_id: str,
    tag_result: TagResult,
) -> None:
    """Persist tag audit event to SQLite."""
    top = tag_result.top_candidate
    stage = "deterministic"
    if top and "dynamic_rule" in (top.matched_rules or []):
        stage = "dynamic_rule"
    features = {
        "top_score": top.score if top else 0.0,
        "candidate_count": len(tag_result.all_candidates or []),
    }
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO tagging_events (
                doc_id, stage, confidence, needs_manual_review,
                top_candidate_json, all_candidates_json, reason, features_json, tagged_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                doc_id,
                stage,
                float(tag_result.confidence or 0.0),
                1 if bool(tag_result.needs_manual_review) else 0,
                json.dumps(
                    {
                        "farm_id": top.farm_id,
                        "farm_name": top.farm_name,
                        "score": top.score,
                        "matched_rules": top.matched_rules,
                    }
                )
                if top
                else None,
                json.dumps(
                    [
                        {
                            "farm_id": c.farm_id,
                            "farm_name": c.farm_name,
                            "score": c.score,
                            "matched_rules": c.matched_rules,
                        }
                        for c in (tag_result.all_candidates or [])[:5]
                    ]
                ),
                tag_result.reason,
                json.dumps(features),
            ),
        )
        connection.commit()


def insert_manual_review_queue(
    doc_id: str,
    vision_text: str,
    tag_result: TagResult,
) -> None:
    """Insert manual review queue row."""
    preview = vision_text[:500] + ("..." if len(vision_text) > 500 else "")
    candidates = [
        {
            "farm_id": c.farm_id,
            "farm_name": c.farm_name,
            "score": c.score,
            "matched_rules": c.matched_rules,
        }
        for c in (tag_result.all_candidates or [])[:5]
    ]
    with get_connection() as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO manual_review_queue (
                doc_id, extracted_text_preview, candidates_json, confidence, reason, status, queued_at
            )
            VALUES (?, ?, ?, ?, ?, 'open', datetime('now'))
            """,
            (
                doc_id,
                preview,
                json.dumps(candidates, ensure_ascii=False),
                float(tag_result.confidence or 0.0),
                tag_result.reason,
            ),
        )
        connection.commit()


def insert_transaction_line_items(doc_id: str, line_items: list[dict]) -> None:
    """Insert parsed transaction line items, if provided."""
    if not line_items:
        return
    with get_connection() as connection:
        for idx, item in enumerate(line_items, start=1):
            if not isinstance(item, dict):
                continue
            description = str(
                item.get("description")
                or item.get("name")
                or item.get("item")
                or ""
            ).strip()
            raw_amount = (
                item.get("amount")
                if item.get("amount") is not None
                else item.get("total")
            )
            connection.execute(
                """
                INSERT INTO transaction_line_items (doc_id, line_number, description, amount_cents)
                VALUES (?, ?, ?, ?)
                """,
                (doc_id, idx, description or None, to_cents(raw_amount)),
            )
        connection.commit()


def insert_transaction_record(
    record: dict,
    status: str,
    error_reason: str | None = None,
    duplicate_detected: bool = False,
    duplicate_reason: str | None = None,
    duplicate_of_doc_id: str | None = None,
    parse_status: str = PARSE_STATUS_SUCCESS,
    parse_failure_reason: str | None = None,
) -> None:
    """Insert transaction row from canonical record."""
    assert set(record.keys()) >= REQUIRED_TRANSACTION_KEYS, "Transaction schema violation"
    vendor_key = record.get("vendor_key")
    vendor_name = record.get("vendor_name")
    # Ensure vendor exists before inserting transaction (required for FK integrity)
    if vendor_key:
        execute(
            """
            INSERT OR IGNORE INTO vendors (vendor_key, display_name)
            VALUES (?, ?)
            """,
            (vendor_key, vendor_name or vendor_key),
        )
    total_cents_val = (
        record["total_cents"]
        if record.get("total_cents") is not None
        else to_cents(record.get("total_amount"))
    )
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO transactions (
                doc_id, farm_key, farm_name, vendor_key, vendor_name,
                invoice_number, invoice_date, due_date, total_cents,
                service_address, account_number, confidence, needs_manual_review,
                manual_override, processed_at, status, error_reason, content_fingerprint,
                invoice_key, duplicate_detected, duplicate_reason, duplicate_of_doc_id,
                parse_status, parse_failure_reason, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                record.get("doc_id"),
                record.get("farm_id"),
                record.get("farm_name"),
                record.get("vendor_key"),
                record.get("vendor_name"),
                record.get("invoice_number"),
                normalize_date_iso(record.get("invoice_date")),
                normalize_date_iso(record.get("due_date")),
                total_cents_val,
                record.get("service_address"),
                record.get("account_number"),
                float(record.get("confidence") or 0.0),
                1 if bool(record.get("needs_manual_review")) else 0,
                1 if bool(record.get("manual_override")) else 0,
                record.get("processed_at"),
                status,
                error_reason or record.get("error"),
                record.get("content_fingerprint"),
                record.get("invoice_key"),
                1 if duplicate_detected else 0,
                duplicate_reason,
                duplicate_of_doc_id,
                parse_status,
                parse_failure_reason,
            ),
        )
        connection.commit()


def resolve_vendor_key(farm_id: str, vendor_name: str | None, farms_config: dict) -> str | None:
    """Resolve vendor key from farm config by vendor name (case-insensitive contains)."""
    if not farm_id or not vendor_name or not farms_config:
        return None
    farms_list = farms_config.get("farms") or []
    farm = next(
        (f for f in farms_list if (f.get("id") or f.get("farm_id")) == farm_id),
        None,
    )
    if not farm or "vendors" not in farm:
        return None
    vendor_name_lower = (vendor_name or "").lower()
    for v_key, v_data in (farm.get("vendors") or {}).items():
        if isinstance(v_data, dict):
            v_name = (v_data.get("name") or "").lower()
            if v_name and v_name in vendor_name_lower:
                return v_key
    return None


def append_tag_audit(
    doc_id: str,
    pdf_path: str,
    tag_result: TagResult,
) -> None:
    """Append tag audit record."""
    _ = pdf_path  # Kept to preserve function signature.
    insert_tagging_event(doc_id, tag_result)


def append_manual_review_queue(
    doc_id: str,
    vision_text: str,
    tag_result: TagResult,
) -> None:
    """Add document to manual review queue with rich context."""
    insert_manual_review_queue(doc_id, vision_text, tag_result)


def create_transaction_record(
    doc_id: str,
    vision_text: str,
    content_fingerprint: str,
    farm_tag_result: TagResult | None = None,
    parsed_invoice: dict | None = None,
    vendor_key: str | None = None,
    error: str | None = None,
    manual_override: bool = False,
) -> dict:
    """
    Create unified transaction record with complete canonical schema.
    Single builder for all execution paths; guarantees identical keys.
    """
    record = copy.deepcopy(CANONICAL_TRANSACTION_SCHEMA)
    record["doc_id"] = doc_id
    record["raw_text_hash"] = hash_text(vision_text)
    record["processed_at"] = datetime.datetime.now(datetime.UTC).isoformat()
    record["error"] = error
    record["manual_override"] = manual_override
    record["content_fingerprint"] = content_fingerprint
    record["duplicate_detected"] = False
    record["duplicate_reason"] = None
    record["duplicate_of"] = None

    if farm_tag_result and farm_tag_result.top_candidate:
        record["farm_id"] = farm_tag_result.top_candidate.farm_id
        record["farm_name"] = farm_tag_result.top_candidate.farm_name
        record["confidence"] = farm_tag_result.confidence
        record["needs_manual_review"] = farm_tag_result.needs_manual_review
    else:
        record["confidence"] = 0.0
        record["needs_manual_review"] = True

    if parsed_invoice:
        record["vendor_name"] = parsed_invoice.get("vendor_name")
        record["invoice_number"] = parsed_invoice.get("invoice_number")
        record["invoice_date"] = parsed_invoice.get("invoice_date")
        record["due_date"] = parsed_invoice.get("due_date")
        record["total_amount"] = parsed_invoice.get("total_amount")
        record["service_address"] = parsed_invoice.get("service_address")
        record["account_number"] = parsed_invoice.get("account_number")
        record["line_items"] = parsed_invoice.get("line_items") or []
        record["invoice_key"] = compute_invoice_key(vendor_key, parsed_invoice)
    else:
        record["invoice_key"] = None

    record["vendor_key"] = vendor_key

    return record


def create_duplicate_stub_record(
    doc_id: str,
    content_fingerprint: str,
    farm_tag_result: TagResult,
    parsed_invoice: dict,
    vendor_key: str | None,
    invoice_key: str,
    duplicate_of_doc_id: str | None,
) -> dict:
    """
    Create a complete canonical audit stub for Layer 2 duplicate detections.
    """
    record = copy.deepcopy(CANONICAL_TRANSACTION_SCHEMA)
    record["doc_id"] = doc_id
    record["processed_at"] = datetime.datetime.now(datetime.UTC).isoformat()
    record["content_fingerprint"] = content_fingerprint
    record["invoice_key"] = invoice_key
    record["duplicate_detected"] = True
    record["duplicate_reason"] = "invoice_key"
    record["duplicate_of"] = duplicate_of_doc_id
    record["vendor_key"] = vendor_key
    record["vendor_name"] = (parsed_invoice or {}).get("vendor_name")
    record["needs_manual_review"] = False
    record["manual_override"] = False
    record["error"] = None
    if farm_tag_result and farm_tag_result.top_candidate:
        record["farm_id"] = farm_tag_result.top_candidate.farm_id
        record["farm_name"] = farm_tag_result.top_candidate.farm_name
        record["confidence"] = farm_tag_result.confidence
    return record


def save_invoice_to_json(
    invoice_data: dict,
    pdf_path: str,
    outputs_dir: str | Path,
) -> str:
    """Save validated invoice data to a JSON file."""
    output_dir = Path(outputs_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    base = Path(pdf_path).name
    name_without_ext = Path(base).stem
    json_filename = name_without_ext + ".json"
    out_path = output_dir / json_filename
    json_str = json.dumps(invoice_data, indent=2, ensure_ascii=False)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as f:
            f.write(json_str)
        tmp_path.replace(out_path)
    except OSError:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise
    return str(out_path)


def _resolve_and_validate_pdf_path(pdf_path: str, script_dir: str | Path) -> str:
    """Resolve relative path from script_dir and validate file exists and is PDF."""
    candidate = Path(pdf_path)
    if not candidate.is_absolute():
        candidate = (Path(script_dir) / candidate).resolve()
    if not candidate.exists():
        raise FileNotFoundError(f"File not found: {candidate}")
    if candidate.suffix.lower() != ".pdf":
        raise ValueError(f"File must be a PDF: {candidate}")
    return str(candidate)


def process_single_invoice(
    pdf_path: str,
    config: dict,
    script_dir: str | Path,
    farms_config: dict,
    outputs_dir: str | Path,
    dynamic_rules_config: dict | None = None,
    silent: bool = False,
    verbose: bool = False,
) -> dict:
    """
    Process single invoice: vision text (cached) -> farm resolution -> tag audit ->
    conditional parse -> unified transaction record. All paths use create_transaction_record.
    """
    doc_id = str(uuid.uuid4())
    file_name = Path(pdf_path).name
    api_key = config.get("openai_api_key") or config.get("OPENAI_API_KEY", "")
    content_fingerprint = compute_content_fingerprint("")

    if not silent:
        print(f"\n[{file_name}]", end=" ")

    try:
        pdf_path = _resolve_and_validate_pdf_path(pdf_path, script_dir)
    except (FileNotFoundError, ValueError) as e:
        _print_debug_exception("path_validation", e, file_name, verbose)
        insert_document(
            doc_id=doc_id,
            file_name=file_name,
            file_path=str(pdf_path),
            content_fingerprint=None,
            raw_text_hash=None,
        )
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text="",
            content_fingerprint=content_fingerprint,
            error=f"path_validation: {e}",
        )
        insert_transaction_record(record, status="failed", error_reason=record["error"])
        if not silent:
            print(f"status=failed confidence=0.00 farm=None")
        return {"status": "failed", "reason": "path_validation", "confidence": 0.0}

    try:
        vision_text = extract_invoice_text_with_vision(pdf_path, api_key, max_pages=3)
    except Exception as e:
        _print_debug_exception("vision_extraction_failed", e, file_name, verbose)
        insert_document(
            doc_id=doc_id,
            file_name=file_name,
            file_path=str(pdf_path),
            content_fingerprint=None,
            raw_text_hash=None,
        )
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text="",
            content_fingerprint=content_fingerprint,
            error=f"vision_extraction_failed: {e}",
        )
        insert_transaction_record(record, status="failed", error_reason=record["error"])
        if not silent:
            print(f"status=failed confidence=0.00 farm=None")
        return {"status": "failed", "reason": "vision_extraction_failed", "confidence": 0.0}

    if not (vision_text or "").strip():
        insert_document(
            doc_id=doc_id,
            file_name=file_name,
            file_path=str(pdf_path),
            content_fingerprint=None,
            raw_text_hash=None,
        )
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text="",
            content_fingerprint=content_fingerprint,
            error="empty_vision_extraction",
        )
        insert_transaction_record(record, status="failed", error_reason=record["error"])
        if not silent:
            print(f"status=failed confidence=0.00 farm=None")
        return {"status": "failed", "reason": "empty_extraction", "confidence": 0.0}

    vision_text = vision_text.strip()
    content_fingerprint = compute_content_fingerprint(vision_text)
    if not insert_document(
        doc_id=doc_id,
        file_name=file_name,
        file_path=str(pdf_path),
        content_fingerprint=content_fingerprint,
        raw_text_hash=hash_text(vision_text),
        raw_text=vision_text,
    ):
        if not silent:
            print("status=skipped_duplicate reason=content_fingerprint")
        return {"status": "skipped_duplicate", "reason": "content_fingerprint"}

    try:
        dynamic_rules = (dynamic_rules_config or {}).get("rules") or []
        tag_result = apply_dynamic_rules(vision_text, dynamic_rules, farms_config)
        if tag_result is None:
            tag_result = tag_document_text(vision_text, farms_config)
    except Exception as e:
        _print_debug_exception("farm_tagging_failed", e, file_name, verbose)
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text=vision_text,
            content_fingerprint=content_fingerprint,
            error=f"farm_tagging_failed: {e}",
        )
        insert_transaction_record(record, status="failed", error_reason=record["error"])
        if not silent:
            print(f"status=failed confidence=0.00 farm=None")
        return {"status": "failed", "reason": "farm_tagging_failed", "confidence": 0.0}

    append_tag_audit(doc_id, pdf_path, tag_result)

    invoice_data = None
    parse_error_msg: str | None = None
    parse_status_val = PARSE_STATUS_SUCCESS
    parse_failure_reason_val: str | None = None

    try:
        invoice_data = parse_invoice_with_llm(vision_text, api_key)
    except LLMParseError as e:
        _print_debug_exception("llm_parsing_failed", e, doc_id, verbose)
        parse_error_msg = f"llm_parsing_failed: {e}"
        parse_status_val = PARSE_STATUS_INVALID_JSON
        parse_failure_reason_val = PARSE_STATUS_INVALID_JSON
        invoice_data = None

    if invoice_data is not None:
        valid, failure_reason = validate_invoice_payload(invoice_data)
        if not valid:
            parse_error_msg = f"validation_failed: {failure_reason}"
            parse_status_val = PARSE_STATUS_VALIDATION_FAILED
            parse_failure_reason_val = failure_reason or PARSE_STATUS_VALIDATION_FAILED
            invoice_data = None

    vendor_key = None
    if invoice_data and tag_result.top_candidate:
        vendor_key = resolve_vendor_key(
            tag_result.top_candidate.farm_id,
            invoice_data.get("vendor_name"),
            farms_config,
        )

    if parse_error_msg is not None:
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text=vision_text,
            content_fingerprint=content_fingerprint,
            farm_tag_result=tag_result,
            parsed_invoice=None,
            vendor_key=None,
            error=parse_error_msg,
        )
        record["farm_id"] = None
        record["farm_name"] = None
        insert_transaction_record(
            record,
            status="failed",
            error_reason=parse_error_msg,
            parse_status=parse_status_val,
            parse_failure_reason=parse_failure_reason_val,
        )
        if not silent:
            print(f"status=failed confidence={tag_result.confidence:.2f} farm=None")
        reason = "llm_parsing_failed" if parse_status_val == PARSE_STATUS_INVALID_JSON else "validation_failed"
        return {"status": "failed", "reason": reason, "confidence": tag_result.confidence}

    if tag_result.needs_manual_review or tag_result.confidence < 0.85:
        append_manual_review_queue(doc_id, vision_text, tag_result)
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text=vision_text,
            content_fingerprint=content_fingerprint,
            farm_tag_result=tag_result,
            parsed_invoice=invoice_data,
            vendor_key=vendor_key,
        )
        record["farm_id"] = None
        record["farm_name"] = None
        record["total_cents"] = normalize_total_to_cents(invoice_data["total_amount"])
        insert_transaction_record(
            record,
            status="pending_manual",
            parse_status=PARSE_STATUS_SUCCESS,
            parse_failure_reason=None,
        )
        insert_transaction_line_items(doc_id, record.get("line_items") or [])
        if not silent:
            farm_label = (tag_result.top_candidate.farm_id or "None").upper() if tag_result.top_candidate else "None"
            print(f"status=manual confidence={tag_result.confidence:.2f} farm={farm_label}")
        return {"status": "manual_review", "confidence": tag_result.confidence, "reason": tag_result.reason}

    invoice_key = compute_invoice_key(vendor_key, invoice_data)
    record = create_transaction_record(
        doc_id=doc_id,
        vision_text=vision_text,
        content_fingerprint=content_fingerprint,
        farm_tag_result=tag_result,
        parsed_invoice=invoice_data,
        vendor_key=vendor_key,
    )
    record["total_cents"] = normalize_total_to_cents(invoice_data["total_amount"])
    try:
        insert_transaction_record(
            record,
            status="auto",
            parse_status=PARSE_STATUS_SUCCESS,
            parse_failure_reason=None,
        )
    except sqlite3.IntegrityError as exc:
        message = str(exc)
        if "idx_transactions_invoice_key_original" not in message and "transactions.invoice_key" not in message:
            raise
        original = fetchone(
            """
            SELECT doc_id
            FROM transactions
            WHERE invoice_key = ? AND duplicate_detected = 0
            LIMIT 1
            """,
            (invoice_key,),
        )
        original_doc_id = (original or {}).get("doc_id")
        stub_record = create_duplicate_stub_record(
            doc_id=doc_id,
            content_fingerprint=content_fingerprint,
            farm_tag_result=tag_result,
            parsed_invoice=invoice_data,
            vendor_key=vendor_key,
            invoice_key=invoice_key,
            duplicate_of_doc_id=original_doc_id,
        )
        insert_transaction_record(
            stub_record,
            status="duplicate",
            duplicate_detected=True,
            duplicate_reason="invoice_key",
            duplicate_of_doc_id=original_doc_id,
        )
        if not silent:
            farm_label = (tag_result.top_candidate.farm_id or "UNKNOWN").upper()
            print(
                f"status=skipped_duplicate reason=invoice_key "
                f"confidence={tag_result.confidence:.2f} farm={farm_label}"
            )
        return {
            "status": "skipped_duplicate",
            "reason": "invoice_key",
            "confidence": tag_result.confidence,
            "transaction": stub_record,
        }
    insert_transaction_line_items(doc_id, record.get("line_items") or [])

    saved_path = None
    try:
        saved_path = save_invoice_to_json(invoice_data, pdf_path, outputs_dir)
    except OSError:
        pass

    if not silent:
        farm_label = (tag_result.top_candidate.farm_id or "UNKNOWN").upper()
        print(f"status=auto confidence={tag_result.confidence:.2f} farm={farm_label}")
    if not silent and saved_path:
        print(f"  Saved to {Path(saved_path).name}")

    return {
        "status": "success",
        "confidence": tag_result.confidence,
        "transaction": record,
        "saved_path": saved_path,
        "invoice_data": invoice_data,
    }


def process_batch(
    invoices_dir: str,
    config: dict,
    script_dir: str | Path,
    farms_config: dict,
    dynamic_rules_config: dict | None = None,
    verbose: bool = False,
) -> dict:
    """Process all PDFs in invoices directory with vision text and farm resolution."""
    outputs_dir = STRUCTURED_OUTPUTS_DIR
    invoices_path = Path(script_dir) / invoices_dir
    if not invoices_path.is_dir():
        pdf_files: list[Path] = []
    else:
        pdf_files = sorted(invoices_path.glob("*.pdf"))

    total = len(pdf_files)
    auto_processed = 0
    manual_review = 0
    skipped_duplicates = 0
    failed = 0

    print("Processing all PDFs in invoices/...\n")

    if total == 0:
        print("No PDF files found in invoices/")
        return {
            "total": 0,
            "auto_processed": 0,
            "manual_review": 0,
            "skipped_duplicates": 0,
            "failed": 0,
            "transactions_recorded": 0,
            "review_queue": 0,
        }

    for i, pdf_path in enumerate(pdf_files):
        one_indexed = i + 1
        try:
            display_path = pdf_path.relative_to(Path(script_dir)).as_posix()
        except ValueError:
            display_path = pdf_path.as_posix()
        print(f"[{one_indexed}/{total}] Processing: {display_path}")

        try:
            result = process_single_invoice(
                pdf_path,
                config,
                script_dir,
                farms_config,
                outputs_dir,
                dynamic_rules_config=dynamic_rules_config,
                silent=True,
                verbose=verbose,
            )
        except Exception as e:
            _print_debug_exception("unexpected_error", e, pdf_path.name, verbose)
            failed += 1
            doc_id = str(uuid.uuid4())
            file_name = pdf_path.name
            insert_document(
                doc_id=doc_id,
                file_name=file_name,
                file_path=str(pdf_path),
                content_fingerprint=None,
                raw_text_hash=None,
            )
            record = create_transaction_record(
                doc_id=doc_id,
                vision_text="",
                content_fingerprint=compute_content_fingerprint(""),
                error=f"unexpected_error: {e}",
            )
            insert_transaction_record(record, status="failed", error_reason=record["error"])
            print(f"  status=failed confidence=0.00 farm=None")
            continue

        status = result.get("status", "failed")
        conf = result.get("confidence", 0)
        if status == "success":
            auto_processed += 1
            tx = result.get("transaction")
            farm_label = (tx.get("farm_id") or "UNKNOWN").upper() if tx else "UNKNOWN"
            print(f"  status=auto confidence={conf:.2f} farm={farm_label}")
            if result.get("saved_path"):
                print(f"  Saved to {Path(result['saved_path']).name}")
        elif status == "manual_review":
            manual_review += 1
            farm_label = "None"
            if result.get("transaction") and result["transaction"].get("farm_id"):
                farm_label = (result["transaction"]["farm_id"] or "None").upper()
            print(f"  status=manual confidence={conf:.2f} farm={farm_label}")
        elif status == "skipped_duplicate":
            skipped_duplicates += 1
            reason = result.get("reason", "unknown")
            farm_label = "None"
            tx = result.get("transaction")
            if tx and tx.get("farm_id"):
                farm_label = (tx["farm_id"] or "None").upper()
            if reason == "invoice_key":
                print(
                    f"  status=skipped_duplicate reason=invoice_key "
                    f"confidence={conf:.2f} farm={farm_label}"
                )
            else:
                print("  status=skipped_duplicate reason=content_fingerprint")
        else:
            failed += 1
            print(f"  status=failed confidence={conf:.2f} farm=None")

    transactions_recorded = auto_processed
    review_queue = manual_review

    print("\n=====================================")
    print("Batch Processing Summary")
    print("=====================================")
    print(f"Total files: {total}")
    print(f"Auto-processed: {auto_processed}")
    print(f"Manual review: {manual_review}")
    print(f"Skipped duplicates: {skipped_duplicates}")
    print(f"Failed: {failed}")
    print("=====================================")
    print(f"Transactions recorded: {transactions_recorded}")
    print(f"Review queue: {review_queue}")
    print("=====================================")
    print("\nLedger saved to: data/ledger.db")

    return {
        "total": total,
        "auto_processed": auto_processed,
        "manual_review": manual_review,
        "skipped_duplicates": skipped_duplicates,
        "failed": failed,
        "transactions_recorded": transactions_recorded,
        "review_queue": review_queue,
    }


def main() -> None:
    """Entry point with CLI argument parsing."""
    ensure_data_dirs()
    init_db()
    parser = argparse.ArgumentParser(
        description="Farm Expense Command Center - Invoice Ingestion Pipeline"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--file",
        type=str,
        help="Process a specific PDF file (e.g., invoices/my_invoice.pdf)",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Process all PDF files in the invoices/ directory",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print caught exceptions and full tracebacks for debugging.",
    )
    args = parser.parse_args()

    try:
        config = load_config()
    except ValueError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        farms_config = load_farms(FARMS_CONFIG_PATH)
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        print(f"Farms config error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        dynamic_rules_config = load_dynamic_rules(DYNAMIC_RULES_PATH)
    except Exception as e:
        print(f"Dynamic rules config error: {e}", file=sys.stderr)
        sys.exit(1)

    if args.all:
        process_batch(
            "invoices",
            config,
            BASE_DIR,
            farms_config,
            dynamic_rules_config=dynamic_rules_config,
            verbose=args.verbose,
        )
        return

    if args.file is not None:
        pdf_path = args.file
    else:
        pdf_path = INVOICES_DIR / "PGE_sample_invoice_10-9-25.pdf"

    result = process_single_invoice(
        pdf_path,
        config,
        BASE_DIR,
        farms_config,
        STRUCTURED_OUTPUTS_DIR,
        dynamic_rules_config=dynamic_rules_config,
        silent=False,
        verbose=args.verbose,
    )
    status = result.get("status", "failed")
    if status == "failed":
        sys.exit(1)
    if status == "manual_review":
        sys.exit(0)
    if status == "success" and result.get("invoice_data") is not None:
        print("\nStructured Output:")
        print(json.dumps(result["invoice_data"], indent=2, ensure_ascii=False))


def _print_debug_exception(stage: str, error: Exception, doc_id: str, verbose: bool) -> None:
    """Print traceback details only when verbose debugging is enabled."""
    if not verbose:
        return
    print(
        f"\n[DEBUG] process_single_invoice doc_id={doc_id} stage={stage}: {error}",
        file=sys.stderr,
    )
    traceback.print_exc()


if __name__ == "__main__":
    main()
