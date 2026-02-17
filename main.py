"""Entry point and orchestration for the invoice ingestion pipeline."""

import argparse
import copy
import datetime
import hashlib
import json
import re
import sys
import traceback
from pathlib import Path

from config import load_config
from farm_tagger import TagResult, load_farms, tag_document_text
from rules import apply_dynamic_rules, load_dynamic_rules
from llm_parser import (
    LLMParseError,
    extract_invoice_text_with_vision,
    parse_invoice_with_llm,
)
from validator import validate_invoice
from paths import (
    BASE_DIR,
    DYNAMIC_RULES_PATH,
    FARMS_CONFIG_PATH,
    INVOICES_DIR,
    LEDGER_PATH,
    QUEUE_PATH,
    STRUCTURED_OUTPUTS_DIR,
    TAGS_PATH,
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
seen_content_fingerprints: set[str] = set()
seen_invoice_keys: set[str] = set()


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


def load_ledger_index(ledger_path: str | Path) -> tuple[set[str], set[str]]:
    """
    Build in-memory index of seen fingerprints and invoice keys.

    Returns (seen_content_fingerprints, seen_invoice_keys).
    """
    seen_fingerprints: set[str] = set()
    seen_keys: set[str] = set()
    ledger_file = Path(ledger_path)
    if not ledger_file.exists():
        return seen_fingerprints, seen_keys
    with ledger_file.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            fingerprint = record.get("content_fingerprint")
            if fingerprint:
                seen_fingerprints.add(fingerprint)
            invoice_key = record.get("invoice_key")
            if invoice_key:
                seen_keys.add(invoice_key)
    return seen_fingerprints, seen_keys


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


def update_jsonl_record(filepath: str | Path, updated_record: dict) -> None:
    """
    Update existing record in JSONL file by doc_id.
    Reads entire file, replaces matching record, rewrites file.
    """
    file_path = Path(filepath)
    doc_id = updated_record["doc_id"]
    records = []
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rec = json.loads(line)
                if rec.get("doc_id") == doc_id:
                    records.append(updated_record)
                else:
                    records.append(rec)
    with file_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def append_jsonl(filepath: str | Path, record: dict) -> None:
    """
    Append record to JSONL file with schema validation and duplicate prevention.
    For transactions.jsonl: validates schema, checks for duplicate doc_id,
    updates existing record instead of appending.
    """
    file_path = Path(filepath)
    if file_path.name == "transactions.jsonl":
        record_keys = set(record.keys())
        if record_keys != REQUIRED_TRANSACTION_KEYS:
            missing = REQUIRED_TRANSACTION_KEYS - record_keys
            extra = record_keys - REQUIRED_TRANSACTION_KEYS
            error_msg = []
            if missing:
                error_msg.append(f"Missing keys: {missing}")
            if extra:
                error_msg.append(f"Extra keys: {extra}")
            raise ValueError(f"Transaction record schema violation. {' '.join(error_msg)}")
        if file_path.exists():
            with file_path.open("r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        existing = json.loads(line)
                        if existing.get("doc_id") == record.get("doc_id"):
                            print(f"  → Updating existing transaction for {record['doc_id']}")
                            update_jsonl_record(file_path, record)
                            return
    if file_path.parent:
        file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


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
    top = tag_result.top_candidate
    record = {
        "doc_id": doc_id,
        "pdf_path": pdf_path,
        "confidence": tag_result.confidence,
        "needs_manual_review": tag_result.needs_manual_review,
        "top_candidate": (
            {
                "farm_id": top.farm_id,
                "farm_name": top.farm_name,
                "score": top.score,
                "matched_rules": top.matched_rules,
            }
            if top
            else None
        ),
        "all_candidates": [
            {
                "farm_id": c.farm_id,
                "farm_name": c.farm_name,
                "score": c.score,
                "matched_rules": c.matched_rules,
            }
            for c in tag_result.all_candidates[:5]
        ],
        "reason": tag_result.reason,
        "tagged_at": datetime.datetime.now(datetime.UTC).isoformat(),
    }
    append_jsonl(TAGS_PATH, record)


def append_manual_review_queue(
    doc_id: str,
    vision_text: str,
    tag_result: TagResult,
) -> None:
    """Add document to manual review queue with rich context."""
    preview = vision_text[:500] + ("..." if len(vision_text) > 500 else "")
    record = {
        "doc_id": doc_id,
        "extracted_text_preview": preview,
        "candidates": [
            {
                "farm_id": c.farm_id,
                "farm_name": c.farm_name,
                "score": c.score,
                "matched_rules": c.matched_rules,
            }
            for c in tag_result.all_candidates[:5]
        ],
        "confidence": tag_result.confidence,
        "reason": tag_result.reason,
        "queued_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "resolved": False,
    }
    append_jsonl(QUEUE_PATH, record)


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
    record["duplicate_of"] = invoice_key
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
    doc_id = Path(pdf_path).name
    api_key = config.get("openai_api_key") or config.get("OPENAI_API_KEY", "")
    tx_path = LEDGER_PATH
    content_fingerprint = compute_content_fingerprint("")

    if not silent:
        print(f"\n[{doc_id}]", end=" ")

    try:
        pdf_path = _resolve_and_validate_pdf_path(pdf_path, script_dir)
    except (FileNotFoundError, ValueError) as e:
        _print_debug_exception("path_validation", e, doc_id, verbose)
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text="",
            content_fingerprint=content_fingerprint,
            error=f"path_validation: {e}",
        )
        append_jsonl(tx_path, record)
        if not silent:
            print(f"status=failed confidence=0.00 farm=None")
        return {"status": "failed", "reason": "path_validation", "confidence": 0.0}

    try:
        vision_text = extract_invoice_text_with_vision(pdf_path, api_key, max_pages=3)
    except Exception as e:
        _print_debug_exception("vision_extraction_failed", e, doc_id, verbose)
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text="",
            content_fingerprint=content_fingerprint,
            error=f"vision_extraction_failed: {e}",
        )
        append_jsonl(tx_path, record)
        if not silent:
            print(f"status=failed confidence=0.00 farm=None")
        return {"status": "failed", "reason": "vision_extraction_failed", "confidence": 0.0}

    if not (vision_text or "").strip():
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text="",
            content_fingerprint=content_fingerprint,
            error="empty_vision_extraction",
        )
        append_jsonl(tx_path, record)
        if not silent:
            print(f"status=failed confidence=0.00 farm=None")
        return {"status": "failed", "reason": "empty_extraction", "confidence": 0.0}

    vision_text = vision_text.strip()
    content_fingerprint = compute_content_fingerprint(vision_text)
    global seen_content_fingerprints
    if content_fingerprint in seen_content_fingerprints:
        if not silent:
            print("status=skipped_duplicate reason=content_fingerprint")
        return {"status": "skipped_duplicate", "reason": "content_fingerprint"}
    seen_content_fingerprints.add(content_fingerprint)

    try:
        dynamic_rules = (dynamic_rules_config or {}).get("rules") or []
        tag_result = apply_dynamic_rules(vision_text, dynamic_rules, farms_config)
        if tag_result is None:
            tag_result = tag_document_text(vision_text, farms_config)
    except Exception as e:
        _print_debug_exception("farm_tagging_failed", e, doc_id, verbose)
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text=vision_text,
            content_fingerprint=content_fingerprint,
            error=f"farm_tagging_failed: {e}",
        )
        append_jsonl(tx_path, record)
        if not silent:
            print(f"status=failed confidence=0.00 farm=None")
        return {"status": "failed", "reason": "farm_tagging_failed", "confidence": 0.0}

    append_tag_audit(doc_id, pdf_path, tag_result)

    invoice_data = None
    parse_error = None
    try:
        invoice_data = parse_invoice_with_llm(vision_text, api_key)
        validate_invoice(invoice_data)
    except LLMParseError as e:
        _print_debug_exception("llm_parsing_failed", e, doc_id, verbose)
        parse_error = f"llm_parsing_failed: {e}"
    except ValueError as e:
        _print_debug_exception("validation_failed", e, doc_id, verbose)
        parse_error = f"validation_failed: {e}"

    vendor_key = None
    if invoice_data and tag_result.top_candidate:
        vendor_key = resolve_vendor_key(
            tag_result.top_candidate.farm_id,
            invoice_data.get("vendor_name"),
            farms_config,
        )

    if tag_result.needs_manual_review or tag_result.confidence < 0.85:
        append_manual_review_queue(doc_id, vision_text, tag_result)
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text=vision_text,
            content_fingerprint=content_fingerprint,
            farm_tag_result=tag_result,
            parsed_invoice=invoice_data,
            vendor_key=vendor_key,
            error=parse_error,
        )
        append_jsonl(tx_path, record)
        if not silent:
            farm_label = (tag_result.top_candidate.farm_id or "None").upper() if tag_result.top_candidate else "None"
            print(f"status=manual confidence={tag_result.confidence:.2f} farm={farm_label}")
        return {"status": "manual_review", "confidence": tag_result.confidence, "reason": tag_result.reason}

    if parse_error:
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text=vision_text,
            content_fingerprint=content_fingerprint,
            farm_tag_result=tag_result,
            parsed_invoice=None,
            vendor_key=None,
            error=parse_error,
        )
        append_jsonl(tx_path, record)
        if not silent:
            print(f"status=failed confidence={tag_result.confidence:.2f} farm=None")
        reason = "llm_parsing_failed" if parse_error.startswith("llm_parsing_failed") else "validation_failed"
        return {"status": "failed", "reason": reason, "confidence": tag_result.confidence}

    invoice_key = compute_invoice_key(vendor_key, invoice_data)
    global seen_invoice_keys
    if invoice_key and invoice_key in seen_invoice_keys:
        stub_record = create_duplicate_stub_record(
            doc_id=doc_id,
            content_fingerprint=content_fingerprint,
            farm_tag_result=tag_result,
            parsed_invoice=invoice_data,
            vendor_key=vendor_key,
            invoice_key=invoice_key,
        )
        append_jsonl(tx_path, stub_record)
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
    if invoice_key:
        seen_invoice_keys.add(invoice_key)

    record = create_transaction_record(
        doc_id=doc_id,
        vision_text=vision_text,
        content_fingerprint=content_fingerprint,
        farm_tag_result=tag_result,
        parsed_invoice=invoice_data,
        vendor_key=vendor_key,
    )
    append_jsonl(tx_path, record)

    saved_path = None
    try:
        saved_path = save_invoice_to_json(invoice_data, pdf_path, outputs_dir)
    except OSError:
        pass

    if not silent:
        farm_label = (tag_result.top_candidate.farm_id or "UNKNOWN").upper()
        print(f"status=auto confidence={tag_result.confidence:.2f} farm={farm_label}")
    if not silent and saved_path:
        print(f"  ✓ Saved to {Path(saved_path).name}")

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
            doc_id = pdf_path.name
            record = create_transaction_record(
                doc_id=doc_id,
                vision_text="",
                content_fingerprint=compute_content_fingerprint(""),
                error=f"unexpected_error: {e}",
            )
            append_jsonl(LEDGER_PATH, record)
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
                print(f"  ✓ Saved to {Path(result['saved_path']).name}")
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
    print(f"\nLedger saved to: {LEDGER_PATH}")

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

    global seen_content_fingerprints, seen_invoice_keys
    seen_content_fingerprints, seen_invoice_keys = load_ledger_index(LEDGER_PATH)
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
