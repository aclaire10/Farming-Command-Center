"""Entry point and orchestration for the invoice ingestion pipeline."""

import argparse
import copy
import datetime
import glob
import hashlib
import json
import os
import re
import sys

from config import load_config
from farm_tagger import TagResult, load_farms, tag_document_text
from llm_parser import (
    LLMParseError,
    extract_invoice_text_with_vision,
    parse_invoice_with_llm,
)
from validator import validate_invoice


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
}

REQUIRED_TRANSACTION_KEYS = set(CANONICAL_TRANSACTION_SCHEMA.keys())


def hash_text(text: str) -> str:
    """Generate SHA256 hash of text for deduplication."""
    return f"sha256:{hashlib.sha256(text.encode('utf-8')).hexdigest()[:16]}"


def update_jsonl_record(filepath: str, updated_record: dict) -> None:
    """
    Update existing record in JSONL file by doc_id.
    Reads entire file, replaces matching record, rewrites file.
    """
    doc_id = updated_record["doc_id"]
    records = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rec = json.loads(line)
                if rec.get("doc_id") == doc_id:
                    records.append(updated_record)
                else:
                    records.append(rec)
    with open(filepath, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def append_jsonl(filepath: str, record: dict) -> None:
    """
    Append record to JSONL file with schema validation and duplicate prevention.
    For transactions.jsonl: validates schema, checks for duplicate doc_id,
    updates existing record instead of appending.
    """
    if filepath.endswith("transactions.jsonl"):
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
        if os.path.exists(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        existing = json.loads(line)
                        if existing.get("doc_id") == record.get("doc_id"):
                            print(f"  → Updating existing transaction for {record['doc_id']}")
                            update_jsonl_record(filepath, record)
                            return
    d = os.path.dirname(filepath)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(filepath, "a", encoding="utf-8") as f:
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
    outputs_dir: str,
) -> None:
    """Append tag audit record to outputs/tags.jsonl."""
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
    append_jsonl(os.path.join(outputs_dir, "tags.jsonl"), record)


def append_manual_review_queue(
    doc_id: str,
    vision_text: str,
    tag_result: TagResult,
    outputs_dir: str,
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
    append_jsonl(os.path.join(outputs_dir, "manual_review_queue.jsonl"), record)


def create_transaction_record(
    doc_id: str,
    vision_text: str,
    farm_tag_result: TagResult | None = None,
    parsed_invoice: dict | None = None,
    error: str | None = None,
    manual_override: bool = False,
    farms_config: dict | None = None,
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
        if record["farm_id"] and record["vendor_name"] and farms_config:
            record["vendor_key"] = resolve_vendor_key(
                record["farm_id"], record["vendor_name"], farms_config
            )

    return record


def save_invoice_to_json(
    invoice_data: dict,
    pdf_path: str,
    outputs_dir: str,
) -> str:
    """Save validated invoice data to a JSON file."""
    os.makedirs(outputs_dir, exist_ok=True)
    base = os.path.basename(pdf_path)
    name_without_ext, _ = os.path.splitext(base)
    json_filename = name_without_ext + ".json"
    out_path = os.path.join(outputs_dir, json_filename)
    json_str = json.dumps(invoice_data, indent=2, ensure_ascii=False)
    tmp_path = out_path + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(json_str)
        os.replace(tmp_path, out_path)
    except OSError:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise
    return out_path


def _resolve_and_validate_pdf_path(pdf_path: str, script_dir: str) -> str:
    """Resolve relative path from script_dir and validate file exists and is PDF."""
    if not os.path.isabs(pdf_path):
        pdf_path = os.path.normpath(os.path.join(script_dir, pdf_path))
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"File not found: {pdf_path}")
    if not pdf_path.lower().endswith(".pdf"):
        raise ValueError(f"File must be a PDF: {pdf_path}")
    return pdf_path


def process_single_invoice(
    pdf_path: str,
    config: dict,
    script_dir: str,
    farms_config: dict,
    outputs_dir: str,
    silent: bool = False,
) -> dict:
    """
    Process single invoice: vision text (cached) -> farm resolution -> tag audit ->
    conditional parse -> unified transaction record. All paths use create_transaction_record.
    """
    doc_id = os.path.basename(pdf_path)
    api_key = config.get("openai_api_key") or config.get("OPENAI_API_KEY", "")
    tx_path = os.path.join(outputs_dir, "transactions.jsonl")

    if not silent:
        print(f"\n[{doc_id}]", end=" ")

    try:
        pdf_path = _resolve_and_validate_pdf_path(pdf_path, script_dir)
    except (FileNotFoundError, ValueError) as e:
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text="",
            error=f"path_validation: {e}",
        )
        append_jsonl(tx_path, record)
        if not silent:
            print(f"status=failed confidence=0.00 farm=None")
        return {"status": "failed", "reason": "path_validation", "confidence": 0.0}

    try:
        vision_text = extract_invoice_text_with_vision(pdf_path, api_key, max_pages=3)
    except Exception as e:
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text="",
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
            error="empty_vision_extraction",
        )
        append_jsonl(tx_path, record)
        if not silent:
            print(f"status=failed confidence=0.00 farm=None")
        return {"status": "failed", "reason": "empty_extraction", "confidence": 0.0}

    vision_text = vision_text.strip()

    try:
        tag_result = tag_document_text(vision_text, farms_config)
    except Exception as e:
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text=vision_text,
            error=f"farm_tagging_failed: {e}",
        )
        append_jsonl(tx_path, record)
        if not silent:
            print(f"status=failed confidence=0.00 farm=None")
        return {"status": "failed", "reason": "farm_tagging_failed", "confidence": 0.0}

    append_tag_audit(doc_id, pdf_path, tag_result, outputs_dir)

    if tag_result.needs_manual_review or tag_result.confidence < 0.85:
        append_manual_review_queue(doc_id, vision_text, tag_result, outputs_dir)
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text=vision_text,
            farm_tag_result=tag_result,
            parsed_invoice=None,
            farms_config=farms_config,
        )
        append_jsonl(tx_path, record)
        if not silent:
            farm_label = (tag_result.top_candidate.farm_id or "None").upper() if tag_result.top_candidate else "None"
            print(f"status=manual confidence={tag_result.confidence:.2f} farm={farm_label}")
        return {"status": "manual_review", "confidence": tag_result.confidence, "reason": tag_result.reason}

    try:
        invoice_data = parse_invoice_with_llm(pdf_path, api_key)
        validate_invoice(invoice_data)
    except LLMParseError as e:
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text=vision_text,
            farm_tag_result=tag_result,
            parsed_invoice=None,
            error=f"llm_parsing_failed: {e}",
            farms_config=farms_config,
        )
        append_jsonl(tx_path, record)
        if not silent:
            print(f"status=failed confidence={tag_result.confidence:.2f} farm=None")
        return {"status": "failed", "reason": "llm_parsing_failed", "confidence": tag_result.confidence}
    except ValueError as e:
        record = create_transaction_record(
            doc_id=doc_id,
            vision_text=vision_text,
            farm_tag_result=tag_result,
            parsed_invoice=None,
            error=f"validation_failed: {e}",
            farms_config=farms_config,
        )
        append_jsonl(tx_path, record)
        if not silent:
            print(f"status=failed confidence={tag_result.confidence:.2f} farm=None")
        return {"status": "failed", "reason": "validation_failed", "confidence": tag_result.confidence}

    record = create_transaction_record(
        doc_id=doc_id,
        vision_text=vision_text,
        farm_tag_result=tag_result,
        parsed_invoice=invoice_data,
        farms_config=farms_config,
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
        print(f"  ✓ Saved to outputs/{os.path.basename(saved_path)}")

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
    script_dir: str,
    farms_config: dict,
) -> dict:
    """Process all PDFs in invoices directory with vision text and farm resolution."""
    outputs_dir = os.path.join(script_dir, "outputs")
    invoices_path = os.path.join(script_dir, invoices_dir)
    if not os.path.isdir(invoices_path):
        pdf_files = []
    else:
        pdf_files = sorted(glob.glob(os.path.join(invoices_path, "*.pdf")))

    total = len(pdf_files)
    auto_processed = 0
    manual_review = 0
    failed = 0

    print("Processing all PDFs in invoices/...\n")

    if total == 0:
        print("No PDF files found in invoices/")
        return {
            "total": 0,
            "auto_processed": 0,
            "manual_review": 0,
            "failed": 0,
            "transactions_recorded": 0,
            "review_queue": 0,
        }

    for i, pdf_path in enumerate(pdf_files):
        one_indexed = i + 1
        display_path = os.path.relpath(pdf_path, script_dir)
        if os.path.sep != "/":
            display_path = display_path.replace(os.path.sep, "/")
        print(f"[{one_indexed}/{total}] Processing: {display_path}")

        try:
            result = process_single_invoice(
                pdf_path, config, script_dir, farms_config, outputs_dir, silent=True
            )
        except Exception as e:
            failed += 1
            doc_id = os.path.basename(pdf_path)
            record = create_transaction_record(
                doc_id=doc_id,
                vision_text="",
                error=f"unexpected_error: {e}",
            )
            append_jsonl(os.path.join(outputs_dir, "transactions.jsonl"), record)
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
                print(f"  ✓ Saved to outputs/{os.path.basename(result['saved_path'])}")
        elif status == "manual_review":
            manual_review += 1
            farm_label = "None"
            if result.get("transaction") and result["transaction"].get("farm_id"):
                farm_label = (result["transaction"]["farm_id"] or "None").upper()
            print(f"  status=manual confidence={conf:.2f} farm={farm_label}")
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
    print(f"Failed: {failed}")
    print("=====================================")
    print(f"Transactions recorded: {transactions_recorded}")
    print(f"Review queue: {review_queue}")
    print("=====================================")
    print(f"\nLedger saved to: {os.path.join(outputs_dir, 'transactions.jsonl')}")

    return {
        "total": total,
        "auto_processed": auto_processed,
        "manual_review": manual_review,
        "failed": failed,
        "transactions_recorded": transactions_recorded,
        "review_queue": review_queue,
    }


def main() -> None:
    """Entry point with CLI argument parsing."""
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
    args = parser.parse_args()

    try:
        config = load_config()
    except ValueError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    outputs_dir = os.path.join(script_dir, "outputs")

    farms_config_path = os.path.join(script_dir, "config", "farms.json")
    try:
        farms_config = load_farms(farms_config_path)
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        print(f"Farms config error: {e}", file=sys.stderr)
        sys.exit(1)

    if args.all:
        process_batch("invoices", config, script_dir, farms_config)
        return

    if args.file is not None:
        pdf_path = args.file
    else:
        pdf_path = os.path.join(script_dir, "invoices", "PGE_sample_invoice_10-9-25.pdf")

    result = process_single_invoice(
        pdf_path, config, script_dir, farms_config, outputs_dir, silent=False
    )
    status = result.get("status", "failed")
    if status == "failed":
        sys.exit(1)
    if status == "manual_review":
        sys.exit(0)
    if status == "success" and result.get("invoice_data") is not None:
        print("\nStructured Output:")
        print(json.dumps(result["invoice_data"], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
