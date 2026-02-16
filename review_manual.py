"""Interactive manual review CLI for unresolved farm assignments."""

from __future__ import annotations

import argparse
import datetime
import os
import re
import sys
from typing import Any

from farm_tagger import load_farms
from ledger import LedgerIOError, append_jsonl, atomic_rewrite_jsonl, read_jsonl
from rules import (
    check_account_collision,
    ensure_dynamic_rules_file,
    generate_rule_id,
    normalize_text,
    upsert_dynamic_rule,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Resolve manual review queue and reinforce deterministic rules."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show actions without writing files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    outputs_dir = os.path.join(script_dir, "outputs")
    queue_path = os.path.join(outputs_dir, "manual_review_queue.jsonl")
    decisions_path = os.path.join(outputs_dir, "manual_review_decisions.jsonl")
    transactions_path = os.path.join(outputs_dir, "transactions.jsonl")
    farms_path = os.path.join(script_dir, "config", "farms.json")
    dynamic_rules_path = os.path.join(script_dir, "config", "dynamic_rules.json")

    try:
        farms_config = load_farms(farms_path)
        queue_rows = read_jsonl(queue_path)
        transactions_rows = read_jsonl(transactions_path)
        dynamic_rules_payload = ensure_dynamic_rules_file(dynamic_rules_path)
    except (FileNotFoundError, ValueError, LedgerIOError) as exc:
        print(f"Initialization failed: {exc}", file=sys.stderr)
        sys.exit(1)

    unresolved = [
        row for row in queue_rows if isinstance(row, dict) and not bool(row.get("resolved"))
    ]
    print(f"You have {len(unresolved)} unresolved items")
    if not unresolved:
        return

    farms_lookup = build_farm_lookup(farms_config)
    queue_writes_needed = False
    transaction_writes_needed = False
    resolved_count = 0
    accepted_rule_count = 0

    for item in unresolved:
        print("\n" + "=" * 72)
        doc_id = str(item.get("doc_id") or "")
        reason = item.get("reason") or "No reason provided"
        confidence = float(item.get("confidence") or 0.0)
        print(f"Document: {doc_id}")
        print(f"Reason: {reason}")
        print(f"Confidence: {confidence:.2f}")

        top_candidate = (item.get("candidates") or [None])[0]
        if isinstance(top_candidate, dict):
            predicted_farm = top_candidate.get("farm_id") or "unknown"
            predicted_name = top_candidate.get("farm_name") or predicted_farm
            predicted_score = float(top_candidate.get("score") or 0.0)
            print(
                "System prediction: "
                f"{predicted_farm} ({predicted_name}) "
                f"with candidate score {predicted_score:.2f} and confidence {confidence:.2f}"
            )
        else:
            print("System prediction: unavailable")

        print("\nOCR text preview:")
        print((item.get("extracted_text_preview") or "")[:500])

        selection = prompt_farm_selection(item, farms_lookup)
        if selection is None:
            print("Skipped.")
            continue
        selected_farm_id, selected_farm_name = selection

        tx_meta = locate_transaction_for_queue_item(transactions_rows, doc_id)
        if tx_meta is None:
            print(
                "Unable to resolve transaction row for this queue item. "
                "No changes were applied."
            )
            continue

        content_fingerprint = tx_meta.get("content_fingerprint")
        invoice_key = tx_meta.get("invoice_key")
        if not content_fingerprint:
            print(
                "Missing content_fingerprint in transaction row; cannot safely update "
                "transactions. No changes were applied."
            )
            continue

        decision = {
            "doc_id": doc_id,
            "content_fingerprint": content_fingerprint,
            "invoice_key": invoice_key,
            "selected_farm_id": selected_farm_id,
            "selected_farm_name": selected_farm_name,
            "decision_source": "manual_review",
            "created_at": datetime.datetime.now(datetime.UTC).isoformat(),
            "notes": None,
        }

        proposals = propose_dynamic_rules(
            doc_id=doc_id,
            selected_farm_id=selected_farm_id,
            selected_farm_name=selected_farm_name,
            transaction_row=tx_meta,
            farms_config=farms_config,
            dynamic_rules_payload=dynamic_rules_payload,
            transactions_rows=transactions_rows,
            script_dir=script_dir,
        )

        accepted_rules: list[dict[str, Any]] = []
        if proposals:
            print("\nProposed reinforcement rules:")
            for idx, proposal in enumerate(proposals, start=1):
                readable = format_rule_prompt(idx, proposal)
                print(readable)
                answer = input("Add this rule? (y/n): ").strip().lower()
                if answer in {"y", "yes"}:
                    accepted_rules.append(proposal)
        else:
            print("\nNo safe reinforcement rule candidates were found for this item.")

        if args.dry_run:
            print("\n[DRY-RUN] Would append decision:")
            print(decision)
            print(
                "[DRY-RUN] Would mark queue item resolved and update matching transaction "
                "by content_fingerprint."
            )
            for proposal in accepted_rules:
                candidate_rule_id = generate_rule_id(proposal)
                duplicate_exists = any(
                    r.get("rule_id") == candidate_rule_id
                    for r in (dynamic_rules_payload.get("rules") or [])
                )
                if duplicate_exists:
                    print(f"[DRY-RUN] Rule already exists: {candidate_rule_id}")
                else:
                    print(f"[DRY-RUN] Would add dynamic rule: {candidate_rule_id}")
            resolved_count += 1
            continue

        append_jsonl(decisions_path, decision)
        item["resolved"] = True
        queue_writes_needed = True

        updated_index, ambiguous_matches = update_transaction_assignment(
            transactions_rows,
            content_fingerprint=content_fingerprint,
            doc_id=doc_id,
            selected_farm_id=selected_farm_id,
            selected_farm_name=selected_farm_name,
        )
        if updated_index is None:
            print(
                "Warning: Transaction row update failed by content_fingerprint. "
                "Queue decision recorded; transaction row unchanged."
            )
        else:
            transaction_writes_needed = True
            if ambiguous_matches:
                print(
                    f"Warning: {ambiguous_matches} ambiguous transaction matches found; "
                    "updated first non-duplicate row."
                )

        for proposal in accepted_rules:
            created, rule_id = upsert_dynamic_rule(dynamic_rules_path, proposal)
            if created:
                accepted_rule_count += 1
                print(f"Added dynamic rule: {rule_id}")
                dynamic_rules_payload = ensure_dynamic_rules_file(dynamic_rules_path)
            else:
                print(f"Rule already exists: {rule_id}")

        resolved_count += 1

    if args.dry_run:
        print(
            f"\nDry-run complete. Reviewed {resolved_count} item(s). "
            "No files were modified."
        )
        return

    try:
        if queue_writes_needed:
            atomic_rewrite_jsonl(queue_path, queue_rows)
        if transaction_writes_needed:
            atomic_rewrite_jsonl(transactions_path, transactions_rows)
    except LedgerIOError as exc:
        print(f"Write failed: {exc}", file=sys.stderr)
        sys.exit(1)

    print(
        "\nManual review complete. "
        f"Resolved {resolved_count} item(s); added {accepted_rule_count} rule(s)."
    )


def build_farm_lookup(farms_config: dict[str, Any]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for farm in farms_config.get("farms") or []:
        farm_id = str(farm.get("id") or farm.get("farm_id") or "").strip()
        farm_name = str(farm.get("name") or farm_id).strip()
        if farm_id:
            lookup[farm_id] = farm_name
    return lookup


def prompt_farm_selection(
    queue_item: dict[str, Any],
    farms_lookup: dict[str, str],
) -> tuple[str, str] | None:
    candidates = queue_item.get("candidates") or []
    if candidates:
        print("\nCandidates (terminal dropdown):")
        for idx, candidate in enumerate(candidates, start=1):
            farm_id = candidate.get("farm_id") or "unknown"
            farm_name = candidate.get("farm_name") or farm_id
            score = float(candidate.get("score") or 0.0)
            print(f"{idx}. {farm_id} ({farm_name}) - score: {score:.2f}")
    else:
        print("\nNo candidates found in queue item.")

    print("\nOr enter farm ID manually.")
    while True:
        raw = input("Select farm (number, farm_id, s=skip): ").strip()
        if not raw:
            continue
        if raw.lower() == "s":
            return None
        if raw.isdigit():
            idx = int(raw) - 1
            if 0 <= idx < len(candidates):
                farm_id = str(candidates[idx].get("farm_id") or "").strip()
                farm_name = str(candidates[idx].get("farm_name") or farm_id).strip()
                if farm_id:
                    return farm_id, farm_name
            print("Invalid candidate number. Try again.")
            continue
        farm_id = raw
        if farm_id in farms_lookup:
            return farm_id, farms_lookup[farm_id]
        print("Unknown farm_id. Enter a listed candidate number or valid farm_id.")


def locate_transaction_for_queue_item(
    transactions_rows: list[dict[str, Any]],
    doc_id: str,
) -> dict[str, Any] | None:
    matches = [row for row in transactions_rows if row.get("doc_id") == doc_id]
    if not matches:
        return None
    non_duplicate = [row for row in matches if not bool(row.get("duplicate_detected"))]
    return non_duplicate[0] if non_duplicate else matches[0]


def update_transaction_assignment(
    transactions_rows: list[dict[str, Any]],
    content_fingerprint: str,
    doc_id: str,
    selected_farm_id: str,
    selected_farm_name: str,
) -> tuple[int | None, int]:
    matching_indices = [
        idx
        for idx, row in enumerate(transactions_rows)
        if row.get("content_fingerprint") == content_fingerprint
    ]
    if not matching_indices:
        return None, 0

    narrowed = [
        idx for idx in matching_indices if str(transactions_rows[idx].get("doc_id") or "") == doc_id
    ]
    candidate_indices = narrowed if narrowed else matching_indices
    ambiguous_count = len(candidate_indices)

    selected_index = next(
        (idx for idx in candidate_indices if not bool(transactions_rows[idx].get("duplicate_detected"))),
        candidate_indices[0],
    )
    target = transactions_rows[selected_index]
    target["farm_id"] = selected_farm_id
    target["farm_name"] = selected_farm_name
    target["needs_manual_review"] = False
    target["manual_override"] = True
    return selected_index, ambiguous_count if ambiguous_count > 1 else 0


def propose_dynamic_rules(
    doc_id: str,
    selected_farm_id: str,
    selected_farm_name: str,
    transaction_row: dict[str, Any],
    farms_config: dict[str, Any],
    dynamic_rules_payload: dict[str, Any],
    transactions_rows: list[dict[str, Any]],
    script_dir: str,
) -> list[dict[str, Any]]:
    ocr_text = load_cached_ocr_text(script_dir, doc_id)
    vendor_key = transaction_row.get("vendor_key") or infer_vendor_key_from_text(
        ocr_text, farms_config
    )
    account_number = transaction_row.get("account_number") or extract_account_number(ocr_text)
    invoice_number = transaction_row.get("invoice_number")
    service_address = transaction_row.get("service_address") or extract_service_address_hint(ocr_text)

    if not vendor_key or not account_number:
        return []

    collision = check_account_collision(
        vendor_key=str(vendor_key),
        account_number=str(account_number),
        farms_config=farms_config,
        dynamic_rules_payload=dynamic_rules_payload,
        transactions_rows=transactions_rows,
    )

    service_disambiguators = extract_service_disambiguators(service_address)
    keyword_disambiguators = extract_keyword_disambiguators(
        selected_farm_name=selected_farm_name,
        selected_farm_id=selected_farm_id,
        ocr_text=ocr_text,
    )

    if collision and not service_disambiguators and not keyword_disambiguators:
        return []

    base_payload = {
        "vendor_key": str(vendor_key),
        "account_number": str(account_number),
        "invoice_number": str(invoice_number) if invoice_number else None,
        "farm_id": selected_farm_id,
        "priority": 100,
        "created_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "evidence": {
            "doc_id": doc_id,
            "content_fingerprint": transaction_row.get("content_fingerprint"),
            "invoice_key": transaction_row.get("invoice_key"),
        },
    }

    proposals: list[dict[str, Any]] = []
    if service_disambiguators:
        proposal = dict(base_payload)
        proposal["service_address_contains"] = service_disambiguators[:2]
        proposal["keywords_any"] = []
        proposals.append(proposal)
    if keyword_disambiguators:
        proposal = dict(base_payload)
        proposal["service_address_contains"] = []
        proposal["keywords_any"] = keyword_disambiguators[:3]
        proposals.append(proposal)

    # Enforce contextual disambiguator when collision exists.
    if collision:
        proposals = [
            p
            for p in proposals
            if (p.get("service_address_contains") or p.get("keywords_any"))
        ]
    return proposals[:3]


def load_cached_ocr_text(script_dir: str, doc_id: str) -> str:
    cache_path = os.path.join(script_dir, "outputs", "vision_text_cache", f"{doc_id}.txt")
    if not os.path.exists(cache_path):
        return ""
    with open(cache_path, "r", encoding="utf-8") as handle:
        return handle.read()


def extract_account_number(text: str) -> str | None:
    patterns = [
        r"account\s*(?:no|number|#)?\s*[:\-]?\s*([a-z0-9\-]{4,})",
        r"acct\s*[:\-]?\s*([a-z0-9\-]{4,})",
    ]
    lower = normalize_text(text)
    for pattern in patterns:
        match = re.search(pattern, lower, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None


def extract_service_address_hint(text: str) -> str | None:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    candidates: list[str] = []
    for line in lines:
        lowered = normalize_text(line)
        if (
            "service for" in lowered
            or "po box" in lowered
            or re.search(r"\b(?:rd|road|street|st|ave|blvd|ca)\b", lowered)
        ):
            candidates.append(line)
    if not candidates:
        return None
    return ", ".join(candidates[:2])


def extract_service_disambiguators(service_address: str | None) -> list[str]:
    if not service_address:
        return []
    normalized = normalize_text(service_address)
    tokens = [segment.strip() for segment in re.split(r"[,;]", normalized) if segment.strip()]
    out: list[str] = []
    for token in tokens:
        cleaned = re.sub(r"\s+", " ", token).strip()
        if len(cleaned) < 4:
            continue
        if cleaned not in out:
            out.append(cleaned)
    return out[:3]


def extract_keyword_disambiguators(
    selected_farm_name: str,
    selected_farm_id: str,
    ocr_text: str,
) -> list[str]:
    normalized_ocr = normalize_text(ocr_text)
    farm_tokens = re.findall(r"[a-z0-9]{4,}", normalize_text(selected_farm_name))
    farm_tokens.extend(re.findall(r"[a-z0-9]{4,}", normalize_text(selected_farm_id)))
    keywords: list[str] = []
    for token in farm_tokens:
        if token in {"farm", "farms", "expenses", "ranch"}:
            continue
        if token in normalized_ocr and token not in keywords:
            keywords.append(token)
    return keywords[:3]


def infer_vendor_key_from_text(ocr_text: str, farms_config: dict[str, Any]) -> str | None:
    lowered = normalize_text(ocr_text)
    scores: dict[str, int] = {}
    for farm in farms_config.get("farms") or []:
        vendors = farm.get("vendors") or {}
        for vendor_key, vendor_cfg in vendors.items():
            terms: list[str] = [normalize_text(str(vendor_key))]
            if isinstance(vendor_cfg, dict):
                terms.append(normalize_text(vendor_cfg.get("name")))
                for kw in vendor_cfg.get("keywords") or []:
                    terms.append(normalize_text(str(kw)))
            unique_terms = {t for t in terms if t and len(t) >= 3}
            for term in unique_terms:
                if term in lowered:
                    scores[vendor_key] = scores.get(vendor_key, 0) + 1
    if not scores:
        return None
    ranked = sorted(scores.items(), key=lambda pair: pair[1], reverse=True)
    if len(ranked) > 1 and ranked[0][1] == ranked[1][1]:
        return None
    return ranked[0][0]


def format_rule_prompt(index: int, proposal: dict[str, Any]) -> str:
    vendor_key = proposal.get("vendor_key")
    account_number = proposal.get("account_number")
    farm_id = proposal.get("farm_id")
    priority = proposal.get("priority")
    service = proposal.get("service_address_contains") or []
    keywords = proposal.get("keywords_any") or []
    if service:
        disambiguator = f"service_address_contains={service}"
    else:
        disambiguator = f"keywords_any={keywords}"
    return (
        f"{index}. vendor_key={vendor_key} AND account={account_number} AND "
        f"{disambiguator} -> {farm_id} (priority: {priority})"
    )


if __name__ == "__main__":
    main()
