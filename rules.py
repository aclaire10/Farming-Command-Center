"""Dynamic deterministic rules for farm assignment reinforcement."""

from __future__ import annotations

import datetime
import hashlib
import re
from typing import Any

from farm_tagger import TagCandidate, TagResult
from ledger import append_jsonl, atomic_rewrite_json, read_json

DEFAULT_DYNAMIC_RULES = {"version": "1.0", "rules": []}


def normalize_text(value: str | None) -> str:
    """Lowercase and normalize whitespace."""
    normalized = (value or "").lower()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def normalize_identifier(value: str | None) -> str:
    """Normalize identifiers for matching across punctuation variants."""
    normalized = normalize_text(value)
    normalized = re.sub(r"[-_/\s]", "", normalized)
    normalized = re.sub(r"[^\w]", "", normalized)
    return normalized


def generate_rule_id(rule_payload: dict[str, Any]) -> str:
    """Generate deterministic rule ID from core rule payload."""
    key_parts = [
        normalize_identifier(rule_payload.get("vendor_key")),
        normalize_identifier(rule_payload.get("account_number")),
        normalize_identifier(rule_payload.get("farm_id")),
        "|".join(sorted(normalize_text(x) for x in (rule_payload.get("service_address_contains") or []))),
        "|".join(sorted(normalize_text(x) for x in (rule_payload.get("keywords_any") or []))),
    ]
    payload_str = "|".join(key_parts)
    digest = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()
    return f"rule_{digest[:12]}"


def load_dynamic_rules(path: str) -> dict[str, Any]:
    """Load dynamic rules file; return default structure if file missing."""
    payload = read_json(path, default=DEFAULT_DYNAMIC_RULES)
    version = payload.get("version") if isinstance(payload, dict) else None
    rules = payload.get("rules") if isinstance(payload, dict) else None
    if not isinstance(version, str) or not isinstance(rules, list):
        return dict(DEFAULT_DYNAMIC_RULES)
    return payload


def ensure_dynamic_rules_file(path: str) -> dict[str, Any]:
    """Ensure dynamic rules file exists with valid shape."""
    payload = load_dynamic_rules(path)
    atomic_rewrite_json(path, payload)
    return payload


def append_manual_decision(path: str, decision: dict[str, Any]) -> None:
    """Append manual decision record in append-only mode."""
    append_jsonl(path, decision)


def upsert_dynamic_rule(
    dynamic_rules_path: str,
    new_rule: dict[str, Any],
) -> tuple[bool, str]:
    """
    Add a dynamic rule if absent.

    Returns `(created, rule_id)`.
    """
    payload = ensure_dynamic_rules_file(dynamic_rules_path)
    existing_rules = payload.get("rules") or []
    new_rule_id = generate_rule_id(new_rule)
    for existing in existing_rules:
        if existing.get("rule_id") == new_rule_id:
            return False, new_rule_id

    rule_record = {
        "rule_id": new_rule_id,
        "vendor_key": new_rule.get("vendor_key"),
        "account_number": new_rule.get("account_number"),
        "invoice_number": new_rule.get("invoice_number"),
        "service_address_contains": list(new_rule.get("service_address_contains") or []),
        "keywords_any": list(new_rule.get("keywords_any") or []),
        "farm_id": new_rule.get("farm_id"),
        "priority": int(new_rule.get("priority", 100)),
        "created_at": new_rule.get("created_at")
        or datetime.datetime.now(datetime.UTC).isoformat(),
        "evidence": dict(new_rule.get("evidence") or {}),
    }
    existing_rules.append(rule_record)
    payload["rules"] = existing_rules
    atomic_rewrite_json(dynamic_rules_path, payload)
    return True, new_rule_id


def apply_dynamic_rules(
    vision_text: str,
    rules: list[dict[str, Any]],
    farms_config: dict[str, Any],
) -> TagResult | None:
    """
    Match dynamic rules against OCR text before fallback tagger.

    Rules are sorted by priority descending then rule_id ascending.
    """
    if not rules:
        return None

    document_lower = normalize_text(vision_text)
    document_identifier = normalize_identifier(vision_text)
    farms_by_id = _build_farm_lookup(farms_config)

    ordered = sorted(
        [r for r in rules if isinstance(r, dict)],
        key=lambda r: (-int(r.get("priority", 100)), str(r.get("rule_id", ""))),
    )
    for rule in ordered:
        if _matches_rule(rule, document_lower, document_identifier, farms_config):
            farm_id = str(rule.get("farm_id") or "").strip()
            if not farm_id:
                continue
            farm_name = farms_by_id.get(farm_id, {}).get("name") or farm_id
            candidate = TagCandidate(
                farm_id=farm_id,
                farm_name=farm_name,
                score=float(rule.get("priority", 100)),
                matched_rules=["dynamic_rule"],
            )
            return TagResult(
                top_candidate=candidate,
                all_candidates=[candidate],
                confidence=0.99,
                needs_manual_review=False,
                reason=f"Dynamic rule match ({rule.get('rule_id')})",
            )
    return None


def check_account_collision(
    vendor_key: str,
    account_number: str,
    farms_config: dict[str, Any],
    dynamic_rules_payload: dict[str, Any],
    transactions_rows: list[dict[str, Any]],
) -> bool:
    """Return True if vendor/account maps to multiple farms across sources."""
    normalized_vendor = normalize_identifier(vendor_key)
    normalized_account = normalize_identifier(account_number)
    if not normalized_vendor or not normalized_account:
        return False

    farm_ids: set[str] = set()
    farm_ids.update(
        _scan_farms_config_mappings(normalized_vendor, normalized_account, farms_config)
    )
    farm_ids.update(
        _scan_dynamic_rule_mappings(
            normalized_vendor, normalized_account, dynamic_rules_payload
        )
    )
    farm_ids.update(
        _scan_transaction_mappings(normalized_vendor, normalized_account, transactions_rows)
    )
    return len(farm_ids) > 1


def _build_farm_lookup(farms_config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    farms = farms_config.get("farms") or []
    lookup: dict[str, dict[str, Any]] = {}
    for farm in farms:
        farm_id = str(farm.get("id") or farm.get("farm_id") or "").strip()
        if farm_id:
            lookup[farm_id] = farm
    return lookup


def _matches_rule(
    rule: dict[str, Any],
    document_lower: str,
    document_identifier: str,
    farms_config: dict[str, Any],
) -> bool:
    vendor_key = normalize_identifier(rule.get("vendor_key"))
    account_number = normalize_identifier(rule.get("account_number"))
    if not vendor_key or not account_number:
        return False
    if not _vendor_in_text(vendor_key, document_lower, document_identifier, farms_config):
        return False
    if account_number not in document_identifier:
        return False

    service_needles = [normalize_text(x) for x in (rule.get("service_address_contains") or []) if x]
    if service_needles and not all(n in document_lower for n in service_needles):
        return False

    keywords_any = [normalize_text(x) for x in (rule.get("keywords_any") or []) if x]
    if keywords_any and not any(k in document_lower for k in keywords_any):
        return False

    return True


def _vendor_in_text(
    normalized_vendor_key: str,
    document_lower: str,
    document_identifier: str,
    farms_config: dict[str, Any],
) -> bool:
    if normalized_vendor_key in document_identifier:
        return True

    for farm in farms_config.get("farms") or []:
        vendors = farm.get("vendors") or {}
        for vendor_key, vendor_cfg in vendors.items():
            if normalize_identifier(str(vendor_key)) != normalized_vendor_key:
                continue
            candidate_terms = [str(vendor_key)]
            if isinstance(vendor_cfg, dict):
                candidate_terms.append(str(vendor_cfg.get("name") or ""))
                candidate_terms.extend(str(x) for x in (vendor_cfg.get("keywords") or []))
            for term in candidate_terms:
                normalized_term = normalize_text(term)
                if normalized_term and normalized_term in document_lower:
                    return True
    return False


def _scan_farms_config_mappings(
    normalized_vendor: str,
    normalized_account: str,
    farms_config: dict[str, Any],
) -> set[str]:
    farm_ids: set[str] = set()
    for farm in farms_config.get("farms") or []:
        farm_id = str(farm.get("id") or farm.get("farm_id") or "").strip()
        vendors = farm.get("vendors") or {}
        for vendor_cfg_key, vendor_cfg in vendors.items():
            if normalize_identifier(str(vendor_cfg_key)) != normalized_vendor:
                continue
            if not isinstance(vendor_cfg, dict):
                continue
            values = list(vendor_cfg.get("identifiers") or [])
            for field in (
                "account_numbers",
                "meter_numbers",
                "policy_numbers",
                "loan_numbers",
                "order_numbers",
                "customer_numbers",
            ):
                values.extend(vendor_cfg.get(field) or [])
            normalized_values = {normalize_identifier(str(v)) for v in values if v is not None}
            if normalized_account in normalized_values and farm_id:
                farm_ids.add(farm_id)
    return farm_ids


def _scan_dynamic_rule_mappings(
    normalized_vendor: str,
    normalized_account: str,
    dynamic_rules_payload: dict[str, Any],
) -> set[str]:
    farm_ids: set[str] = set()
    for rule in dynamic_rules_payload.get("rules") or []:
        if normalize_identifier(rule.get("vendor_key")) != normalized_vendor:
            continue
        if normalize_identifier(rule.get("account_number")) != normalized_account:
            continue
        farm_id = str(rule.get("farm_id") or "").strip()
        if farm_id:
            farm_ids.add(farm_id)
    return farm_ids


def _scan_transaction_mappings(
    normalized_vendor: str,
    normalized_account: str,
    transactions_rows: list[dict[str, Any]],
) -> set[str]:
    farm_ids: set[str] = set()
    for row in transactions_rows:
        if row.get("duplicate_detected"):
            continue
        if normalize_identifier(row.get("vendor_key")) != normalized_vendor:
            continue
        if normalize_identifier(row.get("account_number")) != normalized_account:
            continue
        farm_id = str(row.get("farm_id") or "").strip()
        if farm_id:
            farm_ids.add(farm_id)
    return farm_ids
