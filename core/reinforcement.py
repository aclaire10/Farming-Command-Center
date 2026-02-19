"""Append bill_to_contains_all rules from manual override OCR text. No vendor/fuzzy logic."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from core.ledger import LedgerIOError, atomic_rewrite_json, read_json

BILL_TO_CONTAINS_ALL = "bill_to_contains_all"
DEFAULT_RULES = {"version": "1.0", "rules": []}


def extract_bill_to_tokens(raw_text: str | None, max_chars: int = 400) -> list[str]:
    """Extract stable tokens from the start of text (bill-to section). Words 3+ chars, lowercased, deduped order."""
    if not raw_text or not isinstance(raw_text, str):
        return []
    head = raw_text[:max_chars].lower()
    words = re.findall(r"[a-z0-9]{3,}", head)
    seen: set[str] = set()
    tokens: list[str] = []
    for w in words:
        if w not in seen:
            seen.add(w)
            tokens.append(w)
    return tokens


def _rule_equal(r: dict[str, Any], farm_key: str, tokens: list[str]) -> bool:
    if r.get("type") != BILL_TO_CONTAINS_ALL:
        return False
    if r.get("farm_key") != farm_key:
        return False
    existing = r.get("tokens") or []
    return isinstance(existing, list) and sorted(existing) == sorted(tokens)


def append_bill_to_contains_all_rule(
    rules_path: str | Path,
    farm_key: str,
    tokens: list[str],
) -> bool:
    """
    Append a bill_to_contains_all rule if not duplicate. Preserves JSON formatting.
    Returns True if appended, False if duplicate or tokens empty.
    """
    path = Path(rules_path)
    if not tokens or not farm_key:
        return False
    payload = read_json(path, default=DEFAULT_RULES)
    rules = payload.get("rules")
    if not isinstance(rules, list):
        rules = []
    for r in rules:
        if isinstance(r, dict) and _rule_equal(r, farm_key, tokens):
            return False
    new_rule: dict[str, Any] = {
        "type": BILL_TO_CONTAINS_ALL,
        "tokens": tokens,
        "farm_key": farm_key,
    }
    payload["rules"] = [*rules, new_rule]
    try:
        atomic_rewrite_json(path, payload)
    except LedgerIOError:
        return False
    return True
