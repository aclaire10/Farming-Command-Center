"""
Validate structured invoice JSON output.
"""

from __future__ import annotations

from decimal import Decimal
from datetime import datetime
from typing import Any

# Parse status constants
PARSE_STATUS_SUCCESS = "success"
PARSE_STATUS_INVALID_JSON = "invalid_json"
PARSE_STATUS_VALIDATION_FAILED = "validation_failed"

# Parse failure reason constants
PARSE_FAILURE_MISSING_REQUIRED_FIELD = "missing_required_field"
PARSE_FAILURE_INVALID_DATE = "invalid_date_format"
PARSE_FAILURE_INVALID_AMOUNT = "invalid_amount"
PARSE_FAILURE_ZERO_AMOUNT = "zero_amount"

_REQUIRED_FIELDS = ("vendor_name", "invoice_number", "invoice_date", "total_amount")
_DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%B %d, %Y",
    "%b %d, %Y",
)


def normalize_total_to_cents(raw_total: str | int | float) -> int:
    """
    Convert raw total to integer cents. Deterministic, pure.

    - Uses Decimal internally (never float)
    - Strips currency symbols
    - Removes commas
    - Handles parentheses as negative
    - Handles leading/trailing whitespace
    - Raises ValueError on invalid input
    """
    if raw_total is None:
        raise ValueError("total_amount cannot be null")

    if isinstance(raw_total, (int, float)):
        s = str(raw_total).strip()
    else:
        s = str(raw_total).strip()

    if not s:
        raise ValueError("total_amount cannot be empty")

    # Remove currency symbols and commas
    cleaned = s.replace(",", "").strip()
    for sym in ("$", "€", "£", "¥", "USD", "EUR", "GBP"):
        cleaned = cleaned.replace(sym, "").strip()

    # Parentheses = negative
    negative = False
    if cleaned.startswith("(") and cleaned.endswith(")"):
        negative = True
        cleaned = cleaned[1:-1].strip()
    elif cleaned.startswith("-"):
        negative = True
        cleaned = cleaned[1:].strip()

    if not cleaned:
        raise ValueError("total_amount cannot be empty after cleanup")

    try:
        dec = Decimal(cleaned)
    except Exception as exc:
        raise ValueError(f"invalid total_amount: {exc}") from exc

    if negative:
        dec = -dec

    cents = (dec * 100).to_integral_value(rounding="ROUND_HALF_UP")
    return int(cents)


def validate_invoice_payload(payload: dict) -> tuple[bool, str | None]:
    """
    Strict validation of LLM invoice payload.

    Required: vendor_name, invoice_number, invoice_date, total_amount.
    Returns (True, None) if valid, (False, reason_constant) if invalid.
    """
    if not isinstance(payload, dict):
        return (False, PARSE_FAILURE_MISSING_REQUIRED_FIELD)

    for field in _REQUIRED_FIELDS:
        if field not in payload:
            return (False, PARSE_FAILURE_MISSING_REQUIRED_FIELD)
        val = payload.get(field)
        if val is None or (isinstance(val, str) and not val.strip()):
            return (False, PARSE_FAILURE_MISSING_REQUIRED_FIELD)

    raw_date = str(payload.get("invoice_date", "")).strip()
    matched = False
    for fmt in _DATE_FORMATS:
        try:
            datetime.strptime(raw_date, fmt)
            matched = True
            break
        except ValueError:
            continue
    if not matched:
        return (False, PARSE_FAILURE_INVALID_DATE)

    try:
        cents = normalize_total_to_cents(payload.get("total_amount"))
    except ValueError:
        return (False, PARSE_FAILURE_INVALID_AMOUNT)

    if cents == 0:
        return (False, PARSE_FAILURE_ZERO_AMOUNT)

    return (True, None)


def validate_invoice(data: dict[str, Any]) -> None:
    """
    Validate required invoice fields (legacy, for backward compatibility).

    Required (Phase 1):
    - vendor_name
    - total_amount

    Optional:
    - invoice_number
    - invoice_date
    - due_date
    - service_address
    - account_number
    - line_items

    Raises:
        ValueError if validation fails.
    """
    if not isinstance(data, dict):
        raise ValueError("Invoice data must be a dictionary.")

    # -----------------------------
    # Required fields
    # -----------------------------
    required_fields = ["vendor_name", "total_amount"]

    for field in required_fields:
        if field not in data:
            raise ValueError(f"Missing required field: {field}")
        if data[field] is None:
            raise ValueError(f"Required field cannot be null: {field}")

    # -----------------------------
    # Type validation
    # -----------------------------

    # total_amount must be numeric
    if not isinstance(data["total_amount"], (int, float)):
        raise ValueError("total_amount must be a number.")

    # line_items must exist and be a list
    if "line_items" not in data:
        raise ValueError("Missing field: line_items")

    if not isinstance(data["line_items"], list):
        raise ValueError("line_items must be a list.")

    # Phase 1: we expect empty list
    # But don't enforce emptiness, just ensure structure is valid

    # If line_items present, validate structure
    for item in data["line_items"]:
        if not isinstance(item, dict):
            raise ValueError("Each line_item must be a dictionary.")

        if "description" not in item or "amount" not in item:
            raise ValueError("Each line_item must have description and amount.")

        if not isinstance(item["description"], str):
            raise ValueError("line_item description must be a string.")

        if not isinstance(item["amount"], (int, float)):
            raise ValueError("line_item amount must be numeric.")
