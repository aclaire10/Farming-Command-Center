"""
Validate structured invoice JSON output.
"""

from typing import Any, Dict


def validate_invoice(data: Dict[str, Any]) -> None:
    """
    Validate required invoice fields.

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