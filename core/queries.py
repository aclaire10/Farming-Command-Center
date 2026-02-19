"""Centralized SQL for dashboard and manual review. All DB reads/writes for the API layer."""

from __future__ import annotations

from contextlib import closing
from typing import Any

from core.db import execute, fetchall, fetchone, get_connection


def get_dashboard_summary() -> dict[str, Any]:
    """Summary counts and totals for the dashboard."""
    row = fetchone(
        """
        SELECT
          COALESCE(SUM(CASE WHEN duplicate_detected = 0 AND status IN ('auto', 'manual') THEN total_cents ELSE 0 END), 0) AS confirmed_cents,
          COALESCE(SUM(CASE WHEN duplicate_detected = 0 AND status = 'pending_manual' THEN total_cents ELSE 0 END), 0) AS pending_manual_cents,
          COUNT(CASE WHEN duplicate_detected = 0 AND status IN ('auto', 'manual') THEN 1 END) AS confirmed_count,
          COUNT(CASE WHEN duplicate_detected = 0 AND status = 'pending_manual' THEN 1 END) AS pending_manual_count,
          COUNT(CASE WHEN COALESCE(parse_status, '') != 'success' THEN 1 END) AS parse_failure_count
        FROM transactions
        """
    )
    if not row:
        return {
            "confirmed_cents": 0,
            "pending_manual_cents": 0,
            "confirmed_count": 0,
            "pending_manual_count": 0,
            "parse_failure_count": 0,
        }
    return {
        "confirmed_cents": int(row.get("confirmed_cents") or 0),
        "pending_manual_cents": int(row.get("pending_manual_cents") or 0),
        "confirmed_count": int(row.get("confirmed_count") or 0),
        "pending_manual_count": int(row.get("pending_manual_count") or 0),
        "parse_failure_count": int(row.get("parse_failure_count") or 0),
    }


def get_farm_totals() -> list[dict[str, Any]]:
    """Per-farm totals (excluding duplicates), ordered by total_cents DESC."""
    return fetchall(
        """
        SELECT
          t.farm_key,
          COALESCE(f.display_name, t.farm_name, t.farm_key) AS farm_name,
          SUM(t.total_cents) AS total_cents,
          COUNT(*) AS txn_count
        FROM transactions t
        LEFT JOIN farms f ON t.farm_key = f.farm_key
        WHERE t.duplicate_detected = 0
          AND t.status IN ('auto', 'manual')
        GROUP BY t.farm_key, f.display_name, t.farm_name
        ORDER BY total_cents DESC
        """
    )


def get_transactions(
    limit: int = 100,
    farm_key: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    """List transactions with optional farm_key and status filters."""
    conditions = ["1 = 1"]
    params: list[Any] = []
    if farm_key is not None:
        conditions.append("t.farm_key = ?")
        params.append(farm_key)
    if status is not None:
        conditions.append("t.status = ?")
        params.append(status)
    params.append(limit)
    where = " AND ".join(conditions)
    return fetchall(
        f"""
        SELECT t.id, t.doc_id, t.farm_key, t.farm_name, t.vendor_key, t.vendor_name,
               t.invoice_number, t.invoice_date, t.total_cents, t.confidence,
               t.status, t.parse_status, t.parse_failure_reason, t.needs_manual_review,
               d.file_name
        FROM transactions t
        LEFT JOIN documents d ON t.doc_id = d.doc_id
        WHERE {where}
        ORDER BY (t.invoice_date IS NULL) ASC, t.invoice_date DESC, t.id DESC
        LIMIT ?
        """,
        tuple(params),
    )


def get_transaction_by_id(tx_id: int) -> dict[str, Any] | None:
    """Single transaction by id with document file_name and raw_text for reinforcement."""
    return fetchone(
        """
        SELECT t.id, t.doc_id, t.farm_key, t.farm_name, t.vendor_key, t.vendor_name,
               t.invoice_number, t.invoice_date, t.due_date, t.total_cents,
               t.service_address, t.account_number, t.confidence, t.needs_manual_review,
               t.manual_override, t.status, t.parse_status, t.parse_failure_reason,
               t.error_reason, t.content_fingerprint, t.invoice_key, t.duplicate_detected,
               t.duplicate_reason, t.duplicate_of_doc_id, t.processed_at, t.created_at, t.updated_at,
               d.file_name, d.raw_text
        FROM transactions t
        LEFT JOIN documents d ON t.doc_id = d.doc_id
        WHERE t.id = ?
        """,
        (tx_id,),
    )


def get_parse_failure_summary() -> list[dict[str, Any]]:
    """Grouped parse failures: parse_status, parse_failure_reason, count, and sample doc_ids."""
    return fetchall(
        """
        SELECT parse_status, parse_failure_reason,
               COUNT(*) AS cnt,
               GROUP_CONCAT(doc_id) AS sample_doc_ids
        FROM transactions
        WHERE parse_status != 'success' OR parse_status IS NULL
        GROUP BY parse_status, parse_failure_reason
        ORDER BY cnt DESC
        """
    )


def get_parse_failure_transactions(limit: int = 200) -> list[dict[str, Any]]:
    """Transactions that have parse_status != 'success' for the parse-failures page."""
    return fetchall(
        """
        SELECT t.id, t.doc_id, t.farm_key, t.vendor_name, t.invoice_date, t.total_cents,
               t.parse_status, t.parse_failure_reason, t.status, d.file_name
        FROM transactions t
        LEFT JOIN documents d ON t.doc_id = d.doc_id
        WHERE t.parse_status != 'success' OR t.parse_status IS NULL
        ORDER BY t.id DESC
        LIMIT ?
        """,
        (limit,),
    )


def update_transaction_farm(tx_id: int, farm_key: str) -> bool:
    """Set farm_key and farm_name, set status to 'manual'. Preserve parse_status and all other fields. Returns True if a row was updated."""
    with closing(get_connection()) as conn:
        cur = conn.execute(
            """
            UPDATE transactions
            SET farm_key = ?,
                farm_name = (SELECT display_name FROM farms WHERE farm_key = ?),
                status = 'manual',
                updated_at = datetime('now')
            WHERE id = ?
            """,
            (farm_key, farm_key, tx_id),
        )
        conn.commit()
        return cur.rowcount > 0


def get_manual_review_queue() -> list[dict[str, Any]]:
    """Transactions with status = 'pending_manual' for manual review / override."""
    return fetchall(
        """
        SELECT t.id, t.doc_id, t.farm_key, t.farm_name, t.vendor_key, t.vendor_name,
               t.invoice_number, t.invoice_date, t.total_cents, t.confidence,
               t.status, t.parse_status, d.file_name
        FROM transactions t
        LEFT JOIN documents d ON t.doc_id = d.doc_id
        WHERE t.status = 'pending_manual'
        ORDER BY (t.invoice_date IS NULL) ASC, t.invoice_date DESC, t.id ASC
        """
    )


def get_farms() -> list[dict[str, Any]]:
    """All farms for dropdowns (farm_key, display_name)."""
    return fetchall(
        """
        SELECT farm_key, display_name
        FROM farms
        ORDER BY display_name
        """
    )
