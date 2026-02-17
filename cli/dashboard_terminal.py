"""Terminal dashboard for expense visibility by farm."""

from __future__ import annotations

from datetime import datetime

from core.db import fetchall, fetchone
from paths import ensure_data_dirs


def main() -> None:
    ensure_data_dirs()

    while True:
        print("\nFarm Expense Command Center - Dashboard")
        print("=====================================")
        print("1. List farms with total spend")
        print("2. View farm detail")
        print("3. View vendor totals for farm")
        print("4. Show manual review queue status")
        print("5. Exit")
        choice = input("\nSelect option (1-5): ").strip()

        if choice == "1":
            show_farm_totals()
        elif choice == "2":
            show_farm_detail()
        elif choice == "3":
            show_vendor_totals_for_farm()
        elif choice == "4":
            show_manual_review_queue()
        elif choice == "5":
            print("Goodbye.")
            return
        else:
            print("Invalid selection. Please choose 1-5.")


def show_farm_totals() -> None:
    rows = fetchall(
        """
        SELECT
          t.farm_key,
          f.display_name AS farm_name,
          SUM(CASE WHEN t.status IN ('auto','manual') THEN t.total_cents ELSE 0 END) AS confirmed_cents,
          SUM(CASE WHEN t.status = 'pending_manual' THEN t.total_cents ELSE 0 END) AS pending_cents,
          COUNT(*) AS txn_count
        FROM transactions t
        LEFT JOIN farms f ON t.farm_key = f.farm_key
        WHERE t.duplicate_detected = 0
          AND t.status != 'failed'
        GROUP BY t.farm_key, f.display_name
        ORDER BY confirmed_cents DESC
        """
    )

    print("\nFarm Totals (excluding duplicates)")
    print("=====================================")
    print(f"{'Farm':<22}{'Confirmed':>14}{'Pending':>12}")
    total_confirmed = 0.0
    total_pending = 0.0
    for row in rows:
        farm_id = str(row.get("farm_key") or "unknown")
        display_name = str(row.get("farm_name") or farm_id)
        confirmed = cents_to_dollars(row.get("confirmed_cents"))
        pending = cents_to_dollars(row.get("pending_cents"))
        total_confirmed += confirmed
        total_pending += pending
        print(f"{display_name[:20]:<22}{money(confirmed):>14}{money(pending):>12}")
    print("=====================================")
    print(f"{'Total':<22}{money(total_confirmed):>14}{money(total_pending):>12}")


def show_farm_detail() -> None:
    farm_id = input("\nEnter farm ID: ").strip()
    if not farm_id:
        print("Farm ID is required.")
        return

    rows = fetchall(
        """
        SELECT t.*, d.file_name
        FROM transactions t
        JOIN documents d ON t.doc_id = d.doc_id
        WHERE t.farm_key = ?
          AND t.duplicate_detected = 0
          AND t.status != 'failed'
        ORDER BY (t.invoice_date IS NULL) ASC, t.invoice_date DESC
        LIMIT 50
        """,
        (farm_id,),
    )
    farm_name_row = fetchone("SELECT display_name FROM farms WHERE farm_key = ?", (farm_id,))
    farm_name = (farm_name_row or {}).get("display_name") or farm_id
    print(f"\nFarm: {farm_name} ({farm_id})")
    print("=====================================")
    print(f"{'Date':<12}{'Vendor':<22}{'Amount':>12}  {'Doc ID'}")

    total = 0.0
    for row in rows[:20]:
        invoice_date = format_invoice_date(row.get("invoice_date"))
        vendor = str(row.get("vendor_name") or row.get("vendor_key") or "Unknown Vendor")
        amount = cents_to_dollars(row.get("total_cents"))
        total += amount
        doc_id = str(row.get("file_name") or row.get("doc_id") or "")
        print(f"{invoice_date:<12}{vendor[:20]:<22}{money(amount):>12}  {doc_id}")
    print("=====================================")
    print(f"Total: {money(total)}")


def show_vendor_totals_for_farm(
) -> None:
    farm_id = input("\nEnter farm ID: ").strip()
    if not farm_id:
        print("Farm ID is required.")
        return

    rows = fetchall(
        """
        SELECT
            COALESCE(NULLIF(t.vendor_name, ''), t.vendor_key, 'Unknown Vendor') AS vendor,
            SUM(t.total_cents) AS total_cents
        FROM transactions t
        WHERE t.farm_key = ?
          AND t.duplicate_detected = 0
          AND t.status != 'failed'
        GROUP BY COALESCE(NULLIF(t.vendor_name, ''), t.vendor_key, 'Unknown Vendor')
        ORDER BY total_cents DESC
        """,
        (farm_id,),
    )
    vendor_totals: dict[str, float] = {}
    for row in rows:
        vendor = str(row.get("vendor") or "Unknown Vendor")
        vendor_totals[vendor] = cents_to_dollars(row.get("total_cents"))

    sorted_vendors = sorted(vendor_totals.items(), key=lambda item: item[1], reverse=True)
    farm_name_row = fetchone("SELECT display_name FROM farms WHERE farm_key = ?", (farm_id,))
    farm_name = (farm_name_row or {}).get("display_name") or farm_id
    print(f"\nVendors for {farm_name} ({farm_id})")
    print("=====================================")
    overall = 0.0
    for vendor, amount in sorted_vendors:
        overall += amount
        print(f"{vendor[:22]:<22} {money(amount):>12}")
    print("=====================================")
    print(f"Total: {money(overall)}")


def show_manual_review_queue() -> None:
    unresolved_count = fetchone(
        "SELECT COUNT(*) AS n FROM manual_review_queue WHERE status='open'"
    )
    unresolved = fetchall(
        """
        SELECT q.doc_id, q.queued_at, d.file_name
        FROM manual_review_queue q
        JOIN documents d ON d.doc_id = q.doc_id
        WHERE q.status = 'open'
        ORDER BY q.queued_at ASC
        """
    )
    print(f"\nManual Review Queue: {int((unresolved_count or {}).get('n') or 0)} unresolved items")
    print("=====================================")
    for idx, row in enumerate(unresolved, start=1):
        doc_id = row.get("file_name") or row.get("doc_id") or "unknown"
        queued_at = row.get("queued_at") or "unknown"
        print(f"{idx}. {doc_id} (queued {queued_at})")
    if not unresolved:
        print("No unresolved items.")
    print("\nRun 'python review_manual.py' to resolve these items.")


def cents_to_dollars(value: object) -> float:
    try:
        if value is None:
            return 0.0
        return float(int(value)) / 100.0
    except (TypeError, ValueError):
        return 0.0


def format_invoice_date(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "Unknown"
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw[:10]


def money(amount: float) -> str:
    return f"${amount:,.2f}"


if __name__ == "__main__":
    main()
