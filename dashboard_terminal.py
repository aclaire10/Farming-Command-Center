"""Terminal dashboard for expense visibility by farm."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

from farm_tagger import load_farms
from ledger import LedgerIOError, read_jsonl
from paths import FARMS_CONFIG_PATH, QUEUE_PATH, LEDGER_PATH, ensure_data_dirs


def main() -> None:
    ensure_data_dirs()

    try:
        transactions = read_jsonl(LEDGER_PATH)
        queue_rows = read_jsonl(QUEUE_PATH)
        farms_config = load_farms(FARMS_CONFIG_PATH)
    except (LedgerIOError, FileNotFoundError, ValueError) as exc:
        print(f"Dashboard initialization failed: {exc}")
        return

    farm_lookup = {
        str(f.get("id") or f.get("farm_id") or "").strip(): str(f.get("name") or "").strip()
        for f in (farms_config.get("farms") or [])
    }

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
            show_farm_totals(transactions, farm_lookup)
        elif choice == "2":
            show_farm_detail(transactions, farm_lookup)
        elif choice == "3":
            show_vendor_totals_for_farm(transactions, farm_lookup)
        elif choice == "4":
            show_manual_review_queue(queue_rows)
        elif choice == "5":
            print("Goodbye.")
            return
        else:
            print("Invalid selection. Please choose 1-5.")


def show_farm_totals(transactions: list[dict[str, Any]], farm_lookup: dict[str, str]) -> None:
    rows = [row for row in transactions if not bool(row.get("duplicate_detected"))]
    grouped: dict[str, dict[str, float]] = defaultdict(lambda: {"confirmed": 0.0, "pending": 0.0})

    for row in rows:
        farm_id = str(row.get("farm_id") or "unknown")
        amount = safe_amount(row.get("total_amount"))
        if bool(row.get("needs_manual_review")):
            grouped[farm_id]["pending"] += amount
        else:
            grouped[farm_id]["confirmed"] += amount

    sorted_items = sorted(
        grouped.items(),
        key=lambda item: item[1]["confirmed"],
        reverse=True,
    )

    print("\nFarm Totals (excluding duplicates)")
    print("=====================================")
    print(f"{'Farm':<22}{'Confirmed':>14}{'Pending':>12}")
    total_confirmed = 0.0
    total_pending = 0.0
    for farm_id, values in sorted_items:
        display_name = farm_lookup.get(farm_id) or farm_id
        confirmed = values["confirmed"]
        pending = values["pending"]
        total_confirmed += confirmed
        total_pending += pending
        print(f"{display_name[:20]:<22}{money(confirmed):>14}{money(pending):>12}")
    print("=====================================")
    print(f"{'Total':<22}{money(total_confirmed):>14}{money(total_pending):>12}")


def show_farm_detail(transactions: list[dict[str, Any]], farm_lookup: dict[str, str]) -> None:
    farm_id = input("\nEnter farm ID: ").strip()
    if not farm_id:
        print("Farm ID is required.")
        return

    rows = [
        row
        for row in transactions
        if not bool(row.get("duplicate_detected")) and str(row.get("farm_id") or "") == farm_id
    ]
    rows.sort(key=lambda row: sort_invoice_date(row.get("invoice_date")), reverse=True)

    farm_name = farm_lookup.get(farm_id) or farm_id
    print(f"\nFarm: {farm_name} ({farm_id})")
    print("=====================================")
    print(f"{'Date':<12}{'Vendor':<22}{'Amount':>12}  {'Doc ID'}")

    total = 0.0
    for row in rows[:20]:
        invoice_date = str(row.get("invoice_date") or "Unknown")[:10]
        vendor = str(row.get("vendor_name") or row.get("vendor_key") or "Unknown Vendor")
        amount = safe_amount(row.get("total_amount"))
        total += amount
        doc_id = str(row.get("doc_id") or "")
        print(f"{invoice_date:<12}{vendor[:20]:<22}{money(amount):>12}  {doc_id}")
    print("=====================================")
    print(f"Total: {money(total)}")


def show_vendor_totals_for_farm(
    transactions: list[dict[str, Any]],
    farm_lookup: dict[str, str],
) -> None:
    farm_id = input("\nEnter farm ID: ").strip()
    if not farm_id:
        print("Farm ID is required.")
        return

    rows = [
        row
        for row in transactions
        if not bool(row.get("duplicate_detected")) and str(row.get("farm_id") or "") == farm_id
    ]
    vendor_totals: dict[str, float] = defaultdict(float)
    for row in rows:
        vendor = str(row.get("vendor_name") or row.get("vendor_key") or "Unknown Vendor")
        vendor_totals[vendor] += safe_amount(row.get("total_amount"))

    sorted_vendors = sorted(vendor_totals.items(), key=lambda item: item[1], reverse=True)
    farm_name = farm_lookup.get(farm_id) or farm_id
    print(f"\nVendors for {farm_name} ({farm_id})")
    print("=====================================")
    overall = 0.0
    for vendor, amount in sorted_vendors:
        overall += amount
        print(f"{vendor[:22]:<22} {money(amount):>12}")
    print("=====================================")
    print(f"Total: {money(overall)}")


def show_manual_review_queue(queue_rows: list[dict[str, Any]]) -> None:
    unresolved = [
        row for row in queue_rows if isinstance(row, dict) and not bool(row.get("resolved"))
    ]
    print(f"\nManual Review Queue: {len(unresolved)} unresolved items")
    print("=====================================")
    for idx, row in enumerate(unresolved, start=1):
        doc_id = row.get("doc_id") or "unknown"
        queued_at = row.get("queued_at") or "unknown"
        print(f"{idx}. {doc_id} (queued {queued_at})")
    if not unresolved:
        print("No unresolved items.")
    print("\nRun 'python review_manual.py' to resolve these items.")


def safe_amount(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def sort_invoice_date(value: Any) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        return datetime.min
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return datetime.min


def money(amount: float) -> str:
    return f"${amount:,.2f}"


if __name__ == "__main__":
    main()
