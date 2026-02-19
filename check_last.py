"""
Check last inserted row after ingestion test.

Uses correct primary keys and ordering per table:
- transactions: id (PK), ORDER BY id DESC
- documents: doc_id (PK), ORDER BY extracted_at DESC for most recent
"""
from core.db import fetchall, init_db

init_db()

print("=== Last transaction (PK: id) ===")
rows = fetchall(
    "SELECT id, doc_id, vendor_name, status, parse_status, parse_failure_reason "
    "FROM transactions ORDER BY id DESC LIMIT 1"
)
if rows:
    r = rows[0]
    print(f"  id={r['id']} doc_id={r['doc_id']} vendor={r['vendor_name']} "
          f"status={r['status']} parse_status={r['parse_status']} "
          f"parse_failure_reason={r['parse_failure_reason']}")
else:
    print("  No transactions")

print("\n=== Last document (PK: doc_id, ordered by extracted_at) ===")
rows = fetchall(
    "SELECT doc_id, file_name, substr(raw_text, 1, 100) as raw_preview "
    "FROM documents ORDER BY extracted_at DESC LIMIT 1"
)
if rows:
    r = rows[0]
    preview = (r["raw_preview"] or "(none)") + "..." if r["raw_preview"] else "(no raw_text)"
    print(f"  doc_id={r['doc_id']} file={r['file_name']}")
    print(f"  raw_preview={preview}")
else:
    print("  No documents")
