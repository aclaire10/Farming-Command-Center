"""
Remove the 7 specified invoice files from DB and vision cache so they can be
re-run individually as if for the first time.
"""
from pathlib import Path

from core.db import get_connection, init_db
from paths import VISION_CACHE_DIR

FILES_TO_REMOVE = [
    "WEllis_GP_12-31-25.pdf",
    "MHM_pchs_2-21-26.pdf",
    "PNSSNS_pchs_latedue_12-15-25.pdf",
    "Renu- Biome Invoice BASELINE 157 LLC.pdf",
    "Renu- Biome Invoice Ensenada Apartments Deposit.pdf",
    "Renu- Biome Invoice Hillview Orchard Holdings LLC Deposit.pdf",
    "3H_davis_2-6-26.pdf",
]


def main() -> None:
    init_db()
    with get_connection() as conn:
        placeholders = ",".join("?" * len(FILES_TO_REMOVE))
        doc_ids = [
            row[0]
            for row in conn.execute(
                f"SELECT doc_id FROM documents WHERE file_name IN ({placeholders})",
                FILES_TO_REMOVE,
            ).fetchall()
        ]
        if not doc_ids:
            print("No matching documents found.")
            return
        print(f"Found {len(doc_ids)} documents to remove: {doc_ids}")

        id_placeholders = ",".join("?" * len(doc_ids))
        params = tuple(doc_ids)

        conn.execute(
            f"DELETE FROM transaction_line_items WHERE doc_id IN ({id_placeholders})",
            params,
        )
        conn.execute(
            f"DELETE FROM tagging_events WHERE doc_id IN ({id_placeholders})",
            params,
        )
        conn.execute(
            f"DELETE FROM manual_review_queue WHERE doc_id IN ({id_placeholders})",
            params,
        )
        conn.execute(
            f"DELETE FROM manual_review_decisions WHERE doc_id IN ({id_placeholders})",
            params,
        )
        conn.execute(
            f"UPDATE transactions SET duplicate_of_doc_id = NULL WHERE duplicate_of_doc_id IN ({id_placeholders})",
            params,
        )
        conn.execute(
            f"DELETE FROM transactions WHERE doc_id IN ({id_placeholders})",
            params,
        )
        conn.execute(
            f"DELETE FROM documents WHERE doc_id IN ({id_placeholders})",
            params,
        )
        conn.commit()
        print(f"Removed {len(doc_ids)} documents and related rows from DB.")

    removed_cache = 0
    for fname in FILES_TO_REMOVE:
        cache_path = VISION_CACHE_DIR / f"{fname}.txt"
        if cache_path.exists():
            cache_path.unlink()
            removed_cache += 1
            print(f"  Removed vision cache: {fname}.txt")
    print(f"Removed {removed_cache} vision cache files.")
    print("Done. You can now re-run each file individually as if for the first time.")


if __name__ == "__main__":
    main()
