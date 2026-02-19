"""SQLite helpers for the ledger database."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

from paths import FARMS_CONFIG_PATH, LEDGER_DB_PATH


def get_connection() -> sqlite3.Connection:
    """Create a configured SQLite connection."""
    connection = sqlite3.connect(LEDGER_DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA journal_mode = WAL")
    return connection


def _table_has_column(connection: sqlite3.Connection, table: str, column: str) -> bool:
    """Check if a table has a column via PRAGMA table_info."""
    cursor = connection.execute(f"PRAGMA table_info({table})")
    for row in cursor.fetchall():
        if row[1] == column:
            return True
    return False


def _migrate_transactions_parse_columns(connection: sqlite3.Connection) -> None:
    """Add parse_status and parse_failure_reason if missing (backward compatible)."""
    if not _table_has_column(connection, "transactions", "parse_status"):
        connection.execute(
            "ALTER TABLE transactions ADD COLUMN parse_status TEXT NOT NULL DEFAULT 'success'"
        )
    if not _table_has_column(connection, "transactions", "parse_failure_reason"):
        connection.execute(
            "ALTER TABLE transactions ADD COLUMN parse_failure_reason TEXT"
        )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_transactions_parse_status "
        "ON transactions(parse_status)"
    )


def _migrate_documents_raw_text(connection: sqlite3.Connection) -> None:
    """Add raw_text column if missing (backward compatible)."""
    if not _table_has_column(connection, "documents", "raw_text"):
        connection.execute("ALTER TABLE documents ADD COLUMN raw_text TEXT")


def init_db() -> None:
    """Initialize schema and seed stable reference data."""
    with closing(get_connection()) as connection:
        cursor = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='documents'"
        )
        has_schema = cursor.fetchone() is not None

        if not has_schema:
            schema_path = Path(__file__).resolve().parent / "schema.sql"
            schema_sql = schema_path.read_text(encoding="utf-8")
            connection.executescript(schema_sql)
        else:
            _migrate_transactions_parse_columns(connection)
            _migrate_documents_raw_text(connection)

        _seed_farms(connection)
        connection.commit()


def fetchone(query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    """Execute query and return first row as dict."""
    with closing(get_connection()) as connection:
        row = connection.execute(query, params).fetchone()
        return dict(row) if row is not None else None


def fetchall(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    """Execute query and return all rows as dicts."""
    with closing(get_connection()) as connection:
        rows = connection.execute(query, params).fetchall()
        return [dict(row) for row in rows]


def execute(query: str, params: tuple[Any, ...] = ()) -> None:
    """Execute write query and commit."""
    with closing(get_connection()) as connection:
        connection.execute(query, params)
        connection.commit()


def execute_returning_id(query: str, params: tuple[Any, ...] = ()) -> int:
    """Execute write query and return last inserted row id."""
    with closing(get_connection()) as connection:
        cursor = connection.execute(query, params)
        connection.commit()
        return int(cursor.lastrowid)


def _seed_farms(connection: sqlite3.Connection) -> None:
    """Seed farm keys and names from farms config."""
    farms_path = Path(FARMS_CONFIG_PATH)
    if not farms_path.exists():
        return

    farms_payload = json.loads(farms_path.read_text(encoding="utf-8"))
    rows = _extract_farm_rows(farms_payload)
    for farm_key, display_name in rows:
        connection.execute(
            """
            INSERT OR IGNORE INTO farms (farm_key, display_name, created_at, updated_at)
            VALUES (?, ?, datetime('now'), datetime('now'))
            """,
            (farm_key, display_name),
        )


def _extract_farm_rows(payload: Any) -> list[tuple[str, str]]:
    """Return normalized (farm_key, display_name) pairs."""
    rows: list[tuple[str, str]] = []
    seen: set[str] = set()

    if isinstance(payload, dict) and isinstance(payload.get("farms"), list):
        iterable = payload.get("farms") or []
    elif isinstance(payload, dict):
        iterable = payload.values()
    else:
        iterable = []

    for farm in iterable:
        if not isinstance(farm, dict):
            continue
        farm_key = str(farm.get("id") or farm.get("farm_id") or "").strip()
        if not farm_key or farm_key in seen:
            continue
        display_name = str(farm.get("name") or farm_key).strip()
        rows.append((farm_key, display_name))
        seen.add(farm_key)

    return rows
