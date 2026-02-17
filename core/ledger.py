"""Safe JSON file helpers for dynamic rule persistence."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class LedgerIOError(RuntimeError):
    """Raised when ledger read/write operations fail."""


def read_json(path: str | Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    """Read JSON object; return provided default if file does not exist."""
    file_path = Path(path)
    if not file_path.exists():
        return dict(default or {})

    try:
        with file_path.open("r", encoding="utf-8") as handle:
            parsed = json.load(handle)
    except json.JSONDecodeError as exc:
        raise LedgerIOError(
            f"Failed to parse JSON in '{path}'. "
            "Original file unchanged; fix malformed JSON and retry."
        ) from exc

    if not isinstance(parsed, dict):
        raise LedgerIOError(
            f"Expected JSON object in '{path}'. "
            "Original file unchanged; fix malformed JSON and retry."
        )
    return parsed


def atomic_rewrite_json(path: str | Path, data: dict[str, Any]) -> None:
    """Atomically rewrite JSON file with validation."""
    file_path = Path(path)
    tmp_path = file_path.with_suffix(file_path.suffix + ".tmp")
    if file_path.parent:
        file_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, ensure_ascii=False)
            handle.write("\n")

        with tmp_path.open("r", encoding="utf-8") as handle:
            json.load(handle)

        tmp_path.replace(file_path)
    except (OSError, json.JSONDecodeError) as exc:
        _safe_remove(tmp_path)
        raise LedgerIOError(
            f"Failed to atomically rewrite JSON for '{path}'. "
            "Original file unchanged; fix issue and retry."
        ) from exc


def _safe_remove(path: Path) -> None:
    """Best-effort tmp file cleanup."""
    try:
        if path.exists():
            path.unlink()
    except OSError:
        return
