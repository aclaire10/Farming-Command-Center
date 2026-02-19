PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS farms (
    farm_key TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS vendors (
    vendor_key TEXT PRIMARY KEY,
    display_name TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS documents (
    doc_id TEXT PRIMARY KEY,
    file_name TEXT NOT NULL,
    file_path TEXT,
    raw_text_hash TEXT,
    raw_text TEXT,
    content_fingerprint TEXT UNIQUE,
    extracted_at TEXT NOT NULL DEFAULT (datetime('now')),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id TEXT NOT NULL UNIQUE,
    farm_key TEXT,
    farm_name TEXT,
    vendor_key TEXT,
    vendor_name TEXT,
    invoice_number TEXT,
    invoice_date TEXT,
    due_date TEXT,
    total_cents INTEGER NOT NULL DEFAULT 0,
    service_address TEXT,
    account_number TEXT,
    confidence REAL NOT NULL DEFAULT 0.0,
    needs_manual_review INTEGER NOT NULL DEFAULT 1 CHECK (needs_manual_review IN (0, 1)),
    manual_override INTEGER NOT NULL DEFAULT 0 CHECK (manual_override IN (0, 1)),
    processed_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('auto', 'pending_manual', 'manual', 'duplicate', 'failed')),
    error_reason TEXT,
    content_fingerprint TEXT,
    invoice_key TEXT,
    duplicate_detected INTEGER NOT NULL DEFAULT 0 CHECK (duplicate_detected IN (0, 1)),
    duplicate_reason TEXT,
    duplicate_of_doc_id TEXT,
    parse_status TEXT NOT NULL DEFAULT 'success',
    parse_failure_reason TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (doc_id) REFERENCES documents(doc_id),
    FOREIGN KEY (farm_key) REFERENCES farms(farm_key),
    FOREIGN KEY (vendor_key) REFERENCES vendors(vendor_key),
    FOREIGN KEY (duplicate_of_doc_id) REFERENCES documents(doc_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_invoice_key_original
ON transactions(invoice_key)
WHERE duplicate_detected = 0 AND invoice_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_transactions_status
ON transactions(status);

CREATE INDEX IF NOT EXISTS idx_transactions_farm
ON transactions(farm_key);

CREATE INDEX IF NOT EXISTS idx_transactions_duplicate
ON transactions(duplicate_detected);

CREATE INDEX IF NOT EXISTS idx_transactions_parse_status
ON transactions(parse_status);

CREATE TABLE IF NOT EXISTS transaction_line_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id TEXT NOT NULL,
    line_number INTEGER NOT NULL,
    description TEXT,
    amount_cents INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (doc_id) REFERENCES documents(doc_id)
);

CREATE TABLE IF NOT EXISTS manual_review_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id TEXT NOT NULL UNIQUE,
    extracted_text_preview TEXT,
    candidates_json TEXT,
    confidence REAL NOT NULL DEFAULT 0.0,
    reason TEXT,
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
    queued_at TEXT NOT NULL DEFAULT (datetime('now')),
    resolved_at TEXT,
    FOREIGN KEY (doc_id) REFERENCES documents(doc_id)
);

CREATE TABLE IF NOT EXISTS manual_review_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id TEXT NOT NULL,
    content_fingerprint TEXT,
    invoice_key TEXT,
    selected_farm_key TEXT NOT NULL,
    selected_farm_name TEXT,
    decision_source TEXT NOT NULL,
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (doc_id) REFERENCES documents(doc_id),
    FOREIGN KEY (selected_farm_key) REFERENCES farms(farm_key)
);

CREATE TABLE IF NOT EXISTS tagging_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id TEXT NOT NULL,
    stage TEXT NOT NULL CHECK (stage IN ('dynamic_rule', 'deterministic', 'manual')),
    confidence REAL NOT NULL DEFAULT 0.0,
    needs_manual_review INTEGER NOT NULL DEFAULT 1 CHECK (needs_manual_review IN (0, 1)),
    top_candidate_json TEXT,
    all_candidates_json TEXT,
    reason TEXT,
    features_json TEXT,
    tagged_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (doc_id) REFERENCES documents(doc_id)
);
