"""Thin FastAPI dashboard: projection + control only. No auth, no ORM, no ingestion changes."""

from __future__ import annotations

import re
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from paths import BASE_DIR, DYNAMIC_RULES_PATH, INVOICES_DIR

from core import queries
from core.ingest import list_invoice_pdfs, run_all, run_one
from core.reinforcement import append_bill_to_contains_all_rule, extract_bill_to_tokens

app = FastAPI(title="Farm Expense Command Center", docs_url=None, redoc_url=None)

templates_dir = BASE_DIR / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

static_dir = BASE_DIR / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


def _cents_to_dollars(cents: int | None) -> float:
    if cents is None:
        return 0.0
    try:
        return float(int(cents)) / 100.0
    except (TypeError, ValueError):
        return 0.0


def _money(cents: int | None) -> str:
    return f"${_cents_to_dollars(cents):,.2f}"


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    summary = queries.get_dashboard_summary()
    farm_totals = queries.get_farm_totals()
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "summary": summary,
            "farm_totals": farm_totals,
            "cents_to_dollars": _cents_to_dollars,
            "money": _money,
        },
    )


@app.get("/farm/{farm_key}", response_class=HTMLResponse)
async def farm_detail(request: Request, farm_key: str) -> HTMLResponse:
    farms = queries.get_farms()
    farm_key_exists = any(f.get("farm_key") == farm_key for f in farms)
    if not farm_key_exists:
        raise HTTPException(status_code=404, detail="Farm not found")
    transactions = queries.get_transactions(limit=200, farm_key=farm_key)
    farm_name = next((f.get("display_name") or farm_key for f in farms if f.get("farm_key") == farm_key), farm_key)
    total_cents = sum((t.get("total_cents") or 0) for t in transactions)
    return templates.TemplateResponse(
        "farm_detail.html",
        {
            "request": request,
            "farm_key": farm_key,
            "farm_name": farm_name,
            "transactions": transactions,
            "total_cents": total_cents,
            "cents_to_dollars": _cents_to_dollars,
            "money": _money,
        },
    )


@app.get("/transactions", response_class=HTMLResponse)
async def transaction_list(
    request: Request,
    limit: int = 100,
    farm_key: str | None = None,
    status: str | None = None,
) -> HTMLResponse:
    # Treat empty query params as "no filter" so "All" works
    if farm_key == "":
        farm_key = None
    if status == "":
        status = None
    transactions = queries.get_transactions(limit=limit, farm_key=farm_key, status=status)
    farms = queries.get_farms()
    return templates.TemplateResponse(
        "transaction_list.html",
        {
            "request": request,
            "transactions": transactions,
            "farms": farms,
            "selected_farm_key": farm_key,
            "selected_status": status,
            "limit": limit,
            "cents_to_dollars": _cents_to_dollars,
            "money": _money,
        },
    )


@app.get("/transactions/{tx_id}", response_class=HTMLResponse, response_model=None)
async def transaction_detail(request: Request, tx_id: int):
    tx = queries.get_transaction_by_id(tx_id)
    if not tx:
        raise HTTPException(status_code=404, detail="Transaction not found")
    farms = queries.get_farms()
    return templates.TemplateResponse(
        "transaction_detail.html",
        {
            "request": request,
            "tx": tx,
            "farms": farms,
            "cents_to_dollars": _cents_to_dollars,
            "money": _money,
        },
    )


@app.get("/manual-review", response_class=HTMLResponse)
async def manual_review_page(request: Request) -> HTMLResponse:
    queue = queries.get_manual_review_queue()
    farms = queries.get_farms()
    return templates.TemplateResponse(
        "manual_review.html",
        {
            "request": request,
            "queue": queue,
            "farms": farms,
            "cents_to_dollars": _cents_to_dollars,
            "money": _money,
        },
    )


@app.post("/manual-review/{tx_id}")
async def manual_review_override(
    request: Request,
    tx_id: int,
    farm_key: str = Form(...),
    create_reinforcement_rule: str = Form(""),
) -> RedirectResponse:
    tx = queries.get_transaction_by_id(tx_id)
    if not tx:
        raise HTTPException(status_code=404, detail="Transaction not found")
    updated = queries.update_transaction_farm(tx_id, farm_key)
    if not updated:
        return RedirectResponse(f"/manual-review?error=update_failed", status_code=303)
    if create_reinforcement_rule.strip().lower() in ("1", "true", "on", "yes"):
        raw_text = tx.get("raw_text") or ""
        tokens = extract_bill_to_tokens(raw_text)
        if tokens:
            append_bill_to_contains_all_rule(DYNAMIC_RULES_PATH, farm_key, tokens)
    return RedirectResponse(f"/transactions/{tx_id}", status_code=303)


@app.get("/invoices", response_class=HTMLResponse)
async def invoices_page(
    request: Request,
    uploaded: str | None = None,
    processed: str | None = None,
    status: str | None = None,
    batch: str | None = None,
    total: str | None = None,
    auto: str | None = None,
    manual: str | None = None,
    failed: str | None = None,
) -> HTMLResponse:
    pdfs = list_invoice_pdfs()
    return templates.TemplateResponse(
        "invoices.html",
        {
            "request": request,
            "pdfs": pdfs,
            "uploaded": uploaded,
            "processed": processed,
            "process_status": status,
            "batch": batch,
            "batch_total": total,
            "batch_auto": auto,
            "batch_manual": manual,
            "batch_failed": failed,
        },
    )


def _sanitize_pdf_filename(name: str) -> str | None:
    """Allow only safe PDF filenames; return None if invalid."""
    if not name or not name.lower().endswith(".pdf"):
        return None
    base = name[:-4]
    if not re.match(r"^[a-zA-Z0-9._-]+$", base):
        return None
    return base + ".pdf"


@app.post("/invoices/upload")
async def upload_invoice(file: UploadFile = File(...)) -> RedirectResponse:
    filename = _sanitize_pdf_filename(file.filename or "")
    if not filename:
        raise HTTPException(status_code=400, detail="Invalid filename: must be a .pdf with safe characters")
    INVOICES_DIR.mkdir(parents=True, exist_ok=True)
    dest = INVOICES_DIR / filename
    try:
        contents = await file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read file: {e}") from e
    try:
        dest.write_bytes(contents)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Failed to save file: {e}") from e
    return RedirectResponse("/invoices?uploaded=" + filename, status_code=303)


@app.post("/invoices/process")
async def process_one_invoice(filename: str = Form(...)) -> RedirectResponse:
    safe = _sanitize_pdf_filename(filename.strip())
    if not safe:
        raise HTTPException(status_code=400, detail="Invalid filename")
    path = INVOICES_DIR / safe
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found in invoices folder")
    try:
        result = run_one(path)
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    status = result.get("status", "failed")
    return RedirectResponse(
        f"/invoices?processed={safe}&status={status}",
        status_code=303,
    )


@app.post("/invoices/process-all")
async def process_all_invoices() -> RedirectResponse:
    try:
        summary = run_all()
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    total = summary.get("total", 0)
    auto_processed = summary.get("auto_processed", 0)
    manual_review = summary.get("manual_review", 0)
    failed = summary.get("failed", 0)
    return RedirectResponse(
        f"/invoices?batch=1&total={total}&auto={auto_processed}&manual={manual_review}&failed={failed}",
        status_code=303,
    )


@app.get("/parse-failures", response_class=HTMLResponse)
async def parse_failures_page(request: Request) -> HTMLResponse:
    summary = queries.get_parse_failure_summary()
    transactions = queries.get_parse_failure_transactions(limit=200)
    return templates.TemplateResponse(
        "parse_failures.html",
        {
            "request": request,
            "summary": summary,
            "transactions": transactions,
            "money": _money,
        },
    )
