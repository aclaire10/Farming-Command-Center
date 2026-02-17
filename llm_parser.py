"""
LLM extraction helpers for OCR and structured invoice parsing.
"""

import base64
import json
from pathlib import Path
from typing import Dict, Any, List

import fitz  # PyMuPDF
from openai import OpenAI
from paths import VISION_CACHE_DIR


class LLMParseError(Exception):
    """Raised when LLM parsing fails."""


VISION_OCR_SYSTEM_PROMPT = """You are an OCR engine. Extract all visible text from the invoice images exactly as written.

Critical requirements:
- Preserve all numbers exactly (account numbers, invoice numbers, amounts)
- Preserve addresses and identifiers exactly
- Maintain reasonable formatting where helpful
- If text is unclear or unreadable, omit it rather than guess
- Output plain text only - no markdown formatting, no explanations, no JSON

Extract the text now:"""


STRUCTURED_PARSE_SYSTEM_PROMPT = """
You are a financial document extraction engine.

You are given OCR text from a billing document. It may be utility bills, contractor invoices,
vendor portal exports, email-forwarded invoices, QuickBooks exports, or bank feed attachments.

Extract the following fields and return STRICT JSON only with this exact schema:
{
  "vendor_name": string | null,
  "invoice_number": string | null,
  "invoice_date": string | null,
  "due_date": string | null,
  "total_amount": float | null,
  "service_address": string | null,
  "account_number": string | null,
  "line_items": []
}

Field rules:
- Do not invent values. Use null when truly unavailable.
- `line_items` must always be an empty array.
- `total_amount` must be numeric (float/int) or null.
- Prefer payable amount as `total_amount`:
  1) BALANCE DUE / AMOUNT DUE
  2) TOTAL DUE / CURRENT CHARGES
  3) TOTAL
- If both TOTAL and BALANCE DUE are present and BALANCE DUE is clearly payable now, use BALANCE DUE.
- Recognize label variants (examples): INVOICE #, INVOICE NO, ACCOUNT NO, ACCT, STATEMENT DATE.
- Return raw JSON only. No markdown, no comments, no extra keys.
"""

JSON_REPAIR_USER_INSTRUCTION = (
    "Your previous response was not valid JSON. "
    "Return ONLY one valid JSON object matching the required schema. "
    "No markdown, no prose, no code fences."
)


def _render_pdf_pages_for_vision_text(pdf_path: str, max_pages: int = 3) -> List[str]:
    """Render PDF pages at 2x resolution for vision text extraction."""
    doc = fitz.open(pdf_path)
    images_base64 = []
    matrix = fitz.Matrix(2, 2)
    for page_num in range(min(max_pages, len(doc))):
        page = doc.load_page(page_num)
        pix = page.get_pixmap(matrix=matrix)
        img_bytes = pix.tobytes("png")
        img_b64 = base64.b64encode(img_bytes).decode("utf-8")
        images_base64.append(img_b64)
    doc.close()
    return images_base64


def extract_invoice_text_with_vision(
    pdf_path: str,
    api_key: str,
    max_pages: int = 3,
) -> str:
    """
    Extract plain text from PDF using GPT-4o vision with caching.

    Caching: keyed by filename; checks canonical vision text cache by filename
    before any API call. Never caches empty strings.
    """
    VISION_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    doc_id = Path(pdf_path).name
    cache_path = VISION_CACHE_DIR / f"{doc_id}.txt"

    if cache_path.exists():
        with cache_path.open("r", encoding="utf-8") as f:
            cached_text = f.read()
        if cached_text.strip():
            print("  → Using cached vision text")
            return cached_text

    print("  → Extracting with GPT-4o vision")
    client = OpenAI(api_key=api_key)
    images_base64 = _render_pdf_pages_for_vision_text(pdf_path, max_pages=max_pages)
    if not images_base64:
        return ""

    content: List[Dict[str, Any]] = [{"type": "text", "text": "Extract all visible text from these invoice images as plain text."}]
    for img_b64 in images_base64:
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{img_b64}"},
        })

    response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0,
        messages=[
            {"role": "system", "content": VISION_OCR_SYSTEM_PROMPT},
            {"role": "user", "content": content},
        ],
    )
    vision_text = (response.choices[0].message.content or "").strip()
    if vision_text:
        with cache_path.open("w", encoding="utf-8") as f:
            f.write(vision_text)
    return vision_text


def parse_invoice_with_llm(ocr_text: str, api_key: str) -> Dict[str, Any]:
    """
    Use GPT-4o text parsing to extract structured invoice summary fields from OCR text.
    """
    if not (ocr_text or "").strip():
        raise LLMParseError("OCR text is empty; cannot parse structured invoice fields.")
    client = OpenAI(api_key=api_key)
    user_prompt = (
        "Extract structured invoice summary data from this OCR text.\n\n"
        f"OCR_TEXT:\n{ocr_text}"
    )

    try:
        raw_output = _request_structured_parse(client, user_prompt)
        try:
            parsed = _safe_parse_json(raw_output)
        except LLMParseError:
            repair_prompt = (
                f"{JSON_REPAIR_USER_INSTRUCTION}\n\n"
                f"OCR_TEXT:\n{ocr_text}\n\n"
                f"PREVIOUS_RESPONSE_PREVIEW:\n{_preview_text(raw_output, limit=300)}"
            )
            retry_output = _request_structured_parse(client, repair_prompt)
            parsed = _safe_parse_json(retry_output)
        return _normalize_structured_invoice(parsed)

    except Exception as e:
        raise LLMParseError(str(e))


def _request_structured_parse(client: OpenAI, user_prompt: str) -> str:
    """Execute one structured parsing request and return raw model output."""
    response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0,
        messages=[
            {"role": "system", "content": STRUCTURED_PARSE_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    )
    return (response.choices[0].message.content or "").strip()


def _safe_parse_json(raw_output: str) -> Dict[str, Any]:
    """
    Parse model output into a dict with recovery for common formatting wrappers.
    """
    if not (raw_output or "").strip():
        raise LLMParseError(_format_parse_error("empty", raw_output, "Model returned empty response."))

    cleaned = _strip_markdown_fences(raw_output.strip())
    try:
        parsed = json.loads(cleaned)
        if not isinstance(parsed, dict):
            raise LLMParseError(_format_parse_error("invalid-json", raw_output, "Top-level JSON must be an object."))
        return parsed
    except json.JSONDecodeError:
        pass

    extracted = _extract_json_object(cleaned)
    if extracted is None:
        raise LLMParseError(_format_parse_error("no-json", raw_output, "No JSON object found in model output."))
    try:
        parsed = json.loads(extracted)
    except json.JSONDecodeError as exc:
        raise LLMParseError(
            _format_parse_error("invalid-json", raw_output, f"Invalid JSON after cleanup: {exc}")
        ) from exc
    if not isinstance(parsed, dict):
        raise LLMParseError(_format_parse_error("invalid-json", raw_output, "Top-level JSON must be an object."))
    return parsed


def _strip_markdown_fences(text: str) -> str:
    """Strip leading/trailing markdown code fences if present."""
    if not text.startswith("```"):
        return text
    lines = text.splitlines()
    if not lines:
        return text
    if lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip().startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _extract_json_object(text: str) -> str | None:
    """Extract substring spanning first '{' to last '}'."""
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        return None
    return text[start : end + 1].strip()


def _format_parse_error(category: str, raw_output: str, detail: str) -> str:
    """Return compact parser diagnostics with truncated model-output preview."""
    preview = _preview_text(raw_output, limit=300)
    return f"[{category}] {detail} preview='{preview}'"


def _preview_text(text: str, limit: int = 300) -> str:
    """Normalize and truncate preview text for diagnostics."""
    compact = (text or "").replace("\r", "\\r").replace("\n", "\\n").strip()
    if len(compact) <= limit:
        return compact
    return compact[:limit] + "...(truncated)"


def _normalize_structured_invoice(parsed: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize model output to expected schema with stable key presence.
    """
    if not isinstance(parsed, dict):
        raise LLMParseError("Model response is not a JSON object.")

    normalized: Dict[str, Any] = {
        "vendor_name": parsed.get("vendor_name"),
        "invoice_number": parsed.get("invoice_number"),
        "invoice_date": parsed.get("invoice_date"),
        "due_date": parsed.get("due_date"),
        "total_amount": parsed.get("total_amount"),
        "service_address": parsed.get("service_address"),
        "account_number": parsed.get("account_number"),
        "line_items": [],
    }

    amount = normalized.get("total_amount")
    if amount is not None:
        try:
            normalized["total_amount"] = float(amount)
        except (TypeError, ValueError):
            normalized["total_amount"] = None

    for key in ("vendor_name", "invoice_number", "invoice_date", "due_date", "service_address", "account_number"):
        value = normalized.get(key)
        if value is None:
            continue
        text_value = str(value).strip()
        normalized[key] = text_value if text_value else None

    return normalized