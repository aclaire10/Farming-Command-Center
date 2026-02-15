"""
Vision-based LLM invoice parsing using GPT-4o.
Phase 1: Extract summary-level invoice fields only.
Includes vision-based text extraction for farm tagging (works for all PDF types).
"""

import base64
import json
import os
from typing import Dict, Any, List

import fitz  # PyMuPDF
from openai import OpenAI


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

    Caching: keyed by filename; checks outputs/vision_text_cache/<filename>.txt
    before any API call. Never caches empty strings.
    """
    cache_dir = "outputs/vision_text_cache"
    os.makedirs(cache_dir, exist_ok=True)
    doc_id = os.path.basename(pdf_path)
    cache_path = os.path.join(cache_dir, f"{doc_id}.txt")

    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
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
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write(vision_text)
    return vision_text


def _render_pdf_pages_to_base64_images(pdf_path: str, max_pages: int = 3) -> List[str]:
    """
    Render first N pages of a PDF to base64-encoded PNG images.
    """
    doc = fitz.open(pdf_path)
    images_base64 = []

    for page_num in range(min(max_pages, len(doc))):
        page = doc.load_page(page_num)
        pix = page.get_pixmap(dpi=200)
        img_bytes = pix.tobytes("png")
        img_b64 = base64.b64encode(img_bytes).decode("utf-8")
        images_base64.append(img_b64)

    doc.close()
    return images_base64


def parse_invoice_with_llm(pdf_path: str, api_key: str) -> Dict[str, Any]:
    """
    Use GPT-4o vision to extract structured invoice summary fields.
    """

    client = OpenAI(api_key=api_key)

    images_base64 = _render_pdf_pages_to_base64_images(pdf_path)

    system_prompt = """
You are a financial document extraction engine.

Extract the following invoice summary fields from the provided invoice images.

Return STRICT JSON only with this exact schema:

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

Rules:
- Do not guess values.
- If a field is not clearly visible, return null.
- total_amount must be numeric.
- line_items must always be an empty array.
- Return raw JSON only.
- No markdown.
- No explanation.
"""

    # Build multimodal message
    content = [
        {"type": "text", "text": "Extract structured invoice summary data."}
    ]

    for img_b64 in images_base64:
        content.append(
            {
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/png;base64,{img_b64}"
                },
            }
        )

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": content},
            ],
        )

        raw_output = response.choices[0].message.content

        try:
            parsed = json.loads(raw_output)
        except json.JSONDecodeError:
            raise LLMParseError("Model returned invalid JSON.")

        return parsed

    except Exception as e:
        raise LLMParseError(str(e))