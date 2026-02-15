"""Extract markdown from PDF files using pymupdf4llm."""

import os
from typing import Optional

import pymupdf
from pymupdf4llm import to_markdown


def extract_markdown_from_pdf(file_path: str) -> str:
    """Extract markdown from PDF using pymupdf4llm.

    Args:
        file_path: Absolute or relative path to the PDF file.

    Returns:
        Markdown string representation of the PDF content. Handles
        multi-page PDFs; all pages are concatenated. Returns empty
        string for PDFs with zero pages.

    Raises:
        FileNotFoundError: If file_path does not exist.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"PDF not found: {file_path}")

    doc: Optional[pymupdf.Document] = None
    try:
        doc = pymupdf.open(file_path)
        if doc.page_count == 0:
            return ""
        return to_markdown(doc, pages=None)
    finally:
        if doc is not None:
            doc.close()
