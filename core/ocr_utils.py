"""
OCR output sanitization utilities.
"""

import re
import unicodedata


def sanitize_vision_output(text: str) -> str:
    """
    Remove conversational artifacts or markdown wrappers
    in case the model ignores instructions.
    Deterministic and minimal.
    """
    if not text:
        return ""
    s = text.strip()
    if not s:
        return ""

    prefixes = (
        r"^Sure,\s*",
        r"^Here\s+is\s+(?:the\s+)?(?:extracted\s+)?(?:text\s+)?(?:from\s+the\s+images?)?[:\s]*",
        r"^Here's\s+(?:the\s+)?(?:extracted\s+)?(?:text\s+)?(?:from\s+the\s+images?)?[:\s]*",
        r"^Below\s+is\s+(?:the\s+)?(?:extracted\s+)?(?:text\s+)?[:\s]*",
    )
    for pat in prefixes:
        s = re.sub(pat, "", s, count=1, flags=re.IGNORECASE).strip()

    while True:
        prev = s
        s = re.sub(r"^[\s\n]*---+\s*", "", s).strip()
        s = re.sub(r"^[\s\n]*\*{3,}\s*", "", s).strip()
        if s == prev:
            break

    lines = s.splitlines()
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if stripped == "---" or stripped == "***":
            continue
        if re.match(r"^\s*\*\*.+\*\*\s*$", line):
            continue
        cleaned.append(line)
    s = "\n".join(cleaned)

    while s.startswith("```"):
        idx = s.find("\n")
        if idx == -1:
            s = s.lstrip("`").strip()
            break
        s = s[idx + 1 :].strip()
        if s.endswith("```"):
            s = s[:-3].strip()
        break

    s = s.strip()
    s = unicodedata.normalize("NFC", s)
    s = s.replace("\uFFFD", "")
    s = s.encode("utf-8", "ignore").decode("utf-8")
    return s.strip()
