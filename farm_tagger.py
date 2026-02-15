"""
Farm Identity Resolution Engine — deterministic, rule-based farm matching.
No LLM calls. Used to route documents to auto-process vs manual review.
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class TagCandidate:
    """A single farm matching candidate with confidence score."""

    farm_id: str
    farm_name: str
    score: float
    matched_rules: List[str]


@dataclass
class TagResult:
    """Result of farm identity resolution."""

    top_candidate: Optional[TagCandidate]
    all_candidates: List[TagCandidate]
    confidence: float
    needs_manual_review: bool
    reason: str


def load_farms(config_path: str = "config/farms.json") -> Dict[str, Any]:
    """
    Load farm configuration from JSON file.

    Supports (1) spec format: {"farms": [{id, name, identifiers, vendors, keywords}, ...]},
    or (2) flat format: {farm_id: {farm_id, name, address, apns, tagging, vendors}}
    which is normalized to spec format.

    Args:
        config_path: Path to farms.json.

    Returns:
        dict: Parsed farms configuration with "farms" key (list of farm dicts).

    Raises:
        FileNotFoundError: If config file doesn't exist.
        json.JSONDecodeError: If config is invalid JSON.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Farms config not found: {config_path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and "farms" in data and isinstance(data["farms"], list):
        return data

    # Normalize flat format (farm_id -> farm_obj) to spec format
    if isinstance(data, dict) and data and not isinstance(data.get("farms"), list):
        farms_list = _normalize_flat_farms_to_spec(data)
        return {"farms": farms_list}

    raise ValueError("Invalid farms config: expected {'farms': [...]} or flat dict of farms.")


def _normalize_flat_farms_to_spec(flat: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert flat {farm_id: farm_obj} to list of spec-style farm dicts."""
    out = []
    for farm_id, obj in flat.items():
        if not isinstance(obj, dict) or obj.get("farm_id") is None:
            continue
        identifiers = []
        if obj.get("address"):
            identifiers.append(str(obj["address"]).strip())
        for apn in obj.get("apns") or []:
            if apn:
                identifiers.append(str(apn).strip())
        tagging = obj.get("tagging") or {}
        for key in ("account_numbers", "meter_numbers", "policy_numbers", "loan_numbers", "order_numbers", "customer_numbers"):
            for v in tagging.get(key) or []:
                if v is not None and str(v).strip():
                    identifiers.append(str(v).strip())
        keywords = list(tagging.get("vendor_keywords") or []) + list(tagging.get("vendor_variants") or [])
        keywords = [str(k).strip() for k in keywords if k]

        vendors_spec = {}
        for vk, vdata in (obj.get("vendors") or {}).items():
            if not isinstance(vdata, dict):
                continue
            v_identifiers = []
            for key in ("account_numbers", "meter_numbers", "policy_numbers", "loan_numbers", "order_numbers", "customer_numbers"):
                for v in (vdata.get(key) or []):
                    if v is not None and str(v).strip():
                        v_identifiers.append(str(v).strip())
            v_keywords = list(vdata.get("vendor_name_variants") or [])
            if vdata.get("vendor_name_canonical"):
                v_keywords.insert(0, vdata["vendor_name_canonical"])
            v_keywords = [str(k).strip() for k in v_keywords if k]
            vendors_spec[vk] = {
                "name": (vdata.get("vendor_name_canonical") or vk),
                "identifiers": v_identifiers,
                "keywords": v_keywords,
            }
        out.append({
            "id": farm_id,
            "name": obj.get("name") or farm_id,
            "identifiers": identifiers,
            "vendors": vendors_spec,
            "keywords": keywords,
        })
    return out


def _score_farm(document_lower: str, farm: Dict[str, Any]) -> tuple[float, List[str]]:
    """
    Score one farm against document text. Case-insensitive.
    Returns (score, list of matched rule names).
    """
    score = 0.0
    matched_rules: List[str] = []

    farm_identifiers = [str(x).strip().lower() for x in (farm.get("identifiers") or []) if x]
    for ident in farm_identifiers:
        if not ident:
            continue
        if ident in document_lower:
            score += 1.0
            matched_rules.append("identifier_match")

    farm_keywords = [str(x).strip().lower() for x in (farm.get("keywords") or []) if x]
    for kw in farm_keywords:
        if not kw:
            continue
        if kw in document_lower:
            score += 0.15
            matched_rules.append("farm_keyword")

    vendors = farm.get("vendors") or {}
    for vendor_key, vconf in vendors.items():
        if not isinstance(vconf, dict):
            continue
        v_identifiers = [str(x).strip().lower() for x in (vconf.get("identifiers") or []) if x]
        v_keywords = [str(x).strip().lower() for x in (vconf.get("keywords") or []) if x]
        for vi in v_identifiers:
            if vi and vi in document_lower:
                score += 1.0
                matched_rules.append("vendor_identifier_match")
        for vk in v_keywords:
            if vk and vk in document_lower:
                score += 0.25
                matched_rules.append("vendor_keyword")

    return (score, matched_rules)


def tag_document_text(document_text: str, farms_config: Dict[str, Any]) -> TagResult:
    """
    Resolve farm identity using deterministic scoring rules.

    Scoring Rules (additive):
    - Farm identifier match: +1.0
    - Vendor identifier match: +1.0
    - Vendor keyword match: +0.25 per keyword
    - Farm keyword match: +0.15 per keyword

    Confidence:
    - top >= 1.0 and (top - second) >= 0.5 -> 0.95
    - top >= 1.0 and (top - second) >= 0.3 -> 0.85
    - top >= 0.5 -> 0.70
    - else -> top_score / 2.0

    Manual review: confidence < 0.85, or multiple farms same vendor no clear winner, or zero matches.

    Args:
        document_text: Raw text extracted from PDF.
        farms_config: Configuration from load_farms() (must have "farms" list).

    Returns:
        TagResult with top_candidate, all_candidates, confidence, needs_manual_review, reason.
    """
    farms = farms_config.get("farms") or []
    if not farms:
        return TagResult(
            top_candidate=None,
            all_candidates=[],
            confidence=0.0,
            needs_manual_review=True,
            reason="No farms configured",
        )

    document_lower = (document_text or "").lower()

    candidates: List[TagCandidate] = []
    for farm in farms:
        farm_id = farm.get("id") or farm.get("farm_id") or ""
        farm_name = farm.get("name") or farm_id
        score, matched_rules = _score_farm(document_lower, farm)
        if score > 0:
            candidates.append(TagCandidate(
                farm_id=farm_id,
                farm_name=farm_name,
                score=score,
                matched_rules=matched_rules,
            ))

    # Sort by score descending; dedupe matched_rules for display
    candidates.sort(key=lambda c: c.score, reverse=True)
    for c in candidates:
        c.matched_rules = list(dict.fromkeys(c.matched_rules))

    top_score = candidates[0].score if candidates else 0.0
    second_score = candidates[1].score if len(candidates) > 1 else 0.0
    gap = top_score - second_score

    if top_score == 0:
        return TagResult(
            top_candidate=None,
            all_candidates=[],
            confidence=0.0,
            needs_manual_review=True,
            reason="Zero matches found",
        )

    if top_score >= 1.0 and gap >= 0.5:
        confidence = 0.95
    elif top_score >= 1.0 and gap >= 0.3:
        confidence = 0.85
    elif top_score >= 0.5:
        confidence = 0.70
    else:
        confidence = top_score / 2.0

    # Multiple farms with same vendor and no clear winner (small gap with only keyword matches)
    only_keywords_top = candidates and not any(
        r in ("identifier_match", "vendor_identifier_match") for r in candidates[0].matched_rules
    )
    if only_keywords_top and len(candidates) >= 2 and gap < 0.3:
        confidence = min(confidence, 0.70)
        needs_manual_review = True
        reason = "Multiple farm candidates with similar keyword scores; identifier match required"
    else:
        needs_manual_review = confidence < 0.85
        reason = "Low confidence" if needs_manual_review else "High confidence match"

    # PCH vs PCHS safety: vendor ambiguity - if same vendor in multiple farms, require identifier match
    if candidates and len(candidates) >= 2:
        top_farm_id = candidates[0].farm_id
        second_farm_id = candidates[1].farm_id
        top_farm = next((f for f in farms if (f.get("id") or f.get("farm_id")) == top_farm_id), None)
        second_farm = next((f for f in farms if (f.get("id") or f.get("farm_id")) == second_farm_id), None)
        if top_farm and second_farm:
            top_vendors = set((top_farm.get("vendors") or {}).keys())
            second_vendors = set((second_farm.get("vendors") or {}).keys())
            shared_vendors = top_vendors & second_vendors
            if shared_vendors:
                has_identifier = any(
                    r in ("identifier_match", "vendor_identifier_match")
                    for r in candidates[0].matched_rules
                )
                if not has_identifier:
                    confidence = min(confidence, 0.70)
                    needs_manual_review = True
                    reason = f"Vendor ambiguity: {shared_vendors} appears in multiple farms; identifier match required"

    top_candidate = candidates[0] if candidates else None
    return TagResult(
        top_candidate=top_candidate,
        all_candidates=candidates,
        confidence=round(confidence, 2),
        needs_manual_review=needs_manual_review,
        reason=reason,
    )
