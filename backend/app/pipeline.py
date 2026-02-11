from __future__ import annotations

import re
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Any, Optional

from .config import CONFIDENCE_THRESHOLD, FORCE_REVIEW_DOC_TYPES, URGENCY_KEYWORDS
from .providers import (
    try_anthropic_classification,
    try_anthropic_field_enrichment,
    try_external_classification,
    try_external_ocr,
)
from .rules import get_active_rules

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - optional dependency fallback
    PdfReader = None


FIELD_PATTERNS: dict[str, re.Pattern[str]] = {
    "applicant_name": re.compile(
        r"(?:applicant|name|owner)\s*[:\-]\s*([A-Za-z][A-Za-z ,.'-]{2,80})",
        re.IGNORECASE,
    ),
    "address": re.compile(
        r"(?:address|property address)\s*[:\-]\s*([0-9A-Za-z .,'#-]{5,120})",
        re.IGNORECASE,
    ),
    "date": re.compile(
        r"(?:date|submitted|filed)\s*[:\-]\s*([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{2,4}|[0-9]{4}[/-][0-9]{1,2}[/-][0-9]{1,2}|[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})",
        re.IGNORECASE,
    ),
    "parcel_number": re.compile(
        r"(?:parcel(?:\s*(?:id|number|no))?)\s*[:\-]\s*([A-Za-z0-9-]{4,30})",
        re.IGNORECASE,
    ),
    "case_number": re.compile(
        r"(?:case(?:\s*(?:id|number|no))?)\s*[:\-]\s*([A-Za-z0-9-]{4,30})",
        re.IGNORECASE,
    ),
    "amount": re.compile(
        r"(?:amount|fee|total)\s*[:\-]?\s*\$?\s*([0-9]+(?:,[0-9]{3})*(?:\.[0-9]{2})?)",
        re.IGNORECASE,
    ),
    "email": re.compile(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})"),
}


def _read_text_file(file_path: Path) -> str:
    for encoding in ("utf-8", "latin-1"):
        try:
            return file_path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return ""


def _read_pdf_file(file_path: Path) -> str:
    if PdfReader is None:
        return ""

    try:
        reader = PdfReader(str(file_path))
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception:
        return ""


def _read_docx_file(file_path: Path) -> str:
    try:
        with zipfile.ZipFile(file_path) as archive:
            xml_bytes = archive.read("word/document.xml")
    except Exception:
        return ""

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return ""

    namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    paragraphs: list[str] = []
    for paragraph in root.findall(".//w:p", namespace):
        chunks = [
            node.text for node in paragraph.findall(".//w:t", namespace) if node.text
        ]
        line = " ".join(part.strip() for part in chunks if part.strip())
        if line:
            paragraphs.append(line)

    return "\n".join(paragraphs)


def extract_text(
    file_path: str, content_type: Optional[str] = None
) -> tuple[str, str, float]:
    external_result = try_external_ocr(file_path=file_path, content_type=content_type)
    if external_result:
        return external_result

    path = Path(file_path)
    extension = path.suffix.lower()

    if extension in {".txt", ".md", ".csv"}:
        text = _read_text_file(path)
        return text, "native_text", 0.99 if text else 0.1

    if extension == ".pdf":
        text = _read_pdf_file(path)
        return text, "pypdf" if text else "pdf_unavailable", 0.87 if text else 0.25

    if extension in {".docx", ".docm"}:
        text = _read_docx_file(path)
        return text, "docx_xml" if text else "docx_unavailable", 0.9 if text else 0.2

    if extension in {".json"}:
        text = _read_text_file(path)
        return text, "json_text", 0.95 if text else 0.2

    if content_type and content_type.startswith("text/"):
        text = _read_text_file(path)
        return text, "content_type_text", 0.9 if text else 0.2

    return "", "ocr_placeholder", 0.2


def extract_fields(text: str) -> dict[str, Any]:
    extracted: dict[str, Any] = {}

    for field_name, pattern in FIELD_PATTERNS.items():
        match = pattern.search(text)
        if match:
            extracted[field_name] = match.group(1).strip()

    return extracted


def classify_document(
    text: str, active_rules: Optional[dict[str, dict[str, Any]]] = None
) -> tuple[str, float, dict[str, Any]]:
    rules = active_rules or get_active_rules()[0]
    normalized_text = text.lower()
    best_doc_type = "other"
    best_hits = 0
    best_keyword_count = 1

    for doc_type, rule in rules.items():
        keywords: list[str] = rule.get("keywords", [])
        if not keywords:
            continue

        hits = sum(1 for keyword in keywords if keyword in normalized_text)
        if hits > best_hits:
            best_doc_type = doc_type
            best_hits = hits
            best_keyword_count = len(keywords)

    if best_hits == 0:
        return "other", 0.45, {"matched_keywords": []}

    ratio = best_hits / max(best_keyword_count, 1)
    if best_hits == 1:
        confidence = 0.65 + (ratio * 0.08)
    elif best_hits == 2:
        confidence = 0.78 + (ratio * 0.08)
    else:
        confidence = 0.86 + min((best_hits - 3) * 0.03, 0.1)

    confidence = min(confidence, 0.99)
    matched_keywords = [
        keyword
        for keyword in rules[best_doc_type]["keywords"]
        if keyword in normalized_text
    ]

    return best_doc_type, round(confidence, 4), {"matched_keywords": matched_keywords}


def detect_urgency(text: str) -> str:
    normalized_text = text.lower()

    for keyword in URGENCY_KEYWORDS["high"]:
        if keyword in normalized_text:
            return "high"

    for keyword in URGENCY_KEYWORDS["normal"]:
        if keyword in normalized_text:
            return "normal"

    return "normal"


def validate_document(
    doc_type: str,
    extracted_fields: dict[str, Any],
    active_rules: Optional[dict[str, dict[str, Any]]] = None,
) -> tuple[list[str], list[str]]:
    rules = active_rules or get_active_rules()[0]
    rule = rules.get(doc_type, rules["other"])
    required_fields = rule.get("required_fields", [])
    missing_fields = [
        field for field in required_fields if not extracted_fields.get(field)
    ]

    validation_errors: list[str] = []
    for field in missing_fields:
        validation_errors.append(f"Missing required field: {field}")

    parcel = extracted_fields.get("parcel_number")
    if parcel and not re.match(r"^[A-Za-z0-9-]{4,30}$", parcel):
        validation_errors.append("Parcel number format looks invalid")

    date = extracted_fields.get("date")
    if date and len(date) < 6:
        validation_errors.append("Date format looks invalid")

    return missing_fields, validation_errors


def route_document(
    doc_type: str, active_rules: Optional[dict[str, dict[str, Any]]] = None
) -> str:
    rules = active_rules or get_active_rules()[0]
    rule = rules.get(doc_type, rules["other"])
    return rule.get("department", "General Intake")


def process_document(
    *,
    file_path: str,
    content_type: Optional[str] = None,
    force_anthropic_classification: bool = False,
) -> dict[str, Any]:
    active_rules = get_active_rules()[0]

    text, extraction_method, extraction_confidence = extract_text(
        file_path=file_path, content_type=content_type
    )
    fields = extract_fields(text)

    external_classification = None
    if force_anthropic_classification:
        external_classification = try_anthropic_classification(
            text, fields, active_rules=active_rules
        )
    if not external_classification:
        external_classification = try_external_classification(
            text, fields, active_rules=active_rules
        )
    if external_classification:
        doc_type = external_classification["doc_type"]
        urgency = external_classification["urgency"]
        classification_confidence = external_classification["confidence"]
        classification_meta = {
            "provider": external_classification.get("provider", "external"),
            "matched_keywords": external_classification.get("matched_keywords", []),
            "rationale": external_classification.get("rationale"),
        }
        department = external_classification["department"]
    else:
        doc_type, classification_confidence, classification_meta = classify_document(
            text, active_rules=active_rules
        )
        urgency = detect_urgency(text)
        department = route_document(doc_type, active_rules=active_rules)

    rule = active_rules.get(doc_type, active_rules["other"])
    required_fields = [
        str(field).strip()
        for field in rule.get("required_fields", [])
        if str(field).strip()
    ]
    missing_fields, validation_errors = validate_document(
        doc_type, fields, active_rules=active_rules
    )
    enrichment_meta: dict[str, Any] | None = None
    if missing_fields:
        enrichment_result = try_anthropic_field_enrichment(
            text=text,
            doc_type=doc_type,
            required_fields=required_fields,
            extracted_fields=fields,
        )
        if enrichment_result:
            enriched_fields = enrichment_result.get("fields", {})
            if isinstance(enriched_fields, dict):
                newly_filled: list[str] = []
                for key, value in enriched_fields.items():
                    if key not in fields or not fields.get(key):
                        fields[key] = value
                        newly_filled.append(str(key))
                if newly_filled:
                    missing_fields, validation_errors = validate_document(
                        doc_type, fields, active_rules=active_rules
                    )
                    enrichment_meta = {
                        "provider": enrichment_result.get("provider", "anthropic"),
                        "confidence": enrichment_result.get("confidence", 0.0),
                        "filled_fields": sorted(newly_filled),
                        "notes": enrichment_result.get("notes"),
                    }

    validation_penalty = min(len(validation_errors) * 0.08, 0.35)
    effective_confidence = max(
        min(classification_confidence, extraction_confidence) - validation_penalty, 0.0
    )

    requires_review = (
        effective_confidence < CONFIDENCE_THRESHOLD or len(validation_errors) > 0
    )
    if doc_type.lower() in FORCE_REVIEW_DOC_TYPES:
        requires_review = True

    return {
        "doc_type": doc_type,
        "department": department,
        "urgency": urgency,
        "confidence": round(effective_confidence, 4),
        "requires_review": requires_review,
        "extracted_text": text,
        "extracted_fields": fields,
        "missing_fields": missing_fields,
        "validation_errors": validation_errors,
        "pipeline_meta": {
            "extraction_method": extraction_method,
            "extraction_confidence": extraction_confidence,
            "classification_meta": classification_meta,
            "field_enrichment": enrichment_meta,
        },
    }
