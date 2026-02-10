from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Optional

from .config import (
    ANTHROPIC_API_KEY,
    ANTHROPIC_MODEL,
    AZURE_DI_API_KEY,
    AZURE_DI_API_VERSION,
    AZURE_DI_ENDPOINT,
    AZURE_DI_MODEL,
    CLASSIFIER_PROVIDER,
    OCR_PROVIDER,
    OPENAI_API_KEY,
    OPENAI_MODEL,
)
from .rules import get_active_rules

REQUEST_TIMEOUT_SECONDS = 60
AI_PLACEHOLDER_VALUES = {"", "n/a", "na", "none", "null", "unknown", "not provided", "missing"}


def _json_request(
    *,
    url: str,
    method: str,
    headers: dict[str, str],
    payload: Optional[dict[str, Any]] = None,
    data: Optional[bytes] = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
) -> tuple[int, dict[str, str], dict[str, Any]]:
    body: Optional[bytes] = data
    request_headers = dict(headers)

    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")

    request = urllib.request.Request(url=url, data=body, method=method, headers=request_headers)

    with urllib.request.urlopen(request, timeout=timeout) as response:
        status_code = response.getcode()
        response_headers = dict(response.headers.items())
        raw = response.read().decode("utf-8") if response.length != 0 else ""

    parsed_body: dict[str, Any] = {}
    if raw.strip():
        try:
            parsed_body = json.loads(raw)
        except json.JSONDecodeError:
            parsed_body = {"raw": raw}

    return status_code, response_headers, parsed_body


def _extract_json_payload(text: str) -> Optional[dict[str, Any]]:
    candidate = text.strip()

    if candidate.startswith("```"):
        candidate = candidate.strip("`")
        if candidate.startswith("json"):
            candidate = candidate[4:]

    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return None

    try:
        parsed = json.loads(match.group(0))
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        return None

    return None


def _normalize_classifier_payload(payload: dict[str, Any], active_rules: dict[str, dict[str, Any]]) -> dict[str, Any]:
    allowed_types = set(active_rules.keys())
    doc_type = str(payload.get("doc_type", "other")).strip().lower()
    if doc_type not in allowed_types:
        doc_type = "other"

    confidence = payload.get("confidence", 0.0)
    try:
        confidence_value = float(confidence)
    except (TypeError, ValueError):
        confidence_value = 0.0

    confidence_value = max(0.0, min(confidence_value, 0.99))

    department = str(payload.get("department") or active_rules[doc_type]["department"])
    urgency = str(payload.get("urgency", "normal")).strip().lower()
    if urgency not in {"high", "normal"}:
        urgency = "normal"

    matched_keywords = payload.get("matched_keywords", [])
    if not isinstance(matched_keywords, list):
        matched_keywords = []

    rationale = payload.get("rationale")
    if rationale is not None:
        rationale = str(rationale)

    return {
        "doc_type": doc_type,
        "department": department,
        "urgency": urgency,
        "confidence": round(confidence_value, 4),
        "matched_keywords": [str(item) for item in matched_keywords][:20],
        "rationale": rationale,
    }


def _classification_prompt(
    text: str, extracted_fields: dict[str, Any], active_rules: dict[str, dict[str, Any]]
) -> str:
    categories = ", ".join(sorted(active_rules.keys()))
    department_map = {doc_type: rule["department"] for doc_type, rule in active_rules.items()}

    return (
        "Classify this local-government intake document. "
        "Return JSON only with keys: doc_type, department, urgency, confidence, matched_keywords, rationale. "
        f"Allowed doc_type values: {categories}. "
        f"Preferred departments by type: {json.dumps(department_map)}. "
        "urgency must be either high or normal. "
        "confidence must be numeric from 0.0 to 0.99.\n\n"
        f"Extracted fields (best-effort OCR): {json.dumps(extracted_fields)}\n\n"
        f"Document text:\n{text[:12000]}"
    )


def _field_enrichment_prompt(
    *,
    text: str,
    doc_type: str,
    required_fields: list[str],
    extracted_fields: dict[str, Any],
) -> str:
    field_list = ", ".join(required_fields)
    return (
        "Extract missing fields from this local-government intake document. "
        "Return strict JSON only with shape: "
        '{"fields": {"field_name": "value"}, "confidence": 0.0-0.99, "notes": "short reason"}.\n'
        f"Document type: {doc_type}\n"
        f"Target fields: {field_list}\n"
        f"Existing extracted fields: {json.dumps(extracted_fields)}\n"
        "Rules:\n"
        "- Use only values explicitly present in the provided text.\n"
        "- If a value is not present, omit it from fields.\n"
        "- Do not invent names, addresses, dates, or IDs.\n"
        "- Keep values concise and literal.\n\n"
        f"Document text:\n{text[:12000]}"
    )


def _normalize_enriched_fields(
    payload: dict[str, Any],
    *,
    allowed_fields: set[str],
) -> dict[str, Any]:
    raw_fields = payload.get("fields", payload)
    if not isinstance(raw_fields, dict):
        return {}

    normalized: dict[str, Any] = {}
    for key, value in raw_fields.items():
        field_name = str(key).strip()
        if not field_name or field_name not in allowed_fields:
            continue
        text_value = str(value).strip()
        if text_value.lower() in AI_PLACEHOLDER_VALUES:
            continue
        if len(text_value) > 240:
            text_value = text_value[:240].strip()
        if not text_value:
            continue
        normalized[field_name] = text_value
    return normalized


def _guess_mime(file_path: str, content_type: Optional[str]) -> str:
    if content_type:
        return content_type

    extension = Path(file_path).suffix.lower()
    if extension == ".pdf":
        return "application/pdf"
    if extension in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if extension == ".png":
        return "image/png"
    if extension in {".tif", ".tiff"}:
        return "image/tiff"

    return "application/octet-stream"


def try_external_ocr(file_path: str, content_type: Optional[str] = None) -> Optional[tuple[str, str, float]]:
    if OCR_PROVIDER != "azure_di":
        return None

    if not AZURE_DI_ENDPOINT or not AZURE_DI_API_KEY:
        return None

    file_bytes = Path(file_path).read_bytes()
    base_url = AZURE_DI_ENDPOINT.rstrip("/")
    analyze_url = (
        f"{base_url}/documentintelligence/documentModels/{AZURE_DI_MODEL}:analyze"
        f"?api-version={AZURE_DI_API_VERSION}&outputContentFormat=text"
    )

    try:
        status_code, headers, _ = _json_request(
            url=analyze_url,
            method="POST",
            headers={
                "Ocp-Apim-Subscription-Key": AZURE_DI_API_KEY,
                "Content-Type": _guess_mime(file_path, content_type),
            },
            data=file_bytes,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        return None

    if status_code not in {200, 202}:
        return None

    operation_location = headers.get("Operation-Location") or headers.get("operation-location")
    if not operation_location:
        return None

    attempts = 30
    poll_delay = 1.0
    for _ in range(attempts):
        try:
            _, _, poll_body = _json_request(
                url=operation_location,
                method="GET",
                headers={"Ocp-Apim-Subscription-Key": AZURE_DI_API_KEY},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
            return None

        status = str(poll_body.get("status", "")).lower()
        if status == "succeeded":
            analyze_result = poll_body.get("analyzeResult", {})
            content = str(analyze_result.get("content") or "").strip()
            if not content:
                lines: list[str] = []
                for page in analyze_result.get("pages", []):
                    for line in page.get("lines", []):
                        line_text = line.get("content")
                        if line_text:
                            lines.append(str(line_text))
                content = "\n".join(lines)

            if not content:
                return None

            confidence_scores = [
                float(item.get("confidence", 0.0))
                for item in analyze_result.get("documents", [])
                if isinstance(item, dict)
            ]

            confidence = 0.92
            if confidence_scores:
                confidence = sum(confidence_scores) / len(confidence_scores)

            return content, "azure_di", round(max(0.0, min(confidence, 0.99)), 4)

        if status in {"failed", "canceled", "cancelled"}:
            return None

        time.sleep(poll_delay)

    return None


def _classify_with_openai(
    text: str, extracted_fields: dict[str, Any], active_rules: dict[str, dict[str, Any]]
) -> Optional[dict[str, Any]]:
    if not OPENAI_API_KEY:
        return None

    prompt = _classification_prompt(text, extracted_fields, active_rules)
    payload = {
        "model": OPENAI_MODEL,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": "You classify city government documents. Return strict JSON only.",
            },
            {"role": "user", "content": prompt},
        ],
    }

    try:
        _, _, response_body = _json_request(
            url="https://api.openai.com/v1/chat/completions",
            method="POST",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
            payload=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        return None

    choices = response_body.get("choices", [])
    if not choices:
        return None

    content = choices[0].get("message", {}).get("content")
    if not content or not isinstance(content, str):
        return None

    parsed = _extract_json_payload(content)
    if not parsed:
        return None

    normalized = _normalize_classifier_payload(parsed, active_rules)
    normalized["provider"] = "openai"
    return normalized


def _classify_with_anthropic(
    text: str, extracted_fields: dict[str, Any], active_rules: dict[str, dict[str, Any]]
) -> Optional[dict[str, Any]]:
    if not ANTHROPIC_API_KEY:
        return None

    prompt = _classification_prompt(text, extracted_fields, active_rules)
    payload = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 800,
        "temperature": 0,
        "system": "You classify city government documents and return strict JSON only.",
        "messages": [{"role": "user", "content": prompt}],
    }

    try:
        _, _, response_body = _json_request(
            url="https://api.anthropic.com/v1/messages",
            method="POST",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            payload=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        import logging
        logging.getLogger(__name__).error("Anthropic API call failed: %s", exc)
        return None

    content_blocks = response_body.get("content", [])
    text_blocks = [block.get("text") for block in content_blocks if isinstance(block, dict) and block.get("type") == "text"]
    if not text_blocks:
        return None

    parsed = _extract_json_payload("\n".join(text_blocks))
    if not parsed:
        return None

    normalized = _normalize_classifier_payload(parsed, active_rules)
    normalized["provider"] = "anthropic"
    return normalized


def try_anthropic_field_enrichment(
    *,
    text: str,
    doc_type: str,
    required_fields: list[str],
    extracted_fields: dict[str, Any],
) -> Optional[dict[str, Any]]:
    if not ANTHROPIC_API_KEY:
        return None
    if not text.strip():
        return None
    target_fields = [str(field).strip() for field in required_fields if str(field).strip()]
    if not target_fields:
        return None

    prompt = _field_enrichment_prompt(
        text=text,
        doc_type=doc_type,
        required_fields=target_fields,
        extracted_fields=extracted_fields,
    )
    payload = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 900,
        "temperature": 0,
        "system": "You extract explicit fields from documents and return strict JSON only.",
        "messages": [{"role": "user", "content": prompt}],
    }

    try:
        _, _, response_body = _json_request(
            url="https://api.anthropic.com/v1/messages",
            method="POST",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            payload=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        return None

    content_blocks = response_body.get("content", [])
    text_blocks = [
        block.get("text")
        for block in content_blocks
        if isinstance(block, dict) and block.get("type") == "text"
    ]
    if not text_blocks:
        return None

    parsed = _extract_json_payload("\n".join(text_blocks))
    if not parsed:
        return None

    allowed_fields = set(target_fields)
    # Allow email aliases when the model returns one variant.
    allowed_fields.update({"email", "applicant_email", "contact_email", "sender_email"})
    normalized_fields = _normalize_enriched_fields(parsed, allowed_fields=allowed_fields)
    if not normalized_fields:
        return None

    confidence_raw = parsed.get("confidence", 0.0)
    try:
        confidence = float(confidence_raw)
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(confidence, 0.99))

    notes = parsed.get("notes")
    return {
        "provider": "anthropic",
        "fields": normalized_fields,
        "confidence": round(confidence, 4),
        "notes": str(notes).strip() if notes is not None else None,
    }


def try_anthropic_classification(
    text: str,
    extracted_fields: dict[str, Any],
    active_rules: Optional[dict[str, dict[str, Any]]] = None,
) -> Optional[dict[str, Any]]:
    rules = active_rules or get_active_rules()[0]
    return _classify_with_anthropic(text, extracted_fields, rules)


def try_external_classification(
    text: str, extracted_fields: dict[str, Any], active_rules: Optional[dict[str, dict[str, Any]]] = None
) -> Optional[dict[str, Any]]:
    rules = active_rules or get_active_rules()[0]

    if CLASSIFIER_PROVIDER == "openai":
        return _classify_with_openai(text, extracted_fields, rules)

    if CLASSIFIER_PROVIDER == "anthropic":
        return _classify_with_anthropic(text, extracted_fields, rules)

    return None
