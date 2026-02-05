from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Optional, Tuple

from .config import DATA_DIR, DOCUMENT_TYPE_RULES, RULES_CONFIG_PATH

RuleDefinition = dict[str, Any]
RuleMap = dict[str, RuleDefinition]


def _default_other_rule() -> RuleDefinition:
    return {
        "keywords": [],
        "department": "General Intake",
        "required_fields": ["applicant_name", "date"],
    }


def get_rules_path() -> Path:
    return RULES_CONFIG_PATH


def _ensure_rules_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    get_rules_path().parent.mkdir(parents=True, exist_ok=True)


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def normalize_rules(candidate: dict[str, Any]) -> RuleMap:
    if not isinstance(candidate, dict) or not candidate:
        raise ValueError("rules must be a non-empty JSON object")

    normalized: RuleMap = {}

    for raw_doc_type, raw_rule in candidate.items():
        doc_type = str(raw_doc_type).strip()
        if not doc_type:
            raise ValueError("document type keys cannot be empty")

        if not isinstance(raw_rule, dict):
            raise ValueError(f"rule for '{doc_type}' must be an object")

        keywords_raw = raw_rule.get("keywords", [])
        if not isinstance(keywords_raw, list):
            raise ValueError(f"rule '{doc_type}.keywords' must be a list")
        keywords = _dedupe([str(item).strip().lower() for item in keywords_raw if str(item).strip()])

        department = str(raw_rule.get("department", "General Intake")).strip()
        if not department:
            department = "General Intake"

        required_fields_raw = raw_rule.get("required_fields", [])
        if not isinstance(required_fields_raw, list):
            raise ValueError(f"rule '{doc_type}.required_fields' must be a list")
        required_fields = _dedupe([str(item).strip() for item in required_fields_raw if str(item).strip()])

        normalized[doc_type] = {
            "keywords": keywords,
            "department": department,
            "required_fields": required_fields,
        }

    if "other" not in normalized:
        normalized["other"] = _default_other_rule()

    return normalized


def get_default_rules() -> RuleMap:
    defaults = deepcopy(DOCUMENT_TYPE_RULES)
    return normalize_rules(defaults)


def get_active_rules() -> Tuple[RuleMap, str]:
    path = get_rules_path()
    if not path.exists():
        return get_default_rules(), "default"

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and "rules" in payload and isinstance(payload["rules"], dict):
            payload = payload["rules"]
        rules = normalize_rules(payload)
        return rules, "custom"
    except Exception:
        return get_default_rules(), "default"


def save_rules(rules: dict[str, Any]) -> RuleMap:
    normalized = normalize_rules(rules)
    _ensure_rules_dir()
    get_rules_path().write_text(json.dumps(normalized, indent=2, sort_keys=True), encoding="utf-8")
    return normalized


def reset_rules_to_default() -> RuleMap:
    _ensure_rules_dir()
    path = get_rules_path()
    if path.exists():
        path.unlink()
    return get_default_rules()
