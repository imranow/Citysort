from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
PROCESSED_DIR = DATA_DIR / "processed"
DATABASE_PATH = DATA_DIR / "citysort.db"

try:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")
except Exception:
    pass


def _env_float(name: str, default: float, *, min_value: float = 0.0, max_value: float = 0.99) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        parsed = float(raw.strip())
    except ValueError:
        return default
    return max(min_value, min(max_value, parsed))


def _env_csv_set(name: str) -> set[str]:
    raw = os.getenv(name, "")
    if not raw.strip():
        return set()
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


CONFIDENCE_THRESHOLD = _env_float("CITYSORT_CONFIDENCE_THRESHOLD", 0.82)
FORCE_REVIEW_DOC_TYPES = _env_csv_set("CITYSORT_FORCE_REVIEW_DOC_TYPES")

OCR_PROVIDER = os.getenv("CITYSORT_OCR_PROVIDER", "local").strip().lower()
CLASSIFIER_PROVIDER = os.getenv("CITYSORT_CLASSIFIER_PROVIDER", "rules").strip().lower()

AZURE_DI_ENDPOINT = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT", "").strip()
AZURE_DI_API_KEY = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_API_KEY", "").strip()
AZURE_DI_MODEL = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_MODEL", "prebuilt-layout").strip()
AZURE_DI_API_VERSION = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_API_VERSION", "2024-11-30").strip()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest").strip()

RULES_PATH_RAW = os.getenv("CITYSORT_RULES_PATH", str(DATA_DIR / "document_rules.json")).strip()
RULES_CONFIG_PATH = Path(RULES_PATH_RAW)
if not RULES_CONFIG_PATH.is_absolute():
    RULES_CONFIG_PATH = PROJECT_ROOT / RULES_CONFIG_PATH

DOCUMENT_TYPE_RULES = {
    "building_permit": {
        "keywords": ["building permit", "construction", "parcel", "zoning", "site plan", "inspection"],
        "department": "Building Department",
        "required_fields": ["applicant_name", "address", "parcel_number", "date"],
    },
    "business_license": {
        "keywords": ["business license", "license renewal", "tax id", "llc", "business owner"],
        "department": "Finance & Licensing",
        "required_fields": ["applicant_name", "address", "date"],
    },
    "foi_request": {
        "keywords": ["freedom of information", "foia", "public records", "open records", "records request"],
        "department": "City Clerk",
        "required_fields": ["applicant_name", "date"],
    },
    "zoning_variance": {
        "keywords": ["zoning variance", "variance", "land use", "planning commission", "setback"],
        "department": "Planning & Zoning",
        "required_fields": ["applicant_name", "address", "parcel_number", "date"],
    },
    "complaint": {
        "keywords": ["complaint", "code violation", "noise", "nuisance", "unsafe", "report"],
        "department": "Code Enforcement",
        "required_fields": ["applicant_name", "address", "date"],
    },
    "benefits_application": {
        "keywords": ["benefits", "assistance", "eligibility", "application", "income", "household"],
        "department": "Human Services",
        "required_fields": ["applicant_name", "address", "date"],
    },
    "court_filing": {
        "keywords": ["court", "filing", "case", "petition", "respondent", "docket"],
        "department": "Municipal Court",
        "required_fields": ["applicant_name", "case_number", "date"],
    },
    "other": {
        "keywords": [],
        "department": "General Intake",
        "required_fields": ["applicant_name", "date"],
    },
}

URGENCY_KEYWORDS = {
    "high": ["urgent", "immediate", "emergency", "deadline", "hearing date", "time sensitive"],
    "normal": ["standard", "routine", "non-urgent"],
}
