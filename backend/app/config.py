from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
UPLOAD_DIR = DATA_DIR / "uploads"
PROCESSED_DIR = DATA_DIR / "processed"
DATABASE_PATH = DATA_DIR / "citysort.db"

try:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env", override=True)
except Exception:
    pass


def _env_float(
    name: str, default: float, *, min_value: float = 0.0, max_value: float = 0.99
) -> float:
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


def _env_csv_list(name: str, default: str = "") -> list[str]:
    raw = os.getenv(name, default)
    if raw is None or not raw.strip():
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _env_int(
    name: str,
    default: int,
    *,
    min_value: Optional[int] = None,
    max_value: Optional[int] = None,
) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        parsed = default
    else:
        try:
            parsed = int(raw.strip())
        except ValueError:
            parsed = default

    if min_value is not None:
        parsed = max(min_value, parsed)
    if max_value is not None:
        parsed = min(max_value, parsed)
    return parsed


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _normalize_database_url(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    if parsed.scheme:
        return raw_url
    # Plain filesystem path -> sqlite file.
    path = Path(raw_url).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    return f"sqlite:///{path}"


CONFIDENCE_THRESHOLD = _env_float("CITYSORT_CONFIDENCE_THRESHOLD", 0.82)
FORCE_REVIEW_DOC_TYPES = _env_csv_set("CITYSORT_FORCE_REVIEW_DOC_TYPES")

# Runtime environment
APP_ENV = os.getenv("CITYSORT_ENV", "development").strip().lower() or "development"
IS_PRODUCTION = APP_ENV in {"prod", "production"}

# Primary datastore (SQLite for local dev, PostgreSQL for production).
DATABASE_URL = _normalize_database_url(
    os.getenv("CITYSORT_DATABASE_URL", f"sqlite:///{DATABASE_PATH}").strip()
)
_db_scheme = urlparse(DATABASE_URL).scheme.lower()
if _db_scheme in {"postgres", "postgresql", "postgresql+psycopg2"}:
    DATABASE_BACKEND = "postgresql"
elif _db_scheme in {"sqlite", ""}:
    DATABASE_BACKEND = "sqlite"
else:
    DATABASE_BACKEND = _db_scheme
DATABASE_CONNECT_TIMEOUT_SECONDS = _env_int(
    "CITYSORT_DATABASE_CONNECT_TIMEOUT_SECONDS", 8, min_value=1, max_value=120
)

OCR_PROVIDER = os.getenv("CITYSORT_OCR_PROVIDER", "local").strip().lower()
CLASSIFIER_PROVIDER = os.getenv("CITYSORT_CLASSIFIER_PROVIDER", "rules").strip().lower()

AZURE_DI_ENDPOINT = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT", "").strip()
AZURE_DI_API_KEY = os.getenv("AZURE_DOCUMENT_INTELLIGENCE_API_KEY", "").strip()
AZURE_DI_MODEL = os.getenv(
    "AZURE_DOCUMENT_INTELLIGENCE_MODEL", "prebuilt-layout"
).strip()
AZURE_DI_API_VERSION = os.getenv(
    "AZURE_DOCUMENT_INTELLIGENCE_API_VERSION", "2024-11-30"
).strip()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514").strip()

RULES_PATH_RAW = os.getenv(
    "CITYSORT_RULES_PATH", str(DATA_DIR / "document_rules.json")
).strip()
RULES_CONFIG_PATH = Path(RULES_PATH_RAW)
if not RULES_CONFIG_PATH.is_absolute():
    RULES_CONFIG_PATH = PROJECT_ROOT / RULES_CONFIG_PATH

DOCUMENT_TYPE_RULES = {
    "building_permit": {
        "keywords": [
            "building permit",
            "construction",
            "parcel",
            "zoning",
            "site plan",
            "inspection",
        ],
        "department": "Building Department",
        "required_fields": ["applicant_name", "address", "parcel_number", "date"],
    },
    "business_license": {
        "keywords": [
            "business license",
            "license renewal",
            "tax id",
            "llc",
            "business owner",
        ],
        "department": "Finance & Licensing",
        "required_fields": ["applicant_name", "address", "date"],
    },
    "foi_request": {
        "keywords": [
            "freedom of information",
            "foia",
            "public records",
            "open records",
            "records request",
        ],
        "department": "City Clerk",
        "required_fields": ["applicant_name", "date"],
    },
    "zoning_variance": {
        "keywords": [
            "zoning variance",
            "variance",
            "land use",
            "planning commission",
            "setback",
        ],
        "department": "Planning & Zoning",
        "required_fields": ["applicant_name", "address", "parcel_number", "date"],
    },
    "complaint": {
        "keywords": [
            "complaint",
            "code violation",
            "noise",
            "nuisance",
            "unsafe",
            "report",
        ],
        "department": "Code Enforcement",
        "required_fields": ["applicant_name", "address", "date"],
    },
    "benefits_application": {
        "keywords": [
            "benefits",
            "assistance",
            "eligibility",
            "application",
            "income",
            "household",
        ],
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
    "high": [
        "urgent",
        "immediate",
        "emergency",
        "deadline",
        "hearing date",
        "time sensitive",
    ],
    "normal": ["standard", "routine", "non-urgent"],
}

# Auth / RBAC
REQUIRE_AUTH = _env_bool("CITYSORT_REQUIRE_AUTH", False)
AUTH_SECRET = (
    os.getenv("CITYSORT_AUTH_SECRET", "change-me-in-production").strip()
    or "change-me-in-production"
)
AUTH_SECRET_PLACEHOLDER_VALUES = {
    "",
    "change-me-in-production",
    "dev",
    "test",
    "secret",
}
STRICT_AUTH_SECRET = _env_bool(
    "CITYSORT_STRICT_AUTH_SECRET", True if IS_PRODUCTION else False
)
ACCESS_TOKEN_TTL_MINUTES = _env_int(
    "CITYSORT_ACCESS_TOKEN_TTL_MINUTES", 60 * 12, min_value=5, max_value=60 * 24 * 30
)

# API hardening
CORS_ALLOWED_ORIGINS = _env_csv_list(
    "CITYSORT_CORS_ALLOWED_ORIGINS",
    "http://localhost:8000,http://127.0.0.1:8000",
)
CORS_ALLOW_CREDENTIALS = _env_bool("CITYSORT_CORS_ALLOW_CREDENTIALS", False)
TRUSTED_HOSTS = _env_csv_list("CITYSORT_TRUSTED_HOSTS", "localhost,127.0.0.1")
ENFORCE_HTTPS = _env_bool("CITYSORT_ENFORCE_HTTPS", True if IS_PRODUCTION else False)
SECURITY_HEADERS_ENABLED = _env_bool("CITYSORT_SECURITY_HEADERS_ENABLED", True)
CONTENT_SECURITY_POLICY = os.getenv(
    "CITYSORT_CONTENT_SECURITY_POLICY",
    "default-src 'self'; base-uri 'self'; frame-ancestors 'none'; object-src 'none'; "
    "img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self';",
).strip()
REFERRER_POLICY = os.getenv(
    "CITYSORT_REFERRER_POLICY", "strict-origin-when-cross-origin"
).strip()
UPLOAD_MAX_BYTES = _env_int(
    "CITYSORT_UPLOAD_MAX_BYTES",
    25 * 1024 * 1024,
    min_value=1024,
    max_value=500 * 1024 * 1024,
)
UPLOAD_ALLOWED_EXTENSIONS = {
    ext.lower().lstrip(".")
    for ext in _env_csv_list(
        "CITYSORT_UPLOAD_ALLOWED_EXTENSIONS",
        "pdf,txt,md,csv,json,docx,docm,png,jpg,jpeg,tif,tiff",
    )
}
UPLOAD_ALLOWED_MIME_PREFIXES = {
    item.strip().lower()
    for item in _env_csv_list(
        "CITYSORT_UPLOAD_ALLOWED_MIME_PREFIXES",
        "application/,text/,image/",
    )
}
UPLOAD_VIRUS_SCAN_ENABLED = _env_bool("CITYSORT_UPLOAD_VIRUS_SCAN_ENABLED", False)
UPLOAD_VIRUS_SCAN_BLOCK_ON_ERROR = _env_bool(
    "CITYSORT_UPLOAD_VIRUS_SCAN_BLOCK_ON_ERROR", True
)
CLAMAV_HOST = os.getenv("CITYSORT_CLAMAV_HOST", "127.0.0.1").strip()
CLAMAV_PORT = _env_int("CITYSORT_CLAMAV_PORT", 3310, min_value=1, max_value=65535)
RATE_LIMIT_ENABLED = _env_bool("CITYSORT_RATE_LIMIT_ENABLED", True)
RATE_LIMIT_WINDOW_SECONDS = _env_int(
    "CITYSORT_RATE_LIMIT_WINDOW_SECONDS", 60, min_value=1, max_value=3600
)
RATE_LIMIT_DEFAULT_PER_WINDOW = _env_int(
    "CITYSORT_RATE_LIMIT_DEFAULT_PER_WINDOW", 120, min_value=1, max_value=50000
)
RATE_LIMIT_UPLOAD_PER_WINDOW = _env_int(
    "CITYSORT_RATE_LIMIT_UPLOAD_PER_WINDOW", 20, min_value=1, max_value=50000
)
RATE_LIMIT_AI_PER_WINDOW = _env_int(
    "CITYSORT_RATE_LIMIT_AI_PER_WINDOW", 30, min_value=1, max_value=50000
)

# At-rest encryption for uploaded files (optional).
ENCRYPTION_AT_REST_ENABLED = _env_bool("CITYSORT_ENCRYPTION_AT_REST_ENABLED", False)
ENCRYPTION_KEY = os.getenv("CITYSORT_ENCRYPTION_KEY", "").strip()

# Durable async jobs
WORKER_POLL_INTERVAL_SECONDS = _env_int(
    "CITYSORT_WORKER_POLL_INTERVAL_SECONDS", 2, min_value=1, max_value=30
)
WORKER_MAX_ATTEMPTS = _env_int(
    "CITYSORT_WORKER_MAX_ATTEMPTS", 3, min_value=1, max_value=10
)
WORKER_ENABLED = _env_bool("CITYSORT_WORKER_ENABLED", True)
QUEUE_BACKEND = (
    os.getenv("CITYSORT_QUEUE_BACKEND", "sqlite").strip().lower() or "sqlite"
)
REDIS_URL = os.getenv("CITYSORT_REDIS_URL", "redis://127.0.0.1:6379/0").strip()
REDIS_JOB_QUEUE_NAME = (
    os.getenv("CITYSORT_REDIS_JOB_QUEUE_NAME", "citysort:jobs").strip()
    or "citysort:jobs"
)

# Watched folder ingestion
WATCH_DIR = os.getenv("CITYSORT_WATCH_DIR", "").strip() or None
WATCH_INTERVAL_SECONDS = _env_int(
    "CITYSORT_WATCH_INTERVAL_SECONDS", 30, min_value=5, max_value=300
)
WATCH_ENABLED = _env_bool("CITYSORT_WATCH_ENABLED", False)

# Notifications / Webhooks
WEBHOOK_URL = os.getenv("CITYSORT_WEBHOOK_URL", "").strip()
WEBHOOK_ENABLED = _env_bool("CITYSORT_WEBHOOK_ENABLED", False)

# Outbound email delivery (SMTP)
EMAIL_ENABLED = _env_bool("CITYSORT_EMAIL_ENABLED", False)
EMAIL_FROM_ADDRESS = os.getenv("CITYSORT_EMAIL_FROM_ADDRESS", "").strip()
EMAIL_FROM_NAME = (
    os.getenv("CITYSORT_EMAIL_FROM_NAME", "City Records Office").strip()
    or "City Records Office"
)
SMTP_HOST = os.getenv("CITYSORT_SMTP_HOST", "").strip()
SMTP_PORT = _env_int("CITYSORT_SMTP_PORT", 587, min_value=1, max_value=65535)
SMTP_USERNAME = os.getenv("CITYSORT_SMTP_USERNAME", "").strip()
SMTP_PASSWORD = os.getenv("CITYSORT_SMTP_PASSWORD", "").strip()
SMTP_USE_TLS = _env_bool("CITYSORT_SMTP_USE_TLS", True)
SMTP_USE_SSL = _env_bool("CITYSORT_SMTP_USE_SSL", False)
SMTP_TIMEOUT_SECONDS = _env_int(
    "CITYSORT_SMTP_TIMEOUT_SECONDS", 20, min_value=3, max_value=120
)

# Automation toggles
AUTO_ACK_EMAIL_ENABLED = _env_bool("CITYSORT_AUTO_ACK_EMAIL", True)
AUTO_STATUS_EMAIL_ENABLED = _env_bool("CITYSORT_AUTO_STATUS_EMAIL", True)
AUTO_MISSING_INFO_EMAIL_ENABLED = _env_bool("CITYSORT_AUTO_MISSING_INFO_EMAIL", True)
AUTO_ASSIGN_ENABLED = _env_bool("CITYSORT_AUTO_ASSIGN", False)
ESCALATION_ENABLED = _env_bool("CITYSORT_ESCALATION_ENABLED", True)
ESCALATION_DAYS = _env_int("CITYSORT_ESCALATION_DAYS", 3, min_value=1, max_value=30)
ESCALATION_FALLBACK_USER = os.getenv("CITYSORT_ESCALATION_FALLBACK_USER", "").strip()

# Separation of duties (optional stricter workflow approvals).
STRICT_APPROVAL_ROLE = os.getenv("CITYSORT_STRICT_APPROVAL_ROLE", "").strip().lower()

# Data governance
AUDIT_RETENTION_DAYS = _env_int(
    "CITYSORT_AUDIT_RETENTION_DAYS", 365 * 7, min_value=1, max_value=365 * 20
)
OUTBOUND_EMAIL_RETENTION_DAYS = _env_int(
    "CITYSORT_OUTBOUND_EMAIL_RETENTION_DAYS", 365 * 7, min_value=1, max_value=365 * 20
)
NOTIFICATION_RETENTION_DAYS = _env_int(
    "CITYSORT_NOTIFICATION_RETENTION_DAYS", 365 * 2, min_value=1, max_value=365 * 20
)

# Observability
LOG_LEVEL = os.getenv("CITYSORT_LOG_LEVEL", "INFO").strip().upper() or "INFO"
LOG_JSON = _env_bool("CITYSORT_LOG_JSON", True)
SENTRY_DSN = os.getenv("CITYSORT_SENTRY_DSN", "").strip()
SENTRY_TRACES_SAMPLE_RATE = _env_float(
    "CITYSORT_SENTRY_TRACES_SAMPLE_RATE", 0.0, min_value=0.0, max_value=1.0
)
PROMETHEUS_ENABLED = _env_bool("CITYSORT_PROMETHEUS_ENABLED", True)

# Deployment integration
DEPLOY_PROVIDER = os.getenv("CITYSORT_DEPLOY_PROVIDER", "local").strip().lower()
DEPLOY_COMMAND = os.getenv("CITYSORT_DEPLOY_COMMAND", "").strip()
DEPLOY_COMMAND_TIMEOUT_SECONDS = _env_int(
    "CITYSORT_DEPLOY_COMMAND_TIMEOUT_SECONDS", 300, min_value=10, max_value=3600
)

RENDER_DEPLOY_HOOK_URL = os.getenv("CITYSORT_RENDER_DEPLOY_HOOK_URL", "").strip()
RENDER_API_TOKEN = os.getenv("CITYSORT_RENDER_API_TOKEN", "").strip()
RENDER_SERVICE_ID = os.getenv("CITYSORT_RENDER_SERVICE_ID", "").strip()

GITHUB_TOKEN = os.getenv("CITYSORT_GITHUB_TOKEN", "").strip()
GITHUB_OWNER = os.getenv("CITYSORT_GITHUB_OWNER", "").strip()
GITHUB_REPO = os.getenv("CITYSORT_GITHUB_REPO", "").strip()
GITHUB_WORKFLOW_ID = os.getenv("CITYSORT_GITHUB_WORKFLOW_ID", "").strip()
GITHUB_REF = os.getenv("CITYSORT_GITHUB_REF", "main").strip()
