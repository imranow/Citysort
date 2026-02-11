from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

from .config import LOG_JSON, LOG_LEVEL

EMAIL_RE = re.compile(r"([A-Za-z0-9._%+-]+)@([A-Za-z0-9.-]+\.[A-Za-z]{2,})")
API_KEY_RE = re.compile(r"\b(cs_[A-Za-z0-9_\-]{10,})\b")


def _mask_pii(value: str) -> str:
    text = EMAIL_RE.sub("***@\\2", value)
    text = API_KEY_RE.sub("cs_***redacted***", text)
    return text


class JsonLogFormatter(logging.Formatter):
    """Minimal JSON formatter for machine-readable logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": _mask_pii(record.getMessage()),
            "module": record.module,
            "line": record.lineno,
        }
        request_id = getattr(record, "request_id", None)
        if request_id:
            payload["request_id"] = str(request_id)
        if record.exc_info:
            payload["exception"] = _mask_pii(self.formatException(record.exc_info))
        return json.dumps(payload, ensure_ascii=True)


def configure_logging() -> None:
    if os.getenv("CITYSORT_LOGGING_CONFIGURED", "").strip().lower() == "1":
        return

    level = getattr(logging, LOG_LEVEL, logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(level)
    if LOG_JSON:
        handler.setFormatter(JsonLogFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
        )

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    root.addHandler(handler)

    # Keep uvicorn logs consistent with application logs.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.setLevel(level)
        logger.propagate = True

    os.environ["CITYSORT_LOGGING_CONFIGURED"] = "1"
