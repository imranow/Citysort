from __future__ import annotations

import socket
import struct
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, Optional

from fastapi import Request
from starlette.responses import Response

from .config import (
    CLAMAV_HOST,
    CLAMAV_PORT,
    CONTENT_SECURITY_POLICY,
    ENFORCE_HTTPS,
    REFERRER_POLICY,
    SECURITY_HEADERS_ENABLED,
    UPLOAD_ALLOWED_EXTENSIONS,
    UPLOAD_ALLOWED_MIME_PREFIXES,
    UPLOAD_MAX_BYTES,
    UPLOAD_VIRUS_SCAN_BLOCK_ON_ERROR,
    UPLOAD_VIRUS_SCAN_ENABLED,
)


class UploadValidationError(ValueError):
    pass


@dataclass
class RateLimitDecision:
    allowed: bool
    limit: int
    remaining: int
    reset_seconds: int


class SlidingWindowRateLimiter:
    """In-process rate limiter keyed by (client, scope)."""

    def __init__(self) -> None:
        self._buckets: dict[str, Deque[float]] = {}
        self._lock = threading.Lock()

    def check(self, key: str, *, limit: int, window_seconds: int) -> RateLimitDecision:
        if limit <= 0:
            return RateLimitDecision(
                allowed=True, limit=limit, remaining=0, reset_seconds=window_seconds
            )
        now = time.monotonic()
        window_start = now - window_seconds
        with self._lock:
            bucket = self._buckets.setdefault(key, deque())
            while bucket and bucket[0] < window_start:
                bucket.popleft()
            used = len(bucket)
            if used >= limit:
                reset_seconds = (
                    max(1, int(window_seconds - (now - bucket[0])))
                    if bucket
                    else window_seconds
                )
                return RateLimitDecision(
                    allowed=False,
                    limit=limit,
                    remaining=0,
                    reset_seconds=reset_seconds,
                )
            bucket.append(now)
            return RateLimitDecision(
                allowed=True,
                limit=limit,
                remaining=max(limit - len(bucket), 0),
                reset_seconds=window_seconds,
            )


def client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for", "").strip()
    if forwarded_for:
        # First IP is the originating client according to RFC 7239 patterns.
        return forwarded_for.split(",")[0].strip() or "unknown"
    if request.client and request.client.host:
        return str(request.client.host)
    return "unknown"


def request_is_secure(request: Request) -> bool:
    if request.url.scheme.lower() == "https":
        return True
    proto = request.headers.get("x-forwarded-proto", "").lower()
    return proto == "https"


def should_block_insecure_request(request: Request) -> bool:
    if not ENFORCE_HTTPS:
        return False
    if request.url.path in {"/health", "/livez", "/readyz"}:
        return False
    return not request_is_secure(request)


def apply_security_headers(response: Response) -> None:
    if not SECURITY_HEADERS_ENABLED:
        return
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = REFERRER_POLICY
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Cross-Origin-Opener-Policy"] = "same-origin"
    response.headers["Cross-Origin-Resource-Policy"] = "same-origin"
    if ENFORCE_HTTPS:
        response.headers["Strict-Transport-Security"] = (
            "max-age=63072000; includeSubDomains; preload"
        )
    if CONTENT_SECURITY_POLICY:
        response.headers["Content-Security-Policy"] = CONTENT_SECURITY_POLICY


def _allowed_extension(filename: str) -> bool:
    extension = Path(filename).suffix.lower().lstrip(".")
    if not UPLOAD_ALLOWED_EXTENSIONS:
        return True
    return extension in UPLOAD_ALLOWED_EXTENSIONS


def _allowed_content_type(content_type: Optional[str]) -> bool:
    if not content_type:
        return False
    normalized = content_type.strip().lower()
    return any(normalized.startswith(prefix) for prefix in UPLOAD_ALLOWED_MIME_PREFIXES)


def _clamav_scan(payload: bytes) -> tuple[bool, Optional[str]]:
    """Return (is_clean, reason_if_blocked)."""
    chunk_size = 1024 * 16
    try:
        with socket.create_connection((CLAMAV_HOST, CLAMAV_PORT), timeout=5.0) as sock:
            sock.sendall(b"zINSTREAM\0")
            for index in range(0, len(payload), chunk_size):
                chunk = payload[index : index + chunk_size]
                sock.sendall(struct.pack("!I", len(chunk)))
                sock.sendall(chunk)
            sock.sendall(struct.pack("!I", 0))
            result = sock.recv(4096).decode("utf-8", errors="replace").strip()
    except Exception as exc:
        if UPLOAD_VIRUS_SCAN_BLOCK_ON_ERROR:
            return False, f"Virus scanner unavailable: {exc}"
        return True, None

    if "FOUND" in result:
        return False, result
    return True, None


def validate_upload(
    *,
    filename: str,
    content_type: Optional[str],
    payload: bytes,
) -> None:
    if not filename.strip():
        raise UploadValidationError("File name is required.")
    if not payload:
        raise UploadValidationError("Uploaded file is empty.")
    if len(payload) > UPLOAD_MAX_BYTES:
        raise UploadValidationError(
            f"File too large. Maximum allowed size is {UPLOAD_MAX_BYTES} bytes."
        )
    if not _allowed_extension(filename):
        allowed = ", ".join(sorted(UPLOAD_ALLOWED_EXTENSIONS))
        raise UploadValidationError(
            f"File extension is not allowed. Allowed: {allowed}"
        )
    if not _allowed_content_type(content_type):
        raise UploadValidationError("Unsupported content type.")
    if UPLOAD_VIRUS_SCAN_ENABLED:
        clean, reason = _clamav_scan(payload)
        if not clean:
            raise UploadValidationError(
                f"Upload blocked by malware scanner: {reason or 'malicious content detected'}"
            )
