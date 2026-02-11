from __future__ import annotations

import pytest

from app.security import (
    SlidingWindowRateLimiter,
    UploadValidationError,
    validate_upload,
)


def test_rate_limiter_blocks_after_limit() -> None:
    limiter = SlidingWindowRateLimiter()
    first = limiter.check("127.0.0.1:upload", limit=2, window_seconds=60)
    second = limiter.check("127.0.0.1:upload", limit=2, window_seconds=60)
    third = limiter.check("127.0.0.1:upload", limit=2, window_seconds=60)
    assert first.allowed is True
    assert second.allowed is True
    assert third.allowed is False


def test_upload_validation_rejects_disallowed_extension(monkeypatch) -> None:
    from app import security

    monkeypatch.setattr(security, "UPLOAD_ALLOWED_EXTENSIONS", {"txt"})
    with pytest.raises(UploadValidationError):
        validate_upload(
            filename="malware.exe",
            content_type="application/octet-stream",
            payload=b"abc",
        )


def test_upload_validation_allows_expected_text(monkeypatch) -> None:
    from app import security

    monkeypatch.setattr(security, "UPLOAD_ALLOWED_EXTENSIONS", {"txt"})
    monkeypatch.setattr(security, "UPLOAD_ALLOWED_MIME_PREFIXES", {"text/"})
    validate_upload(filename="safe.txt", content_type="text/plain", payload=b"hello")
