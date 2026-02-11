from __future__ import annotations

import time
from pathlib import Path

import pytest
from starlette.requests import Request


@pytest.fixture()
def isolated_modules(tmp_path, monkeypatch):
    from app import auth
    from app import db
    from app import jobs
    from app import repository

    monkeypatch.setattr(db, "DATA_DIR", tmp_path)
    monkeypatch.setattr(db, "UPLOAD_DIR", tmp_path / "uploads")
    monkeypatch.setattr(db, "PROCESSED_DIR", tmp_path / "processed")
    monkeypatch.setattr(db, "DATABASE_PATH", tmp_path / "citysort.db")
    db.init_db()

    monkeypatch.setattr(auth, "REQUIRE_AUTH", True)
    monkeypatch.setattr(auth, "AUTH_SECRET", "unit-test-secret")
    monkeypatch.setattr(jobs, "WORKER_POLL_INTERVAL_SECONDS", 0)

    return {"auth": auth, "db": db, "jobs": jobs, "repository": repository}


def _request_with_bearer(token: str) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [(b"authorization", f"Bearer {token}".encode("utf-8"))],
        "scheme": "http",
        "server": ("testserver", 80),
        "client": ("127.0.0.1", 1234),
        "query_string": b"",
    }
    return Request(scope)


def test_bootstrap_login_and_authorize(isolated_modules) -> None:
    auth = isolated_modules["auth"]

    bootstrap = auth.bootstrap_admin(email="admin@example.com", password="StrongPass123!", full_name="Admin")
    assert bootstrap["user"]["role"] == "admin"
    assert bootstrap["access_token"]

    login = auth.authenticate_user(email="admin@example.com", password="StrongPass123!")
    token = login["access_token"]
    assert token

    identity = auth.authorize_request(_request_with_bearer(token), required_role="admin", allow_api_key=False)
    assert identity["auth_type"] == "user"
    assert identity["role"] == "admin"


def test_durable_job_worker_processes_document(isolated_modules) -> None:
    jobs = isolated_modules["jobs"]
    repository = isolated_modules["repository"]
    db = isolated_modules["db"]

    upload_dir = db.UPLOAD_DIR
    upload_dir.mkdir(parents=True, exist_ok=True)
    sample_path = upload_dir / "sample.txt"
    sample_path.write_text(
        "Building Permit\nApplicant: John Doe\nAddress: 12 Main Street\nParcel Number: P-1234\nDate: 02/07/2026",
        encoding="utf-8",
    )

    document = repository.create_document(
        document={
            "id": "doc-1",
            "filename": "sample.txt",
            "storage_path": str(sample_path),
            "source_channel": "test",
            "content_type": "text/plain",
            "status": "ingested",
            "requires_review": False,
            "confidence": 0.0,
            "doc_type": None,
            "department": None,
            "urgency": "normal",
        }
    )
    assert document["status"] == "ingested"

    job = jobs.enqueue_document_processing(document_id="doc-1", actor="test_worker")
    jobs.start_job_worker()
    try:
        for _ in range(500):
            current = jobs.get_job_by_id(job["id"])
            if current and current["status"] in {"completed", "failed"}:
                break
            time.sleep(0.02)
        current = jobs.get_job_by_id(job["id"])
        assert current is not None
        assert current["status"] == "completed"

        updated_doc = repository.get_document("doc-1")
        assert updated_doc is not None
        assert updated_doc["status"] in {"routed", "needs_review"}
    finally:
        jobs.stop_job_worker()
