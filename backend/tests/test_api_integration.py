from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    """Create a FastAPI test client with isolated database."""
    from app import db, config, main as main_module

    monkeypatch.setattr(db, "DATA_DIR", tmp_path)
    monkeypatch.setattr(db, "UPLOAD_DIR", tmp_path / "uploads")
    monkeypatch.setattr(db, "PROCESSED_DIR", tmp_path / "processed")
    monkeypatch.setattr(db, "DATABASE_PATH", tmp_path / "citysort.db")
    monkeypatch.setattr(main_module, "UPLOAD_DIR", tmp_path / "uploads")
    (tmp_path / "uploads").mkdir(exist_ok=True)
    (tmp_path / "processed").mkdir(exist_ok=True)

    monkeypatch.setattr(config, "REQUIRE_AUTH", False)
    monkeypatch.setattr(config, "RATE_LIMIT_ENABLED", False)
    monkeypatch.setattr(config, "WORKER_ENABLED", False)
    monkeypatch.setattr(config, "WATCH_ENABLED", False)
    monkeypatch.setattr(config, "PROMETHEUS_ENABLED", False)

    db.init_db()

    from app.main import app

    return TestClient(app, raise_server_exceptions=False, headers={"host": "localhost"})


def test_health_endpoint(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"


def test_livez_endpoint(client):
    resp = client.get("/livez")
    assert resp.status_code == 200
    assert resp.json()["status"] == "alive"


def test_readyz_endpoint(client):
    resp = client.get("/readyz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ready"


def test_upload_document(client):
    resp = client.post(
        "/api/documents/upload",
        files={"file": ("test.txt", b"Building Permit\nApplicant: Test User\nDate: 01/01/2026", "text/plain")},
        data={"source_channel": "test", "process_async": "false"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["filename"] == "test.txt"
    assert data["status"] in {"ingested", "routed", "needs_review"}


def test_upload_rejects_empty_file(client):
    resp = client.post(
        "/api/documents/upload",
        files={"file": ("empty.txt", b"", "text/plain")},
        data={"source_channel": "test"},
    )
    assert resp.status_code == 400


def test_upload_rejects_disallowed_extension(client):
    resp = client.post(
        "/api/documents/upload",
        files={"file": ("malware.exe", b"content", "application/octet-stream")},
        data={"source_channel": "test"},
    )
    assert resp.status_code == 400


def test_get_documents_empty(client):
    resp = client.get("/api/documents")
    assert resp.status_code == 200
    assert resp.json()["items"] == []


def test_get_documents_after_upload(client):
    client.post(
        "/api/documents/upload",
        files={"file": ("doc.txt", b"Some test content", "text/plain")},
        data={"source_channel": "test", "process_async": "false"},
    )
    resp = client.get("/api/documents")
    assert resp.status_code == 200
    assert len(resp.json()["items"]) == 1


def test_get_single_document(client):
    upload_resp = client.post(
        "/api/documents/upload",
        files={"file": ("doc.txt", b"Some test content", "text/plain")},
        data={"source_channel": "test", "process_async": "false"},
    )
    doc_id = upload_resp.json()["id"]
    resp = client.get(f"/api/documents/{doc_id}")
    assert resp.status_code == 200
    assert resp.json()["id"] == doc_id


def test_get_nonexistent_document(client):
    resp = client.get("/api/documents/nonexistent-id")
    assert resp.status_code == 404


def test_analytics_endpoint(client):
    resp = client.get("/api/analytics")
    assert resp.status_code == 200
    data = resp.json()
    assert "total_documents" in data
    assert "by_status" in data


def test_queues_endpoint(client):
    resp = client.get("/api/queues")
    assert resp.status_code == 200


def test_rules_get(client):
    resp = client.get("/api/config/rules")
    assert resp.status_code == 200
    data = resp.json()
    assert "rules" in data


def test_notifications_empty(client):
    resp = client.get("/api/notifications")
    assert resp.status_code == 200
    data = resp.json()
    assert "items" in data


def test_audit_trail_for_document(client):
    upload_resp = client.post(
        "/api/documents/upload",
        files={"file": ("doc.txt", b"Some test content", "text/plain")},
        data={"source_channel": "test", "process_async": "false"},
    )
    doc_id = upload_resp.json()["id"]
    resp = client.get(f"/api/documents/{doc_id}/audit")
    assert resp.status_code == 200
    data = resp.json()
    assert "items" in data


def test_request_id_header(client):
    resp = client.get("/health")
    assert "x-request-id" in resp.headers


def test_security_headers_present(client):
    resp = client.get("/health")
    assert resp.headers.get("x-content-type-options") == "nosniff"
    assert resp.headers.get("x-frame-options") == "DENY"


def test_connector_types(client):
    resp = client.get("/api/connectors/types")
    if resp.status_code == 200:
        data = resp.json()
        assert isinstance(data, (list, dict))
