from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def admin_client(tmp_path, monkeypatch):
    from app import auth, config, db
    from app import main as main_module

    monkeypatch.setattr(db, "DATA_DIR", tmp_path)
    monkeypatch.setattr(db, "UPLOAD_DIR", tmp_path / "uploads")
    monkeypatch.setattr(db, "PROCESSED_DIR", tmp_path / "processed")
    monkeypatch.setattr(db, "APPROVED_EXPORT_ENABLED", True)
    monkeypatch.setattr(db, "APPROVED_EXPORT_DIR", tmp_path / "approved")
    monkeypatch.setattr(db, "DATABASE_PATH", tmp_path / "citysort.db")
    monkeypatch.setattr(main_module, "UPLOAD_DIR", tmp_path / "uploads")
    monkeypatch.setattr(main_module, "APPROVED_EXPORT_ENABLED", True)
    monkeypatch.setattr(main_module, "APPROVED_EXPORT_DIR", tmp_path / "approved")

    (tmp_path / "uploads").mkdir(exist_ok=True)
    (tmp_path / "processed").mkdir(exist_ok=True)
    (tmp_path / "approved").mkdir(exist_ok=True)

    monkeypatch.setattr(config, "REQUIRE_AUTH", True)
    monkeypatch.setattr(auth, "REQUIRE_AUTH", True)
    monkeypatch.setattr(config, "RATE_LIMIT_ENABLED", False)
    monkeypatch.setattr(main_module, "RATE_LIMIT_ENABLED", False)
    monkeypatch.setattr(config, "WORKER_ENABLED", False)
    monkeypatch.setattr(config, "WATCH_ENABLED", False)
    monkeypatch.setattr(config, "PROMETHEUS_ENABLED", False)
    monkeypatch.setattr(main_module, "STRICT_AUTH_SECRET", False)

    db.init_db()

    from app.main import app

    return TestClient(app, raise_server_exceptions=False, headers={"host": "localhost"})


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _bootstrap_admin(client: TestClient) -> dict[str, object]:
    response = client.post(
        "/api/auth/bootstrap",
        json={
            "email": "admin@example.com",
            "password": "StrongPass123!",
            "full_name": "Admin User",
        },
    )
    assert response.status_code == 200
    return response.json()


def _create_viewer_user(
    client: TestClient, admin_token: str, email: str
) -> dict[str, object]:
    invite_response = client.post(
        "/api/platform/invitations",
        headers=_auth_headers(admin_token),
        json={
            "email": email,
            "role": "member",
            "actor": "admin@example.com",
            "expires_in_days": 7,
        },
    )
    assert invite_response.status_code == 200
    invite_token = invite_response.json()["invite_token"]

    signup_response = client.post(
        "/api/auth/signup",
        json={
            "email": email,
            "password": "StrongPass123!",
            "full_name": "Viewer",
            "invitation_token": invite_token,
        },
    )
    assert signup_response.status_code == 200
    return signup_response.json()


def test_admin_billing_stats_returns_aggregates(admin_client):
    from app import repository

    bootstrap = _bootstrap_admin(admin_client)
    admin_token = str(bootstrap["access_token"])

    pro_user = repository.create_user(
        email="pro@example.com",
        full_name="Pro User",
        password_hash="hash",
        role="viewer",
        plan_tier="pro",
    )
    repository.create_subscription(
        user_id=pro_user["id"],
        plan_tier="pro",
        billing_type="monthly",
        stripe_subscription_id="sub_admin_stats",
        stripe_customer_id="cus_admin_stats",
        status="active",
    )
    repository.create_payment_event(
        user_id=pro_user["id"],
        stripe_event_id="evt_admin_stats_1",
        event_type="invoice.paid",
        amount_cents=2900,
        plan_tier="pro",
        billing_type="monthly",
        raw_payload="{}",
    )

    response = admin_client.get(
        "/api/admin/billing-stats", headers=_auth_headers(admin_token)
    )
    assert response.status_code == 200
    data = response.json()
    assert data["active_subscriptions"] >= 1
    assert data["mrr_cents"] >= 2900
    assert data["revenue_last_30_days_cents"] >= 2900
    assert "pro" in data["plan_distribution"]
    assert isinstance(data["recent_payments"], list)
    assert len(data["recent_payments"]) >= 1


def test_admin_system_health_returns_connectivity_and_queue(admin_client):
    bootstrap = _bootstrap_admin(admin_client)
    admin_token = str(bootstrap["access_token"])

    upload_response = admin_client.post(
        "/api/documents/upload",
        headers=_auth_headers(admin_token),
        files={
            "file": (
                "health.txt",
                b"Building Permit\nApplicant: Health Test\nDate: 01/01/2026",
                "text/plain",
            )
        },
        data={"source_channel": "test", "process_async": "false"},
    )
    assert upload_response.status_code == 200

    response = admin_client.get(
        "/api/admin/system-health", headers=_auth_headers(admin_token)
    )
    assert response.status_code == 200
    data = response.json()
    assert "job_queue" in data
    assert "documents_last_24h" in data
    assert "errors_last_24h" in data
    assert "emails_last_24h" in data
    assert data["connectivity"]["database"]["status"] in {"ok", "error"}
    assert data["connectivity"]["storage"]["status"] in {"ok", "error"}


def test_admin_audit_log_supports_filtering_and_pagination(admin_client):
    from app import repository

    bootstrap = _bootstrap_admin(admin_client)
    admin_token = str(bootstrap["access_token"])

    document = repository.create_document(
        document={
            "id": "admin-audit-doc",
            "filename": "audit.txt",
            "storage_path": "data/uploads/audit.txt",
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
    assert document["id"] == "admin-audit-doc"

    repository.create_audit_event(
        document_id="admin-audit-doc",
        action="admin_test_action",
        actor="admin_user",
        details="first",
    )
    repository.create_audit_event(
        document_id="admin-audit-doc",
        action="admin_test_action",
        actor="admin_user",
        details="second",
    )

    response = admin_client.get(
        "/api/admin/audit-log?action=admin_test_action&actor=admin_user&limit=1&offset=0",
        headers=_auth_headers(admin_token),
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 2
    assert len(data["items"]) == 1
    assert data["items"][0]["action"] == "admin_test_action"
    assert data["items"][0]["actor"] == "admin_user"
    assert data["items"][0]["filename"] == "audit.txt"

    next_page = admin_client.get(
        "/api/admin/audit-log?action=admin_test_action&actor=admin_user&limit=1&offset=1",
        headers=_auth_headers(admin_token),
    )
    assert next_page.status_code == 200
    assert len(next_page.json()["items"]) == 1


def test_non_admin_cannot_access_admin_endpoints(admin_client):
    bootstrap = _bootstrap_admin(admin_client)
    admin_token = str(bootstrap["access_token"])
    viewer = _create_viewer_user(
        admin_client, admin_token=admin_token, email="viewer@example.com"
    )
    viewer_token = str(viewer["access_token"])

    billing_response = admin_client.get(
        "/api/admin/billing-stats", headers=_auth_headers(viewer_token)
    )
    health_response = admin_client.get(
        "/api/admin/system-health", headers=_auth_headers(viewer_token)
    )
    audit_response = admin_client.get(
        "/api/admin/audit-log", headers=_auth_headers(viewer_token)
    )

    assert billing_response.status_code == 403
    assert health_response.status_code == 403
    assert audit_response.status_code == 403
