from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def auth_client(tmp_path, monkeypatch):
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


def _create_invited_user(
    client: TestClient, *, admin_token: str, email: str, role: str = "member"
) -> dict[str, object]:
    invite_response = client.post(
        "/api/platform/invitations",
        headers=_auth_headers(admin_token),
        json={
            "email": email,
            "role": role,
            "actor": "admin@example.com",
            "expires_in_days": 7,
        },
    )
    assert invite_response.status_code == 200
    invite_payload = invite_response.json()

    signup_response = client.post(
        "/api/auth/signup",
        json={
            "email": email,
            "password": "StrongPass123!",
            "full_name": "Invited User",
            "invitation_token": invite_payload["invite_token"],
        },
    )
    assert signup_response.status_code == 200
    return signup_response.json()


def test_email_preferences_crud_endpoints(auth_client):
    bootstrap = _bootstrap_admin(auth_client)
    token = str(bootstrap["access_token"])

    get_response = auth_client.get(
        "/api/auth/me/email-preferences", headers=_auth_headers(token)
    )
    assert get_response.status_code == 200
    data = get_response.json()
    assert data["account_welcome"] is True
    assert data["doc_assigned"] is True

    update_response = auth_client.put(
        "/api/auth/me/email-preferences",
        headers=_auth_headers(token),
        json={
            "account_plan_change": False,
            "doc_assigned": False,
        },
    )
    assert update_response.status_code == 200
    updated = update_response.json()
    assert updated["account_plan_change"] is False
    assert updated["doc_assigned"] is False
    assert updated["doc_review_complete"] is True

    verify_response = auth_client.get(
        "/api/auth/me/email-preferences", headers=_auth_headers(token)
    )
    assert verify_response.status_code == 200
    verified = verify_response.json()
    assert verified["account_plan_change"] is False
    assert verified["doc_assigned"] is False


def test_signup_sends_welcome_email_and_records_outbound(auth_client, monkeypatch):
    from app import account_emails
    from app.db import get_connection

    monkeypatch.setattr(account_emails, "EMAIL_ENABLED", True)
    monkeypatch.setattr(account_emails, "email_configured", lambda: True)
    send_mock = MagicMock()
    monkeypatch.setattr(account_emails, "send_email", send_mock)

    bootstrap = _bootstrap_admin(auth_client)
    admin_token = str(bootstrap["access_token"])
    signup_payload = _create_invited_user(
        auth_client, admin_token=admin_token, email="welcomee@example.com"
    )
    assert signup_payload["user"]["email"] == "welcomee@example.com"

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT document_id, to_email, subject, status
            FROM outbound_emails
            WHERE to_email = ? AND subject = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            ("welcomee@example.com", "Welcome to CitySort AI"),
        ).fetchone()

    assert row is not None
    assert row["document_id"] == "__account__"
    assert row["status"] == "sent"
    send_mock.assert_called()


def test_assignment_triggers_notification_email(auth_client, monkeypatch):
    from app import auto_emails
    from app.db import get_connection

    monkeypatch.setattr(auto_emails, "AUTO_ASSIGNMENT_EMAIL_ENABLED", True)
    monkeypatch.setattr(auto_emails, "email_configured", lambda: True)
    send_mock = MagicMock()
    monkeypatch.setattr(auto_emails, "send_email", send_mock)

    bootstrap = _bootstrap_admin(auth_client)
    admin_token = str(bootstrap["access_token"])
    invited = _create_invited_user(
        auth_client, admin_token=admin_token, email="assignee@example.com"
    )
    assignee_id = invited["user"]["id"]

    upload_response = auth_client.post(
        "/api/documents/upload",
        headers=_auth_headers(admin_token),
        files={
            "file": (
                "assignment.txt",
                b"Building Permit\nApplicant: Assignment User\nDate: 01/01/2026",
                "text/plain",
            )
        },
        data={"source_channel": "test", "process_async": "false"},
    )
    assert upload_response.status_code == 200
    document_id = upload_response.json()["id"]

    assign_response = auth_client.post(
        f"/api/documents/{document_id}/assign",
        headers=_auth_headers(admin_token),
        json={"user_id": assignee_id, "actor": "admin@example.com"},
    )
    assert assign_response.status_code == 200
    send_mock.assert_called_once()

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT to_email, document_id, status
            FROM outbound_emails
            WHERE document_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (document_id,),
        ).fetchone()

    assert row is not None
    assert row["to_email"] == "assignee@example.com"
    assert row["status"] == "sent"


def test_assignment_opt_out_preference_suppresses_email(auth_client, monkeypatch):
    from app import auto_emails
    from app.db import get_connection

    monkeypatch.setattr(auto_emails, "AUTO_ASSIGNMENT_EMAIL_ENABLED", True)
    monkeypatch.setattr(auto_emails, "email_configured", lambda: True)
    send_mock = MagicMock()
    monkeypatch.setattr(auto_emails, "send_email", send_mock)

    bootstrap = _bootstrap_admin(auth_client)
    admin_token = str(bootstrap["access_token"])
    invited = _create_invited_user(
        auth_client, admin_token=admin_token, email="optout@example.com"
    )
    assignee_id = invited["user"]["id"]
    assignee_token = str(invited["access_token"])

    update_response = auth_client.put(
        "/api/auth/me/email-preferences",
        headers=_auth_headers(assignee_token),
        json={"doc_assigned": False},
    )
    assert update_response.status_code == 200
    assert update_response.json()["doc_assigned"] is False

    upload_response = auth_client.post(
        "/api/documents/upload",
        headers=_auth_headers(admin_token),
        files={
            "file": (
                "optout.txt",
                b"Business License\nApplicant: Opt Out\nDate: 01/01/2026",
                "text/plain",
            )
        },
        data={"source_channel": "test", "process_async": "false"},
    )
    assert upload_response.status_code == 200
    document_id = upload_response.json()["id"]

    assign_response = auth_client.post(
        f"/api/documents/{document_id}/assign",
        headers=_auth_headers(admin_token),
        json={"user_id": assignee_id, "actor": "admin@example.com"},
    )
    assert assign_response.status_code == 200
    send_mock.assert_not_called()

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id
            FROM outbound_emails
            WHERE document_id = ? AND to_email = ?
            """,
            (document_id, "optout@example.com"),
        ).fetchone()

    assert row is None
