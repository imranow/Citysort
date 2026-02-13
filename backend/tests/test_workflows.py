from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def workflow_client(tmp_path, monkeypatch):
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


def _invite_and_signup_member(
    client: TestClient, *, admin_token: str, email: str
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
    invite_payload = invite_response.json()

    signup_response = client.post(
        "/api/auth/signup",
        json={
            "email": email,
            "password": "StrongPass123!",
            "full_name": "Member User",
            "invitation_token": invite_payload["invite_token"],
        },
    )
    assert signup_response.status_code == 200
    return signup_response.json()


def test_workflow_rule_crud_and_auto_assignment(workflow_client):
    bootstrap = _bootstrap_admin(workflow_client)
    token = str(bootstrap["access_token"])
    admin_id = bootstrap["user"]["id"]

    create_response = workflow_client.post(
        "/api/workflows",
        headers=_auth_headers(token),
        json={
            "name": "Auto-assign manual intake",
            "enabled": True,
            "trigger_event": "document_needs_review",
            "filters": {"doc_type": "other"},
            "actions": [{"type": "assign", "config": {"assignee": "workspace_owner"}}],
        },
    )
    assert create_response.status_code == 200
    created = create_response.json()
    assert created["name"] == "Auto-assign manual intake"
    assert created["trigger_event"] == "document_needs_review"

    list_response = workflow_client.get(
        "/api/workflows",
        headers=_auth_headers(token),
    )
    assert list_response.status_code == 200
    items = list_response.json()["items"]
    assert any(item["id"] == created["id"] for item in items)

    upload_response = workflow_client.post(
        "/api/documents/upload",
        headers=_auth_headers(token),
        files={
            "file": (
                "intake.txt",
                b"Hello there\nApplicant: John Doe\nDate: 01/02/2026",
                "text/plain",
            )
        },
        data={"source_channel": "test", "process_async": "false"},
    )
    assert upload_response.status_code == 200
    doc = upload_response.json()
    assert doc["doc_type"] == "other"
    assert doc["assigned_to"] == admin_id
    assert doc["status"] == "assigned"


def test_workflow_template_email_on_approval_records_outbound(
    workflow_client, monkeypatch
):
    from app.db import get_connection
    from app import workflows

    monkeypatch.setattr(workflows, "email_configured", lambda: True)
    send_mock = MagicMock()
    monkeypatch.setattr(workflows, "send_email", send_mock)

    bootstrap = _bootstrap_admin(workflow_client)
    token = str(bootstrap["access_token"])

    template_response = workflow_client.post(
        "/api/templates",
        headers=_auth_headers(token),
        json={
            "name": "Approval Notice",
            "doc_type": None,
            "template_body": (
                "Hello {{applicant_name}},\n\n"
                "Your submission ({{filename}}) is now {{status}}.\n"
                "Reference: {{id}}\n"
            ),
        },
    )
    assert template_response.status_code == 200
    template_id = int(template_response.json()["id"])

    workflow_response = workflow_client.post(
        "/api/workflows",
        headers=_auth_headers(token),
        json={
            "name": "Email citizen on approval",
            "enabled": True,
            "trigger_event": "document_approved",
            "filters": {"doc_type": "other"},
            "actions": [
                {"type": "send_template_email", "config": {"template_id": template_id}}
            ],
        },
    )
    assert workflow_response.status_code == 200

    upload_response = workflow_client.post(
        "/api/documents/upload",
        headers=_auth_headers(token),
        files={
            "file": (
                "emailme.txt",
                b"Hello\nApplicant: Jane Roe\nEmail: jane@example.com\nDate: 01/02/2026",
                "text/plain",
            )
        },
        data={"source_channel": "test", "process_async": "false"},
    )
    assert upload_response.status_code == 200
    document_id = upload_response.json()["id"]

    review_response = workflow_client.post(
        f"/api/documents/{document_id}/review",
        headers=_auth_headers(token),
        json={"approve": True, "notes": "ok", "actor": "admin@example.com"},
    )
    assert review_response.status_code == 200
    send_mock.assert_called()

    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT to_email, document_id, subject, status
            FROM outbound_emails
            WHERE document_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (document_id,),
        ).fetchone()

    assert row is not None
    assert row["to_email"] == "jane@example.com"
    assert row["status"] == "sent"


def test_workspace_member_cannot_create_workflow(workflow_client):
    bootstrap = _bootstrap_admin(workflow_client)
    admin_token = str(bootstrap["access_token"])

    invited = _invite_and_signup_member(
        workflow_client, admin_token=admin_token, email="member@example.com"
    )
    member_token = str(invited["access_token"])

    create_response = workflow_client.post(
        "/api/workflows",
        headers=_auth_headers(member_token),
        json={
            "name": "Not allowed",
            "enabled": True,
            "trigger_event": "document_ingested",
            "filters": {},
            "actions": [],
        },
    )
    assert create_response.status_code == 403


def test_workflow_presets_list_and_apply(workflow_client):
    bootstrap = _bootstrap_admin(workflow_client)
    token = str(bootstrap["access_token"])

    presets_response = workflow_client.get(
        "/api/workflows/presets",
        headers=_auth_headers(token),
    )
    assert presets_response.status_code == 200
    presets = presets_response.json()["items"]
    assert any(p["id"] == "gov-intake-triage" for p in presets)

    apply_response = workflow_client.post(
        "/api/workflows/presets/gov-intake-triage/apply",
        headers=_auth_headers(token),
    )
    assert apply_response.status_code == 200
    payload = apply_response.json()
    assert payload["preset_id"] == "gov-intake-triage"
    assert len(payload["created_rules"]) >= 1

    workflows_response = workflow_client.get(
        "/api/workflows",
        headers=_auth_headers(token),
    )
    assert workflows_response.status_code == 200
    names = {item["name"] for item in workflows_response.json()["items"]}
    assert "Triage: Auto-assign needs_review" in names


def test_workspace_member_cannot_apply_workflow_preset(workflow_client):
    bootstrap = _bootstrap_admin(workflow_client)
    admin_token = str(bootstrap["access_token"])

    invited = _invite_and_signup_member(
        workflow_client, admin_token=admin_token, email="member2@example.com"
    )
    member_token = str(invited["access_token"])

    response = workflow_client.post(
        "/api/workflows/presets/gov-intake-triage/apply",
        headers=_auth_headers(member_token),
    )
    assert response.status_code == 403


def test_workflow_transition_action_updates_status(workflow_client):
    bootstrap = _bootstrap_admin(workflow_client)
    token = str(bootstrap["access_token"])

    create_response = workflow_client.post(
        "/api/workflows",
        headers=_auth_headers(token),
        json={
            "name": "Auto-acknowledge routed permits",
            "enabled": True,
            "trigger_event": "document_routed",
            "filters": {"doc_type": "building_permit"},
            "actions": [{"type": "transition", "config": {"status": "acknowledged"}}],
        },
    )
    assert create_response.status_code == 200

    upload_response = workflow_client.post(
        "/api/documents/upload",
        headers=_auth_headers(token),
        files={
            "file": (
                "permit.txt",
                (
                    b"Building Permit\n"
                    b"Applicant: Jane Doe\n"
                    b"Address: 12 Main Street\n"
                    b"Parcel Number: P-1234\n"
                    b"Date: 02/12/2026\n"
                    b"Construction zoning site plan inspection parcel\n"
                ),
                "text/plain",
            )
        },
        data={"source_channel": "test", "process_async": "false"},
    )
    assert upload_response.status_code == 200
    doc = upload_response.json()
    assert doc["doc_type"] == "building_permit"
    assert doc["status"] == "acknowledged"
