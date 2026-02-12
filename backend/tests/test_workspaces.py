from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def workspace_client(tmp_path, monkeypatch):
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


def _bootstrap_admin(client: TestClient) -> dict[str, Any]:
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


def _create_workspace(client: TestClient, token: str, name: str) -> dict[str, Any]:
    response = client.post(
        "/api/workspaces",
        headers=_auth_headers(token),
        json={"name": name},
    )
    assert response.status_code == 200
    return response.json()


def _switch_workspace(client: TestClient, token: str, workspace_id: str) -> str:
    response = client.post(
        f"/api/workspaces/switch/{workspace_id}",
        headers=_auth_headers(token),
    )
    assert response.status_code == 200
    payload = response.json()
    return str(payload["access_token"])


def _upload_document(
    client: TestClient, token: str, filename: str, body: bytes
) -> dict[str, Any]:
    response = client.post(
        "/api/documents/upload",
        headers=_auth_headers(token),
        files={"file": (filename, body, "text/plain")},
        data={"source_channel": "test", "process_async": "false"},
    )
    assert response.status_code == 200
    return response.json()


def _invite_and_signup(
    client: TestClient,
    *,
    inviter_token: str,
    email: str,
    role: str = "member",
    workspace_id: str | None = None,
) -> dict[str, Any]:
    if workspace_id:
        invite_response = client.post(
            f"/api/workspaces/{workspace_id}/members",
            headers=_auth_headers(inviter_token),
            json={"email": email, "role": role},
        )
    else:
        invite_response = client.post(
            "/api/platform/invitations",
            headers=_auth_headers(inviter_token),
            json={
                "email": email,
                "role": role,
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
            "full_name": "Invited User",
            "invitation_token": invite_token,
        },
    )
    assert signup_response.status_code == 200
    return signup_response.json()


def test_bootstrap_creates_personal_workspace_and_token_wid(workspace_client):
    from app.auth import decode_access_token

    payload = _bootstrap_admin(workspace_client)
    workspace_id = payload["user"].get("workspace_id")
    assert workspace_id

    token_payload = decode_access_token(str(payload["access_token"]))
    assert token_payload.get("wid") == workspace_id

    list_response = workspace_client.get(
        "/api/workspaces",
        headers=_auth_headers(str(payload["access_token"])),
    )
    assert list_response.status_code == 200
    workspace_ids = {item["id"] for item in list_response.json()["items"]}
    assert workspace_id in workspace_ids


def test_workspace_crud_and_switch(workspace_client):
    from app.auth import decode_access_token

    bootstrap = _bootstrap_admin(workspace_client)
    token = str(bootstrap["access_token"])

    created = _create_workspace(workspace_client, token, "Planning Team")
    workspace_id = str(created["id"])
    assert created["name"] == "Planning Team"

    list_response = workspace_client.get(
        "/api/workspaces", headers=_auth_headers(token)
    )
    assert list_response.status_code == 200
    assert any(item["id"] == workspace_id for item in list_response.json()["items"])

    detail_response = workspace_client.get(
        f"/api/workspaces/{workspace_id}",
        headers=_auth_headers(token),
    )
    assert detail_response.status_code == 200
    assert detail_response.json()["id"] == workspace_id

    switched_token = _switch_workspace(workspace_client, token, workspace_id)
    switched_payload = decode_access_token(switched_token)
    assert switched_payload.get("wid") == workspace_id


def test_workspace_document_isolation_between_workspaces(workspace_client):
    bootstrap = _bootstrap_admin(workspace_client)
    ws1_token = str(bootstrap["access_token"])

    ws2 = _create_workspace(workspace_client, ws1_token, "Second Workspace")
    ws2_token = _switch_workspace(workspace_client, ws1_token, str(ws2["id"]))

    doc_ws1 = _upload_document(
        workspace_client,
        ws1_token,
        "ws1.txt",
        b"Building Permit\nApplicant: WS1\nDate: 01/01/2026",
    )
    doc_ws2 = _upload_document(
        workspace_client,
        ws2_token,
        "ws2.txt",
        b"Building Permit\nApplicant: WS2\nDate: 01/02/2026",
    )

    list_ws1 = workspace_client.get("/api/documents", headers=_auth_headers(ws1_token))
    list_ws2 = workspace_client.get("/api/documents", headers=_auth_headers(ws2_token))
    assert list_ws1.status_code == 200
    assert list_ws2.status_code == 200

    ws1_ids = {item["id"] for item in list_ws1.json()["items"]}
    ws2_ids = {item["id"] for item in list_ws2.json()["items"]}
    assert doc_ws1["id"] in ws1_ids
    assert doc_ws1["id"] not in ws2_ids
    assert doc_ws2["id"] in ws2_ids
    assert doc_ws2["id"] not in ws1_ids

    hidden_doc = workspace_client.get(
        f"/api/documents/{doc_ws2['id']}",
        headers=_auth_headers(ws1_token),
    )
    assert hidden_doc.status_code == 404


def test_workspace_scoped_analytics(workspace_client):
    bootstrap = _bootstrap_admin(workspace_client)
    ws1_token = str(bootstrap["access_token"])
    ws2 = _create_workspace(workspace_client, ws1_token, "Analytics Two")
    ws2_token = _switch_workspace(workspace_client, ws1_token, str(ws2["id"]))

    _upload_document(
        workspace_client,
        ws1_token,
        "a1.txt",
        b"Building Permit\nApplicant: A1\nDate: 01/01/2026",
    )
    _upload_document(
        workspace_client,
        ws2_token,
        "a2.txt",
        b"Building Permit\nApplicant: A2\nDate: 01/01/2026",
    )
    _upload_document(
        workspace_client,
        ws2_token,
        "a3.txt",
        b"Building Permit\nApplicant: A3\nDate: 01/01/2026",
    )

    analytics_ws1 = workspace_client.get(
        "/api/analytics", headers=_auth_headers(ws1_token)
    )
    analytics_ws2 = workspace_client.get(
        "/api/analytics", headers=_auth_headers(ws2_token)
    )
    assert analytics_ws1.status_code == 200
    assert analytics_ws2.status_code == 200

    assert int(analytics_ws1.json()["total_documents"]) == 1
    assert int(analytics_ws2.json()["total_documents"]) == 2


def test_workspace_member_invite_signup_and_listing(workspace_client):
    bootstrap = _bootstrap_admin(workspace_client)
    admin_token = str(bootstrap["access_token"])

    workspace = _create_workspace(workspace_client, admin_token, "Member Test")
    workspace_id = str(workspace["id"])

    invited = _invite_and_signup(
        workspace_client,
        inviter_token=admin_token,
        email="member1@example.com",
        role="member",
        workspace_id=workspace_id,
    )
    invited_token = str(invited["access_token"])

    members_response = workspace_client.get(
        f"/api/workspaces/{workspace_id}/members",
        headers=_auth_headers(admin_token),
    )
    assert members_response.status_code == 200
    member_ids = {item["user_id"] for item in members_response.json()["items"]}
    assert invited["user"]["id"] in member_ids

    invited_workspace_access = workspace_client.get(
        f"/api/workspaces/{workspace_id}",
        headers=_auth_headers(invited_token),
    )
    assert invited_workspace_access.status_code == 200


def test_non_member_cannot_access_workspace(workspace_client):
    bootstrap = _bootstrap_admin(workspace_client)
    admin_token = str(bootstrap["access_token"])

    hidden_workspace = _create_workspace(workspace_client, admin_token, "Private Team")
    hidden_workspace_id = str(hidden_workspace["id"])

    outsider = _invite_and_signup(
        workspace_client,
        inviter_token=admin_token,
        email="outsider@example.com",
        role="member",
    )
    outsider_token = str(outsider["access_token"])

    response = workspace_client.get(
        f"/api/workspaces/{hidden_workspace_id}",
        headers=_auth_headers(outsider_token),
    )
    assert response.status_code == 403


def test_workspace_admin_permissions_for_updates(workspace_client):
    bootstrap = _bootstrap_admin(workspace_client)
    admin_token = str(bootstrap["access_token"])
    workspace = _create_workspace(workspace_client, admin_token, "Permissions Team")
    workspace_id = str(workspace["id"])

    member = _invite_and_signup(
        workspace_client,
        inviter_token=admin_token,
        email="member2@example.com",
        role="member",
        workspace_id=workspace_id,
    )
    member_token = str(member["access_token"])

    update_response = workspace_client.patch(
        f"/api/workspaces/{workspace_id}",
        headers=_auth_headers(member_token),
        json={"name": "Attempted Rename"},
    )
    assert update_response.status_code == 403

    invite_response = workspace_client.post(
        f"/api/workspaces/{workspace_id}/members",
        headers=_auth_headers(member_token),
        json={"email": "another@example.com", "role": "member"},
    )
    assert invite_response.status_code == 403


def test_workspace_member_role_update_and_remove(workspace_client):
    bootstrap = _bootstrap_admin(workspace_client)
    admin_token = str(bootstrap["access_token"])
    workspace = _create_workspace(workspace_client, admin_token, "Role Team")
    workspace_id = str(workspace["id"])

    member = _invite_and_signup(
        workspace_client,
        inviter_token=admin_token,
        email="roleuser@example.com",
        role="member",
        workspace_id=workspace_id,
    )
    member_id = str(member["user"]["id"])

    role_update = workspace_client.patch(
        f"/api/workspaces/{workspace_id}/members/{member_id}",
        headers=_auth_headers(admin_token),
        json={"role": "operator"},
    )
    assert role_update.status_code == 200
    assert role_update.json()["role"] == "operator"

    remove_response = workspace_client.delete(
        f"/api/workspaces/{workspace_id}/members/{member_id}",
        headers=_auth_headers(admin_token),
    )
    assert remove_response.status_code == 200
    assert remove_response.json()["removed"] is True

    members_response = workspace_client.get(
        f"/api/workspaces/{workspace_id}/members",
        headers=_auth_headers(admin_token),
    )
    assert members_response.status_code == 200
    member_ids = {item["user_id"] for item in members_response.json()["items"]}
    assert member_id not in member_ids


def test_billing_subscription_is_workspace_scoped(workspace_client):
    from app import repository

    bootstrap = _bootstrap_admin(workspace_client)
    ws1_token = str(bootstrap["access_token"])
    admin_id = str(bootstrap["user"]["id"])
    ws1_id = str(bootstrap["user"]["workspace_id"])

    ws2 = _create_workspace(workspace_client, ws1_token, "Billing Team")
    ws2_id = str(ws2["id"])
    ws2_token = _switch_workspace(workspace_client, ws1_token, ws2_id)

    repository.create_subscription(
        user_id=admin_id,
        workspace_id=ws2_id,
        plan_tier="pro",
        billing_type="monthly",
        stripe_subscription_id="sub_ws_billing",
        stripe_customer_id="cus_ws_billing",
        status="active",
    )

    ws1_sub = workspace_client.get(
        "/api/billing/subscription",
        headers=_auth_headers(ws1_token),
    )
    ws2_sub = workspace_client.get(
        "/api/billing/subscription",
        headers=_auth_headers(ws2_token),
    )
    assert ws1_sub.status_code == 200
    assert ws2_sub.status_code == 200
    assert ws1_sub.json()["plan_tier"] == "free"
    assert ws2_sub.json()["plan_tier"] == "pro"
    assert ws1_id != ws2_id


def test_enforce_plan_limits_uses_workspace_plan(isolated_repo, monkeypatch):
    from app import stripe_billing

    user = isolated_repo.create_user(
        email="limits@example.com",
        full_name="Limits User",
        password_hash="hash",
        role="admin",
        plan_tier="free",
    )
    ws_a = isolated_repo.create_workspace(
        name="Limits A", owner_id=user["id"], plan_tier="free"
    )
    ws_b = isolated_repo.create_workspace(
        name="Limits B", owner_id=user["id"], plan_tier="pro"
    )

    monkeypatch.setattr(stripe_billing, "PLAN_FREE_DOCUMENT_LIMIT", 2)
    monkeypatch.setattr(stripe_billing, "PLAN_PRO_DOCUMENT_LIMIT", 10)

    for idx in range(2):
        isolated_repo.create_document(
            document={
                "id": f"limit-doc-{idx}",
                "workspace_id": ws_a["id"],
                "filename": f"limit-{idx}.txt",
                "storage_path": f"/tmp/limit-{idx}.txt",
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

    from fastapi import HTTPException

    with pytest.raises(HTTPException):
        stripe_billing.enforce_plan_limits(
            user["id"],
            "upload_document",
            workspace_id=ws_a["id"],
        )

    stripe_billing.enforce_plan_limits(
        user["id"],
        "upload_document",
        workspace_id=ws_b["id"],
    )


def test_workspace_bootstrap_backfills_existing_records(isolated_db, isolated_repo):
    from app import db

    user = isolated_repo.create_user(
        email="bootstrap@example.com",
        full_name="Bootstrap User",
        password_hash="hash",
        role="admin",
    )
    isolated_repo.create_document(
        document={
            "id": "legacy-doc",
            "workspace_id": None,
            "filename": "legacy.txt",
            "storage_path": "/tmp/legacy.txt",
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

    with db.get_connection() as connection:
        connection.execute("DELETE FROM workspace_members")
        connection.execute("DELETE FROM workspaces")

    db.init_db()

    workspaces = isolated_repo.list_user_workspaces(user["id"])
    assert workspaces
    document = isolated_repo.get_document("legacy-doc")
    assert document
    assert document.get("workspace_id")
