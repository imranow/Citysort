from __future__ import annotations

import pytest
from starlette.requests import Request


@pytest.fixture()
def isolated_app(tmp_path, monkeypatch):
    from app import db
    from app import main
    from app.db import init_db

    monkeypatch.setattr(db, "DATA_DIR", tmp_path)
    monkeypatch.setattr(db, "UPLOAD_DIR", tmp_path / "uploads")
    monkeypatch.setattr(db, "PROCESSED_DIR", tmp_path / "processed")
    monkeypatch.setattr(db, "DATABASE_PATH", tmp_path / "citysort.db")
    monkeypatch.setattr(main, "OCR_PROVIDER", "local")
    monkeypatch.setattr(main, "CLASSIFIER_PROVIDER", "rules")
    init_db()
    return main


def test_platform_connectivity_check(isolated_app) -> None:
    payload = isolated_app.run_platform_connectivity_check().model_dump()
    assert payload["database"]["status"] == "ok"
    assert payload["ocr_provider"]["status"] == "ok"
    assert payload["classifier_provider"]["status"] == "ok"


def test_manual_deployment_and_history(isolated_app) -> None:
    created = isolated_app.run_manual_deployment(
        isolated_app.ManualDeploymentRequest(
            environment="staging", actor="test_user", notes="release candidate"
        )
    ).model_dump()
    assert created["environment"] == "staging"
    assert created["status"] == "completed"

    items = isolated_app.get_platform_deployments(limit=5).model_dump()["items"]
    assert items
    assert items[0]["id"] == created["id"]


def _build_request() -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/platform/invitations",
        "headers": [],
        "scheme": "http",
        "server": ("testserver", 80),
        "client": ("127.0.0.1", 1234),
        "query_string": b"",
    }
    return Request(scope)


def test_invitation_and_api_key_lifecycle(isolated_app) -> None:
    invite_payload = isolated_app.create_platform_invitation(
        isolated_app.InvitationCreateRequest(
            email="person@example.com",
            role="member",
            actor="admin",
            expires_in_days=14,
        ),
        _build_request(),
    )
    invite_payload = invite_payload.model_dump()
    assert invite_payload["invitation"]["email"] == "person@example.com"
    assert invite_payload["invite_token"]
    assert "/invite/" in invite_payload["invite_link"]

    key_payload = isolated_app.create_platform_api_key(
        isolated_app.ApiKeyCreateRequest(name="ci-key", actor="admin")
    )
    key_payload = key_payload.model_dump()
    assert key_payload["api_key"]["name"] == "ci-key"
    assert key_payload["raw_key"].startswith("cs_")

    key_id = key_payload["api_key"]["id"]

    active_items = isolated_app.get_platform_api_keys(
        include_revoked=False, limit=100
    ).model_dump()["items"]
    assert any(item["id"] == key_id for item in active_items)

    revoked = isolated_app.revoke_platform_api_key(key_id=key_id).model_dump()
    assert revoked["status"] == "revoked"

    revoked_items = isolated_app.get_platform_api_keys(
        include_revoked=True, limit=100
    ).model_dump()["items"]
    assert any(
        item["id"] == key_id and item["status"] == "revoked" for item in revoked_items
    )


def test_platform_summary_counts(isolated_app) -> None:
    isolated_app.create_platform_invitation(
        isolated_app.InvitationCreateRequest(
            email="one@example.com", role="member", actor="admin"
        ),
        _build_request(),
    )
    isolated_app.create_platform_api_key(
        isolated_app.ApiKeyCreateRequest(name="ops-key", actor="admin")
    )

    payload = isolated_app.get_platform_summary().model_dump()
    assert payload["active_api_keys"] >= 1
    assert payload["pending_invitations"] >= 1
    assert payload["connectivity"]["database"]["status"] == "ok"
