"""Tests for connector framework and import orchestration with mocked external APIs."""

from __future__ import annotations

import pytest


@pytest.fixture()
def isolated_connector_env(tmp_path, monkeypatch):
    """Set up isolated DB + upload dirs for connector tests."""
    from app import db, config

    monkeypatch.setattr(db, "DATA_DIR", tmp_path)
    monkeypatch.setattr(db, "UPLOAD_DIR", tmp_path / "uploads")
    monkeypatch.setattr(db, "PROCESSED_DIR", tmp_path / "processed")
    monkeypatch.setattr(db, "DATABASE_PATH", tmp_path / "citysort.db")
    monkeypatch.setattr(config, "UPLOAD_DIR", tmp_path / "uploads")
    monkeypatch.setattr(config, "REQUIRE_AUTH", False)
    monkeypatch.setattr(config, "WORKER_ENABLED", False)
    (tmp_path / "uploads").mkdir(exist_ok=True)
    (tmp_path / "processed").mkdir(exist_ok=True)
    db.init_db()
    return {"db": db, "config": config, "tmp_path": tmp_path}


def test_connector_registry_has_all_types():
    """Verify all 7 SaaS connectors are registered."""
    # Import connector modules to trigger registration via @register_connector
    import app.connectors.servicenow  # noqa: F401
    import app.connectors.confluence  # noqa: F401
    import app.connectors.salesforce  # noqa: F401
    import app.connectors.gcs  # noqa: F401
    import app.connectors.s3  # noqa: F401
    import app.connectors.jira_connector  # noqa: F401
    import app.connectors.sharepoint  # noqa: F401

    from app.connectors.base import get_connector

    expected = [
        "servicenow",
        "confluence",
        "salesforce",
        "google_cloud_storage",
        "amazon_s3",
        "jira",
        "sharepoint",
    ]
    for connector_type in expected:
        connector = get_connector(connector_type)
        assert connector is not None, f"Connector {connector_type} not registered"


def test_connector_test_connection_requires_config():
    """Each connector's test_connection should fail gracefully with empty config."""
    import app.connectors.servicenow  # noqa: F401
    import app.connectors.confluence  # noqa: F401
    import app.connectors.jira_connector  # noqa: F401

    from app.connectors.base import get_connector

    connector_types = [
        "servicenow",
        "confluence",
        "jira",
    ]
    for ctype in connector_types:
        connector = get_connector(ctype)
        success, message = connector.test_connection({})
        assert success is False, f"{ctype} should fail with empty config"
        assert message, f"{ctype} should return error message"


def test_import_deduplication(isolated_connector_env, monkeypatch):
    """Verify that importing the same document twice skips the duplicate."""
    from app.connectors.base import ExternalDocument
    from app.connectors.importer import import_from_connector

    # Create a mock connector
    mock_docs = [
        ExternalDocument(
            external_id="ext-001",
            filename="test_doc.txt",
            content_type="text/plain",
            download_url="https://example.com/test.txt",
            size_bytes=100,
            metadata={"title": "Test Document"},
        ),
    ]

    class MockConnector:
        def test_connection(self, config):
            return True, "OK"

        def list_documents(self, config, limit=50):
            return mock_docs

        def download_document(self, config, doc):
            return doc.filename, b"Hello from mock connector", doc.content_type

    # Patch get_connector where it's used in the importer module
    from app.connectors import importer as importer_mod

    original_get = importer_mod.get_connector

    def patched_get(name):
        if name == "mock_test":
            return MockConnector()
        return original_get(name)

    monkeypatch.setattr(importer_mod, "get_connector", patched_get)

    # First import
    result1 = import_from_connector(
        connector_type="mock_test",
        config={"test": True},
        limit=10,
        process_async=False,
        actor="test",
    )
    assert result1["imported_count"] == 1
    assert result1["skipped_count"] == 0

    # Second import — should skip the duplicate
    result2 = import_from_connector(
        connector_type="mock_test",
        config={"test": True},
        limit=10,
        process_async=False,
        actor="test",
    )
    assert result2["imported_count"] == 0
    assert result2["skipped_count"] == 1


def test_import_handles_download_failure(isolated_connector_env, monkeypatch):
    """Verify that a failed download is recorded as an error, not a crash."""
    from app.connectors.base import ExternalDocument
    from app.connectors.importer import import_from_connector
    from app.connectors import importer as importer_mod

    mock_docs = [
        ExternalDocument(
            external_id="ext-fail-001",
            filename="broken.txt",
            content_type="text/plain",
            download_url="https://example.com/fail.txt",
            size_bytes=100,
            metadata={},
        ),
    ]

    class FailingConnector:
        def test_connection(self, config):
            return True, "OK"

        def list_documents(self, config, limit=50):
            return mock_docs

        def download_document(self, config, doc):
            raise RuntimeError("Network timeout")

    original_get = importer_mod.get_connector

    def patched_get(name):
        if name == "fail_test":
            return FailingConnector()
        return original_get(name)

    monkeypatch.setattr(importer_mod, "get_connector", patched_get)

    result = import_from_connector(
        connector_type="fail_test",
        config={"test": True},
        limit=10,
        process_async=False,
        actor="test",
    )
    assert result["imported_count"] == 0
    assert result["failed_count"] == 1
    assert len(result["errors"]) == 1
    assert "Network timeout" in result["errors"][0]


def test_connector_config_api(isolated_connector_env, monkeypatch):
    """Test saving and retrieving connector config via API."""
    from fastapi.testclient import TestClient

    from app import main as main_module

    monkeypatch.setattr(
        main_module,
        "UPLOAD_DIR",
        isolated_connector_env["tmp_path"] / "uploads",
    )

    from app.main import app

    client = TestClient(
        app, raise_server_exceptions=False, headers={"host": "localhost"}
    )

    # Save config
    resp = client.put(
        "/api/connectors/jira/config",
        json={"config": {"base_url": "https://test.atlassian.net", "email": "a@b.com"}},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["connector_type"] == "jira"
    assert data["config"]["base_url"] == "https://test.atlassian.net"

    # Retrieve config
    resp = client.get("/api/connectors/jira/config")
    assert resp.status_code == 200
    data = resp.json()
    assert data["config"]["base_url"] == "https://test.atlassian.net"
