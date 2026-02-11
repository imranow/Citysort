"""Tests for SQLite schema, migration safety, and PostgreSQL compatibility."""

from __future__ import annotations

import sqlite3

import pytest


@pytest.fixture()
def sqlite_db(tmp_path):
    """Create a fresh SQLite database using init_db."""
    from app import db

    # Point db module at temp dir
    db_path = tmp_path / "citysort.db"
    original_data_dir = db.DATA_DIR
    original_upload_dir = db.UPLOAD_DIR
    original_processed_dir = db.PROCESSED_DIR
    original_db_path = db.DATABASE_PATH

    db.DATA_DIR = tmp_path
    db.UPLOAD_DIR = tmp_path / "uploads"
    db.PROCESSED_DIR = tmp_path / "processed"
    db.DATABASE_PATH = db_path

    try:
        db.init_db()
        yield db_path
    finally:
        db.DATA_DIR = original_data_dir
        db.UPLOAD_DIR = original_upload_dir
        db.PROCESSED_DIR = original_processed_dir
        db.DATABASE_PATH = original_db_path


EXPECTED_TABLES = [
    "documents",
    "audit_events",
    "deployments",
    "invitations",
    "api_keys",
    "users",
    "jobs",
    "notifications",
    "watched_files",
    "templates",
    "outbound_emails",
    "connector_configs",
    "connector_sync_log",
]


def test_all_tables_created(sqlite_db):
    """Verify all 13 expected tables exist after init_db."""
    conn = sqlite3.connect(str(sqlite_db))
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = {row[0] for row in cursor.fetchall()}
    conn.close()

    for table in EXPECTED_TABLES:
        assert table in tables, f"Missing table: {table}"


def test_wal_mode_enabled(sqlite_db):
    """Verify WAL mode is enabled for concurrent access."""
    conn = sqlite3.connect(str(sqlite_db))
    result = conn.execute("PRAGMA journal_mode").fetchone()
    conn.close()
    assert result[0] == "wal"


def test_documents_table_has_all_columns(sqlite_db):
    """Verify documents table has all expected columns including migrations."""
    conn = sqlite3.connect(str(sqlite_db))
    cursor = conn.execute("PRAGMA table_info(documents)")
    columns = {row[1] for row in cursor.fetchall()}
    conn.close()

    expected_columns = {
        "id",
        "filename",
        "storage_path",
        "source_channel",
        "content_type",
        "status",
        "doc_type",
        "department",
        "urgency",
        "confidence",
        "requires_review",
        "extracted_text",
        "extracted_fields",
        "missing_fields",
        "validation_errors",
        "reviewer_notes",
        "created_at",
        "updated_at",
        "due_date",
        "sla_days",
        "assigned_to",
    }

    for col in expected_columns:
        assert col in columns, f"Missing column in documents: {col}"


def test_init_db_is_idempotent(sqlite_db):
    """Running init_db twice should not error or duplicate data."""
    from app import db

    # Run init_db again — should be safe
    db.init_db()

    conn = sqlite3.connect(str(sqlite_db))
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = {row[0] for row in cursor.fetchall()}
    conn.close()

    for table in EXPECTED_TABLES:
        assert table in tables


def test_default_templates_seeded(sqlite_db):
    """Verify default email templates are created on init."""
    conn = sqlite3.connect(str(sqlite_db))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT name FROM templates ORDER BY name").fetchall()
    conn.close()

    names = {row["name"] for row in rows}
    assert "Acknowledgment Letter" in names
    assert "Status Update" in names
    assert "Request for Information" in names


def test_migration_script_table_list():
    """Verify migration script covers all tables."""
    from scripts.migrate_sqlite_to_postgres import TABLES_IN_ORDER

    for table in EXPECTED_TABLES:
        assert table in TABLES_IN_ORDER, f"Migration script missing table: {table}"


def test_connector_configs_unique_constraint(sqlite_db):
    """Verify connector_configs has unique constraint on connector_type."""
    conn = sqlite3.connect(str(sqlite_db))
    conn.execute(
        "INSERT INTO connector_configs (connector_type, config_json, enabled, created_at, updated_at) "
        "VALUES ('jira', '{}', 1, '2026-01-01', '2026-01-01')"
    )
    conn.commit()

    # Second insert with same type should fail
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO connector_configs (connector_type, config_json, enabled, created_at, updated_at) "
            "VALUES ('jira', '{}', 1, '2026-01-01', '2026-01-01')"
        )
    conn.close()


def test_connector_sync_log_compound_unique(sqlite_db):
    """Verify connector_sync_log dedup via compound unique on (connector_type, external_id)."""
    conn = sqlite3.connect(str(sqlite_db))
    conn.execute(
        "INSERT INTO connector_sync_log (connector_type, external_id, filename, document_id, created_at) "
        "VALUES ('jira', 'ext-1', 'file.txt', 'doc-1', '2026-01-01')"
    )
    conn.commit()

    # Duplicate should fail
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO connector_sync_log (connector_type, external_id, filename, document_id, created_at) "
            "VALUES ('jira', 'ext-1', 'file2.txt', 'doc-2', '2026-01-02')"
        )
    conn.close()
