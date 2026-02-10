from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

from .config import DATABASE_PATH, DATA_DIR, PROCESSED_DIR, UPLOAD_DIR


def ensure_directories() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    ensure_directories()
    connection = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def init_db() -> None:
    ensure_directories()
    with get_connection() as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS documents (
                id TEXT PRIMARY KEY,
                filename TEXT NOT NULL,
                storage_path TEXT NOT NULL,
                source_channel TEXT NOT NULL,
                content_type TEXT,
                status TEXT NOT NULL,
                doc_type TEXT,
                department TEXT,
                urgency TEXT,
                confidence REAL,
                requires_review INTEGER NOT NULL DEFAULT 0,
                extracted_text TEXT,
                extracted_fields TEXT,
                missing_fields TEXT,
                validation_errors TEXT,
                reviewer_notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id TEXT NOT NULL,
                action TEXT NOT NULL,
                actor TEXT NOT NULL,
                details TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(document_id) REFERENCES documents(id)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS deployments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                environment TEXT NOT NULL,
                provider TEXT NOT NULL DEFAULT 'local',
                status TEXT NOT NULL,
                actor TEXT NOT NULL,
                notes TEXT,
                details TEXT,
                external_id TEXT,
                created_at TEXT NOT NULL,
                finished_at TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS invitations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                role TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'pending',
                actor TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                accepted_at TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                key_prefix TEXT NOT NULL,
                key_hash TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'active',
                actor TEXT NOT NULL,
                created_at TEXT NOT NULL,
                revoked_at TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                full_name TEXT,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                last_login_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                job_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                status TEXT NOT NULL,
                result TEXT,
                error TEXT,
                actor TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_attempts INTEGER NOT NULL DEFAULT 3,
                worker_id TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_documents_status_updated
            ON documents (status, updated_at DESC)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_documents_department_updated
            ON documents (department, updated_at DESC)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_audit_events_document_id
            ON audit_events (document_id, id DESC)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_deployments_created_at
            ON deployments (created_at DESC)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_deployments_status_created
            ON deployments (status, created_at DESC)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_invitations_status_created
            ON invitations (status, created_at DESC)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_api_keys_status_created
            ON api_keys (status, created_at DESC)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_users_email
            ON users (email)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_jobs_status_created
            ON jobs (status, created_at ASC)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_jobs_type_status
            ON jobs (job_type, status)
            """
        )

        # --- New tables for admin automation features ---

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                type TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT,
                document_id TEXT,
                is_read INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                read_at TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notifications_user_unread
            ON notifications (user_id, is_read, created_at DESC)
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS watched_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                file_hash TEXT NOT NULL UNIQUE,
                source_path TEXT NOT NULL,
                document_id TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_watched_files_hash
            ON watched_files (file_hash)
            """
        )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                doc_type TEXT,
                template_body TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        # Seed default templates if table is empty.
        template_count = connection.execute("SELECT COUNT(*) AS c FROM templates").fetchone()["c"]
        if template_count == 0:
            from datetime import datetime, timezone
            _now = datetime.now(timezone.utc).isoformat()
            connection.executemany(
                "INSERT INTO templates (name, doc_type, template_body, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                [
                    (
                        "Acknowledgment Letter",
                        None,
                        "Dear {{applicant_name}},\n\nThank you for your submission. We have received your {{doc_type}} (file: {{filename}}) and it has been routed to the {{department}} department.\n\nYour reference number is {{id}}.\n\nPlease allow up to 10 business days for processing. If you have questions, reply to this message with your reference number.\n\nSincerely,\nCity Records Office",
                        _now,
                        _now,
                    ),
                    (
                        "Status Update",
                        None,
                        "Dear {{applicant_name}},\n\nThis is an update regarding your {{doc_type}} submission (ref: {{id}}).\n\nCurrent status: {{status}}\nDepartment: {{department}}\n\nIf you have questions or need to provide additional information, please contact the {{department}} department.\n\nSincerely,\nCity Records Office",
                        _now,
                        _now,
                    ),
                    (
                        "Request for Information",
                        None,
                        "Dear {{applicant_name}},\n\nWe are reviewing your {{doc_type}} submission (ref: {{id}}), but require additional information before we can proceed.\n\nPlease provide the following:\n- [Describe missing information here]\n\nYou may reply to this message or visit the {{department}} department in person.\n\nSincerely,\nCity Records Office",
                        _now,
                        _now,
                    ),
                ],
            )

        # --- Safe migrations for existing local databases ---

        deployment_columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(deployments)").fetchall()
        }
        if "provider" not in deployment_columns:
            connection.execute("ALTER TABLE deployments ADD COLUMN provider TEXT NOT NULL DEFAULT 'local'")
        if "external_id" not in deployment_columns:
            connection.execute("ALTER TABLE deployments ADD COLUMN external_id TEXT")

        # Add new columns to documents table for SLA, assignment.
        doc_columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(documents)").fetchall()
        }
        if "due_date" not in doc_columns:
            connection.execute("ALTER TABLE documents ADD COLUMN due_date TEXT")
        if "sla_days" not in doc_columns:
            connection.execute("ALTER TABLE documents ADD COLUMN sla_days INTEGER")
        if "assigned_to" not in doc_columns:
            connection.execute("ALTER TABLE documents ADD COLUMN assigned_to TEXT")

        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_documents_due_date ON documents (due_date)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_documents_assigned_to ON documents (assigned_to, status)"
        )
