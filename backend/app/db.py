from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import unquote

from .config import (
    DATABASE_BACKEND,
    DATABASE_CONNECT_TIMEOUT_SECONDS,
    DATABASE_PATH,
    DATABASE_URL,
    DATA_DIR,
    PROCESSED_DIR,
    UPLOAD_DIR,
)


def ensure_directories() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def _sqlite_target_path() -> str:
    if not os.getenv("CITYSORT_DATABASE_URL", "").strip():
        return str(Path(DATABASE_PATH).expanduser().resolve())

    raw = DATABASE_URL.strip()
    lowered = raw.lower()
    if lowered.startswith("sqlite:///"):
        target = raw[len("sqlite:///") :]
    elif lowered.startswith("sqlite://"):
        target = raw[len("sqlite://") :]
    else:
        target = str(DATABASE_PATH)

    target = unquote(target).strip()
    if not target:
        return str(DATABASE_PATH)
    if target.startswith("file:"):
        return target
    if target == ":memory:":
        return target
    return str(Path(target).expanduser().resolve())


def _convert_placeholders(query: str) -> str:
    # App repositories use "?" placeholders everywhere. psycopg2 expects "%s".
    return query.replace("?", "%s")


@dataclass
class CursorAdapter:
    _cursor: Any
    lastrowid: Any = None

    @property
    def rowcount(self) -> int:
        return int(getattr(self._cursor, "rowcount", -1))

    def fetchone(self) -> Any:
        return self._cursor.fetchone()

    def fetchall(self) -> list[Any]:
        return self._cursor.fetchall()

    def close(self) -> None:
        try:
            self._cursor.close()
        except Exception:
            pass


class ConnectionAdapter:
    def __init__(self, raw_connection: Any, backend: str) -> None:
        self._raw = raw_connection
        self._backend = backend

    def _cursor(self) -> Any:
        if self._backend == "postgresql":
            from psycopg2.extras import RealDictCursor

            return self._raw.cursor(cursor_factory=RealDictCursor)
        return self._raw.cursor()

    def _execute_raw(self, query: str, params: Any = None) -> CursorAdapter:
        cursor = self._cursor()
        if params is None:
            cursor.execute(query)
        else:
            cursor.execute(query, tuple(params))

        lastrowid = getattr(cursor, "lastrowid", None)
        if self._backend == "postgresql" and query.lstrip().lower().startswith(
            "insert"
        ):
            # Preserve sqlite-style cursor.lastrowid semantics used throughout repository code.
            try:
                id_cursor = self._raw.cursor()
                id_cursor.execute("SELECT LASTVAL()")
                row = id_cursor.fetchone()
                if row:
                    lastrowid = row[0]
                id_cursor.close()
            except Exception:
                pass
        return CursorAdapter(cursor, lastrowid=lastrowid)

    def execute(self, query: str, params: Any = None) -> CursorAdapter:
        sql = _convert_placeholders(query) if self._backend == "postgresql" else query
        return self._execute_raw(sql, params)

    def executemany(self, query: str, seq_of_params: Any) -> CursorAdapter:
        cursor = self._cursor()
        sql = _convert_placeholders(query) if self._backend == "postgresql" else query
        cursor.executemany(sql, list(seq_of_params))
        return CursorAdapter(cursor, lastrowid=getattr(cursor, "lastrowid", None))

    def commit(self) -> None:
        self._raw.commit()

    def rollback(self) -> None:
        self._raw.rollback()

    def close(self) -> None:
        self._raw.close()


@contextmanager
def get_connection() -> Iterator[ConnectionAdapter]:
    ensure_directories()
    if DATABASE_BACKEND == "postgresql":
        try:
            import psycopg2
        except Exception as exc:  # pragma: no cover - runtime safeguard
            raise RuntimeError(
                "CITYSORT_DATABASE_URL targets PostgreSQL but psycopg2 is unavailable."
            ) from exc
        raw = psycopg2.connect(
            DATABASE_URL, connect_timeout=DATABASE_CONNECT_TIMEOUT_SECONDS
        )
        raw.autocommit = False
        connection = ConnectionAdapter(raw, backend="postgresql")
    else:
        raw = sqlite3.connect(_sqlite_target_path(), check_same_thread=False)
        raw.row_factory = sqlite3.Row
        raw.execute("PRAGMA journal_mode=WAL")
        raw.execute("PRAGMA busy_timeout=5000")
        connection = ConnectionAdapter(raw, backend="sqlite")

    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _table_columns(connection: ConnectionAdapter, table_name: str) -> set[str]:
    if DATABASE_BACKEND == "postgresql":
        rows = connection.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ?
            """,
            (table_name,),
        ).fetchall()
        return {str(row["column_name"]) for row in rows}
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


def _create_tables(connection: ConnectionAdapter) -> None:
    if DATABASE_BACKEND == "sqlite":
        auto_id = "INTEGER PRIMARY KEY AUTOINCREMENT"
        boolean_default = "INTEGER NOT NULL DEFAULT 0"
    else:
        auto_id = "BIGSERIAL PRIMARY KEY"
        boolean_default = "INTEGER NOT NULL DEFAULT 0"

    if DATABASE_BACKEND == "sqlite":
        connection.execute("PRAGMA foreign_keys = ON")

    connection.execute(
        f"""
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
            requires_review {boolean_default},
            extracted_text TEXT,
            extracted_fields TEXT,
            missing_fields TEXT,
            validation_errors TEXT,
            reviewer_notes TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            due_date TEXT,
            sla_days INTEGER,
            assigned_to TEXT
        )
        """
    )

    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS audit_events (
            id {auto_id},
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
        f"""
        CREATE TABLE IF NOT EXISTS deployments (
            id {auto_id},
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
        f"""
        CREATE TABLE IF NOT EXISTS invitations (
            id {auto_id},
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
        f"""
        CREATE TABLE IF NOT EXISTS api_keys (
            id {auto_id},
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
        f"""
        CREATE TABLE IF NOT EXISTS notifications (
            id {auto_id},
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
        f"""
        CREATE TABLE IF NOT EXISTS watched_files (
            id {auto_id},
            filename TEXT NOT NULL,
            file_hash TEXT NOT NULL UNIQUE,
            source_path TEXT NOT NULL,
            document_id TEXT,
            created_at TEXT NOT NULL
        )
        """
    )

    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS templates (
            id {auto_id},
            name TEXT NOT NULL,
            doc_type TEXT,
            template_body TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS outbound_emails (
            id {auto_id},
            document_id TEXT NOT NULL,
            to_email TEXT NOT NULL,
            subject TEXT NOT NULL,
            body TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            provider TEXT NOT NULL DEFAULT 'smtp',
            error TEXT,
            created_at TEXT NOT NULL,
            sent_at TEXT
        )
        """
    )

    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS connector_configs (
            id {auto_id},
            connector_type TEXT NOT NULL UNIQUE,
            config_json TEXT NOT NULL DEFAULT '{{}}',
            enabled INTEGER NOT NULL DEFAULT 1,
            last_sync_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )

    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS connector_sync_log (
            id {auto_id},
            connector_type TEXT NOT NULL,
            external_id TEXT NOT NULL,
            filename TEXT,
            document_id TEXT,
            metadata_json TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(connector_type, external_id)
        )
        """
    )

    # Shared indexes.
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_documents_status_updated ON documents (status, updated_at DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_documents_department_updated ON documents (department, updated_at DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_documents_due_date ON documents (due_date)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_documents_assigned_to ON documents (assigned_to, status)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_audit_events_document_id ON audit_events (document_id, id DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_deployments_created_at ON deployments (created_at DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_deployments_status_created ON deployments (status, created_at DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_invitations_status_created ON invitations (status, created_at DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_api_keys_status_created ON api_keys (status, created_at DESC)"
    )
    connection.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)")
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs (status, created_at ASC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_jobs_type_status ON jobs (job_type, status)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_notifications_user_unread ON notifications (user_id, is_read, created_at DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_watched_files_hash ON watched_files (file_hash)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_outbound_emails_document_created ON outbound_emails (document_id, created_at DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_connector_configs_type ON connector_configs (connector_type)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_connector_sync_type_ext ON connector_sync_log (connector_type, external_id)"
    )

    # Idempotent seed templates.
    template_count_row = connection.execute(
        "SELECT COUNT(*) AS c FROM templates"
    ).fetchone()
    template_count = int(template_count_row["c"]) if template_count_row else 0
    if template_count == 0:
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()
        connection.executemany(
            "INSERT INTO templates (name, doc_type, template_body, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            [
                (
                    "Acknowledgment Letter",
                    None,
                    "Dear {{applicant_name}},\n\nThank you for your submission. We have received your {{doc_type}} (file: {{filename}}) and it has been routed to the {{department}} department.\n\nYour reference number is {{id}}.\n\nPlease allow up to 10 business days for processing. If you have questions, reply to this message with your reference number.\n\nSincerely,\nCity Records Office",
                    now,
                    now,
                ),
                (
                    "Status Update",
                    None,
                    "Dear {{applicant_name}},\n\nThis is an update regarding your {{doc_type}} submission (ref: {{id}}).\n\nCurrent status: {{status}}\nDepartment: {{department}}\n\nIf you have questions or need to provide additional information, please contact the {{department}} department.\n\nSincerely,\nCity Records Office",
                    now,
                    now,
                ),
                (
                    "Request for Information",
                    None,
                    "Dear {{applicant_name}},\n\nWe are reviewing your {{doc_type}} submission (ref: {{id}}), but require additional information before we can proceed.\n\nPlease provide the following:\n- [Describe missing information here]\n\nYou may reply to this message or visit the {{department}} department in person.\n\nSincerely,\nCity Records Office",
                    now,
                    now,
                ),
            ],
        )


def _run_safe_migrations(connection: ConnectionAdapter) -> None:
    deployment_columns = _table_columns(connection, "deployments")
    if "provider" not in deployment_columns:
        connection.execute(
            "ALTER TABLE deployments ADD COLUMN provider TEXT NOT NULL DEFAULT 'local'"
        )
    if "external_id" not in deployment_columns:
        connection.execute("ALTER TABLE deployments ADD COLUMN external_id TEXT")

    document_columns = _table_columns(connection, "documents")
    if "due_date" not in document_columns:
        connection.execute("ALTER TABLE documents ADD COLUMN due_date TEXT")
    if "sla_days" not in document_columns:
        connection.execute("ALTER TABLE documents ADD COLUMN sla_days INTEGER")
    if "assigned_to" not in document_columns:
        connection.execute("ALTER TABLE documents ADD COLUMN assigned_to TEXT")


def init_db() -> None:
    ensure_directories()
    with get_connection() as connection:
        _create_tables(connection)
        _run_safe_migrations(connection)
