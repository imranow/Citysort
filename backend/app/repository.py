from __future__ import annotations

import hashlib
import json
import secrets
from datetime import datetime, timezone
from datetime import timedelta
from typing import Any, Optional
from uuid import uuid4

from .db import get_connection

JSON_OBJECT_FIELDS = {"extracted_fields"}
JSON_LIST_FIELDS = {"missing_fields", "validation_errors"}


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _serialize_value(key: str, value: Any) -> Any:
    if value is None:
        return None
    if key in JSON_OBJECT_FIELDS or key in JSON_LIST_FIELDS:
        return json.dumps(value)
    if key == "requires_review":
        return int(bool(value))
    return value


def _deserialize_row(row: Any) -> dict[str, Any]:
    record = dict(row)
    for key in JSON_OBJECT_FIELDS:
        raw = record.get(key)
        record[key] = json.loads(raw) if raw else {}
    for key in JSON_LIST_FIELDS:
        raw = record.get(key)
        record[key] = json.loads(raw) if raw else []
    record["requires_review"] = bool(record.get("requires_review", 0))
    return record


def create_document(*, document: dict[str, Any]) -> dict[str, Any]:
    now = utcnow_iso()
    payload = {
        "id": document["id"],
        "filename": document["filename"],
        "storage_path": document["storage_path"],
        "source_channel": document.get("source_channel", "upload_portal"),
        "content_type": document.get("content_type"),
        "status": document.get("status", "ingested"),
        "doc_type": document.get("doc_type"),
        "department": document.get("department"),
        "urgency": document.get("urgency", "normal"),
        "confidence": document.get("confidence", 0.0),
        "requires_review": document.get("requires_review", False),
        "extracted_text": document.get("extracted_text"),
        "extracted_fields": document.get("extracted_fields", {}),
        "missing_fields": document.get("missing_fields", []),
        "validation_errors": document.get("validation_errors", []),
        "reviewer_notes": document.get("reviewer_notes"),
        "created_at": now,
        "updated_at": now,
    }

    columns = list(payload.keys())
    serialized_values = [_serialize_value(column, payload[column]) for column in columns]
    placeholders = ", ".join("?" for _ in columns)

    with get_connection() as connection:
        connection.execute(
            f"INSERT INTO documents ({', '.join(columns)}) VALUES ({placeholders})",
            serialized_values,
        )
        row = connection.execute("SELECT * FROM documents WHERE id = ?", (payload["id"],)).fetchone()

    return _deserialize_row(row)


def get_document(document_id: str) -> Optional[dict[str, Any]]:
    with get_connection() as connection:
        row = connection.execute("SELECT * FROM documents WHERE id = ?", (document_id,)).fetchone()

    return _deserialize_row(row) if row else None


def list_documents(
    *,
    status: Optional[str] = None,
    department: Optional[str] = None,
    assigned_to: Optional[str] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = "SELECT * FROM documents"
    conditions: list[str] = []
    params: list[Any] = []

    if status:
        if status == "overdue":
            conditions.append("due_date IS NOT NULL AND due_date < ?")
            conditions.append("status NOT IN ('approved', 'corrected', 'completed', 'archived')")
            params.append(utcnow_iso())
        else:
            conditions.append("status = ?")
            params.append(status)

    if department:
        conditions.append("department = ?")
        params.append(department)

    if assigned_to:
        conditions.append("assigned_to = ?")
        params.append(assigned_to)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY updated_at DESC LIMIT ?"
    params.append(limit)

    with get_connection() as connection:
        rows = connection.execute(query, params).fetchall()

    return [_deserialize_row(row) for row in rows]


def update_document(document_id: str, *, updates: dict[str, Any]) -> Optional[dict[str, Any]]:
    if not updates:
        return get_document(document_id)

    payload = dict(updates)
    payload["updated_at"] = utcnow_iso()

    assignments = ", ".join(f"{key} = ?" for key in payload)
    values = [_serialize_value(key, value) for key, value in payload.items()]
    values.append(document_id)

    with get_connection() as connection:
        connection.execute(f"UPDATE documents SET {assignments} WHERE id = ?", values)
        row = connection.execute("SELECT * FROM documents WHERE id = ?", (document_id,)).fetchone()

    return _deserialize_row(row) if row else None


def create_audit_event(*, document_id: str, action: str, actor: str, details: Optional[str] = None) -> None:
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO audit_events (document_id, action, actor, details, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (document_id, action, actor, details, utcnow_iso()),
        )


def list_audit_events(document_id: str, *, limit: int = 50) -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT id, document_id, action, actor, details, created_at
            FROM audit_events
            WHERE document_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (document_id, limit),
        ).fetchall()

    return [dict(row) for row in rows]


def get_queue_snapshot() -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT
                COALESCE(department, 'Unassigned') AS department,
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'needs_review' THEN 1 ELSE 0 END) AS needs_review,
                SUM(CASE WHEN status IN ('routed', 'approved', 'corrected', 'acknowledged', 'assigned', 'in_progress', 'completed') THEN 1 ELSE 0 END) AS ready
            FROM documents
            GROUP BY COALESCE(department, 'Unassigned')
            ORDER BY total DESC, department ASC
            """
        ).fetchall()

    return [dict(row) for row in rows]


def get_analytics_snapshot() -> dict[str, Any]:
    analytics: dict[str, Any] = {
        "total_documents": 0,
        "needs_review": 0,
        "routed_or_approved": 0,
        "average_confidence": 0.0,
        "automated_documents": 0,
        "manual_documents": 0,
        "automation_rate": 0.0,
        "manual_rate": 0.0,
        "manual_unassigned": 0,
        "missing_contact_email": 0,
        "by_type": [],
        "by_status": [],
    }

    with get_connection() as connection:
        totals = connection.execute(
            """
            SELECT
                COUNT(*) AS total_documents,
                SUM(CASE WHEN requires_review = 1 THEN 1 ELSE 0 END) AS needs_review,
                SUM(CASE WHEN status IN ('routed', 'approved', 'corrected') THEN 1 ELSE 0 END) AS routed_or_approved,
                SUM(CASE WHEN status IN ('routed', 'approved', 'corrected', 'completed', 'archived') THEN 1 ELSE 0 END) AS automated_documents,
                AVG(COALESCE(confidence, 0)) AS average_confidence
            FROM documents
            """
        ).fetchone()

        if totals:
            analytics.update(
                {
                    "total_documents": totals["total_documents"] or 0,
                    "needs_review": totals["needs_review"] or 0,
                    "routed_or_approved": totals["routed_or_approved"] or 0,
                    "automated_documents": totals["automated_documents"] or 0,
                    "average_confidence": round(float(totals["average_confidence"] or 0.0), 4),
                }
            )

        manual_unassigned_row = connection.execute(
            """
            SELECT COUNT(*) AS total
            FROM documents
            WHERE status IN ('ingested', 'needs_review', 'acknowledged', 'assigned', 'in_progress', 'failed')
              AND (assigned_to IS NULL OR TRIM(assigned_to) = '')
            """
        ).fetchone()

        by_type_rows = connection.execute(
            """
            SELECT COALESCE(doc_type, 'unclassified') AS label, COUNT(*) AS count
            FROM documents
            GROUP BY COALESCE(doc_type, 'unclassified')
            ORDER BY count DESC, label ASC
            """
        ).fetchall()

        by_status_rows = connection.execute(
            """
            SELECT status AS label, COUNT(*) AS count
            FROM documents
            GROUP BY status
            ORDER BY count DESC, label ASC
            """
        ).fetchall()

        overdue_row = connection.execute(
            """
            SELECT COUNT(*) AS total FROM documents
            WHERE due_date IS NOT NULL AND due_date < ?
            AND status NOT IN ('approved', 'corrected', 'completed', 'archived')
            """,
            (utcnow_iso(),),
        ).fetchone()

        missing_contact_total = 0
        contact_rows = connection.execute(
            """
            SELECT extracted_fields
            FROM documents
            WHERE status IN ('ingested', 'needs_review', 'acknowledged', 'assigned', 'in_progress', 'failed')
            """
        ).fetchall()
        for row in contact_rows:
            raw = row["extracted_fields"] if isinstance(row, dict) or hasattr(row, "__getitem__") else None
            fields: dict[str, Any]
            try:
                fields = json.loads(raw) if raw else {}
            except Exception:
                fields = {}
            contact = (
                str(fields.get("applicant_email", "")).strip()
                or str(fields.get("contact_email", "")).strip()
                or str(fields.get("sender_email", "")).strip()
                or str(fields.get("email", "")).strip()
            )
            if not contact:
                missing_contact_total += 1
        analytics["missing_contact_email"] = missing_contact_total

        # Emails sent today.
        try:
            today_start = utcnow_iso()[:10] + "T00:00:00"
            emails_today_row = connection.execute(
                "SELECT COUNT(*) AS total FROM outbound_emails WHERE status = 'sent' AND sent_at >= ?",
                (today_start,),
            ).fetchone()
            analytics["emails_sent_today"] = int(emails_today_row["total"]) if emails_today_row else 0
        except Exception:
            analytics["emails_sent_today"] = 0

    analytics["by_type"] = [dict(row) for row in by_type_rows]
    analytics["by_status"] = [dict(row) for row in by_status_rows]
    analytics["overdue"] = int(overdue_row["total"]) if overdue_row else 0
    analytics["manual_unassigned"] = int(manual_unassigned_row["total"]) if manual_unassigned_row else 0
    total_documents = int(analytics["total_documents"] or 0)
    automated_documents = int(analytics["automated_documents"] or 0)
    manual_documents = max(total_documents - automated_documents, 0)
    analytics["manual_documents"] = manual_documents
    analytics["automation_rate"] = round((automated_documents / total_documents), 4) if total_documents else 0.0
    analytics["manual_rate"] = round((manual_documents / total_documents), 4) if total_documents else 0.0

    return analytics


def get_latest_deployment() -> Optional[dict[str, Any]]:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id, environment, provider, status, actor, notes, details, external_id, created_at, finished_at
            FROM deployments
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

    return dict(row) if row else None


def list_deployments(*, limit: int = 20) -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT id, environment, provider, status, actor, notes, details, external_id, created_at, finished_at
            FROM deployments
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [dict(row) for row in rows]


def create_deployment(
    *,
    environment: str,
    actor: str,
    provider: str = "local",
    notes: Optional[str] = None,
    status: str = "completed",
    details: Optional[str] = None,
    external_id: Optional[str] = None,
) -> dict[str, Any]:
    created_at = utcnow_iso()
    finished_at = created_at if status in {"completed", "failed"} else None

    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO deployments (environment, provider, status, actor, notes, details, external_id, created_at, finished_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (environment, provider, status, actor, notes, details, external_id, created_at, finished_at),
        )
        row = connection.execute(
            """
            SELECT id, environment, provider, status, actor, notes, details, external_id, created_at, finished_at
            FROM deployments
            WHERE id = ?
            """,
            (cursor.lastrowid,),
        ).fetchone()

    return dict(row)


def update_deployment(
    deployment_id: int,
    *,
    status: Optional[str] = None,
    details: Optional[str] = None,
    external_id: Optional[str] = None,
    finished: bool = False,
) -> Optional[dict[str, Any]]:
    updates: dict[str, Any] = {}
    if status is not None:
        updates["status"] = status
    if details is not None:
        updates["details"] = details
    if external_id is not None:
        updates["external_id"] = external_id
    if finished:
        updates["finished_at"] = utcnow_iso()

    if not updates:
        with get_connection() as connection:
            row = connection.execute(
                """
                SELECT id, environment, provider, status, actor, notes, details, external_id, created_at, finished_at
                FROM deployments
                WHERE id = ?
                """,
                (deployment_id,),
            ).fetchone()
        return dict(row) if row else None

    assignments = ", ".join(f"{key} = ?" for key in updates)
    values = list(updates.values())
    values.append(deployment_id)
    with get_connection() as connection:
        connection.execute(f"UPDATE deployments SET {assignments} WHERE id = ?", values)
        row = connection.execute(
            """
            SELECT id, environment, provider, status, actor, notes, details, external_id, created_at, finished_at
            FROM deployments
            WHERE id = ?
            """,
            (deployment_id,),
        ).fetchone()

    return dict(row) if row else None


def _hash_secret(secret_value: str) -> str:
    return hashlib.sha256(secret_value.encode("utf-8")).hexdigest()


def create_api_key(*, name: str, actor: str) -> tuple[dict[str, Any], str]:
    random_part = secrets.token_urlsafe(26)
    plain_key = f"cs_{random_part}"
    key_prefix = plain_key[:10]
    key_hash = _hash_secret(plain_key)
    created_at = utcnow_iso()

    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO api_keys (name, key_prefix, key_hash, status, actor, created_at, revoked_at)
            VALUES (?, ?, ?, 'active', ?, ?, NULL)
            """,
            (name, key_prefix, key_hash, actor, created_at),
        )
        row = connection.execute(
            """
            SELECT id, name, key_prefix, status, actor, created_at, revoked_at
            FROM api_keys
            WHERE id = ?
            """,
            (cursor.lastrowid,),
        ).fetchone()

    return dict(row), plain_key


def list_api_keys(*, include_revoked: bool = False, limit: int = 100) -> list[dict[str, Any]]:
    query = """
        SELECT id, name, key_prefix, status, actor, created_at, revoked_at
        FROM api_keys
    """
    params: list[Any] = []
    if not include_revoked:
        query += " WHERE status = 'active'"

    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    with get_connection() as connection:
        rows = connection.execute(query, params).fetchall()

    return [dict(row) for row in rows]


def count_api_keys(*, status: Optional[str] = None) -> int:
    query = "SELECT COUNT(*) AS total FROM api_keys"
    params: list[Any] = []
    if status:
        query += " WHERE status = ?"
        params.append(status)

    with get_connection() as connection:
        row = connection.execute(query, params).fetchone()

    return int(row["total"]) if row else 0


def revoke_api_key(*, key_id: int) -> Optional[dict[str, Any]]:
    revoked_at = utcnow_iso()
    with get_connection() as connection:
        connection.execute(
            """
            UPDATE api_keys
            SET status = 'revoked', revoked_at = ?
            WHERE id = ? AND status = 'active'
            """,
            (revoked_at, key_id),
        )
        row = connection.execute(
            """
            SELECT id, name, key_prefix, status, actor, created_at, revoked_at
            FROM api_keys
            WHERE id = ?
            """,
            (key_id,),
        ).fetchone()

    return dict(row) if row else None


def create_invitation(
    *,
    email: str,
    role: str,
    actor: str,
    expires_in_days: int = 7,
) -> tuple[dict[str, Any], str]:
    token = secrets.token_urlsafe(24)
    token_hash = _hash_secret(token)
    created_at = utcnow_iso()
    expires_at = (datetime.now(timezone.utc) + timedelta(days=expires_in_days)).isoformat()

    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO invitations (email, role, token_hash, status, actor, created_at, expires_at, accepted_at)
            VALUES (?, ?, ?, 'pending', ?, ?, ?, NULL)
            """,
            (email, role, token_hash, actor, created_at, expires_at),
        )
        row = connection.execute(
            """
            SELECT id, email, role, status, actor, created_at, expires_at, accepted_at
            FROM invitations
            WHERE id = ?
            """,
            (cursor.lastrowid,),
        ).fetchone()

    return dict(row), token


def list_invitations(*, status: Optional[str] = None, limit: int = 100) -> list[dict[str, Any]]:
    query = """
        SELECT id, email, role, status, actor, created_at, expires_at, accepted_at
        FROM invitations
    """
    params: list[Any] = []

    if status:
        query += " WHERE status = ?"
        params.append(status)

    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    with get_connection() as connection:
        rows = connection.execute(query, params).fetchall()

    return [dict(row) for row in rows]


def count_invitations(*, status: Optional[str] = None) -> int:
    query = "SELECT COUNT(*) AS total FROM invitations"
    params: list[Any] = []
    if status:
        query += " WHERE status = ?"
        params.append(status)

    with get_connection() as connection:
        row = connection.execute(query, params).fetchone()

    return int(row["total"]) if row else 0


def count_users() -> int:
    with get_connection() as connection:
        row = connection.execute("SELECT COUNT(*) AS total FROM users").fetchone()
    return int(row["total"]) if row else 0


def create_user(
    *,
    email: str,
    full_name: Optional[str],
    password_hash: str,
    role: str,
    status: str = "active",
) -> dict[str, Any]:
    user_id = str(uuid4())
    now = utcnow_iso()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO users (id, email, full_name, password_hash, role, status, last_login_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)
            """,
            (user_id, email, full_name, password_hash, role, status, now, now),
        )
        row = connection.execute(
            """
            SELECT id, email, full_name, role, status, last_login_at, created_at, updated_at
            FROM users
            WHERE id = ?
            """,
            (user_id,),
        ).fetchone()
    return dict(row)


def get_user_by_email(email: str, *, include_password_hash: bool = False) -> Optional[dict[str, Any]]:
    select_fields = "id, email, full_name, role, status, last_login_at, created_at, updated_at"
    if include_password_hash:
        select_fields = f"{select_fields}, password_hash"

    with get_connection() as connection:
        row = connection.execute(
            f"SELECT {select_fields} FROM users WHERE lower(email) = lower(?)",
            (email,),
        ).fetchone()
    return dict(row) if row else None


def get_user_by_id(user_id: str) -> Optional[dict[str, Any]]:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id, email, full_name, role, status, last_login_at, created_at, updated_at
            FROM users
            WHERE id = ?
            """,
            (user_id,),
        ).fetchone()
    return dict(row) if row else None


def list_users(*, limit: int = 200) -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT id, email, full_name, role, status, last_login_at, created_at, updated_at
            FROM users
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def update_user_login(user_id: str) -> None:
    now = utcnow_iso()
    with get_connection() as connection:
        connection.execute(
            "UPDATE users SET last_login_at = ?, updated_at = ? WHERE id = ?",
            (now, now, user_id),
        )


def update_user_role(user_id: str, *, role: str) -> Optional[dict[str, Any]]:
    now = utcnow_iso()
    with get_connection() as connection:
        connection.execute(
            "UPDATE users SET role = ?, updated_at = ? WHERE id = ?",
            (role, now, user_id),
        )
        row = connection.execute(
            """
            SELECT id, email, full_name, role, status, last_login_at, created_at, updated_at
            FROM users
            WHERE id = ?
            """,
            (user_id,),
        ).fetchone()
    return dict(row) if row else None


def get_api_key_by_hash(key_hash: str) -> Optional[dict[str, Any]]:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id, name, key_prefix, status, actor, created_at, revoked_at, key_hash
            FROM api_keys
            WHERE key_hash = ?
            """,
            (key_hash,),
        ).fetchone()
    return dict(row) if row else None


def create_job(
    *,
    job_type: str,
    payload: dict[str, Any],
    actor: str,
    max_attempts: int = 3,
) -> dict[str, Any]:
    job_id = str(uuid4())
    created_at = utcnow_iso()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO jobs (id, job_type, payload, status, result, error, actor, attempts, max_attempts, worker_id, created_at, started_at, finished_at)
            VALUES (?, ?, ?, 'queued', NULL, NULL, ?, 0, ?, NULL, ?, NULL, NULL)
            """,
            (job_id, job_type, json.dumps(payload), actor, max_attempts, created_at),
        )
        row = connection.execute(
            """
            SELECT *
            FROM jobs
            WHERE id = ?
            """,
            (job_id,),
        ).fetchone()
    return _deserialize_job(row) if row else {}


def get_job(job_id: str) -> Optional[dict[str, Any]]:
    with get_connection() as connection:
        row = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return _deserialize_job(row) if row else None


def list_jobs(*, status: Optional[str] = None, limit: int = 100) -> list[dict[str, Any]]:
    query = "SELECT * FROM jobs"
    params: list[Any] = []
    if status:
        query += " WHERE status = ?"
        params.append(status)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    with get_connection() as connection:
        rows = connection.execute(query, params).fetchall()
    return [_deserialize_job(row) for row in rows]


def claim_next_job(*, worker_id: str) -> Optional[dict[str, Any]]:
    started_at = utcnow_iso()
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id
            FROM jobs
            WHERE status = 'queued'
            ORDER BY created_at ASC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None

        job_id = row["id"]
        updated = connection.execute(
            """
            UPDATE jobs
            SET status = 'running',
                started_at = ?,
                worker_id = ?,
                attempts = attempts + 1,
                error = NULL
            WHERE id = ? AND status = 'queued'
            """,
            (started_at, worker_id, job_id),
        )
        if updated.rowcount != 1:
            return None

        claimed = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return _deserialize_job(claimed) if claimed else None


def claim_job_by_id(*, job_id: str, worker_id: str) -> Optional[dict[str, Any]]:
    started_at = utcnow_iso()
    with get_connection() as connection:
        updated = connection.execute(
            """
            UPDATE jobs
            SET status = 'running',
                started_at = ?,
                worker_id = ?,
                attempts = attempts + 1,
                error = NULL
            WHERE id = ? AND status = 'queued'
            """,
            (started_at, worker_id, job_id),
        )
        if updated.rowcount != 1:
            return None
        row = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return _deserialize_job(row) if row else None


def complete_job(*, job_id: str, result: dict[str, Any]) -> Optional[dict[str, Any]]:
    finished_at = utcnow_iso()
    with get_connection() as connection:
        connection.execute(
            """
            UPDATE jobs
            SET status = 'completed',
                result = ?,
                finished_at = ?,
                worker_id = worker_id
            WHERE id = ?
            """,
            (json.dumps(result), finished_at, job_id),
        )
        row = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return _deserialize_job(row) if row else None


def fail_job(*, job_id: str, error: str) -> Optional[dict[str, Any]]:
    finished_at = utcnow_iso()
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id, attempts, max_attempts
            FROM jobs
            WHERE id = ?
            """,
            (job_id,),
        ).fetchone()
        if not row:
            return None

        should_retry = int(row["attempts"]) < int(row["max_attempts"])
        status = "queued" if should_retry else "failed"
        started_at = None if should_retry else connection.execute(
            "SELECT started_at FROM jobs WHERE id = ?",
            (job_id,),
        ).fetchone()["started_at"]

        connection.execute(
            """
            UPDATE jobs
            SET status = ?,
                error = ?,
                finished_at = ?,
                started_at = CASE WHEN ? THEN NULL ELSE started_at END
            WHERE id = ?
            """,
            (status, error, finished_at if not should_retry else None, 1 if should_retry else 0, job_id),
        )
        updated = connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return _deserialize_job(updated) if updated else None


def count_overdue_documents() -> int:
    now = utcnow_iso()
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT COUNT(*) AS total FROM documents
            WHERE due_date IS NOT NULL AND due_date < ?
            AND status NOT IN ('approved', 'corrected', 'completed', 'archived')
            """,
            (now,),
        ).fetchone()
    return int(row["total"]) if row else 0


def list_overdue_documents(*, limit: int = 100) -> list[dict[str, Any]]:
    now = utcnow_iso()
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT * FROM documents
            WHERE due_date IS NOT NULL AND due_date < ?
            AND status NOT IN ('approved', 'corrected', 'completed', 'archived')
            ORDER BY due_date ASC LIMIT ?
            """,
            (now, limit),
        ).fetchall()
    return [_deserialize_row(row) for row in rows]


def list_assigned_to(user_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            "SELECT * FROM documents WHERE assigned_to = ? ORDER BY updated_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
    return [_deserialize_row(row) for row in rows]


def list_unassigned_manual_documents(*, limit: int = 200) -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT * FROM documents
            WHERE status IN ('needs_review', 'acknowledged')
              AND (assigned_to IS NULL OR TRIM(assigned_to) = '')
            ORDER BY
              CASE WHEN due_date IS NULL THEN 1 ELSE 0 END,
              due_date ASC,
              updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [_deserialize_row(row) for row in rows]


def count_unassigned_manual_documents() -> int:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT COUNT(*) AS total
            FROM documents
            WHERE status IN ('needs_review', 'acknowledged')
              AND (assigned_to IS NULL OR TRIM(assigned_to) = '')
            """
        ).fetchone()
    return int(row["total"]) if row else 0


def create_outbound_email(
    *,
    document_id: str,
    to_email: str,
    subject: str,
    body: str,
    status: str = "pending",
    provider: str = "smtp",
    error: Optional[str] = None,
    sent_at: Optional[str] = None,
) -> dict[str, Any]:
    created_at = utcnow_iso()
    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO outbound_emails (document_id, to_email, subject, body, status, provider, error, created_at, sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (document_id, to_email, subject, body, status, provider, error, created_at, sent_at),
        )
        row = connection.execute(
            "SELECT * FROM outbound_emails WHERE id = ?",
            (cursor.lastrowid,),
        ).fetchone()
    return dict(row)


def update_outbound_email(
    email_id: int,
    *,
    status: Optional[str] = None,
    error: Optional[str] = None,
    sent_at: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    updates: dict[str, Any] = {}
    if status is not None:
        updates["status"] = status
    if error is not None:
        updates["error"] = error
    if sent_at is not None:
        updates["sent_at"] = sent_at

    if not updates:
        with get_connection() as connection:
            row = connection.execute(
                "SELECT * FROM outbound_emails WHERE id = ?",
                (email_id,),
            ).fetchone()
        return dict(row) if row else None

    assignments = ", ".join(f"{key} = ?" for key in updates)
    values = list(updates.values()) + [email_id]
    with get_connection() as connection:
        connection.execute(
            f"UPDATE outbound_emails SET {assignments} WHERE id = ?",
            values,
        )
        row = connection.execute(
            "SELECT * FROM outbound_emails WHERE id = ?",
            (email_id,),
        ).fetchone()
    return dict(row) if row else None


def _deserialize_job(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    record = dict(row)
    for key in ("payload", "result"):
        raw = record.get(key)
        if raw:
            try:
                record[key] = json.loads(raw)
            except Exception:
                record[key] = {}
        else:
            record[key] = {}
    return record


def purge_audit_events_before(older_than_iso: str) -> int:
    with get_connection() as connection:
        cursor = connection.execute(
            "DELETE FROM audit_events WHERE created_at < ?",
            (older_than_iso,),
        )
    return max(int(cursor.rowcount), 0)


def purge_notifications_before(older_than_iso: str) -> int:
    with get_connection() as connection:
        cursor = connection.execute(
            "DELETE FROM notifications WHERE created_at < ?",
            (older_than_iso,),
        )
    return max(int(cursor.rowcount), 0)


def purge_outbound_emails_before(older_than_iso: str) -> int:
    with get_connection() as connection:
        cursor = connection.execute(
            "DELETE FROM outbound_emails WHERE created_at < ?",
            (older_than_iso,),
        )
    return max(int(cursor.rowcount), 0)
