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


def _apply_workspace_scope(
    *,
    conditions: list[str],
    params: list[Any],
    workspace_id: Optional[str],
    column: str = "workspace_id",
) -> None:
    if workspace_id is not None:
        conditions.append(f"{column} = ?")
        params.append(workspace_id)


def create_document(*, document: dict[str, Any]) -> dict[str, Any]:
    now = utcnow_iso()
    payload = {
        "id": document["id"],
        "workspace_id": document.get("workspace_id"),
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
    serialized_values = [
        _serialize_value(column, payload[column]) for column in columns
    ]
    placeholders = ", ".join("?" for _ in columns)

    with get_connection() as connection:
        connection.execute(
            f"INSERT INTO documents ({', '.join(columns)}) VALUES ({placeholders})",
            serialized_values,
        )
        row = connection.execute(
            "SELECT * FROM documents WHERE id = ?", (payload["id"],)
        ).fetchone()

    return _deserialize_row(row)


def get_document(
    document_id: str, workspace_id: Optional[str] = None
) -> Optional[dict[str, Any]]:
    conditions = ["id = ?"]
    params: list[Any] = [document_id]
    _apply_workspace_scope(
        conditions=conditions,
        params=params,
        workspace_id=workspace_id,
        column="workspace_id",
    )
    query = f"SELECT * FROM documents WHERE {' AND '.join(conditions)}"
    with get_connection() as connection:
        row = connection.execute(query, params).fetchone()

    return _deserialize_row(row) if row else None


def list_documents(
    *,
    status: Optional[str] = None,
    department: Optional[str] = None,
    assigned_to: Optional[str] = None,
    workspace_id: Optional[str] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = "SELECT * FROM documents"
    conditions: list[str] = []
    params: list[Any] = []

    if status:
        if status == "overdue":
            conditions.append("due_date IS NOT NULL AND due_date < ?")
            conditions.append(
                "status NOT IN ('approved', 'corrected', 'completed', 'archived')"
            )
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
    _apply_workspace_scope(
        conditions=conditions,
        params=params,
        workspace_id=workspace_id,
        column="workspace_id",
    )

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY updated_at DESC LIMIT ?"
    params.append(limit)

    with get_connection() as connection:
        rows = connection.execute(query, params).fetchall()

    return [_deserialize_row(row) for row in rows]


def update_document(
    document_id: str,
    *,
    updates: dict[str, Any],
    workspace_id: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    if not updates:
        return get_document(document_id, workspace_id=workspace_id)

    payload = dict(updates)
    payload["updated_at"] = utcnow_iso()

    assignments = ", ".join(f"{key} = ?" for key in payload)
    values = [_serialize_value(key, value) for key, value in payload.items()]
    values.append(document_id)
    where_clause = "id = ?"
    if workspace_id is not None:
        where_clause += " AND workspace_id = ?"
        values.append(workspace_id)

    with get_connection() as connection:
        connection.execute(
            f"UPDATE documents SET {assignments} WHERE {where_clause}", values
        )
        select_conditions = ["id = ?"]
        select_params: list[Any] = [document_id]
        _apply_workspace_scope(
            conditions=select_conditions,
            params=select_params,
            workspace_id=workspace_id,
            column="workspace_id",
        )
        row = connection.execute(
            f"SELECT * FROM documents WHERE {' AND '.join(select_conditions)}",
            select_params,
        ).fetchone()

    return _deserialize_row(row) if row else None


def create_audit_event(
    *,
    document_id: str,
    action: str,
    actor: str,
    details: Optional[str] = None,
    workspace_id: Optional[str] = None,
) -> None:
    resolved_workspace_id = workspace_id
    if resolved_workspace_id is None:
        document = get_document(document_id)
        if document:
            resolved_workspace_id = document.get("workspace_id")
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO audit_events (workspace_id, document_id, action, actor, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (resolved_workspace_id, document_id, action, actor, details, utcnow_iso()),
        )


def list_audit_events(
    document_id: str,
    *,
    workspace_id: Optional[str] = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    conditions = ["document_id = ?"]
    params: list[Any] = [document_id]
    _apply_workspace_scope(
        conditions=conditions,
        params=params,
        workspace_id=workspace_id,
        column="workspace_id",
    )
    params.append(limit)
    with get_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT id, workspace_id, document_id, action, actor, details, created_at
            FROM audit_events
            WHERE {" AND ".join(conditions)}
            ORDER BY id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()

    return [dict(row) for row in rows]


def get_queue_snapshot(workspace_id: Optional[str] = None) -> list[dict[str, Any]]:
    where_sql = ""
    params: list[Any] = []
    if workspace_id is not None:
        where_sql = "WHERE workspace_id = ?"
        params.append(workspace_id)
    with get_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT
                COALESCE(department, 'Unassigned') AS department,
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'needs_review' THEN 1 ELSE 0 END) AS needs_review,
                SUM(CASE WHEN status IN ('routed', 'approved', 'corrected', 'acknowledged', 'assigned', 'in_progress', 'completed') THEN 1 ELSE 0 END) AS ready
            FROM documents
            {where_sql}
            GROUP BY COALESCE(department, 'Unassigned')
            ORDER BY total DESC, department ASC
            """,
            params,
        ).fetchall()

    return [dict(row) for row in rows]


def get_analytics_snapshot(workspace_id: Optional[str] = None) -> dict[str, Any]:
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

    where_sql = ""
    where_params: list[Any] = []
    if workspace_id is not None:
        where_sql = "WHERE workspace_id = ?"
        where_params.append(workspace_id)

    with get_connection() as connection:
        totals = connection.execute(
            f"""
            SELECT
                COUNT(*) AS total_documents,
                SUM(CASE WHEN requires_review = 1 THEN 1 ELSE 0 END) AS needs_review,
                SUM(CASE WHEN status IN ('routed', 'approved', 'corrected') THEN 1 ELSE 0 END) AS routed_or_approved,
                SUM(CASE WHEN status IN ('routed', 'approved', 'corrected', 'completed', 'archived') THEN 1 ELSE 0 END) AS automated_documents,
                AVG(COALESCE(confidence, 0)) AS average_confidence
            FROM documents
            {where_sql}
            """,
            where_params,
        ).fetchone()

        if totals:
            analytics.update(
                {
                    "total_documents": totals["total_documents"] or 0,
                    "needs_review": totals["needs_review"] or 0,
                    "routed_or_approved": totals["routed_or_approved"] or 0,
                    "automated_documents": totals["automated_documents"] or 0,
                    "average_confidence": round(
                        float(totals["average_confidence"] or 0.0), 4
                    ),
                }
            )

        manual_unassigned_row = connection.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM documents
            {"WHERE workspace_id = ? AND" if workspace_id is not None else "WHERE"} status IN ('ingested', 'needs_review', 'acknowledged', 'assigned', 'in_progress', 'failed')
              AND (assigned_to IS NULL OR TRIM(assigned_to) = '')
            """,
            where_params,
        ).fetchone()

        by_type_rows = connection.execute(
            f"""
            SELECT COALESCE(doc_type, 'unclassified') AS label, COUNT(*) AS count
            FROM documents
            {where_sql}
            GROUP BY COALESCE(doc_type, 'unclassified')
            ORDER BY count DESC, label ASC
            """,
            where_params,
        ).fetchall()

        by_status_rows = connection.execute(
            f"""
            SELECT status AS label, COUNT(*) AS count
            FROM documents
            {where_sql}
            GROUP BY status
            ORDER BY count DESC, label ASC
            """,
            where_params,
        ).fetchall()

        overdue_row = connection.execute(
            f"""
            SELECT COUNT(*) AS total FROM documents
            {"WHERE workspace_id = ? AND" if workspace_id is not None else "WHERE"} due_date IS NOT NULL AND due_date < ?
            AND status NOT IN ('approved', 'corrected', 'completed', 'archived')
            """,
            [*where_params, utcnow_iso()],
        ).fetchone()

        missing_contact_total = 0
        contact_rows = connection.execute(
            f"""
            SELECT extracted_fields
            FROM documents
            {"WHERE workspace_id = ? AND" if workspace_id is not None else "WHERE"} status IN ('ingested', 'needs_review', 'acknowledged', 'assigned', 'in_progress', 'failed')
            """,
            where_params,
        ).fetchall()
        for row in contact_rows:
            raw = (
                row["extracted_fields"]
                if isinstance(row, dict) or hasattr(row, "__getitem__")
                else None
            )
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
            if workspace_id is None:
                emails_today_row = connection.execute(
                    "SELECT COUNT(*) AS total FROM outbound_emails WHERE status = 'sent' AND sent_at >= ?",
                    (today_start,),
                ).fetchone()
            else:
                emails_today_row = connection.execute(
                    "SELECT COUNT(*) AS total FROM outbound_emails WHERE workspace_id = ? AND status = 'sent' AND sent_at >= ?",
                    (workspace_id, today_start),
                ).fetchone()
            analytics["emails_sent_today"] = (
                int(emails_today_row["total"]) if emails_today_row else 0
            )
        except Exception:
            analytics["emails_sent_today"] = 0

    analytics["by_type"] = [dict(row) for row in by_type_rows]
    analytics["by_status"] = [dict(row) for row in by_status_rows]
    analytics["overdue"] = int(overdue_row["total"]) if overdue_row else 0
    analytics["manual_unassigned"] = (
        int(manual_unassigned_row["total"]) if manual_unassigned_row else 0
    )
    total_documents = int(analytics["total_documents"] or 0)
    automated_documents = int(analytics["automated_documents"] or 0)
    manual_documents = max(total_documents - automated_documents, 0)
    analytics["manual_documents"] = manual_documents
    analytics["automation_rate"] = (
        round((automated_documents / total_documents), 4) if total_documents else 0.0
    )
    analytics["manual_rate"] = (
        round((manual_documents / total_documents), 4) if total_documents else 0.0
    )

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
            (
                environment,
                provider,
                status,
                actor,
                notes,
                details,
                external_id,
                created_at,
                finished_at,
            ),
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


def list_api_keys(
    *, include_revoked: bool = False, limit: int = 100
) -> list[dict[str, Any]]:
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
    workspace_id: Optional[str] = None,
    email: str,
    role: str,
    actor: str,
    expires_in_days: int = 7,
) -> tuple[dict[str, Any], str]:
    token = secrets.token_urlsafe(24)
    token_hash = _hash_secret(token)
    created_at = utcnow_iso()
    expires_at = (
        datetime.now(timezone.utc) + timedelta(days=expires_in_days)
    ).isoformat()

    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO invitations (workspace_id, email, role, token_hash, status, actor, created_at, expires_at, accepted_at)
            VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, NULL)
            """,
            (workspace_id, email, role, token_hash, actor, created_at, expires_at),
        )
        row = connection.execute(
            """
            SELECT id, workspace_id, email, role, status, actor, created_at, expires_at, accepted_at
            FROM invitations
            WHERE id = ?
            """,
            (cursor.lastrowid,),
        ).fetchone()

    return dict(row), token


def list_invitations(
    *,
    workspace_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = """
        SELECT id, workspace_id, email, role, status, actor, created_at, expires_at, accepted_at
        FROM invitations
    """
    params: list[Any] = []

    if workspace_id is not None:
        query += " WHERE workspace_id = ?"
        params.append(workspace_id)

    if status:
        query += (" AND" if params else " WHERE") + " status = ?"
        params.append(status)

    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    with get_connection() as connection:
        rows = connection.execute(query, params).fetchall()

    return [dict(row) for row in rows]


def count_invitations(
    *, workspace_id: Optional[str] = None, status: Optional[str] = None
) -> int:
    query = "SELECT COUNT(*) AS total FROM invitations"
    params: list[Any] = []
    if workspace_id is not None:
        query += " WHERE workspace_id = ?"
        params.append(workspace_id)
    if status:
        query += (" AND" if params else " WHERE") + " status = ?"
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
    plan_tier: str = "free",
) -> dict[str, Any]:
    user_id = str(uuid4())
    now = utcnow_iso()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO users (id, email, full_name, password_hash, role, status, plan_tier, last_login_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
            """,
            (
                user_id,
                email,
                full_name,
                password_hash,
                role,
                status,
                plan_tier,
                now,
                now,
            ),
        )
        row = connection.execute(
            """
            SELECT id, email, full_name, role, status, plan_tier, last_login_at, created_at, updated_at
            FROM users
            WHERE id = ?
            """,
            (user_id,),
        ).fetchone()
    return dict(row)


def get_user_by_email(
    email: str, *, include_password_hash: bool = False
) -> Optional[dict[str, Any]]:
    select_fields = "id, email, full_name, role, status, plan_tier, last_login_at, created_at, updated_at"
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
            SELECT id, email, full_name, role, status, plan_tier, stripe_customer_id,
                   email_preferences, last_login_at, created_at, updated_at
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
            SELECT id, email, full_name, role, status, plan_tier, last_login_at, created_at, updated_at
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
            SELECT id, email, full_name, role, status, plan_tier, last_login_at, created_at, updated_at
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
    workspace_id: Optional[str] = None,
    max_attempts: int = 3,
) -> dict[str, Any]:
    job_id = str(uuid4())
    created_at = utcnow_iso()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO jobs (id, workspace_id, job_type, payload, status, result, error, actor, attempts, max_attempts, worker_id, created_at, started_at, finished_at)
            VALUES (?, ?, ?, ?, 'queued', NULL, NULL, ?, 0, ?, NULL, ?, NULL, NULL)
            """,
            (
                job_id,
                workspace_id,
                job_type,
                json.dumps(payload),
                actor,
                max_attempts,
                created_at,
            ),
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


def get_job(
    job_id: str, workspace_id: Optional[str] = None
) -> Optional[dict[str, Any]]:
    conditions = ["id = ?"]
    params: list[Any] = [job_id]
    _apply_workspace_scope(
        conditions=conditions,
        params=params,
        workspace_id=workspace_id,
        column="workspace_id",
    )
    with get_connection() as connection:
        row = connection.execute(
            f"SELECT * FROM jobs WHERE {' AND '.join(conditions)}", params
        ).fetchone()
    return _deserialize_job(row) if row else None


def list_jobs(
    *,
    status: Optional[str] = None,
    workspace_id: Optional[str] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    query = "SELECT * FROM jobs"
    params: list[Any] = []
    conditions: list[str] = []
    _apply_workspace_scope(
        conditions=conditions,
        params=params,
        workspace_id=workspace_id,
        column="workspace_id",
    )
    if status:
        conditions.append("status = ?")
        params.append(status)
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
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

        claimed = connection.execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
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
        row = connection.execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
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
        row = connection.execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
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
        connection.execute(
            """
            UPDATE jobs
            SET status = ?,
                error = ?,
                finished_at = ?,
                started_at = CASE WHEN ? THEN NULL ELSE started_at END
            WHERE id = ?
            """,
            (
                status,
                error,
                finished_at if not should_retry else None,
                1 if should_retry else 0,
                job_id,
            ),
        )
        updated = connection.execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
    return _deserialize_job(updated) if updated else None


def count_overdue_documents(workspace_id: Optional[str] = None) -> int:
    now = utcnow_iso()
    params: list[Any] = [now]
    where_sql = "due_date IS NOT NULL AND due_date < ?"
    if workspace_id is not None:
        where_sql = "workspace_id = ? AND " + where_sql
        params.insert(0, workspace_id)
    with get_connection() as connection:
        row = connection.execute(
            f"""
            SELECT COUNT(*) AS total FROM documents
            WHERE {where_sql}
            AND status NOT IN ('approved', 'corrected', 'completed', 'archived')
            """,
            params,
        ).fetchone()
    return int(row["total"]) if row else 0


def list_overdue_documents(
    *, workspace_id: Optional[str] = None, limit: int = 100
) -> list[dict[str, Any]]:
    now = utcnow_iso()
    params: list[Any] = [now, limit]
    where_sql = "due_date IS NOT NULL AND due_date < ?"
    if workspace_id is not None:
        where_sql = "workspace_id = ? AND " + where_sql
        params = [workspace_id, now, limit]
    with get_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT * FROM documents
            WHERE {where_sql}
            AND status NOT IN ('approved', 'corrected', 'completed', 'archived')
            ORDER BY due_date ASC LIMIT ?
            """,
            params,
        ).fetchall()
    return [_deserialize_row(row) for row in rows]


def list_assigned_to(
    user_id: str, *, workspace_id: Optional[str] = None, limit: int = 100
) -> list[dict[str, Any]]:
    conditions = ["assigned_to = ?"]
    params: list[Any] = [user_id]
    _apply_workspace_scope(
        conditions=conditions,
        params=params,
        workspace_id=workspace_id,
        column="workspace_id",
    )
    params.append(limit)
    with get_connection() as connection:
        rows = connection.execute(
            f"SELECT * FROM documents WHERE {' AND '.join(conditions)} ORDER BY updated_at DESC LIMIT ?",
            params,
        ).fetchall()
    return [_deserialize_row(row) for row in rows]


def list_unassigned_manual_documents(
    *, workspace_id: Optional[str] = None, limit: int = 200
) -> list[dict[str, Any]]:
    workspace_condition = "workspace_id = ? AND" if workspace_id is not None else ""
    params: list[Any] = [limit] if workspace_id is None else [workspace_id, limit]
    with get_connection() as connection:
        rows = connection.execute(
            f"""
            SELECT * FROM documents
            WHERE {workspace_condition} status IN ('needs_review', 'acknowledged')
              AND (assigned_to IS NULL OR TRIM(assigned_to) = '')
            ORDER BY
              CASE WHEN due_date IS NULL THEN 1 ELSE 0 END,
              due_date ASC,
              updated_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [_deserialize_row(row) for row in rows]


def count_unassigned_manual_documents(workspace_id: Optional[str] = None) -> int:
    workspace_condition = "workspace_id = ? AND" if workspace_id is not None else ""
    params: list[Any] = [] if workspace_id is None else [workspace_id]
    with get_connection() as connection:
        row = connection.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM documents
            WHERE {workspace_condition} status IN ('needs_review', 'acknowledged')
              AND (assigned_to IS NULL OR TRIM(assigned_to) = '')
            """,
            params,
        ).fetchone()
    return int(row["total"]) if row else 0


def create_outbound_email(
    *,
    document_id: str,
    workspace_id: Optional[str] = None,
    to_email: str,
    subject: str,
    body: str,
    status: str = "pending",
    provider: str = "smtp",
    error: Optional[str] = None,
    sent_at: Optional[str] = None,
) -> dict[str, Any]:
    created_at = utcnow_iso()
    resolved_workspace_id = workspace_id
    if resolved_workspace_id is None and document_id and document_id != "__account__":
        document = get_document(document_id)
        if document:
            resolved_workspace_id = document.get("workspace_id")
    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO outbound_emails (workspace_id, document_id, to_email, subject, body, status, provider, error, created_at, sent_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                resolved_workspace_id,
                document_id,
                to_email,
                subject,
                body,
                status,
                provider,
                error,
                created_at,
                sent_at,
            ),
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


# --- Invitation acceptance ---


def get_invitation_by_token(
    token: str, workspace_id: Optional[str] = None
) -> Optional[dict[str, Any]]:
    token_hash = _hash_secret(token)
    conditions = ["token_hash = ?"]
    params: list[Any] = [token_hash]
    _apply_workspace_scope(
        conditions=conditions,
        params=params,
        workspace_id=workspace_id,
        column="workspace_id",
    )
    with get_connection() as connection:
        row = connection.execute(
            f"""
            SELECT id, workspace_id, email, role, status, actor, created_at, expires_at, accepted_at
            FROM invitations
            WHERE {" AND ".join(conditions)}
            """,
            params,
        ).fetchone()
    return dict(row) if row else None


def validate_invitation(
    token: str, workspace_id: Optional[str] = None
) -> dict[str, Any]:
    """Validate invitation token and return invitation details."""
    token_hash = _hash_secret(token)
    now = utcnow_iso()
    conditions = ["token_hash = ?"]
    params: list[Any] = [token_hash]
    _apply_workspace_scope(
        conditions=conditions,
        params=params,
        workspace_id=workspace_id,
        column="workspace_id",
    )
    with get_connection() as connection:
        row = connection.execute(
            f"""
            SELECT id, workspace_id, email, role, status, expires_at
            FROM invitations
            WHERE {" AND ".join(conditions)}
            """,
            params,
        ).fetchone()
        if not row:
            raise ValueError("Invalid invitation token.")
        invitation = dict(row)
        if invitation["status"] != "pending":
            raise ValueError("Invitation has already been used or revoked.")
        if invitation["expires_at"] < now:
            raise ValueError("Invitation has expired.")
    return invitation


def mark_invitation_accepted(
    invitation_id: int, workspace_id: Optional[str] = None
) -> dict[str, Any]:
    """Mark a validated invitation as accepted."""
    now = utcnow_iso()
    where_sql = "id = ? AND status = 'pending'"
    update_params: list[Any] = [now, invitation_id]
    if workspace_id is not None:
        where_sql += " AND workspace_id = ?"
        update_params.append(workspace_id)
    with get_connection() as connection:
        updated = connection.execute(
            """
            UPDATE invitations
            SET status = 'accepted', accepted_at = ?
            WHERE """
            + where_sql,
            update_params,
        )
        if updated.rowcount != 1:
            raise ValueError("Invitation has already been used or revoked.")

        select_conditions = ["id = ?"]
        select_params: list[Any] = [invitation_id]
        _apply_workspace_scope(
            conditions=select_conditions,
            params=select_params,
            workspace_id=workspace_id,
            column="workspace_id",
        )
        row = connection.execute(
            f"""
            SELECT id, workspace_id, email, role, status, actor, created_at, expires_at, accepted_at
            FROM invitations
            WHERE {" AND ".join(select_conditions)}
            """,
            select_params,
        ).fetchone()
    if not row:
        raise ValueError("Invitation not found.")
    return dict(row)


def accept_invitation(token: str, workspace_id: Optional[str] = None) -> dict[str, Any]:
    """Validate and mark an invitation as accepted. Returns the updated record."""
    invitation = validate_invitation(token, workspace_id=workspace_id)
    return mark_invitation_accepted(
        int(invitation["id"]), workspace_id=invitation.get("workspace_id")
    )


# --- Billing / Subscription ---


def update_user_plan(
    user_id: str, *, plan_tier: str, stripe_customer_id: Optional[str] = None
) -> Optional[dict[str, Any]]:
    now = utcnow_iso()
    with get_connection() as connection:
        if stripe_customer_id is not None:
            connection.execute(
                "UPDATE users SET plan_tier = ?, stripe_customer_id = ?, updated_at = ? WHERE id = ?",
                (plan_tier, stripe_customer_id, now, user_id),
            )
        else:
            connection.execute(
                "UPDATE users SET plan_tier = ?, updated_at = ? WHERE id = ?",
                (plan_tier, now, user_id),
            )
        row = connection.execute(
            """
            SELECT id, email, full_name, role, status, plan_tier, stripe_customer_id,
                   last_login_at, created_at, updated_at
            FROM users
            WHERE id = ?
            """,
            (user_id,),
        ).fetchone()
    return dict(row) if row else None


def get_user_by_stripe_customer(stripe_customer_id: str) -> Optional[dict[str, Any]]:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id, email, full_name, role, status, plan_tier, stripe_customer_id,
                   last_login_at, created_at, updated_at
            FROM users
            WHERE stripe_customer_id = ?
            """,
            (stripe_customer_id,),
        ).fetchone()
    return dict(row) if row else None


def create_subscription(
    *,
    user_id: str,
    workspace_id: Optional[str] = None,
    plan_tier: str,
    billing_type: str,
    stripe_subscription_id: Optional[str] = None,
    stripe_customer_id: Optional[str] = None,
    status: str = "active",
    current_period_start: Optional[str] = None,
    current_period_end: Optional[str] = None,
) -> dict[str, Any]:
    now = utcnow_iso()
    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO subscriptions
                (workspace_id, user_id, plan_tier, billing_type, stripe_subscription_id, stripe_customer_id,
                 status, current_period_start, current_period_end, canceled_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)
            """,
            (
                workspace_id,
                user_id,
                plan_tier,
                billing_type,
                stripe_subscription_id,
                stripe_customer_id,
                status,
                current_period_start,
                current_period_end,
                now,
                now,
            ),
        )
        row = connection.execute(
            "SELECT * FROM subscriptions WHERE id = ?",
            (cursor.lastrowid,),
        ).fetchone()
    return dict(row)


def get_active_subscription(
    user_id: str, workspace_id: Optional[str] = None
) -> Optional[dict[str, Any]]:
    conditions = ["user_id = ?", "status IN ('active', 'past_due')"]
    params: list[Any] = [user_id]
    _apply_workspace_scope(
        conditions=conditions,
        params=params,
        workspace_id=workspace_id,
        column="workspace_id",
    )
    with get_connection() as connection:
        row = connection.execute(
            f"""
            SELECT * FROM subscriptions
            WHERE {" AND ".join(conditions)}
            ORDER BY created_at DESC LIMIT 1
            """,
            params,
        ).fetchone()
    return dict(row) if row else None


def update_subscription_status(
    stripe_subscription_id: str,
    *,
    status: str,
    current_period_end: Optional[str] = None,
    canceled_at: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    now = utcnow_iso()
    with get_connection() as connection:
        connection.execute(
            """
            UPDATE subscriptions
            SET status = ?, current_period_end = COALESCE(?, current_period_end),
                canceled_at = COALESCE(?, canceled_at), updated_at = ?
            WHERE stripe_subscription_id = ?
            """,
            (status, current_period_end, canceled_at, now, stripe_subscription_id),
        )
        row = connection.execute(
            "SELECT * FROM subscriptions WHERE stripe_subscription_id = ?",
            (stripe_subscription_id,),
        ).fetchone()
    return dict(row) if row else None


def create_payment_event(
    *,
    user_id: Optional[str],
    workspace_id: Optional[str] = None,
    stripe_event_id: str,
    event_type: str,
    amount_cents: Optional[int] = None,
    currency: str = "usd",
    plan_tier: Optional[str] = None,
    billing_type: Optional[str] = None,
    raw_payload: Optional[str] = None,
) -> dict[str, Any]:
    now = utcnow_iso()
    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO payment_events
                (workspace_id, user_id, stripe_event_id, event_type, amount_cents, currency,
                 plan_tier, billing_type, raw_payload, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                workspace_id,
                user_id,
                stripe_event_id,
                event_type,
                amount_cents,
                currency,
                plan_tier,
                billing_type,
                raw_payload,
                now,
            ),
        )
        row = connection.execute(
            "SELECT * FROM payment_events WHERE id = ?",
            (cursor.lastrowid,),
        ).fetchone()
    return dict(row)


def count_user_documents_this_month(user_id: Optional[str] = None) -> int:
    """Count documents created in the current calendar month (for plan limits)."""
    now = datetime.now(timezone.utc)
    month_start = now.replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    ).isoformat()
    with get_connection() as connection:
        row = connection.execute(
            "SELECT COUNT(*) AS total FROM documents WHERE created_at >= ?",
            (month_start,),
        ).fetchone()
    return int(row["total"]) if row else 0


def count_workspace_documents_this_month(workspace_id: str) -> int:
    """Count documents created in the current month for a workspace."""
    now = datetime.now(timezone.utc)
    month_start = now.replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    ).isoformat()
    with get_connection() as connection:
        row = connection.execute(
            "SELECT COUNT(*) AS total FROM documents WHERE workspace_id = ? AND created_at >= ?",
            (workspace_id, month_start),
        ).fetchone()
    return int(row["total"]) if row else 0


# --- Workspaces ---


def _slugify_workspace_name(name: str) -> str:
    base = "".join(
        c.lower() if c.isalnum() else "-" for c in str(name or "workspace").strip()
    )
    base = "-".join(filter(None, base.split("-"))) or "workspace"
    return base[:64]


def _unique_workspace_slug(connection: Any, base_slug: str) -> str:
    candidate = base_slug
    suffix = 1
    while True:
        row = connection.execute(
            "SELECT id FROM workspaces WHERE slug = ?",
            (candidate,),
        ).fetchone()
        if not row:
            return candidate
        suffix += 1
        candidate = f"{base_slug}-{suffix}"


def create_workspace(
    *,
    name: str,
    owner_id: str,
    plan_tier: str = "free",
    settings: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    workspace_id = str(uuid4())
    now = utcnow_iso()
    with get_connection() as connection:
        slug = _unique_workspace_slug(connection, _slugify_workspace_name(name))
        connection.execute(
            """
            INSERT INTO workspaces (id, name, slug, owner_id, plan_tier, stripe_customer_id, settings, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?)
            """,
            (
                workspace_id,
                name,
                slug,
                owner_id,
                plan_tier,
                json.dumps(settings or {}),
                now,
                now,
            ),
        )
        connection.execute(
            """
            INSERT INTO workspace_members (workspace_id, user_id, role, joined_at)
            VALUES (?, ?, 'admin', ?)
            ON CONFLICT(workspace_id, user_id) DO NOTHING
            """,
            (workspace_id, owner_id, now),
        )
        row = connection.execute(
            "SELECT * FROM workspaces WHERE id = ?",
            (workspace_id,),
        ).fetchone()
    return dict(row)


def get_workspace(workspace_id: str) -> Optional[dict[str, Any]]:
    with get_connection() as connection:
        row = connection.execute(
            "SELECT * FROM workspaces WHERE id = ?",
            (workspace_id,),
        ).fetchone()
    return dict(row) if row else None


def get_workspace_by_stripe_customer(
    stripe_customer_id: str,
) -> Optional[dict[str, Any]]:
    with get_connection() as connection:
        row = connection.execute(
            "SELECT * FROM workspaces WHERE stripe_customer_id = ?",
            (stripe_customer_id,),
        ).fetchone()
    return dict(row) if row else None


def get_workspace_by_slug(slug: str) -> Optional[dict[str, Any]]:
    with get_connection() as connection:
        row = connection.execute(
            "SELECT * FROM workspaces WHERE slug = ?",
            (slug,),
        ).fetchone()
    return dict(row) if row else None


def list_user_workspaces(user_id: str) -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT w.*, m.role AS member_role
            FROM workspaces w
            JOIN workspace_members m ON m.workspace_id = w.id
            WHERE m.user_id = ?
            ORDER BY w.created_at ASC
            """,
            (user_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_workspace_role(user_id: str, workspace_id: str) -> Optional[str]:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT role
            FROM workspace_members
            WHERE user_id = ? AND workspace_id = ?
            """,
            (user_id, workspace_id),
        ).fetchone()
    if not row:
        return None
    return str(row["role"])


def add_workspace_member(
    *, workspace_id: str, user_id: str, role: str = "member"
) -> dict[str, Any]:
    joined_at = utcnow_iso()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO workspace_members (workspace_id, user_id, role, joined_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(workspace_id, user_id) DO UPDATE SET role = excluded.role
            """,
            (workspace_id, user_id, role, joined_at),
        )
        row = connection.execute(
            """
            SELECT id, workspace_id, user_id, role, joined_at
            FROM workspace_members
            WHERE workspace_id = ? AND user_id = ?
            """,
            (workspace_id, user_id),
        ).fetchone()
    return dict(row)


def remove_workspace_member(*, workspace_id: str, user_id: str) -> bool:
    with get_connection() as connection:
        cursor = connection.execute(
            "DELETE FROM workspace_members WHERE workspace_id = ? AND user_id = ?",
            (workspace_id, user_id),
        )
    return int(cursor.rowcount) > 0


def list_workspace_members(workspace_id: str) -> list[dict[str, Any]]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT m.id, m.workspace_id, m.user_id, m.role, m.joined_at,
                   u.email, u.full_name, u.status
            FROM workspace_members m
            JOIN users u ON u.id = m.user_id
            WHERE m.workspace_id = ?
            ORDER BY m.joined_at ASC
            """,
            (workspace_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def update_workspace(
    workspace_id: str,
    *,
    name: Optional[str] = None,
    settings: Optional[dict[str, Any]] = None,
) -> Optional[dict[str, Any]]:
    updates: dict[str, Any] = {}
    if name is not None:
        updates["name"] = str(name).strip() or "Workspace"
    if settings is not None:
        updates["settings"] = json.dumps(settings)
    if not updates:
        return get_workspace(workspace_id)
    updates["updated_at"] = utcnow_iso()
    assignments = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [workspace_id]
    with get_connection() as connection:
        connection.execute(
            f"UPDATE workspaces SET {assignments} WHERE id = ?",
            values,
        )
        row = connection.execute(
            "SELECT * FROM workspaces WHERE id = ?",
            (workspace_id,),
        ).fetchone()
    return dict(row) if row else None


def update_workspace_plan(
    workspace_id: str,
    *,
    plan_tier: str,
    stripe_customer_id: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    updates: dict[str, Any] = {
        "plan_tier": plan_tier,
        "updated_at": utcnow_iso(),
    }
    if stripe_customer_id is not None:
        updates["stripe_customer_id"] = stripe_customer_id
    assignments = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [workspace_id]
    with get_connection() as connection:
        connection.execute(
            f"UPDATE workspaces SET {assignments} WHERE id = ?",
            values,
        )
        row = connection.execute(
            "SELECT * FROM workspaces WHERE id = ?",
            (workspace_id,),
        ).fetchone()
    return dict(row) if row else None


def get_default_workspace_for_user(user_id: str) -> Optional[dict[str, Any]]:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT w.*, m.role AS member_role
            FROM workspaces w
            JOIN workspace_members m ON m.workspace_id = w.id
            WHERE m.user_id = ?
            ORDER BY CASE WHEN m.role = 'admin' THEN 0 ELSE 1 END, w.created_at ASC
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()
    return dict(row) if row else None


# ── Email Preferences ────────────────────────────────────────────────

_DEFAULT_EMAIL_PREFS: dict[str, bool] = {
    "account_welcome": True,
    "account_plan_change": True,
    "account_payment_receipt": True,
    "account_invitation": True,
    "doc_assigned": True,
    "doc_review_complete": True,
    "doc_digest": True,
}


def get_user_email_preferences(user_id: str) -> dict[str, bool]:
    """Return merged email preferences for a user (defaults + stored)."""
    with get_connection() as connection:
        row = connection.execute(
            "SELECT email_preferences FROM users WHERE id = ?", (user_id,)
        ).fetchone()
    if not row or not row["email_preferences"]:
        return dict(_DEFAULT_EMAIL_PREFS)
    try:
        stored = json.loads(row["email_preferences"])
    except (json.JSONDecodeError, TypeError):
        stored = {}
    return {**_DEFAULT_EMAIL_PREFS, **stored}


def update_user_email_preferences(
    user_id: str, prefs: dict[str, bool]
) -> dict[str, bool]:
    """Merge and persist email preferences for a user."""
    current = get_user_email_preferences(user_id)
    merged = {
        **current,
        **{k: v for k, v in prefs.items() if k in _DEFAULT_EMAIL_PREFS},
    }
    now = utcnow_iso()
    with get_connection() as connection:
        connection.execute(
            "UPDATE users SET email_preferences = ?, updated_at = ? WHERE id = ?",
            (json.dumps(merged), now, user_id),
        )
    return merged


# ── Workflow Rules ───────────────────────────────────────────────────


def _deserialize_workflow_rule(row: Any) -> dict[str, Any]:
    record = dict(row)
    record["enabled"] = bool(record.get("enabled", 1))
    filters_raw = record.pop("filters_json", "") if "filters_json" in record else ""
    actions_raw = record.pop("actions_json", "") if "actions_json" in record else ""
    try:
        filters = json.loads(filters_raw) if filters_raw else {}
    except Exception:
        filters = {}
    try:
        actions = json.loads(actions_raw) if actions_raw else []
    except Exception:
        actions = []
    record["filters"] = filters if isinstance(filters, dict) else {}
    record["actions"] = actions if isinstance(actions, list) else []
    return record


def list_workflow_rules(
    *,
    workspace_id: Optional[str] = None,
    trigger_event: Optional[str] = None,
    enabled_only: bool = False,
    include_global: bool = True,
    limit: int = 200,
) -> list[dict[str, Any]]:
    query = "SELECT * FROM workflow_rules"
    conditions: list[str] = []
    params: list[Any] = []
    if workspace_id is None:
        conditions.append("workspace_id IS NULL")
    elif include_global:
        conditions.append("(workspace_id = ? OR workspace_id IS NULL)")
        params.append(workspace_id)
    else:
        conditions.append("workspace_id = ?")
        params.append(workspace_id)
    if trigger_event:
        conditions.append("trigger_event = ?")
        params.append(trigger_event)
    if enabled_only:
        conditions.append("enabled = 1")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY id ASC LIMIT ?"
    params.append(limit)
    with get_connection() as connection:
        rows = connection.execute(query, params).fetchall()
    return [_deserialize_workflow_rule(row) for row in rows]


def get_workflow_rule(
    rule_id: int, *, workspace_id: Optional[str] = None, include_global: bool = True
) -> Optional[dict[str, Any]]:
    conditions = ["id = ?"]
    params: list[Any] = [rule_id]
    if workspace_id is None:
        conditions.append("workspace_id IS NULL")
    elif include_global:
        conditions.append("(workspace_id = ? OR workspace_id IS NULL)")
        params.append(workspace_id)
    else:
        conditions.append("workspace_id = ?")
        params.append(workspace_id)
    with get_connection() as connection:
        row = connection.execute(
            f"SELECT * FROM workflow_rules WHERE {' AND '.join(conditions)}",
            params,
        ).fetchone()
    return _deserialize_workflow_rule(row) if row else None


def create_workflow_rule(
    *,
    workspace_id: Optional[str],
    name: str,
    trigger_event: str,
    filters: Optional[dict[str, Any]] = None,
    actions: Optional[list[dict[str, Any]]] = None,
    enabled: bool = True,
) -> dict[str, Any]:
    now = utcnow_iso()
    filters_json = json.dumps(filters or {}, separators=(",", ":"), ensure_ascii=True)
    actions_json = json.dumps(actions or [], separators=(",", ":"), ensure_ascii=True)
    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO workflow_rules (workspace_id, name, enabled, trigger_event, filters_json, actions_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                workspace_id,
                name,
                1 if enabled else 0,
                trigger_event,
                filters_json,
                actions_json,
                now,
                now,
            ),
        )
        row = connection.execute(
            "SELECT * FROM workflow_rules WHERE id = ?",
            (cursor.lastrowid,),
        ).fetchone()
    if not row:
        raise RuntimeError("Failed to create workflow rule.")
    return _deserialize_workflow_rule(row)


def update_workflow_rule(
    rule_id: int,
    *,
    workspace_id: Optional[str],
    name: Optional[str] = None,
    enabled: Optional[bool] = None,
    trigger_event: Optional[str] = None,
    filters: Optional[dict[str, Any]] = None,
    actions: Optional[list[dict[str, Any]]] = None,
) -> Optional[dict[str, Any]]:
    updates: dict[str, Any] = {}
    if name is not None:
        updates["name"] = name
    if enabled is not None:
        updates["enabled"] = 1 if enabled else 0
    if trigger_event is not None:
        updates["trigger_event"] = trigger_event
    if filters is not None:
        updates["filters_json"] = json.dumps(
            filters, separators=(",", ":"), ensure_ascii=True
        )
    if actions is not None:
        updates["actions_json"] = json.dumps(
            actions, separators=(",", ":"), ensure_ascii=True
        )
    if not updates:
        return get_workflow_rule(
            rule_id, workspace_id=workspace_id, include_global=False
        )
    updates["updated_at"] = utcnow_iso()
    assignments = ", ".join(f"{k} = ?" for k in updates)
    params = list(updates.values()) + [rule_id]
    where = "id = ?"
    if workspace_id is None:
        where += " AND workspace_id IS NULL"
    else:
        where += " AND workspace_id = ?"
        params.append(workspace_id)
    with get_connection() as connection:
        cursor = connection.execute(
            f"UPDATE workflow_rules SET {assignments} WHERE {where}",
            params,
        )
        if int(cursor.rowcount) == 0:
            return None
        row = connection.execute(
            f"SELECT * FROM workflow_rules WHERE {where}",
            [rule_id, *(params[-1:] if workspace_id is not None else [])],
        ).fetchone()
    return _deserialize_workflow_rule(row) if row else None


def delete_workflow_rule(rule_id: int, *, workspace_id: Optional[str]) -> bool:
    query = "DELETE FROM workflow_rules WHERE id = ?"
    params: list[Any] = [rule_id]
    if workspace_id is None:
        query += " AND workspace_id IS NULL"
    else:
        query += " AND workspace_id = ?"
        params.append(workspace_id)
    with get_connection() as connection:
        cursor = connection.execute(query, params)
    return int(cursor.rowcount) > 0
