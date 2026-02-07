from __future__ import annotations

import hashlib
import json
import secrets
from datetime import datetime, timezone
from datetime import timedelta
from typing import Any, Optional

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


def list_documents(*, status: Optional[str] = None, department: Optional[str] = None, limit: int = 100) -> list[dict[str, Any]]:
    query = "SELECT * FROM documents"
    conditions: list[str] = []
    params: list[Any] = []

    if status:
        conditions.append("status = ?")
        params.append(status)

    if department:
        conditions.append("department = ?")
        params.append(department)

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
                SUM(CASE WHEN status IN ('routed', 'approved', 'corrected') THEN 1 ELSE 0 END) AS ready
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
                    "average_confidence": round(float(totals["average_confidence"] or 0.0), 4),
                }
            )

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

    analytics["by_type"] = [dict(row) for row in by_type_rows]
    analytics["by_status"] = [dict(row) for row in by_status_rows]

    return analytics


def get_latest_deployment() -> Optional[dict[str, Any]]:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id, environment, status, actor, notes, details, created_at, finished_at
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
            SELECT id, environment, status, actor, notes, details, created_at, finished_at
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
    notes: Optional[str] = None,
    status: str = "completed",
    details: Optional[str] = None,
) -> dict[str, Any]:
    created_at = utcnow_iso()
    finished_at = created_at if status in {"completed", "failed"} else None

    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO deployments (environment, status, actor, notes, details, created_at, finished_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (environment, status, actor, notes, details, created_at, finished_at),
        )
        row = connection.execute(
            """
            SELECT id, environment, status, actor, notes, details, created_at, finished_at
            FROM deployments
            WHERE id = ?
            """,
            (cursor.lastrowid,),
        ).fetchone()

    return dict(row)


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
