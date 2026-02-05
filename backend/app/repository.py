from __future__ import annotations

import json
from datetime import datetime, timezone
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
