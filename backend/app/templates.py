"""Response template CRUD and rendering."""
from __future__ import annotations

from typing import Any, Optional

from .db import get_connection
from .repository import get_document, utcnow_iso


def create_template(
    *, name: str, doc_type: Optional[str] = None, template_body: str
) -> dict[str, Any]:
    now = utcnow_iso()
    with get_connection() as conn:
        cursor = conn.execute(
            """INSERT INTO templates (name, doc_type, template_body, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            (name, doc_type, template_body, now, now),
        )
        row = conn.execute(
            "SELECT * FROM templates WHERE id = ?", (cursor.lastrowid,)
        ).fetchone()
    return dict(row)


def list_templates(
    *, doc_type: Optional[str] = None, limit: int = 50
) -> list[dict[str, Any]]:
    query = "SELECT * FROM templates"
    params: list[Any] = []
    if doc_type:
        query += " WHERE doc_type = ? OR doc_type IS NULL"
        params.append(doc_type)
    query += " ORDER BY name ASC LIMIT ?"
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def get_template(template_id: int) -> Optional[dict[str, Any]]:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM templates WHERE id = ?", (template_id,)
        ).fetchone()
    return dict(row) if row else None


def update_template(
    template_id: int,
    *,
    name: Optional[str] = None,
    doc_type: Optional[str] = None,
    template_body: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    updates: dict[str, Any] = {}
    if name is not None:
        updates["name"] = name
    if doc_type is not None:
        updates["doc_type"] = doc_type
    if template_body is not None:
        updates["template_body"] = template_body
    updates["updated_at"] = utcnow_iso()
    assignments = ", ".join(f"{k} = ?" for k in updates)
    params = list(updates.values()) + [template_id]
    with get_connection() as conn:
        conn.execute(f"UPDATE templates SET {assignments} WHERE id = ?", params)
        row = conn.execute(
            "SELECT * FROM templates WHERE id = ?", (template_id,)
        ).fetchone()
    return dict(row) if row else None


def delete_template(template_id: int) -> bool:
    with get_connection() as conn:
        cursor = conn.execute("DELETE FROM templates WHERE id = ?", (template_id,))
    return cursor.rowcount > 0


def render_template(template_id: int, document_id: str) -> str:
    template = get_template(template_id)
    if not template:
        raise ValueError("Template not found")
    doc = get_document(document_id)
    if not doc:
        raise ValueError("Document not found")

    body = template["template_body"]

    # Core document fields.
    replacements: dict[str, str] = {
        "id": doc.get("id", ""),
        "filename": doc.get("filename", ""),
        "doc_type": doc.get("doc_type", ""),
        "department": doc.get("department", ""),
        "status": doc.get("status", ""),
        "urgency": doc.get("urgency", ""),
    }

    # Add all extracted fields.
    fields = doc.get("extracted_fields", {})
    if isinstance(fields, dict):
        for key, value in fields.items():
            replacements[key] = str(value) if value is not None else ""

    # Replace {{key}} placeholders.
    for key, value in replacements.items():
        body = body.replace(f"{{{{{key}}}}}", value)

    return body
