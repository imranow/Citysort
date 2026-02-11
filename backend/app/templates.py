"""Response template CRUD and rendering."""

from __future__ import annotations

import re
from typing import Any, Optional

from .db import get_connection
from .repository import get_document, utcnow_iso

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


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


def _document_context(document: dict[str, Any]) -> dict[str, str]:
    replacements: dict[str, str] = {
        "id": str(document.get("id", "")),
        "filename": str(document.get("filename", "")),
        "doc_type": str(document.get("doc_type", "")),
        "department": str(document.get("department", "")),
        "status": str(document.get("status", "")),
        "urgency": str(document.get("urgency", "")),
    }
    fields = document.get("extracted_fields", {})
    if isinstance(fields, dict):
        for key, value in fields.items():
            replacements[key] = str(value) if value is not None else ""
    return replacements


def _resolve_recipient_email(document: dict[str, Any]) -> Optional[str]:
    fields = document.get("extracted_fields", {})
    if not isinstance(fields, dict):
        return None
    for candidate_key in ("applicant_email", "contact_email", "sender_email", "email"):
        value = fields.get(candidate_key)
        if value is None:
            continue
        email = str(value).strip()
        if EMAIL_RE.match(email):
            return email
    return None


def _render_body(template_body: str, context: dict[str, str]) -> str:
    body = template_body
    for key, value in context.items():
        body = body.replace(f"{{{{{key}}}}}", value)
    return body


def render_template(template_id: int, document_id: str) -> str:
    template = get_template(template_id)
    if not template:
        raise ValueError("Template not found")
    document = get_document(document_id)
    if not document:
        raise ValueError("Document not found")
    context = _document_context(document)
    return _render_body(template["template_body"], context)


def compose_template_email(template_id: int, document_id: str) -> dict[str, Any]:
    template = get_template(template_id)
    if not template:
        raise ValueError("Template not found")
    document = get_document(document_id)
    if not document:
        raise ValueError("Document not found")

    context = _document_context(document)
    body = _render_body(template["template_body"], context)
    recipient = _resolve_recipient_email(document)
    subject = f"{template['name']} - {context.get('filename', '').strip() or 'CitySort Update'}"
    return {
        "template_id": int(template["id"]),
        "template_name": str(template["name"]),
        "document_id": str(document_id),
        "to_email": recipient,
        "subject": subject,
        "body": body,
    }
