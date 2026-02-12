"""Notification creation, listing, and webhook dispatch."""

from __future__ import annotations

import json
import logging
import threading
import urllib.request
from typing import Any, Optional

from .db import get_connection
from .repository import utcnow_iso

logger = logging.getLogger(__name__)


def create_notification(
    *,
    type: str,
    title: str,
    message: str = "",
    user_id: Optional[str] = None,
    document_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
) -> dict[str, Any]:
    resolved_workspace_id = workspace_id
    if resolved_workspace_id is None and document_id:
        with get_connection() as connection:
            row = connection.execute(
                "SELECT workspace_id FROM documents WHERE id = ?",
                (document_id,),
            ).fetchone()
            if row:
                resolved_workspace_id = row["workspace_id"]
    created_at = utcnow_iso()
    with get_connection() as connection:
        cursor = connection.execute(
            """INSERT INTO notifications (workspace_id, user_id, type, title, message, document_id, is_read, created_at)
               VALUES (?, ?, ?, ?, ?, ?, 0, ?)""",
            (
                resolved_workspace_id,
                user_id,
                type,
                title,
                message,
                document_id,
                created_at,
            ),
        )
        row = connection.execute(
            "SELECT * FROM notifications WHERE id = ?", (cursor.lastrowid,)
        ).fetchone()
    notification = dict(row)
    _fire_webhook(notification)
    return notification


def list_notifications(
    *,
    user_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
    unread_only: bool = False,
    limit: int = 50,
) -> list[dict[str, Any]]:
    query = "SELECT * FROM notifications"
    conditions: list[str] = []
    params: list[Any] = []
    if user_id:
        conditions.append("(user_id = ? OR user_id IS NULL)")
        params.append(user_id)
    if workspace_id is not None:
        conditions.append("workspace_id = ?")
        params.append(workspace_id)
    if unread_only:
        conditions.append("is_read = 0")
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    with get_connection() as connection:
        rows = connection.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def count_unread(
    *, user_id: Optional[str] = None, workspace_id: Optional[str] = None
) -> int:
    query = "SELECT COUNT(*) AS total FROM notifications WHERE is_read = 0"
    params: list[Any] = []
    if user_id:
        query += " AND (user_id = ? OR user_id IS NULL)"
        params.append(user_id)
    if workspace_id is not None:
        query += " AND workspace_id = ?"
        params.append(workspace_id)
    with get_connection() as connection:
        row = connection.execute(query, params).fetchone()
    return int(row["total"]) if row else 0


def mark_read(
    notification_id: int,
    *,
    user_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    read_at = utcnow_iso()
    query = "UPDATE notifications SET is_read = 1, read_at = ? WHERE id = ?"
    params: list[Any] = [read_at, notification_id]
    select_query = "SELECT * FROM notifications WHERE id = ?"
    select_params: list[Any] = [notification_id]
    if user_id:
        query += " AND (user_id = ? OR user_id IS NULL)"
        params.append(user_id)
        select_query += " AND (user_id = ? OR user_id IS NULL)"
        select_params.append(user_id)
    if workspace_id is not None:
        query += " AND workspace_id = ?"
        params.append(workspace_id)
        select_query += " AND workspace_id = ?"
        select_params.append(workspace_id)
    with get_connection() as connection:
        connection.execute(query, params)
        row = connection.execute(select_query, select_params).fetchone()
    return dict(row) if row else None


def mark_all_read(
    *, user_id: Optional[str] = None, workspace_id: Optional[str] = None
) -> int:
    read_at = utcnow_iso()
    query = "UPDATE notifications SET is_read = 1, read_at = ? WHERE is_read = 0"
    params: list[Any] = [read_at]
    if user_id:
        query += " AND (user_id = ? OR user_id IS NULL)"
        params.append(user_id)
    if workspace_id is not None:
        query += " AND workspace_id = ?"
        params.append(workspace_id)
    with get_connection() as connection:
        cursor = connection.execute(query, params)
    return cursor.rowcount


def _fire_webhook(notification: dict[str, Any]) -> None:
    from .config import WEBHOOK_ENABLED, WEBHOOK_URL

    if not WEBHOOK_ENABLED or not WEBHOOK_URL:
        return

    thread = threading.Thread(
        target=_post_webhook,
        args=(WEBHOOK_URL, notification),
        name="notifications-webhook",
        daemon=True,
    )
    thread.start()


def _post_webhook(webhook_url: str, notification: dict[str, Any]) -> None:
    try:
        data = json.dumps(notification).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=data,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        logger.debug("Webhook dispatch failed", exc_info=True)
