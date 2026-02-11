"""Automatic email triggers for document lifecycle events."""

from __future__ import annotations

import logging
from typing import Any, Optional

from .config import (
    AUTO_ACK_EMAIL_ENABLED,
    AUTO_MISSING_INFO_EMAIL_ENABLED,
    AUTO_STATUS_EMAIL_ENABLED,
)
from .emailer import email_configured, send_email
from .notifications import create_notification
from .repository import (
    create_audit_event,
    create_outbound_email,
    get_document,
    update_outbound_email,
)
from .templates import compose_template_email, list_templates

logger = logging.getLogger(__name__)


def _find_template_by_name(name_fragment: str) -> Optional[dict[str, Any]]:
    """Find first template whose name contains the given fragment (case-insensitive)."""
    templates = list_templates(limit=200)
    for t in templates:
        if name_fragment.lower() in t["name"].lower():
            return t
    return None


def _send_auto_email(
    *,
    template_name_hint: str,
    document_id: str,
    email_type: str,
    actor: str = "system_auto",
) -> bool:
    """Compose and send an automatic email for a document.

    Returns True if sent, False if skipped or failed.
    """
    if not email_configured():
        return False

    template = _find_template_by_name(template_name_hint)
    if not template:
        logger.debug(
            "No template matching '%s' found; skipping auto email.", template_name_hint
        )
        return False

    try:
        composed = compose_template_email(int(template["id"]), document_id)
    except (ValueError, KeyError) as exc:
        logger.debug("compose_template_email failed for %s: %s", document_id, exc)
        return False

    to_email = composed.get("to_email")
    if not to_email:
        return False  # no recipient — skip silently

    subject = composed.get("subject", "CitySort Update")
    body = composed.get("body", "")
    if not body:
        return False

    # Record outbound email.
    record = create_outbound_email(
        document_id=document_id,
        to_email=to_email,
        subject=subject,
        body=body,
        status="pending",
    )

    try:
        send_email(to_email=to_email, subject=subject, body=body)
        update_outbound_email(
            int(record["id"]),
            status="sent",
            sent_at=__import__("datetime")
            .datetime.now(__import__("datetime").timezone.utc)
            .isoformat(),
        )
        create_audit_event(
            document_id=document_id,
            action=f"auto_{email_type}_sent",
            actor=actor,
            details=f"to={to_email} subject={subject}",
        )
        create_notification(
            type="response_sent",
            title=f"Auto {email_type} sent: {composed.get('template_name', '')}",
            message=f"Sent to {to_email}",
            document_id=document_id,
        )
        logger.info(
            "Auto %s email sent to %s for doc %s", email_type, to_email, document_id
        )
        return True
    except Exception as exc:
        update_outbound_email(int(record["id"]), status="failed", error=str(exc))
        create_audit_event(
            document_id=document_id,
            action=f"auto_{email_type}_failed",
            actor=actor,
            details=f"to={to_email} error={exc}",
        )
        logger.warning("Auto %s email failed for %s: %s", email_type, document_id, exc)
        return False


def send_auto_acknowledgment(document_id: str) -> bool:
    """Send automatic acknowledgment email after document ingestion."""
    if not AUTO_ACK_EMAIL_ENABLED:
        return False
    return _send_auto_email(
        template_name_hint="acknowledgment",
        document_id=document_id,
        email_type="acknowledgment",
    )


def send_auto_missing_info(document_id: str) -> bool:
    """Send automatic missing-info request if document has missing fields."""
    if not AUTO_MISSING_INFO_EMAIL_ENABLED:
        return False
    doc = get_document(document_id)
    if not doc:
        return False
    missing = doc.get("missing_fields")
    if not missing or (isinstance(missing, list) and len(missing) == 0):
        return False
    return _send_auto_email(
        template_name_hint="request for information",
        document_id=document_id,
        email_type="missing_info",
    )


def send_auto_status_update(document_id: str, new_status: str) -> bool:
    """Send automatic status update email when document transitions to key status."""
    if not AUTO_STATUS_EMAIL_ENABLED:
        return False
    notify_statuses = {"approved", "completed", "assigned", "in_progress"}
    if new_status not in notify_statuses:
        return False
    return _send_auto_email(
        template_name_hint="status update",
        document_id=document_id,
        email_type="status_update",
    )
