"""Automatic email triggers for document lifecycle events."""

from __future__ import annotations

import logging
from typing import Any, Optional

from .config import (
    AUTO_ACK_EMAIL_ENABLED,
    AUTO_ASSIGNMENT_EMAIL_ENABLED,
    AUTO_MISSING_INFO_EMAIL_ENABLED,
    AUTO_REVIEW_COMPLETE_EMAIL_ENABLED,
    AUTO_STATUS_EMAIL_ENABLED,
)
from .emailer import email_configured, send_email
from .notifications import create_notification
from .repository import (
    create_audit_event,
    create_outbound_email,
    get_document,
    get_user_by_id,
    get_user_email_preferences,
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


def send_assignment_notification(document_id: str, assigned_user_id: str) -> bool:
    """Send email to the user when a document is assigned to them."""
    if not AUTO_ASSIGNMENT_EMAIL_ENABLED:
        return False
    if not email_configured():
        return False

    user = get_user_by_id(assigned_user_id)
    if not user:
        return False

    # Check user email preference
    prefs = get_user_email_preferences(assigned_user_id)
    if not prefs.get("doc_assigned", True):
        return False

    doc = get_document(document_id)
    if not doc:
        return False

    to_email = user.get("email")
    if not to_email:
        return False

    name = user.get("full_name") or to_email.split("@")[0]
    filename = doc.get("filename", "Unknown")
    doc_type = doc.get("doc_type", "Unknown")
    department = doc.get("department", "Unknown")

    subject = f"CitySort AI — Document assigned to you: {filename}"
    body = (
        f"Hi {name},\n\n"
        f"A document has been assigned to you for review.\n\n"
        f"  Filename: {filename}\n"
        f"  Type: {doc_type}\n"
        f"  Department: {department}\n\n"
        "Please log in to your dashboard to review this document.\n\n"
        "— The CitySort AI Team"
    )

    record = create_outbound_email(
        document_id=document_id,
        to_email=to_email,
        subject=subject,
        body=body,
        status="pending",
    )

    try:
        send_email(to_email=to_email, subject=subject, body=body)
        from datetime import datetime, timezone

        update_outbound_email(
            int(record["id"]),
            status="sent",
            sent_at=datetime.now(timezone.utc).isoformat(),
        )
        create_audit_event(
            document_id=document_id,
            action="assignment_email_sent",
            actor="system_auto",
            details=f"to={to_email}",
        )
        logger.info("Assignment email sent to %s for doc %s", to_email, document_id)
        return True
    except Exception as exc:
        update_outbound_email(int(record["id"]), status="failed", error=str(exc))
        logger.warning("Assignment email failed for %s: %s", document_id, exc)
        return False


def send_review_complete_notification(document_id: str) -> bool:
    """Send email when a document review is completed (approved/corrected)."""
    if not AUTO_REVIEW_COMPLETE_EMAIL_ENABLED:
        return False
    if not email_configured():
        return False

    doc = get_document(document_id)
    if not doc:
        return False

    # Notify the assigned user if there is one
    assigned_to = doc.get("assigned_to")
    if not assigned_to:
        return False

    user = get_user_by_id(assigned_to)
    if not user:
        return False

    prefs = get_user_email_preferences(assigned_to)
    if not prefs.get("doc_review_complete", True):
        return False

    to_email = user.get("email")
    if not to_email:
        return False

    name = user.get("full_name") or to_email.split("@")[0]
    filename = doc.get("filename", "Unknown")
    status = doc.get("status", "approved")

    subject = f"CitySort AI — Document {status}: {filename}"
    body = (
        f"Hi {name},\n\n"
        f"A document assigned to you has been {status}.\n\n"
        f"  Filename: {filename}\n"
        f"  Status: {status}\n"
        f"  Department: {doc.get('department', 'Unknown')}\n\n"
        "You can view the details in your dashboard.\n\n"
        "— The CitySort AI Team"
    )

    record = create_outbound_email(
        document_id=document_id,
        to_email=to_email,
        subject=subject,
        body=body,
        status="pending",
    )

    try:
        send_email(to_email=to_email, subject=subject, body=body)
        from datetime import datetime, timezone

        update_outbound_email(
            int(record["id"]),
            status="sent",
            sent_at=datetime.now(timezone.utc).isoformat(),
        )
        create_audit_event(
            document_id=document_id,
            action="review_complete_email_sent",
            actor="system_auto",
            details=f"to={to_email} status={status}",
        )
        logger.info(
            "Review complete email sent to %s for doc %s", to_email, document_id
        )
        return True
    except Exception as exc:
        update_outbound_email(int(record["id"]), status="failed", error=str(exc))
        logger.warning("Review complete email failed for %s: %s", document_id, exc)
        return False
