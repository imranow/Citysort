"""Account-level email notifications (welcome, billing, invitations)."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from .config import EMAIL_ENABLED
from .emailer import email_configured, send_email
from .repository import (
    create_outbound_email,
    get_user_by_email,
    get_user_email_preferences,
    update_outbound_email,
)

logger = logging.getLogger("citysort.account_emails")

# Sentinel document_id for account-level emails (not tied to a document).
_ACCOUNT_DOC_ID = "__account__"


def _user_wants_email(
    *, user_id: Optional[str], to_email: str, preference_key: str
) -> bool:
    """Check if the user has opted in for this email type."""
    resolved_user_id = user_id
    if not resolved_user_id:
        existing = get_user_by_email(to_email)
        resolved_user_id = existing["id"] if existing else None
    if not resolved_user_id:
        return True  # No user context -> send by default.
    prefs = get_user_email_preferences(resolved_user_id)
    return prefs.get(preference_key, True)  # Default: opted in


def _send_account_email(
    *,
    to_email: str,
    subject: str,
    body: str,
    email_type: str,
    user_id: Optional[str] = None,
    preference_key: Optional[str] = None,
    workspace_id: Optional[str] = None,
) -> bool:
    """Send an account-level email, respecting preferences and config."""
    if not EMAIL_ENABLED or not email_configured():
        return False

    if preference_key and not _user_wants_email(
        user_id=user_id, to_email=to_email, preference_key=preference_key
    ):
        logger.debug("User %s opted out of %s emails", user_id, preference_key)
        return False

    record = create_outbound_email(
        document_id=_ACCOUNT_DOC_ID,
        workspace_id=workspace_id,
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
            sent_at=datetime.now(timezone.utc).isoformat(),
        )
        logger.info("Account email [%s] sent to %s", email_type, to_email)
        return True
    except Exception as exc:
        update_outbound_email(int(record["id"]), status="failed", error=str(exc))
        logger.warning(
            "Account email [%s] failed for %s: %s", email_type, to_email, exc
        )
        return False


def send_welcome_email(
    user_email: str,
    full_name: Optional[str] = None,
    user_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
) -> bool:
    """Send welcome email after user signup."""
    name = full_name or user_email.split("@")[0]
    subject = "Welcome to CitySort AI"
    body = (
        f"Hi {name},\n\n"
        "Welcome to CitySort AI! Your account has been created successfully.\n\n"
        "You can now upload documents, configure routing rules, and start "
        "automating your document workflow.\n\n"
        "Getting started:\n"
        "  1. Upload your first document from the dashboard\n"
        "  2. Configure routing rules for your department\n"
        "  3. Set up connectors to import from external systems\n\n"
        "If you have any questions, please contact your administrator.\n\n"
        "— The CitySort AI Team"
    )
    return _send_account_email(
        to_email=user_email,
        subject=subject,
        body=body,
        email_type="welcome",
        user_id=user_id,
        preference_key="account_welcome",
        workspace_id=workspace_id,
    )


def send_plan_upgrade_email(
    user_email: str,
    plan_tier: str,
    user_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
) -> bool:
    """Send plan upgrade confirmation email."""
    tier_display = plan_tier.capitalize()
    subject = f"CitySort AI — Your plan has been upgraded to {tier_display}"
    body = (
        f"Your CitySort AI plan has been upgraded to {tier_display}.\n\n"
        f"You now have access to all {tier_display} features including "
        "AI classification, connectors, and expanded document limits.\n\n"
        "Visit your dashboard to explore the new capabilities.\n\n"
        "— The CitySort AI Team"
    )
    return _send_account_email(
        to_email=user_email,
        subject=subject,
        body=body,
        email_type="plan_upgrade",
        user_id=user_id,
        preference_key="account_plan_change",
        workspace_id=workspace_id,
    )


def send_payment_receipt_email(
    user_email: str,
    amount_cents: int,
    plan_tier: str,
    user_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
) -> bool:
    """Send payment receipt email."""
    amount_display = f"${amount_cents / 100:.2f}"
    tier_display = plan_tier.capitalize() if plan_tier else "Pro"
    subject = f"CitySort AI — Payment receipt ({amount_display})"
    body = (
        f"Thank you for your payment of {amount_display} for the "
        f"CitySort AI {tier_display} plan.\n\n"
        "You can manage your subscription and view invoices from the "
        "billing section in your dashboard.\n\n"
        "— The CitySort AI Team"
    )
    return _send_account_email(
        to_email=user_email,
        subject=subject,
        body=body,
        email_type="payment_receipt",
        user_id=user_id,
        preference_key="account_payment_receipt",
        workspace_id=workspace_id,
    )


def send_invitation_email(
    to_email: str,
    invite_link: str,
    inviter_name: Optional[str] = None,
    user_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
) -> bool:
    """Send invitation email with signup link."""
    who = inviter_name or "An administrator"
    subject = "You've been invited to CitySort AI"
    body = (
        f"{who} has invited you to join CitySort AI.\n\n"
        "CitySort AI is an AI-powered document classification and routing "
        "platform for local government.\n\n"
        f"Click the link below to create your account:\n{invite_link}\n\n"
        "This invitation link will expire in 7 days.\n\n"
        "— The CitySort AI Team"
    )
    return _send_account_email(
        to_email=to_email,
        subject=subject,
        body=body,
        email_type="invitation",
        user_id=user_id,
        preference_key="account_invitation",
        workspace_id=workspace_id,
    )
