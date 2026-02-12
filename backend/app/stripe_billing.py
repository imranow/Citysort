"""Stripe billing integration: Checkout, Webhooks, Customer Portal, plan enforcement."""

from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import HTTPException

from .config import (
    PLAN_FREE_DOCUMENT_LIMIT,
    PLAN_PRO_DOCUMENT_LIMIT,
    STRIPE_ENABLED,
    STRIPE_ENTERPRISE_LIFETIME_PRICE_ID,
    STRIPE_ENTERPRISE_MONTHLY_PRICE_ID,
    STRIPE_PRO_LIFETIME_PRICE_ID,
    STRIPE_PRO_MONTHLY_PRICE_ID,
    STRIPE_SECRET_KEY,
    STRIPE_WEBHOOK_SECRET,
)
from .repository import (
    count_workspace_documents_this_month,
    count_user_documents_this_month,
    create_payment_event,
    create_subscription,
    get_default_workspace_for_user,
    get_user_by_id,
    get_user_by_stripe_customer,
    get_workspace,
    get_workspace_by_stripe_customer,
    update_subscription_status,
    update_user_plan,
    update_workspace_plan,
)

logger = logging.getLogger("citysort.billing")

_PRICE_MAP: dict[tuple[str, str], str] = {}


def _init_price_map() -> None:
    global _PRICE_MAP
    _PRICE_MAP = {
        ("pro", "monthly"): STRIPE_PRO_MONTHLY_PRICE_ID,
        ("pro", "lifetime"): STRIPE_PRO_LIFETIME_PRICE_ID,
        ("enterprise", "monthly"): STRIPE_ENTERPRISE_MONTHLY_PRICE_ID,
        ("enterprise", "lifetime"): STRIPE_ENTERPRISE_LIFETIME_PRICE_ID,
    }


def _get_stripe():  # noqa: ANN202
    """Lazy import stripe and set API key."""
    if not STRIPE_ENABLED:
        raise HTTPException(status_code=400, detail="Stripe billing is not enabled.")
    try:
        import stripe
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="stripe package not installed. Install with: pip install stripe",
        )
    stripe.api_key = STRIPE_SECRET_KEY
    return stripe


def create_checkout_session(
    *,
    user_id: str,
    user_email: str,
    workspace_id: str | None = None,
    plan_tier: str,
    billing_type: str,
    success_url: str,
    cancel_url: str,
) -> str:
    """Create a Stripe Checkout Session and return the checkout URL."""
    stripe = _get_stripe()
    _init_price_map()

    if plan_tier not in ("pro", "enterprise"):
        raise HTTPException(status_code=400, detail="Invalid plan tier.")
    if billing_type not in ("monthly", "lifetime"):
        raise HTTPException(status_code=400, detail="Invalid billing type.")

    price_id = _PRICE_MAP.get((plan_tier, billing_type))
    if not price_id:
        raise HTTPException(
            status_code=400,
            detail=f"Price not configured for {plan_tier}/{billing_type}.",
        )

    mode = "subscription" if billing_type == "monthly" else "payment"
    metadata = {
        "user_id": user_id,
        "plan_tier": plan_tier,
        "billing_type": billing_type,
    }
    if workspace_id:
        metadata["workspace_id"] = workspace_id

    session_kwargs: dict[str, Any] = {
        "customer_email": user_email,
        "mode": mode,
        "line_items": [{"price": price_id, "quantity": 1}],
        "success_url": success_url,
        "cancel_url": cancel_url,
        "metadata": metadata,
    }
    if mode == "subscription":
        session_kwargs["subscription_data"] = {"metadata": metadata}

    try:
        session = stripe.checkout.Session.create(**session_kwargs)
    except Exception as exc:
        logger.error("Stripe Checkout creation failed: %s", exc)
        raise HTTPException(
            status_code=502, detail="Failed to create checkout session."
        )

    return session.url


def create_portal_session(*, stripe_customer_id: str, return_url: str) -> str:
    """Create a Stripe Customer Portal session and return the URL."""
    stripe = _get_stripe()
    try:
        session = stripe.billing_portal.Session.create(
            customer=stripe_customer_id,
            return_url=return_url,
        )
    except Exception as exc:
        logger.error("Stripe Portal creation failed: %s", exc)
        raise HTTPException(
            status_code=502, detail="Failed to create billing portal session."
        )
    return session.url


def handle_webhook_event(payload: bytes, sig_header: str) -> dict[str, Any]:
    """Verify and process a Stripe webhook event. Returns summary dict."""
    stripe = _get_stripe()

    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=500, detail="Webhook secret not configured.")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid payload.")
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid webhook signature.")

    event_type = event["type"]
    event_id = event["id"]
    data_object = event["data"]["object"]

    logger.info("Stripe webhook: type=%s id=%s", event_type, event_id)

    if event_type == "checkout.session.completed":
        _handle_checkout_completed(event_id, data_object)
    elif event_type == "invoice.paid":
        _handle_invoice_paid(event_id, data_object)
    elif event_type == "invoice.payment_failed":
        _handle_invoice_failed(event_id, data_object)
    elif event_type in (
        "customer.subscription.updated",
        "customer.subscription.deleted",
    ):
        _handle_subscription_change(event_id, event_type, data_object)
    else:
        logger.debug("Ignoring unhandled Stripe event type: %s", event_type)

    return {"received": True, "type": event_type}


def _resolve_workspace_id(
    user_id: str | None, preferred_workspace_id: str | None = None
) -> str | None:
    if preferred_workspace_id:
        return str(preferred_workspace_id)
    if not user_id:
        return None
    workspace = get_default_workspace_for_user(str(user_id))
    if not workspace:
        return None
    workspace_id = workspace.get("id")
    return str(workspace_id) if workspace_id else None


def _handle_checkout_completed(event_id: str, session: dict[str, Any]) -> None:
    metadata = session.get("metadata", {})
    user_id = metadata.get("user_id")
    workspace_id = _resolve_workspace_id(
        user_id,
        metadata.get("workspace_id"),
    )
    plan_tier = metadata.get("plan_tier", "pro")
    billing_type = metadata.get("billing_type", "monthly")
    customer_id = session.get("customer")
    subscription_id = session.get("subscription")
    amount = session.get("amount_total", 0)

    if not user_id:
        logger.warning("checkout.session.completed without user_id in metadata")
        return

    # Workspace-scoped billing: workspace plan is the source of truth.
    if workspace_id:
        update_workspace_plan(
            workspace_id,
            plan_tier=plan_tier,
            stripe_customer_id=customer_id,
        )
    else:
        # Backward compatibility for legacy single-workspace data.
        update_user_plan(user_id, plan_tier=plan_tier, stripe_customer_id=customer_id)

    # Create subscription record
    create_subscription(
        user_id=user_id,
        workspace_id=workspace_id,
        plan_tier=plan_tier,
        billing_type=billing_type,
        stripe_subscription_id=subscription_id,
        stripe_customer_id=customer_id,
        status="active",
    )

    # Log payment event
    try:
        create_payment_event(
            user_id=user_id,
            workspace_id=workspace_id,
            stripe_event_id=event_id,
            event_type="checkout.session.completed",
            amount_cents=amount,
            plan_tier=plan_tier,
            billing_type=billing_type,
            raw_payload=json.dumps(session),
        )
    except Exception:
        logger.warning("Duplicate payment event: %s (idempotent)", event_id)

    # Send plan upgrade email (fire-and-forget)
    try:
        from .account_emails import send_plan_upgrade_email

        user = get_user_by_id(user_id)
        if user:
            send_plan_upgrade_email(
                user["email"],
                plan_tier,
                user_id=user_id,
                workspace_id=workspace_id,
            )
    except Exception:
        logger.debug("Plan upgrade email failed (non-blocking)", exc_info=True)


def _handle_invoice_paid(event_id: str, invoice: dict[str, Any]) -> None:
    customer_id = invoice.get("customer")
    subscription_id = invoice.get("subscription")
    amount = invoice.get("amount_paid", 0)

    workspace = (
        get_workspace_by_stripe_customer(str(customer_id)) if customer_id else None
    )
    workspace_id = str(workspace["id"]) if workspace else None
    user = get_user_by_stripe_customer(customer_id) if customer_id else None
    user_id = user["id"] if user else None

    try:
        create_payment_event(
            user_id=user_id,
            workspace_id=workspace_id,
            stripe_event_id=event_id,
            event_type="invoice.paid",
            amount_cents=amount,
            raw_payload=json.dumps(invoice),
        )
    except Exception:
        logger.warning("Duplicate payment event: %s", event_id)

    # Update subscription period if we have a subscription_id
    if subscription_id:
        period_end = (
            invoice.get("lines", {}).get("data", [{}])[0].get("period", {}).get("end")
        )
        period_end_iso = None
        if period_end:
            from datetime import datetime, timezone

            period_end_iso = datetime.fromtimestamp(
                period_end, tz=timezone.utc
            ).isoformat()
        update_subscription_status(
            subscription_id,
            status="active",
            current_period_end=period_end_iso,
        )

    # Send payment receipt email (fire-and-forget).
    if user:
        try:
            from .account_emails import send_payment_receipt_email

            send_payment_receipt_email(
                user["email"],
                amount_cents=int(amount or 0),
                plan_tier=str(
                    (workspace or {}).get("plan_tier") or user.get("plan_tier", "pro")
                ),
                user_id=user["id"],
                workspace_id=workspace_id,
            )
        except Exception:
            logger.debug("Payment receipt email failed (non-blocking)", exc_info=True)


def _handle_invoice_failed(event_id: str, invoice: dict[str, Any]) -> None:
    customer_id = invoice.get("customer")
    subscription_id = invoice.get("subscription")

    workspace = (
        get_workspace_by_stripe_customer(str(customer_id)) if customer_id else None
    )
    workspace_id = str(workspace["id"]) if workspace else None
    user = get_user_by_stripe_customer(customer_id) if customer_id else None
    user_id = user["id"] if user else None

    try:
        create_payment_event(
            user_id=user_id,
            workspace_id=workspace_id,
            stripe_event_id=event_id,
            event_type="invoice.payment_failed",
            raw_payload=json.dumps(invoice),
        )
    except Exception:
        logger.warning("Duplicate payment event: %s", event_id)

    if subscription_id:
        update_subscription_status(subscription_id, status="past_due")

    logger.warning(
        "Invoice payment failed: customer=%s subscription=%s",
        customer_id,
        subscription_id,
    )


def _handle_subscription_change(
    event_id: str, event_type: str, subscription: dict[str, Any]
) -> None:
    subscription_id = subscription.get("id")
    customer_id = subscription.get("customer")
    status = subscription.get("status", "active")

    workspace = (
        get_workspace_by_stripe_customer(str(customer_id)) if customer_id else None
    )
    workspace_id = str(workspace["id"]) if workspace else None
    user = get_user_by_stripe_customer(customer_id) if customer_id else None
    user_id = user["id"] if user else None

    # Map Stripe statuses to our statuses
    status_map = {
        "active": "active",
        "past_due": "past_due",
        "canceled": "canceled",
        "unpaid": "past_due",
        "incomplete": "past_due",
        "incomplete_expired": "canceled",
    }
    mapped_status = status_map.get(status, "active")

    canceled_at = None
    if event_type == "customer.subscription.deleted" or mapped_status == "canceled":
        from datetime import datetime, timezone

        canceled_at = datetime.now(timezone.utc).isoformat()
        mapped_status = "canceled"

        # Revert workspace to free tier when subscription is canceled.
        if workspace_id:
            update_workspace_plan(workspace_id, plan_tier="free")
        elif user_id:
            # Backward compatibility for legacy single-workspace data.
            update_user_plan(user_id, plan_tier="free")

    if subscription_id:
        update_subscription_status(
            subscription_id,
            status=mapped_status,
            canceled_at=canceled_at,
        )

    try:
        create_payment_event(
            user_id=user_id,
            workspace_id=workspace_id,
            stripe_event_id=event_id,
            event_type=event_type,
            raw_payload=json.dumps(subscription),
        )
    except Exception:
        logger.warning("Duplicate payment event: %s", event_id)


def enforce_plan_limits(
    user_id: str, action: str, workspace_id: str | None = None
) -> None:
    """Raise HTTPException(403) if the active workspace plan blocks the action."""
    user = get_user_by_id(user_id)
    if not user:
        return  # No user = no enforcement (auth handles this)
    resolved_workspace_id = _resolve_workspace_id(user_id, workspace_id)
    workspace = get_workspace(resolved_workspace_id) if resolved_workspace_id else None
    plan = str((workspace or {}).get("plan_tier") or user.get("plan_tier", "free"))

    if action == "upload_document":
        if resolved_workspace_id:
            count = count_workspace_documents_this_month(resolved_workspace_id)
        else:
            count = count_user_documents_this_month(user_id)
        if plan == "free":
            if count >= PLAN_FREE_DOCUMENT_LIMIT:
                raise HTTPException(
                    status_code=403,
                    detail=f"Free plan limit of {PLAN_FREE_DOCUMENT_LIMIT} documents/month reached. Upgrade to Pro.",
                )
        elif plan == "pro":
            if count >= PLAN_PRO_DOCUMENT_LIMIT:
                raise HTTPException(
                    status_code=403,
                    detail=f"Pro plan limit of {PLAN_PRO_DOCUMENT_LIMIT} documents/month reached. Upgrade to Enterprise.",
                )

    elif action == "use_connector" and plan == "free":
        raise HTTPException(
            status_code=403,
            detail="Connectors require a Pro or Enterprise plan.",
        )

    elif action == "use_ai_classifier" and plan == "free":
        raise HTTPException(
            status_code=403,
            detail="AI classification requires a Pro or Enterprise plan.",
        )


def get_plan_info() -> list[dict[str, Any]]:
    """Return public plan information for the pricing page."""
    return [
        {
            "name": "Free",
            "monthly_price_cents": 0,
            "lifetime_price_cents": 0,
            "document_limit": PLAN_FREE_DOCUMENT_LIMIT,
            "features": [
                "50 documents/month",
                "Rules-based classification",
                "1 user",
                "30-day audit retention",
            ],
        },
        {
            "name": "Pro",
            "monthly_price_cents": 2900,
            "lifetime_price_cents": 29900,
            "document_limit": PLAN_PRO_DOCUMENT_LIMIT,
            "features": [
                "5,000 documents/month",
                "AI classification (Claude & GPT)",
                "All 10 connectors",
                "5 users",
                "Email notifications",
                "API access",
                "1-year audit retention",
            ],
        },
        {
            "name": "Enterprise",
            "monthly_price_cents": 9900,
            "lifetime_price_cents": 99900,
            "document_limit": None,
            "features": [
                "Unlimited documents",
                "AI classification (Claude & GPT)",
                "All 10 connectors",
                "Unlimited users",
                "Email notifications",
                "API access",
                "7-year audit retention",
                "Dedicated SLA support",
            ],
        },
    ]
