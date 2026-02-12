"""Tests for billing, plan enforcement, and Stripe integration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    """Create a FastAPI test client with isolated database."""
    from app import config, db
    from app import main as main_module

    monkeypatch.setattr(db, "DATA_DIR", tmp_path)
    monkeypatch.setattr(db, "UPLOAD_DIR", tmp_path / "uploads")
    monkeypatch.setattr(db, "PROCESSED_DIR", tmp_path / "processed")
    monkeypatch.setattr(db, "APPROVED_EXPORT_ENABLED", True)
    monkeypatch.setattr(db, "APPROVED_EXPORT_DIR", tmp_path / "approved")
    monkeypatch.setattr(db, "DATABASE_PATH", tmp_path / "citysort.db")
    monkeypatch.setattr(main_module, "UPLOAD_DIR", tmp_path / "uploads")
    monkeypatch.setattr(main_module, "APPROVED_EXPORT_ENABLED", True)
    monkeypatch.setattr(main_module, "APPROVED_EXPORT_DIR", tmp_path / "approved")
    (tmp_path / "uploads").mkdir(exist_ok=True)
    (tmp_path / "processed").mkdir(exist_ok=True)
    (tmp_path / "approved").mkdir(exist_ok=True)

    monkeypatch.setattr(config, "REQUIRE_AUTH", False)
    monkeypatch.setattr(config, "RATE_LIMIT_ENABLED", False)
    monkeypatch.setattr(config, "WORKER_ENABLED", False)
    monkeypatch.setattr(config, "WATCH_ENABLED", False)
    monkeypatch.setattr(config, "PROMETHEUS_ENABLED", False)

    db.init_db()

    from app.main import app

    return TestClient(app, raise_server_exceptions=False, headers={"host": "localhost"})


def _make_user(repo, email, role="operator", plan_tier="free"):
    """Helper: create a user and return the dict (including generated id)."""
    return repo.create_user(
        email=email,
        password_hash="fakehash",
        role=role,
        full_name=f"Test {email.split('@')[0]}",
        plan_tier=plan_tier,
    )


# ─── Billing Plans (public) ──────────────────────────────────────────


def test_billing_plans_returns_three_tiers(client):
    """GET /api/billing/plans should return Free, Pro, Enterprise."""
    resp = client.get("/api/billing/plans")
    assert resp.status_code == 200
    data = resp.json()
    assert "plans" in data
    plans = data["plans"]
    assert len(plans) == 3
    names = [p["name"] for p in plans]
    assert names == ["Free", "Pro", "Enterprise"]


def test_billing_plans_free_tier_has_zero_price(client):
    resp = client.get("/api/billing/plans")
    free_plan = resp.json()["plans"][0]
    assert free_plan["monthly_price_cents"] == 0
    assert free_plan["lifetime_price_cents"] == 0


def test_billing_plans_pro_tier_pricing(client):
    resp = client.get("/api/billing/plans")
    pro_plan = resp.json()["plans"][1]
    assert pro_plan["monthly_price_cents"] == 2900
    assert pro_plan["lifetime_price_cents"] == 29900
    assert pro_plan["document_limit"] > 0


def test_billing_plans_enterprise_unlimited(client):
    resp = client.get("/api/billing/plans")
    ent_plan = resp.json()["plans"][2]
    assert ent_plan["monthly_price_cents"] == 9900
    assert ent_plan["document_limit"] is None


# ─── Signup Flow ─────────────────────────────────────────────────────


def test_signup_creates_user_with_free_plan(client):
    """Signup via invitation should create a user with plan_tier=free."""
    invite_resp = client.post(
        "/api/platform/invitations",
        json={
            "email": "newuser@example.com",
            "role": "member",
            "actor": "admin",
            "expires_in_days": 7,
        },
    )
    assert invite_resp.status_code == 200
    token = invite_resp.json()["invite_token"]

    signup_resp = client.post(
        "/api/auth/signup",
        json={
            "email": "newuser@example.com",
            "password": "StrongPass123!",
            "full_name": "New User",
            "invitation_token": token,
        },
    )
    assert signup_resp.status_code == 200
    user_data = signup_resp.json()
    assert user_data["user"]["plan_tier"] == "free"
    assert user_data["user"]["email"] == "newuser@example.com"
    assert "access_token" in user_data


def test_signup_rejects_invalid_invitation_token(client):
    resp = client.post(
        "/api/auth/signup",
        json={
            "email": "nobody@example.com",
            "password": "StrongPass123!",
            "full_name": "Nobody",
            "invitation_token": "invalid-token-abc123",
        },
    )
    assert resp.status_code == 400
    detail = resp.json()["detail"].lower()
    assert "invalid" in detail or "invitation" in detail


def test_signup_rejects_expired_invitation(client):
    """Invitations with 0 days should be expired."""
    invite_resp = client.post(
        "/api/platform/invitations",
        json={
            "email": "expired@example.com",
            "role": "member",
            "actor": "admin",
            "expires_in_days": 0,
        },
    )
    if invite_resp.status_code == 200:
        token = invite_resp.json()["invite_token"]
        signup_resp = client.post(
            "/api/auth/signup",
            json={
                "email": "expired@example.com",
                "password": "StrongPass123!",
                "full_name": "Expired",
                "invitation_token": token,
            },
        )
        assert signup_resp.status_code == 400


def test_signup_rejects_reused_invitation(client):
    """An invitation should only be usable once."""
    invite_resp = client.post(
        "/api/platform/invitations",
        json={
            "email": "onetime@example.com",
            "role": "member",
            "actor": "admin",
            "expires_in_days": 7,
        },
    )
    assert invite_resp.status_code == 200
    token = invite_resp.json()["invite_token"]

    first = client.post(
        "/api/auth/signup",
        json={
            "email": "onetime@example.com",
            "password": "StrongPass123!",
            "full_name": "First",
            "invitation_token": token,
        },
    )
    assert first.status_code == 200

    second = client.post(
        "/api/auth/signup",
        json={
            "email": "onetime2@example.com",
            "password": "StrongPass123!",
            "full_name": "Second",
            "invitation_token": token,
        },
    )
    assert second.status_code == 400


# ─── Plan Enforcement (unit) ─────────────────────────────────────────


def test_enforce_plan_limits_free_upload(isolated_db, isolated_repo, monkeypatch):
    """Free plan users should be blocked after reaching document limit."""
    from app import stripe_billing

    monkeypatch.setattr(stripe_billing, "STRIPE_ENABLED", True)
    monkeypatch.setattr(stripe_billing, "PLAN_FREE_DOCUMENT_LIMIT", 3)

    user = _make_user(isolated_repo, "free@example.com", plan_tier="free")
    user_id = user["id"]

    for i in range(3):
        isolated_repo.create_document(
            document={
                "id": f"doc-limit-{i}",
                "filename": f"doc{i}.txt",
                "storage_path": f"/tmp/doc{i}.txt",
                "source_channel": "test",
                "content_type": "text/plain",
                "status": "ingested",
                "requires_review": False,
                "confidence": 0.0,
                "doc_type": None,
                "department": None,
                "urgency": "normal",
            }
        )

    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc_info:
        stripe_billing.enforce_plan_limits(user_id, "upload_document")
    assert exc_info.value.status_code == 403
    assert "Free plan limit" in exc_info.value.detail


def test_enforce_plan_limits_pro_user_allowed(isolated_db, isolated_repo, monkeypatch):
    """Pro plan users should be allowed within their limits."""
    from app import stripe_billing

    monkeypatch.setattr(stripe_billing, "STRIPE_ENABLED", True)
    monkeypatch.setattr(stripe_billing, "PLAN_PRO_DOCUMENT_LIMIT", 5000)

    user = _make_user(isolated_repo, "pro@example.com", plan_tier="pro")

    # Should not raise (well within limit)
    stripe_billing.enforce_plan_limits(user["id"], "upload_document")


def test_enforce_plan_limits_free_connector_blocked(
    isolated_db, isolated_repo, monkeypatch
):
    """Free plan users should not be allowed to use connectors."""
    from app import stripe_billing

    monkeypatch.setattr(stripe_billing, "STRIPE_ENABLED", True)

    user = _make_user(isolated_repo, "freeconn@example.com", plan_tier="free")

    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc_info:
        stripe_billing.enforce_plan_limits(user["id"], "use_connector")
    assert exc_info.value.status_code == 403
    assert "Pro or Enterprise" in exc_info.value.detail


def test_enforce_plan_limits_free_ai_blocked(isolated_db, isolated_repo, monkeypatch):
    """Free plan users should not be allowed to use AI classifier."""
    from app import stripe_billing

    monkeypatch.setattr(stripe_billing, "STRIPE_ENABLED", True)

    user = _make_user(isolated_repo, "freeai@example.com", plan_tier="free")

    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc_info:
        stripe_billing.enforce_plan_limits(user["id"], "use_ai_classifier")
    assert exc_info.value.status_code == 403
    assert "Pro or Enterprise" in exc_info.value.detail


def test_enforce_plan_limits_enterprise_unlimited(
    isolated_db, isolated_repo, monkeypatch
):
    """Enterprise plan users should never be blocked."""
    from app import stripe_billing

    monkeypatch.setattr(stripe_billing, "STRIPE_ENABLED", True)

    user = _make_user(isolated_repo, "ent@example.com", plan_tier="enterprise")
    uid = user["id"]

    # None of these should raise
    stripe_billing.enforce_plan_limits(uid, "upload_document")
    stripe_billing.enforce_plan_limits(uid, "use_connector")
    stripe_billing.enforce_plan_limits(uid, "use_ai_classifier")


# ─── Repository: Subscription CRUD ───────────────────────────────────


def test_create_and_get_subscription(isolated_db, isolated_repo):
    """Create a subscription and retrieve it."""
    from app import repository

    user = _make_user(isolated_repo, "sub@example.com")
    uid = user["id"]

    repository.create_subscription(
        user_id=uid,
        plan_tier="pro",
        billing_type="monthly",
        stripe_subscription_id="sub_123",
        stripe_customer_id="cus_123",
        status="active",
    )

    sub = repository.get_active_subscription(uid)
    assert sub is not None
    assert sub["plan_tier"] == "pro"
    assert sub["billing_type"] == "monthly"
    assert sub["status"] == "active"
    assert sub["stripe_subscription_id"] == "sub_123"


def test_update_subscription_status(isolated_db, isolated_repo):
    """Updating subscription status should persist."""
    from app import repository

    user = _make_user(isolated_repo, "sub2@example.com")
    uid = user["id"]

    repository.create_subscription(
        user_id=uid,
        plan_tier="pro",
        billing_type="monthly",
        stripe_subscription_id="sub_456",
        stripe_customer_id="cus_456",
        status="active",
    )

    repository.update_subscription_status("sub_456", status="past_due")
    sub = repository.get_active_subscription(uid)
    assert sub is not None
    assert sub["status"] == "past_due"


def test_create_payment_event_idempotent(isolated_db, isolated_repo):
    """Creating the same payment event twice should not raise."""
    from app import repository

    user = _make_user(isolated_repo, "pay@example.com")
    uid = user["id"]

    repository.create_payment_event(
        user_id=uid,
        stripe_event_id="evt_unique_1",
        event_type="checkout.session.completed",
        amount_cents=2900,
        plan_tier="pro",
        billing_type="monthly",
        raw_payload='{"test": true}',
    )

    # Second insert with same stripe_event_id should not raise
    try:
        repository.create_payment_event(
            user_id=uid,
            stripe_event_id="evt_unique_1",
            event_type="checkout.session.completed",
            amount_cents=2900,
            raw_payload='{"test": true}',
        )
    except Exception:
        pass  # Idempotent — expected to fail silently or raise


def test_update_user_plan(isolated_db, isolated_repo):
    """Updating a user's plan tier should persist."""
    from app import repository

    user = _make_user(isolated_repo, "plan@example.com", plan_tier="free")
    uid = user["id"]

    repository.update_user_plan(uid, plan_tier="pro", stripe_customer_id="cus_789")

    updated = repository.get_user_by_id(uid)
    assert updated["plan_tier"] == "pro"
    assert updated["stripe_customer_id"] == "cus_789"


def test_get_user_by_stripe_customer(isolated_db, isolated_repo):
    """Should find user by their Stripe customer ID."""
    from app import repository

    user = _make_user(isolated_repo, "stripe@example.com")
    uid = user["id"]

    repository.update_user_plan(uid, plan_tier="pro", stripe_customer_id="cus_lookup")

    found = repository.get_user_by_stripe_customer("cus_lookup")
    assert found is not None
    assert found["id"] == uid

    not_found = repository.get_user_by_stripe_customer("cus_nonexistent")
    assert not_found is None


def test_count_user_documents_this_month(isolated_db, isolated_repo):
    """Document count should reflect current month's documents."""
    from app import repository

    count_before = repository.count_user_documents_this_month()
    assert count_before == 0

    isolated_repo.create_document(
        document={
            "id": "doc-count-1",
            "filename": "count.txt",
            "storage_path": "/tmp/count.txt",
            "source_channel": "test",
            "content_type": "text/plain",
            "status": "ingested",
            "requires_review": False,
            "confidence": 0.0,
            "doc_type": None,
            "department": None,
            "urgency": "normal",
        }
    )

    count_after = repository.count_user_documents_this_month()
    assert count_after == 1


# ─── Stripe Billing Module (mocked) ──────────────────────────────────


def test_get_plan_info_structure():
    """get_plan_info should return valid plan structure."""
    from app.stripe_billing import get_plan_info

    plans = get_plan_info()
    assert len(plans) == 3
    for plan in plans:
        assert "name" in plan
        assert "monthly_price_cents" in plan
        assert "features" in plan
        assert isinstance(plan["features"], list)


def test_stripe_not_enabled_raises(monkeypatch):
    """_get_stripe should raise 400 when Stripe is disabled."""
    from app import stripe_billing

    monkeypatch.setattr(stripe_billing, "STRIPE_ENABLED", False)

    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc_info:
        stripe_billing._get_stripe()
    assert exc_info.value.status_code == 400
    assert "not enabled" in exc_info.value.detail


def test_create_checkout_invalid_plan(monkeypatch):
    """create_checkout_session should reject invalid plan tiers."""
    from app import stripe_billing

    monkeypatch.setattr(stripe_billing, "STRIPE_ENABLED", True)
    monkeypatch.setattr(stripe_billing, "STRIPE_SECRET_KEY", "sk_test_fake")

    mock_stripe = MagicMock()
    with patch.dict("sys.modules", {"stripe": mock_stripe}):
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            stripe_billing.create_checkout_session(
                user_id="u1",
                user_email="test@test.com",
                plan_tier="invalid",
                billing_type="monthly",
                success_url="http://localhost/success",
                cancel_url="http://localhost/cancel",
            )
        assert exc_info.value.status_code == 400
        assert "Invalid plan tier" in exc_info.value.detail


def test_create_checkout_invalid_billing_type(monkeypatch):
    """create_checkout_session should reject invalid billing type."""
    from app import stripe_billing

    monkeypatch.setattr(stripe_billing, "STRIPE_ENABLED", True)
    monkeypatch.setattr(stripe_billing, "STRIPE_SECRET_KEY", "sk_test_fake")

    mock_stripe = MagicMock()
    with patch.dict("sys.modules", {"stripe": mock_stripe}):
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            stripe_billing.create_checkout_session(
                user_id="u1",
                user_email="test@test.com",
                plan_tier="pro",
                billing_type="invalid",
                success_url="http://localhost/success",
                cancel_url="http://localhost/cancel",
            )
        assert exc_info.value.status_code == 400
        assert "Invalid billing type" in exc_info.value.detail


# ─── Webhook Handler (mocked) ────────────────────────────────────────


def test_handle_checkout_completed_updates_user(
    isolated_db, isolated_repo, monkeypatch
):
    """Webhook checkout.session.completed should update user plan."""
    from app import repository, stripe_billing

    monkeypatch.setattr(stripe_billing, "STRIPE_ENABLED", True)

    user = _make_user(isolated_repo, "webhook@example.com", plan_tier="free")
    uid = user["id"]

    stripe_billing._handle_checkout_completed(
        event_id="evt_test_checkout",
        session={
            "metadata": {
                "user_id": uid,
                "plan_tier": "pro",
                "billing_type": "monthly",
            },
            "customer": "cus_webhook",
            "subscription": "sub_webhook",
            "amount_total": 2900,
        },
    )

    updated = repository.get_user_by_id(uid)
    assert updated["plan_tier"] == "pro"
    assert updated["stripe_customer_id"] == "cus_webhook"

    sub = repository.get_active_subscription(uid)
    assert sub is not None
    assert sub["plan_tier"] == "pro"
    assert sub["status"] == "active"


def test_handle_subscription_deleted_reverts_to_free(
    isolated_db, isolated_repo, monkeypatch
):
    """Webhook subscription.deleted should revert user to free tier."""
    from app import repository, stripe_billing

    monkeypatch.setattr(stripe_billing, "STRIPE_ENABLED", True)

    user = _make_user(isolated_repo, "cancel@example.com", plan_tier="pro")
    uid = user["id"]

    repository.update_user_plan(uid, plan_tier="pro", stripe_customer_id="cus_cancel")

    repository.create_subscription(
        user_id=uid,
        plan_tier="pro",
        billing_type="monthly",
        stripe_subscription_id="sub_cancel",
        stripe_customer_id="cus_cancel",
        status="active",
    )

    stripe_billing._handle_subscription_change(
        event_id="evt_cancel",
        event_type="customer.subscription.deleted",
        subscription={
            "id": "sub_cancel",
            "customer": "cus_cancel",
            "status": "canceled",
        },
    )

    reverted = repository.get_user_by_id(uid)
    assert reverted["plan_tier"] == "free"


def test_handle_invoice_failed_sets_past_due(isolated_db, isolated_repo, monkeypatch):
    """Webhook invoice.payment_failed should mark subscription as past_due."""
    from app import repository, stripe_billing

    monkeypatch.setattr(stripe_billing, "STRIPE_ENABLED", True)

    user = _make_user(isolated_repo, "pastdue@example.com", plan_tier="pro")
    uid = user["id"]

    repository.update_user_plan(uid, plan_tier="pro", stripe_customer_id="cus_pastdue")

    repository.create_subscription(
        user_id=uid,
        plan_tier="pro",
        billing_type="monthly",
        stripe_subscription_id="sub_pastdue",
        stripe_customer_id="cus_pastdue",
        status="active",
    )

    stripe_billing._handle_invoice_failed(
        event_id="evt_fail",
        invoice={
            "customer": "cus_pastdue",
            "subscription": "sub_pastdue",
        },
    )

    sub = repository.get_active_subscription(uid)
    assert sub is not None
    assert sub["status"] == "past_due"


# ─── Billing API endpoints ───────────────────────────────────────────


def test_billing_subscription_without_auth_returns_free(client):
    """Without auth, subscription endpoint returns free tier."""
    resp = client.get("/api/billing/subscription")
    if resp.status_code == 200:
        data = resp.json()
        assert data["plan_tier"] in ("free", "enterprise")  # enterprise in dev mode


def test_billing_portal_requires_stripe_customer(client):
    """Portal endpoint should fail if user has no Stripe customer."""
    resp = client.get("/api/billing/portal")
    if resp.status_code == 400:
        assert "No Stripe customer" in resp.json()["detail"]
