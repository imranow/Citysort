from __future__ import annotations

from contextlib import asynccontextmanager
import json
import logging
import mimetypes
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .auth import (
    authenticate_user,
    authorize_request,
    bootstrap_admin,
    create_access_token,
    create_user_account,
    get_workspace_role as get_user_workspace_role,
    get_users,
    role_allows,
    set_user_role,
)
from starlette.responses import RedirectResponse

from .config import (
    APPROVED_EXPORT_DIR,
    APPROVED_EXPORT_ENABLED,
    ANTHROPIC_API_KEY,
    APP_ENV,
    AUTH_SECRET,
    AUTH_SECRET_PLACEHOLDER_VALUES,
    CORS_ALLOW_CREDENTIALS,
    CORS_ALLOWED_ORIGINS,
    DATABASE_BACKEND,
    DEPLOY_PROVIDER,
    ENFORCE_HTTPS,
    IS_PRODUCTION,
    CLASSIFIER_PROVIDER,
    CONFIDENCE_THRESHOLD,
    FORCE_REVIEW_DOC_TYPES,
    OCR_PROVIDER,
    PROMETHEUS_ENABLED,
    RATE_LIMIT_AI_PER_WINDOW,
    RATE_LIMIT_DEFAULT_PER_WINDOW,
    RATE_LIMIT_ENABLED,
    RATE_LIMIT_UPLOAD_PER_WINDOW,
    RATE_LIMIT_WINDOW_SECONDS,
    REQUIRE_AUTH,
    STRICT_APPROVAL_ROLE,
    STRICT_AUTH_SECRET,
    STRIPE_ENABLED,
    STRIPE_PUBLISHABLE_KEY,
    TRUSTED_HOSTS,
    UPLOAD_DIR,
)
from .db_import import (
    ExternalDatabaseError,
    coerce_row_content_to_bytes,
    connect_external_database,
    fetch_import_rows,
    get_row_value,
)
from .db import get_connection, init_db
from .deployments import deployment_provider_health, trigger_manual_deployment
from .emailer import email_configured, send_email
from .jobs import (
    enqueue_document_processing,
    get_job_by_id,
    get_jobs,
    start_job_worker,
    stop_job_worker,
)
from .logging_setup import configure_logging
from .observability import (
    init_observability,
    metrics_response,
    observe_request,
    start_timer,
)
from .document_tasks import process_document_by_id
from .pipeline import route_document
from .repository import (
    add_workspace_member,
    count_api_keys,
    count_invitations,
    count_unassigned_manual_documents,
    create_api_key,
    create_audit_event,
    create_deployment,
    create_document,
    create_outbound_email,
    create_invitation,
    create_workspace,
    get_latest_deployment,
    get_analytics_snapshot,
    get_document,
    get_default_workspace_for_user,
    get_queue_snapshot,
    get_workspace,
    list_user_workspaces,
    list_workspace_members,
    list_api_keys,
    list_audit_events,
    list_deployments,
    list_documents,
    list_invitations,
    list_unassigned_manual_documents,
    list_overdue_documents,
    revoke_api_key,
    get_active_subscription,
    mark_invitation_accepted,
    remove_workspace_member,
    validate_invitation,
    update_outbound_email,
    update_document,
    update_workspace,
    get_user_email_preferences,
    update_user_email_preferences,
)
from .notifications import (
    count_unread,
    create_notification,
    list_notifications,
    mark_all_read,
    mark_read,
)
from .rules import get_active_rules, get_rules_path, reset_rules_to_default, save_rules
from .templates import (
    compose_template_email,
    create_template as create_template_record,
    delete_template,
    get_template,
    list_templates,
    render_template,
    update_template,
)
from .stripe_billing import (
    create_checkout_session,
    create_portal_session,
    enforce_plan_limits,
    get_plan_info,
    handle_webhook_event,
)
from .watcher import start_watcher, stop_watcher
from .security import (
    SlidingWindowRateLimiter,
    UploadValidationError,
    apply_security_headers,
    client_ip,
    should_block_insecure_request,
    validate_upload,
)
from .storage import (
    read_document_bytes,
    validate_encryption_configuration,
    write_document_bytes,
)
from .connectors.base import ConnectorError, get_connector
from .connectors.importer import import_from_connector, get_sync_count

# Register all connectors so they are available via get_connector()
from .connectors import (  # noqa: F401
    servicenow,
    confluence,
    salesforce,
    gcs,
    s3,
    jira_connector,
    sharepoint,
)
from .schemas import (
    AnalyticsResponse,
    AutomationAnthropicSweepRequest,
    AutomationAnthropicSweepResponse,
    AutomationAutoAssignRequest,
    AutomationAutoAssignResponse,
    AssignRequest,
    AuthBootstrapRequest,
    AuthLoginRequest,
    AuthResponse,
    ApiKeyCreateRequest,
    ApiKeyCreateResponse,
    ApiKeyListResponse,
    ApiKeyRecord,
    AuditTrailResponse,
    BulkActionRequest,
    BulkActionResponse,
    ConnectivityResponse,
    ConnectorConfigSaveRequest,
    ConnectorConfigResponse,
    ConnectorImportRequest,
    ConnectorImportResponse,
    ConnectorTestRequest,
    ConnectorTestResponse,
    DatabaseImportRequest,
    DatabaseImportResponse,
    DeploymentListResponse,
    DeploymentRecord,
    DocumentListResponse,
    DocumentResponse,
    JobListResponse,
    JobRecord,
    InvitationCreateRequest,
    InvitationCreateResponse,
    InvitationListResponse,
    ManualDeploymentRequest,
    NotificationListResponse,
    NotificationRecord,
    PlatformSummaryResponse,
    QueueResponse,
    RulesConfigResponse,
    RulesConfigUpdate,
    ReviewRequest,
    ResponseEmailSendRequest,
    ResponseEmailSendResponse,
    ServiceHealth,
    TemplateComposeResponse,
    TemplateCreateRequest,
    TemplateListResponse,
    TemplateRecord,
    TemplateRenderResponse,
    TemplateUpdateRequest,
    TransitionRequest,
    AuthSignupRequest,
    CheckoutRequest,
    CheckoutResponse,
    PlansResponse,
    PortalResponse,
    SubscriptionResponse,
    WorkspaceCreateRequest,
    WorkspaceListResponse,
    WorkspaceMemberInviteRequest,
    WorkspaceMemberRecord,
    WorkspaceMemberUpdateRequest,
    WorkspaceRecord,
    WorkspaceSwitchResponse,
    WorkspaceUpdateRequest,
    EmailPreferencesResponse,
    EmailPreferencesUpdateRequest,
    UserCreateRequest,
    UserListResponse,
    UserRecord,
    UserRoleUpdateRequest,
)

configure_logging()
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FRONTEND_DIR = PROJECT_ROOT / "frontend"


def _startup_initialize() -> None:
    init_observability()
    if STRICT_APPROVAL_ROLE and STRICT_APPROVAL_ROLE not in {
        "viewer",
        "operator",
        "admin",
    }:
        raise RuntimeError(
            "CITYSORT_STRICT_APPROVAL_ROLE must be one of: viewer, operator, admin."
        )
    if STRICT_AUTH_SECRET and (REQUIRE_AUTH or IS_PRODUCTION):
        if AUTH_SECRET.strip().lower() in AUTH_SECRET_PLACEHOLDER_VALUES:
            raise RuntimeError(
                "CITYSORT_AUTH_SECRET must be set to a strong secret for authenticated/production mode."
            )
    validate_encryption_configuration()
    if IS_PRODUCTION:
        if not ENFORCE_HTTPS:
            logger.warning(
                "CITYSORT_ENFORCE_HTTPS is disabled in production — HTTPS strongly recommended."
            )
        if not CORS_ALLOWED_ORIGINS or CORS_ALLOWED_ORIGINS == [
            "http://localhost:8000",
            "http://127.0.0.1:8000",
        ]:
            logger.warning(
                "CITYSORT_CORS_ALLOWED_ORIGINS uses default localhost values in production — update to actual domain(s)."
            )
        if not TRUSTED_HOSTS or TRUSTED_HOSTS == ["localhost", "127.0.0.1"]:
            logger.warning(
                "CITYSORT_TRUSTED_HOSTS uses default localhost values in production — update to actual hostname(s)."
            )
    elif ENFORCE_HTTPS:
        logger.warning("CITYSORT_ENFORCE_HTTPS=true in development mode.")
    init_db()
    start_job_worker()
    start_watcher()


def _shutdown_cleanup() -> None:
    stop_job_worker()
    stop_watcher()


@asynccontextmanager
async def app_lifespan(_: FastAPI):
    _startup_initialize()
    try:
        yield
    finally:
        _shutdown_cleanup()


app = FastAPI(
    title="CitySort AI MVP",
    description="AI-powered document intake, classification, and routing for local government.",
    version="0.1.0",
    lifespan=app_lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOWED_ORIGINS or [],
    allow_credentials=CORS_ALLOW_CREDENTIALS,
    allow_methods=["*"],
    allow_headers=["*"],
)
if TRUSTED_HOSTS:
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=TRUSTED_HOSTS)

app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_rate_limiter = SlidingWindowRateLimiter()


def _rate_limit_scope(path: str) -> Optional[tuple[str, int]]:
    normalized = str(path or "").strip().lower()
    if not normalized.startswith("/api/"):
        return None
    if normalized.startswith("/api/documents/upload") or normalized.startswith(
        "/api/documents/import"
    ):
        return "upload", RATE_LIMIT_UPLOAD_PER_WINDOW
    if (
        normalized.startswith("/api/automation/")
        or normalized.endswith("/reprocess")
        or normalized.startswith("/api/templates/")
    ):
        return "ai", RATE_LIMIT_AI_PER_WINDOW
    return "default", RATE_LIMIT_DEFAULT_PER_WINDOW


@app.middleware("http")
async def secure_request_middleware(request: Request, call_next):
    request_id = str(uuid.uuid4())
    started = start_timer()
    source_ip = client_ip(request)
    path = request.url.path

    if should_block_insecure_request(request):
        response = JSONResponse(
            status_code=400,
            content={"detail": "HTTPS is required for this endpoint."},
        )
        response.headers["X-Request-ID"] = request_id
        apply_security_headers(response)
        return response

    rate_decision = None
    if RATE_LIMIT_ENABLED:
        scoped_limit = _rate_limit_scope(path)
        if scoped_limit is not None:
            scope, limit = scoped_limit
            rate_decision = _rate_limiter.check(
                f"{source_ip}:{scope}",
                limit=limit,
                window_seconds=RATE_LIMIT_WINDOW_SECONDS,
            )
            if not rate_decision.allowed:
                response = JSONResponse(
                    status_code=429,
                    content={"detail": "Rate limit exceeded. Please retry later."},
                )
                response.headers["Retry-After"] = str(rate_decision.reset_seconds)
                response.headers["X-RateLimit-Limit"] = str(rate_decision.limit)
                response.headers["X-RateLimit-Remaining"] = str(rate_decision.remaining)
                response.headers["X-RateLimit-Reset"] = str(rate_decision.reset_seconds)
                response.headers["X-Request-ID"] = request_id
                apply_security_headers(response)
                observe_request(
                    method=request.method,
                    path=path,
                    status_code=response.status_code,
                    started_at=started,
                )
                logger.warning(
                    "rate_limited method=%s path=%s ip=%s window=%ss limit=%s",
                    request.method,
                    path,
                    source_ip,
                    RATE_LIMIT_WINDOW_SECONDS,
                    rate_decision.limit,
                    extra={"request_id": request_id},
                )
                return response

    try:
        response = await call_next(request)
    except Exception:
        logger.exception(
            "unhandled_error method=%s path=%s ip=%s",
            request.method,
            path,
            source_ip,
            extra={"request_id": request_id},
        )
        response = JSONResponse(
            status_code=500, content={"detail": "Internal server error"}
        )

    response.headers["X-Request-ID"] = request_id
    if rate_decision is not None:
        response.headers["X-RateLimit-Limit"] = str(rate_decision.limit)
        response.headers["X-RateLimit-Remaining"] = str(rate_decision.remaining)
        response.headers["X-RateLimit-Reset"] = str(rate_decision.reset_seconds)
    apply_security_headers(response)
    observe_request(
        method=request.method,
        path=path,
        status_code=response.status_code,
        started_at=started,
    )
    logger.info(
        "request_complete method=%s path=%s status=%s ip=%s",
        request.method,
        path,
        response.status_code,
        source_ip,
        extra={"request_id": request_id},
    )
    return response


def _coerce_optional_text(value: object | None) -> Optional[str]:
    if value is None:
        return None
    parsed = str(value).strip()
    return parsed or None


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _status_is_approved(status: object) -> bool:
    normalized = str(status or "").strip().lower()
    return normalized in {"approved", "corrected"}


def _export_approved_snapshot(
    document: dict[str, object], *, actor: str, trigger: str
) -> None:
    if not APPROVED_EXPORT_ENABLED:
        return
    if not _status_is_approved(document.get("status")):
        return

    document_id = str(document.get("id", "")).strip()
    source_path_raw = str(document.get("storage_path", "")).strip()
    if not document_id or not source_path_raw:
        return

    source_path = Path(source_path_raw)
    if not source_path.exists() or not source_path.is_file():
        logger.warning(
            "approved_export_skipped document_id=%s reason=source_missing path=%s",
            document_id,
            source_path_raw,
        )
        return

    safe_filename = Path(str(document.get("filename") or source_path.name)).name
    target_path = APPROVED_EXPORT_DIR / f"{document_id}_{safe_filename}"
    metadata_path = APPROVED_EXPORT_DIR / f"{document_id}.meta.json"
    status = str(document.get("status", "")).strip().lower()

    try:
        APPROVED_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(read_document_bytes(source_path))
        metadata = {
            "document_id": document_id,
            "status": status,
            "trigger": trigger,
            "source_path": str(source_path),
            "export_path": str(target_path),
            "exported_at": _utcnow_iso(),
        }
        metadata_path.write_text(
            json.dumps(metadata, separators=(",", ":"), ensure_ascii=True),
            encoding="utf-8",
        )
        create_audit_event(
            document_id=document_id,
            action="approved_exported",
            actor=actor,
            details=f"status={status} trigger={trigger} path={target_path}",
            workspace_id=_coerce_optional_text(document.get("workspace_id")),
        )
    except Exception as exc:  # pragma: no cover - runtime safety
        logger.warning(
            "approved_export_failed document_id=%s trigger=%s error=%s",
            document_id,
            trigger,
            exc,
        )
        try:
            create_audit_event(
                document_id=document_id,
                action="approved_export_failed",
                actor=actor,
                details=f"trigger={trigger} error={exc}",
                workspace_id=_coerce_optional_text(document.get("workspace_id")),
            )
        except Exception:
            pass


def _enforce(
    request: Optional[Request],
    *,
    role: str = "viewer",
    allow_api_key: bool = True,
) -> dict[str, object]:
    return authorize_request(request, required_role=role, allow_api_key=allow_api_key)


def _identity_workspace_id(identity: dict[str, object]) -> Optional[str]:
    workspace_id = identity.get("workspace_id")
    if workspace_id is None:
        return None
    return str(workspace_id)


def _resolve_workspace_id(identity: dict[str, object]) -> Optional[str]:
    workspace_id = _identity_workspace_id(identity)
    if workspace_id:
        return workspace_id
    user = identity.get("user")
    if isinstance(user, dict):
        default_workspace = get_default_workspace_for_user(str(user.get("id", "")))
        if default_workspace and default_workspace.get("id"):
            return str(default_workspace["id"])
    return None


def _require_workspace(identity: dict[str, object]) -> str:
    workspace_id = _resolve_workspace_id(identity)
    if workspace_id:
        return workspace_id
    raise HTTPException(status_code=400, detail="Workspace context required.")


def _workspace_role_allows(current_role: str, required_role: str) -> bool:
    order = {"member": 1, "operator": 2, "admin": 3}
    return order.get(current_role, 0) >= order.get(required_role, 0)


def _enforce_workspace_role(
    identity: dict[str, object], workspace_id: str, required_role: str = "member"
) -> None:
    user = identity.get("user")
    if not isinstance(user, dict):
        raise HTTPException(status_code=401, detail="User session required.")
    role = get_user_workspace_role(str(user.get("id", "")), workspace_id)
    if role is None:
        raise HTTPException(status_code=403, detail="Workspace access denied.")
    if not _workspace_role_allows(str(role), required_role):
        raise HTTPException(
            status_code=403, detail="Workspace role permissions required."
        )


def _workspace_record_from_row(row: dict[str, object]) -> WorkspaceRecord:
    settings_raw = row.get("settings")
    settings: dict[str, object]
    if isinstance(settings_raw, str):
        try:
            settings = json.loads(settings_raw) if settings_raw else {}
        except Exception:
            settings = {}
    elif isinstance(settings_raw, dict):
        settings = dict(settings_raw)
    else:
        settings = {}
    payload = dict(row)
    payload["settings"] = settings
    return WorkspaceRecord(**payload)


def _database_health() -> ServiceHealth:
    try:
        with get_connection() as connection:
            connection.execute("SELECT 1").fetchone()
        return ServiceHealth(
            name="database",
            status="ok",
            configured=True,
            details="Database connection succeeded.",
        )
    except Exception as exc:  # pragma: no cover - runtime safeguard
        return ServiceHealth(
            name="database", status="error", configured=False, details=str(exc)
        )


def _ocr_provider_health() -> ServiceHealth:
    provider = OCR_PROVIDER
    if provider == "local":
        return ServiceHealth(
            name="ocr",
            status="ok",
            configured=True,
            details="Using local OCR/text extraction pipeline.",
        )

    if provider == "azure_di":
        from .config import AZURE_DI_API_KEY, AZURE_DI_ENDPOINT

        configured = bool(AZURE_DI_ENDPOINT and AZURE_DI_API_KEY)
        return ServiceHealth(
            name="ocr",
            status="ok" if configured else "not_configured",
            configured=configured,
            details=(
                "Azure Document Intelligence is configured."
                if configured
                else "Missing AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT or AZURE_DOCUMENT_INTELLIGENCE_API_KEY."
            ),
        )

    return ServiceHealth(
        name="ocr",
        status="unsupported",
        configured=False,
        details=f"Unknown OCR provider: {provider}",
    )


def _classifier_provider_health() -> ServiceHealth:
    provider = CLASSIFIER_PROVIDER
    if provider == "rules":
        return ServiceHealth(
            name="classifier",
            status="ok",
            configured=True,
            details="Using local rule-based classifier.",
        )

    if provider == "openai":
        from .config import OPENAI_API_KEY

        configured = bool(OPENAI_API_KEY)
        return ServiceHealth(
            name="classifier",
            status="ok" if configured else "not_configured",
            configured=configured,
            details="OpenAI classifier configured."
            if configured
            else "Missing OPENAI_API_KEY.",
        )

    if provider == "anthropic":
        from .config import ANTHROPIC_API_KEY

        configured = bool(ANTHROPIC_API_KEY)
        return ServiceHealth(
            name="classifier",
            status="ok" if configured else "not_configured",
            configured=configured,
            details="Anthropic classifier configured."
            if configured
            else "Missing ANTHROPIC_API_KEY.",
        )

    return ServiceHealth(
        name="classifier",
        status="unsupported",
        configured=False,
        details=f"Unknown classifier provider: {provider}",
    )


def _connectivity_snapshot() -> ConnectivityResponse:
    deploy_health_raw = deployment_provider_health()
    deploy_health = ServiceHealth(
        name="deployment",
        status=str(deploy_health_raw.get("status", "unknown")),
        configured=bool(deploy_health_raw.get("configured", False)),
        details=str(deploy_health_raw.get("details", "")),
    )

    return ConnectivityResponse(
        database=_database_health(),
        ocr_provider=_ocr_provider_health(),
        classifier_provider=_classifier_provider_health(),
        deployment_provider=deploy_health,
        checked_at=_utcnow_iso(),
    )


# --- Workflow state machine ---

ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "ingested": {"needs_review", "routed"},
    "needs_review": {"acknowledged", "approved", "corrected"},
    "routed": {"acknowledged", "approved"},
    "acknowledged": {"assigned", "approved", "in_progress"},
    "assigned": {"in_progress", "approved"},
    "in_progress": {"completed", "approved"},
    "completed": {"archived"},
    "approved": {"archived"},
    "corrected": {"archived"},
    "failed": {"needs_review", "ingested"},
}


@app.get("/health")
def health_check() -> dict[str, str]:
    return {
        "status": "ok",
        "environment": APP_ENV,
        "database_backend": DATABASE_BACKEND,
        "ocr_provider": OCR_PROVIDER,
        "classifier_provider": CLASSIFIER_PROVIDER,
        "deploy_provider": DEPLOY_PROVIDER,
        "auth_required": str(REQUIRE_AUTH).lower(),
        "confidence_threshold": str(CONFIDENCE_THRESHOLD),
        "force_review_doc_types": ",".join(sorted(FORCE_REVIEW_DOC_TYPES)),
    }


@app.get("/livez")
def livez() -> dict[str, str]:
    return {"status": "alive"}


@app.get("/readyz")
def readyz() -> dict[str, str]:
    db_health = _database_health()
    if db_health.status != "ok":
        raise HTTPException(status_code=503, detail=db_health.details)
    return {"status": "ready"}


@app.get("/metrics")
def metrics() -> Response:
    if not PROMETHEUS_ENABLED:
        raise HTTPException(status_code=404, detail="Metrics endpoint is disabled.")
    return metrics_response()


def _asset_version(filename: str) -> str:
    """Return file mtime hex for cache busting (e.g. 'a3f1b2c4')."""
    try:
        mtime = (FRONTEND_DIR / filename).stat().st_mtime
        return format(int(mtime), "x")
    except Exception:
        return "0"


_CACHE_BUST_RE = re.compile(
    r'(/static/(styles\.v2\.css|app\.v2\.js|landing\.css|landing\.js))\?v=[^"\']+',
)


def _inject_cache_busters(html: str) -> str:
    """Replace static ?v= params with file-mtime-based versions."""

    def _replacer(m: re.Match) -> str:
        path = m.group(1)
        filename = m.group(2)
        return f"{path}?v={_asset_version(filename)}"

    return _CACHE_BUST_RE.sub(_replacer, html)


def _serve_landing(invite_token: Optional[str] = None) -> Response:
    """Serve the landing page HTML, injecting Stripe publishable key and invite token."""
    landing_path = FRONTEND_DIR / "landing.html"
    if not landing_path.exists():
        # Fallback to dashboard if no landing page yet
        return _serve_dashboard()
    html = landing_path.read_text(encoding="utf-8")
    html = _inject_cache_busters(html)
    # Inject config into a script tag
    config_payload = {
        "stripe_publishable_key": STRIPE_PUBLISHABLE_KEY,
        "stripe_enabled": STRIPE_ENABLED,
        "invite_token": invite_token or "",
    }
    inject = (
        "<script>window.__CITYSORT_CONFIG__="
        f"{json.dumps(config_payload, separators=(',', ':'))};</script>"
    )
    html = html.replace("</head>", f"{inject}\n</head>", 1)
    return Response(content=html, media_type="text/html")


def _serve_dashboard() -> Response:
    """Serve the existing dashboard SPA with cache busters."""
    index_path = FRONTEND_DIR / "index.html"
    html = index_path.read_text(encoding="utf-8")
    html = _inject_cache_busters(html)
    return Response(content=html, media_type="text/html")


@app.get("/")
def root(request: Request) -> Response:
    """Serve landing page for unauthenticated, redirect to /app for authenticated."""
    # Check for auth token in cookie or Authorization header
    auth_header = request.headers.get("authorization", "").strip()
    token = None
    if auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1].strip()

    if token:
        try:
            from .auth import decode_access_token

            decode_access_token(token)
            return RedirectResponse(url="/app", status_code=302)
        except Exception:
            pass

    return _serve_landing()


@app.get("/app")
def dashboard_app() -> Response:
    """Serve the dashboard SPA."""
    return _serve_dashboard()


@app.get("/invite/{token}")
def invite_page(token: str) -> Response:
    """Serve landing page with invitation token pre-populated."""
    return _serve_landing(invite_token=token)


@app.post("/api/auth/signup", response_model=AuthResponse)
def auth_signup(payload: AuthSignupRequest) -> AuthResponse:
    """Invite-only user registration."""
    from .auth import create_access_token

    requested_email = payload.email.strip().lower()

    try:
        invitation = validate_invitation(payload.invitation_token)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    invited_email = str(invitation.get("email", "")).strip().lower()
    if invited_email and invited_email != requested_email:
        raise HTTPException(
            status_code=400,
            detail="Invitation token is only valid for the invited email address.",
        )

    invited_role = str(invitation.get("role", "viewer")).strip().lower()
    if invited_role == "member":
        invited_role = "viewer"
    if invited_role not in {"viewer", "operator", "admin"}:
        raise HTTPException(status_code=400, detail="Invitation role is invalid.")

    try:
        user = create_user_account(
            email=requested_email,
            password=payload.password,
            role=invited_role,
            full_name=payload.full_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    try:
        mark_invitation_accepted(
            int(invitation["id"]),
            workspace_id=_coerce_optional_text(invitation.get("workspace_id")),
        )
    except ValueError:
        logger.warning(
            "Invitation %s could not be marked accepted after successful signup.",
            invitation.get("id"),
        )

    invitation_workspace_id = invitation.get("workspace_id")
    if invitation_workspace_id:
        try:
            add_workspace_member(
                workspace_id=str(invitation_workspace_id),
                user_id=user["id"],
                role=invited_role
                if invited_role in {"admin", "operator"}
                else "member",
            )
        except Exception:
            logger.debug(
                "Failed to add invited user to workspace %s",
                invitation_workspace_id,
                exc_info=True,
            )

    token = create_access_token(
        user_id=user["id"],
        role=user["role"],
        workspace_id=str(invitation_workspace_id or user.get("workspace_id") or ""),
    )

    # Send welcome email (fire-and-forget)
    try:
        from .account_emails import send_welcome_email

        send_welcome_email(
            user["email"],
            user.get("full_name"),
            user_id=user["id"],
            workspace_id=_coerce_optional_text(
                invitation_workspace_id or user.get("workspace_id")
            ),
        )
    except Exception:
        logger.debug("Welcome email failed (non-blocking)", exc_info=True)

    return AuthResponse(access_token=token, user=UserRecord(**user))


@app.post("/api/auth/bootstrap", response_model=AuthResponse)
def auth_bootstrap(payload: AuthBootstrapRequest) -> AuthResponse:
    try:
        result = bootstrap_admin(
            email=payload.email, password=payload.password, full_name=payload.full_name
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return AuthResponse(
        access_token=result["access_token"], user=UserRecord(**result["user"])
    )


@app.post("/api/auth/login", response_model=AuthResponse)
def auth_login(payload: AuthLoginRequest) -> AuthResponse:
    result = authenticate_user(email=payload.email, password=payload.password)
    return AuthResponse(
        access_token=result["access_token"], user=UserRecord(**result["user"])
    )


@app.get("/api/auth/me", response_model=UserRecord)
def auth_me(request: Request) -> UserRecord:
    identity = _enforce(request, role="viewer", allow_api_key=False)
    user = identity.get("user")
    if not user:
        if not REQUIRE_AUTH:
            now = _utcnow_iso()
            return UserRecord(
                id="local-dev-user",
                email="dev@citysort.local",
                full_name="Local Development User",
                role=str(identity.get("role", "admin")),
                status="active",
                plan_tier="enterprise",
                last_login_at=now,
                created_at=now,
                updated_at=now,
            )
        raise HTTPException(status_code=401, detail="User session required.")
    payload = dict(user)
    active_workspace_id = _resolve_workspace_id(identity)
    if active_workspace_id:
        payload["workspace_id"] = active_workspace_id
    return UserRecord(**payload)


@app.get("/api/auth/me/email-preferences", response_model=EmailPreferencesResponse)
def get_email_preferences(request: Request) -> EmailPreferencesResponse:
    """Return the current user's email notification preferences."""
    identity = _enforce(request, role="viewer", allow_api_key=False)
    user = identity.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="User session required.")
    prefs = get_user_email_preferences(user["id"])
    return EmailPreferencesResponse(**prefs)


@app.put("/api/auth/me/email-preferences", response_model=EmailPreferencesResponse)
def update_email_preferences(
    payload: EmailPreferencesUpdateRequest, request: Request
) -> EmailPreferencesResponse:
    """Update the current user's email notification preferences."""
    identity = _enforce(request, role="viewer", allow_api_key=False)
    user = identity.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="User session required.")
    updates = {k: v for k, v in payload.model_dump().items() if v is not None}
    merged = update_user_email_preferences(user["id"], updates)
    return EmailPreferencesResponse(**merged)


@app.get("/api/auth/users", response_model=UserListResponse)
def auth_list_users(
    request: Request, limit: int = Query(default=200, ge=1, le=500)
) -> UserListResponse:
    _enforce(request, role="admin", allow_api_key=False)
    items = [UserRecord(**item) for item in get_users(limit=limit)]
    return UserListResponse(items=items)


@app.post("/api/auth/users", response_model=UserRecord)
def auth_create_user(request: Request, payload: UserCreateRequest) -> UserRecord:
    _enforce(request, role="admin", allow_api_key=False)
    try:
        user = create_user_account(
            email=payload.email,
            password=payload.password,
            role=payload.role,
            full_name=payload.full_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return UserRecord(**user)


@app.patch("/api/auth/users/{user_id}/role", response_model=UserRecord)
def auth_update_user_role(
    request: Request, user_id: str, payload: UserRoleUpdateRequest
) -> UserRecord:
    _enforce(request, role="admin", allow_api_key=False)
    try:
        updated = set_user_role(user_id=user_id, role=payload.role)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return UserRecord(**updated)


@app.post("/api/workspaces", response_model=WorkspaceRecord)
def create_workspace_endpoint(
    payload: WorkspaceCreateRequest, request: Request
) -> WorkspaceRecord:
    identity = _enforce(request, role="viewer", allow_api_key=False)
    user = identity.get("user")
    if not isinstance(user, dict):
        raise HTTPException(status_code=401, detail="User session required.")
    workspace = create_workspace(name=payload.name, owner_id=user["id"])
    return _workspace_record_from_row(workspace)


@app.get("/api/workspaces", response_model=WorkspaceListResponse)
def list_workspaces_endpoint(request: Request) -> WorkspaceListResponse:
    identity = _enforce(request, role="viewer", allow_api_key=False)
    user = identity.get("user")
    if not isinstance(user, dict):
        raise HTTPException(status_code=401, detail="User session required.")
    rows = list_user_workspaces(user["id"])
    return WorkspaceListResponse(items=[_workspace_record_from_row(r) for r in rows])


@app.get("/api/workspaces/{workspace_id}", response_model=WorkspaceRecord)
def get_workspace_endpoint(workspace_id: str, request: Request) -> WorkspaceRecord:
    identity = _enforce(request, role="viewer", allow_api_key=False)
    _enforce_workspace_role(identity, workspace_id, required_role="member")
    row = get_workspace(workspace_id)
    if not row:
        raise HTTPException(status_code=404, detail="Workspace not found.")
    return _workspace_record_from_row(row)


@app.patch("/api/workspaces/{workspace_id}", response_model=WorkspaceRecord)
def update_workspace_endpoint(
    workspace_id: str, payload: WorkspaceUpdateRequest, request: Request
) -> WorkspaceRecord:
    identity = _enforce(request, role="viewer", allow_api_key=False)
    _enforce_workspace_role(identity, workspace_id, required_role="admin")
    updated = update_workspace(
        workspace_id,
        name=payload.name,
        settings=payload.settings,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Workspace not found.")
    return _workspace_record_from_row(updated)


@app.post(
    "/api/workspaces/{workspace_id}/members", response_model=InvitationCreateResponse
)
def invite_workspace_member(
    workspace_id: str,
    payload: WorkspaceMemberInviteRequest,
    request: Request,
) -> InvitationCreateResponse:
    identity = _enforce(request, role="viewer", allow_api_key=False)
    _enforce_workspace_role(identity, workspace_id, required_role="admin")
    invitation, raw_token = create_invitation(
        workspace_id=workspace_id,
        email=payload.email.strip().lower(),
        role=payload.role.strip().lower(),
        actor=str(identity.get("actor", "workspace_admin")),
        expires_in_days=7,
    )
    invite_link = f"{str(request.base_url).rstrip('/')}/invite/{raw_token}"
    try:
        from .account_emails import send_invitation_email

        send_invitation_email(
            payload.email.strip().lower(),
            invite_link,
            str(identity.get("actor", "Workspace administrator")),
            workspace_id=workspace_id,
        )
    except Exception:
        logger.debug(
            "Workspace invitation email failed (non-blocking)",
            exc_info=True,
        )
    return InvitationCreateResponse(
        invitation=invitation,
        invite_token=raw_token,
        invite_link=invite_link,
    )


@app.get("/api/workspaces/{workspace_id}/members")
def list_workspace_members_endpoint(
    workspace_id: str, request: Request
) -> dict[str, list[WorkspaceMemberRecord]]:
    identity = _enforce(request, role="viewer", allow_api_key=False)
    _enforce_workspace_role(identity, workspace_id, required_role="member")
    rows = list_workspace_members(workspace_id)
    return {"items": [WorkspaceMemberRecord(**row) for row in rows]}


@app.delete("/api/workspaces/{workspace_id}/members/{user_id}")
def remove_workspace_member_endpoint(
    workspace_id: str, user_id: str, request: Request
) -> dict[str, bool]:
    identity = _enforce(request, role="viewer", allow_api_key=False)
    _enforce_workspace_role(identity, workspace_id, required_role="admin")
    removed = remove_workspace_member(workspace_id=workspace_id, user_id=user_id)
    if not removed:
        raise HTTPException(status_code=404, detail="Workspace member not found.")
    return {"removed": True}


@app.patch("/api/workspaces/{workspace_id}/members/{user_id}")
def update_workspace_member_role_endpoint(
    workspace_id: str,
    user_id: str,
    payload: WorkspaceMemberUpdateRequest,
    request: Request,
) -> WorkspaceMemberRecord:
    identity = _enforce(request, role="viewer", allow_api_key=False)
    _enforce_workspace_role(identity, workspace_id, required_role="admin")
    updated = add_workspace_member(
        workspace_id=workspace_id, user_id=user_id, role=payload.role
    )
    return WorkspaceMemberRecord(**updated)


@app.post(
    "/api/workspaces/switch/{workspace_id}", response_model=WorkspaceSwitchResponse
)
def switch_workspace(workspace_id: str, request: Request) -> WorkspaceSwitchResponse:
    identity = _enforce(request, role="viewer", allow_api_key=False)
    user = identity.get("user")
    if not isinstance(user, dict):
        raise HTTPException(status_code=401, detail="User session required.")
    _enforce_workspace_role(identity, workspace_id, required_role="member")
    workspace = get_workspace(workspace_id)
    if not workspace:
        raise HTTPException(status_code=404, detail="Workspace not found.")
    token = create_access_token(
        user_id=user["id"], role=user.get("role", "viewer"), workspace_id=workspace_id
    )
    return WorkspaceSwitchResponse(
        access_token=token,
        workspace=_workspace_record_from_row(workspace),
    )


# --- Billing / Stripe ---


@app.get("/api/billing/plans", response_model=PlansResponse)
def billing_plans() -> PlansResponse:
    """Return available plans and pricing (public)."""
    from .schemas import PlanInfo

    plans = [PlanInfo(**p) for p in get_plan_info()]
    return PlansResponse(plans=plans)


@app.get("/api/billing/subscription", response_model=SubscriptionResponse)
def billing_subscription(request: Request) -> SubscriptionResponse:
    """Return the current user's subscription details."""
    identity = _enforce(request, role="viewer", allow_api_key=False)
    user = identity.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="User session required.")

    workspace_id = _resolve_workspace_id(identity)
    sub = get_active_subscription(user["id"], workspace_id=workspace_id)
    if sub:
        return SubscriptionResponse(
            plan_tier=sub["plan_tier"],
            billing_type=sub.get("billing_type"),
            status=sub["status"],
            current_period_end=sub.get("current_period_end"),
            stripe_enabled=STRIPE_ENABLED,
        )
    workspace_plan = None
    if workspace_id:
        workspace = get_workspace(workspace_id)
        if workspace:
            workspace_plan = workspace.get("plan_tier")
    return SubscriptionResponse(
        plan_tier=str(workspace_plan or user.get("plan_tier", "free")),
        status="active",
        stripe_enabled=STRIPE_ENABLED,
    )


@app.post("/api/billing/checkout", response_model=CheckoutResponse)
def billing_checkout(request: Request, payload: CheckoutRequest) -> CheckoutResponse:
    """Create a Stripe Checkout Session."""
    identity = _enforce(request, role="viewer", allow_api_key=False)
    user = identity.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="User session required.")
    workspace_id = _resolve_workspace_id(identity)

    base_url = str(request.base_url).rstrip("/")
    checkout_url = create_checkout_session(
        user_id=user["id"],
        user_email=user["email"],
        workspace_id=workspace_id,
        plan_tier=payload.plan_tier,
        billing_type=payload.billing_type,
        success_url=f"{base_url}/app?billing=success",
        cancel_url=f"{base_url}/app?billing=canceled",
    )
    return CheckoutResponse(checkout_url=checkout_url)


@app.get("/api/billing/portal", response_model=PortalResponse)
def billing_portal(request: Request) -> PortalResponse:
    """Create a Stripe Customer Portal session."""
    identity = _enforce(request, role="viewer", allow_api_key=False)
    user = identity.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="User session required.")

    workspace_id = _resolve_workspace_id(identity)
    stripe_customer_id = None
    if workspace_id:
        workspace = get_workspace(workspace_id)
        if workspace:
            stripe_customer_id = workspace.get("stripe_customer_id")
    if not stripe_customer_id:
        stripe_customer_id = user.get("stripe_customer_id")
    if not stripe_customer_id:
        raise HTTPException(
            status_code=400, detail="No Stripe customer linked to this account."
        )

    base_url = str(request.base_url).rstrip("/")
    portal_url = create_portal_session(
        stripe_customer_id=stripe_customer_id,
        return_url=f"{base_url}/app",
    )
    return PortalResponse(portal_url=portal_url)


@app.post("/api/billing/webhook")
async def billing_webhook(request: Request) -> dict:
    """Handle Stripe webhook events (verified by signature, no auth middleware)."""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")
    return handle_webhook_event(payload, sig_header)


# --- Document Endpoints ---


@app.post("/api/documents/upload", response_model=DocumentResponse)
async def upload_document(
    request: Request = None,
    file: UploadFile = File(...),
    source_channel: str = Form("upload_portal"),
    process_async: bool = Form(False),
) -> DocumentResponse:
    identity = _enforce(request, role="operator")
    actor = str(identity.get("actor", "upload_portal"))
    workspace_id = _resolve_workspace_id(identity)

    # Plan enforcement: check document upload limits
    user = identity.get("user")
    if user and STRIPE_ENABLED:
        enforce_plan_limits(
            user["id"],
            "upload_document",
            workspace_id=workspace_id,
        )

    if not file.filename:
        raise HTTPException(status_code=400, detail="File name is required")

    document_id = str(uuid.uuid4())
    safe_filename = f"{document_id}_{Path(file.filename).name}"
    file_path = UPLOAD_DIR / safe_filename

    contents = await file.read()
    content_type = (
        file.content_type
        or mimetypes.guess_type(file.filename)[0]
        or "application/octet-stream"
    )
    try:
        validate_upload(
            filename=file.filename, content_type=content_type, payload=contents
        )
    except UploadValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    write_document_bytes(file_path, contents)

    create_document(
        document={
            "id": document_id,
            "workspace_id": workspace_id,
            "filename": file.filename,
            "storage_path": str(file_path),
            "source_channel": source_channel,
            "content_type": content_type,
            "status": "ingested",
            "requires_review": False,
            "confidence": 0.0,
            "doc_type": None,
            "department": None,
            "urgency": "normal",
        }
    )

    create_audit_event(
        document_id=document_id,
        action="uploaded",
        actor=actor,
        details=f"source_channel={source_channel} file={file.filename}",
        workspace_id=workspace_id,
    )

    if process_async:
        enqueue_document_processing(
            document_id=document_id,
            actor=actor,
            workspace_id=workspace_id,
        )
    else:
        process_document_by_id(document_id, actor=actor)

    refreshed = get_document(document_id, workspace_id=workspace_id)
    if not refreshed:
        raise HTTPException(status_code=500, detail="Unable to load processed document")

    return DocumentResponse(**refreshed)


@app.post("/api/documents/import/database", response_model=DatabaseImportResponse)
def import_documents_from_database(
    payload: DatabaseImportRequest,
    request: Request = None,
) -> DatabaseImportResponse:
    identity = _enforce(request, role="operator")
    actor = str(identity.get("actor", payload.actor or "database_importer"))
    workspace_id = _resolve_workspace_id(identity)

    try:
        connection = connect_external_database(payload.database_url)
    except ExternalDatabaseError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    imported_items: list[dict[str, str]] = []
    errors: list[str] = []
    processed_sync_count = 0
    scheduled_async_count = 0

    try:
        try:
            rows = fetch_import_rows(
                connection=connection, query=payload.query, limit=payload.limit
            )
        except ExternalDatabaseError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        for index, row in enumerate(rows, start=1):
            try:
                raw_filename = _coerce_optional_text(
                    get_row_value(row, payload.filename_column)
                )
                fallback_name = f"db_document_{index}.txt"
                filename = Path(raw_filename or fallback_name).name or fallback_name

                source_path: Optional[Path] = None
                if payload.file_path_column:
                    raw_path_value = _coerce_optional_text(
                        get_row_value(row, payload.file_path_column)
                    )
                    if raw_path_value:
                        source_path = Path(raw_path_value).expanduser().resolve()
                        if not source_path.exists() or not source_path.is_file():
                            raise ValueError(f"Path not found: {source_path}")

                file_bytes: Optional[bytes] = None
                if source_path is None and payload.content_column:
                    raw_content = get_row_value(row, payload.content_column)
                    file_bytes = coerce_row_content_to_bytes(raw_content)
                    if not file_bytes:
                        raise ValueError("Content column is empty.")

                if source_path is None and file_bytes is None:
                    raise ValueError("No usable file content found for this row.")

                raw_content_type = None
                if payload.content_type_column:
                    try:
                        raw_content_type = get_row_value(
                            row, payload.content_type_column
                        )
                    except KeyError:
                        raw_content_type = None

                content_type = (
                    _coerce_optional_text(raw_content_type)
                    or mimetypes.guess_type(filename)[0]
                )

                document_id = str(uuid.uuid4())
                safe_filename = f"{document_id}_{filename}"
                storage_path = UPLOAD_DIR / safe_filename

                if source_path is not None:
                    file_bytes = source_path.read_bytes()
                payload_bytes = file_bytes or b""
                content_type = (
                    content_type
                    or mimetypes.guess_type(filename)[0]
                    or "application/octet-stream"
                )
                try:
                    validate_upload(
                        filename=filename,
                        content_type=content_type,
                        payload=payload_bytes,
                    )
                except UploadValidationError as exc:
                    raise ValueError(str(exc))
                write_document_bytes(storage_path, payload_bytes)

                create_document(
                    document={
                        "id": document_id,
                        "workspace_id": workspace_id,
                        "filename": filename,
                        "storage_path": str(storage_path),
                        "source_channel": payload.source_channel,
                        "content_type": content_type,
                        "status": "ingested",
                        "requires_review": False,
                        "confidence": 0.0,
                        "doc_type": None,
                        "department": None,
                        "urgency": "normal",
                    }
                )

                create_audit_event(
                    document_id=document_id,
                    action="database_imported",
                    actor=actor,
                    details=f"source_channel={payload.source_channel} row={index}",
                    workspace_id=workspace_id,
                )

                if payload.process_async:
                    enqueue_document_processing(
                        document_id=document_id,
                        actor=actor,
                        workspace_id=workspace_id,
                    )
                    scheduled_async_count += 1
                else:
                    process_document_by_id(document_id, actor=actor)
                    processed_sync_count += 1

                refreshed = get_document(document_id, workspace_id=workspace_id)
                if refreshed:
                    imported_items.append(
                        {
                            "id": refreshed["id"],
                            "filename": refreshed["filename"],
                            "status": refreshed["status"],
                        }
                    )
                else:
                    imported_items.append(
                        {"id": document_id, "filename": filename, "status": "ingested"}
                    )

            except KeyError as exc:
                missing_column = exc.args[0] if exc.args else "unknown"
                errors.append(
                    f"Row {index}: Missing expected column '{missing_column}'."
                )
            except Exception as exc:  # pragma: no cover - runtime safeguard
                errors.append(f"Row {index}: {exc}")
    finally:
        connection.close()

    return DatabaseImportResponse(
        imported_count=len(imported_items),
        processed_sync_count=processed_sync_count,
        scheduled_async_count=scheduled_async_count,
        failed_count=len(errors),
        documents=imported_items,
        errors=errors[:50],
    )


_DB_CONNECTOR_TYPES = {"postgresql", "mysql", "sqlite"}
_SAAS_CONNECTOR_TYPES = {
    "servicenow",
    "confluence",
    "salesforce",
    "google_cloud_storage",
    "amazon_s3",
    "jira",
    "sharepoint",
}


@app.post("/api/connectors/{connector_type}/test", response_model=ConnectorTestResponse)
def test_connector(
    connector_type: str,
    payload: ConnectorTestRequest,
    request: Request = None,
) -> ConnectorTestResponse:
    _enforce(request, role="operator")

    if connector_type in _DB_CONNECTOR_TYPES:
        database_url = payload.database_url
        if not database_url:
            return ConnectorTestResponse(
                success=False,
                message="Database connection URL is required.",
                connector_type=connector_type,
            )
        try:
            connection = connect_external_database(database_url)
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            connection.close()
            return ConnectorTestResponse(
                success=True,
                message=f"Successfully connected to {connector_type.title()} database.",
                connector_type=connector_type,
                details="Connection established and test query executed.",
            )
        except ExternalDatabaseError as exc:
            return ConnectorTestResponse(
                success=False,
                message=f"Connection failed: {exc}",
                connector_type=connector_type,
            )
        except Exception as exc:
            return ConnectorTestResponse(
                success=False,
                message=f"Unexpected error: {exc}",
                connector_type=connector_type,
            )

    if connector_type in _SAAS_CONNECTOR_TYPES:
        config = payload.config or {}
        try:
            connector = get_connector(connector_type)
            success, message = connector.test_connection(config)
            return ConnectorTestResponse(
                success=success,
                message=message,
                connector_type=connector_type,
            )
        except ConnectorError as exc:
            return ConnectorTestResponse(
                success=False,
                message=f"Connection failed: {exc}",
                connector_type=connector_type,
            )

    raise HTTPException(
        status_code=400, detail=f"Unknown connector type: {connector_type}"
    )


# --- Connector Config persistence ---


@app.get(
    "/api/connectors/{connector_type}/config", response_model=ConnectorConfigResponse
)
def get_connector_config(
    connector_type: str,
    request: Request = None,
) -> ConnectorConfigResponse:
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)
    with get_connection() as conn:
        row = None
        if workspace_id is not None:
            row = conn.execute(
                """
                SELECT * FROM connector_configs
                WHERE connector_type = ? AND workspace_id = ?
                """,
                (connector_type, workspace_id),
            ).fetchone()
        if row is None:
            row = conn.execute(
                """
                SELECT * FROM connector_configs
                WHERE connector_type = ? AND workspace_id IS NULL
                """,
                (connector_type,),
            ).fetchone()
    config_data = {}
    enabled = True
    last_sync = None
    if row:
        import json as _json

        config_data = _json.loads(row["config_json"] or "{}")
        enabled = bool(row["enabled"])
        last_sync = row["last_sync_at"]
    return ConnectorConfigResponse(
        connector_type=connector_type,
        config=config_data,
        enabled=enabled,
        last_sync_at=last_sync,
        total_imported=get_sync_count(connector_type),
    )


@app.put(
    "/api/connectors/{connector_type}/config", response_model=ConnectorConfigResponse
)
def save_connector_config(
    connector_type: str,
    payload: ConnectorConfigSaveRequest,
    request: Request = None,
) -> ConnectorConfigResponse:
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)
    import json as _json
    from .repository import utcnow_iso

    now = utcnow_iso()
    config_json = _json.dumps(payload.config)
    with get_connection() as conn:
        if workspace_id is not None:
            conn.execute(
                """
                INSERT INTO connector_configs (workspace_id, connector_type, config_json, enabled, created_at, updated_at)
                VALUES (?, ?, ?, 1, ?, ?)
                ON CONFLICT(workspace_id, connector_type)
                DO UPDATE SET config_json = excluded.config_json, enabled = excluded.enabled, updated_at = excluded.updated_at
                """,
                (workspace_id, connector_type, config_json, now, now),
            )
        else:
            existing = conn.execute(
                """
                SELECT id FROM connector_configs
                WHERE connector_type = ? AND workspace_id IS NULL
                """,
                (connector_type,),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE connector_configs
                    SET config_json = ?, enabled = 1, updated_at = ?
                    WHERE id = ?
                    """,
                    (config_json, now, int(existing["id"])),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO connector_configs (workspace_id, connector_type, config_json, enabled, created_at, updated_at)
                    VALUES (NULL, ?, ?, 1, ?, ?)
                    """,
                    (connector_type, config_json, now, now),
                )
    return ConnectorConfigResponse(
        connector_type=connector_type,
        config=payload.config,
        enabled=True,
        total_imported=get_sync_count(connector_type),
    )


# --- Connector Import ---


@app.post(
    "/api/connectors/{connector_type}/import", response_model=ConnectorImportResponse
)
def import_from_connector_endpoint(
    connector_type: str,
    payload: ConnectorImportRequest,
    request: Request = None,
) -> ConnectorImportResponse:
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)

    # Plan enforcement: check connector access limits
    user = identity.get("user")
    if user and STRIPE_ENABLED:
        enforce_plan_limits(
            user["id"],
            "use_connector",
            workspace_id=workspace_id,
        )

    # Use provided config, or fall back to saved config
    config = payload.config
    if not config:
        with get_connection() as conn:
            row = None
            if workspace_id is not None:
                row = conn.execute(
                    """
                    SELECT config_json FROM connector_configs
                    WHERE connector_type = ? AND workspace_id = ?
                    """,
                    (connector_type, workspace_id),
                ).fetchone()
            if row is None:
                row = conn.execute(
                    """
                    SELECT config_json FROM connector_configs
                    WHERE connector_type = ? AND workspace_id IS NULL
                    """,
                    (connector_type,),
                ).fetchone()
        if row:
            import json as _json

            config = _json.loads(row["config_json"] or "{}")
        if not config:
            raise HTTPException(
                status_code=400,
                detail="No configuration provided or saved for this connector.",
            )

    try:
        result = import_from_connector(
            connector_type=connector_type,
            config=config,
            limit=payload.limit,
            process_async=payload.process_async,
            actor=payload.actor,
            workspace_id=workspace_id,
        )
    except ConnectorError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return ConnectorImportResponse(
        connector_type=connector_type,
        imported_count=result["imported_count"],
        skipped_count=result["skipped_count"],
        failed_count=result["failed_count"],
        documents=[
            {"id": d["id"], "filename": d["filename"], "status": d["status"]}
            for d in result.get("documents", [])
        ],
        errors=result.get("errors", []),
    )


@app.get("/api/documents", response_model=DocumentListResponse)
def get_documents(
    request: Request = None,
    status: Optional[str] = Query(default=None),
    department: Optional[str] = Query(default=None),
    assigned_to: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> DocumentListResponse:
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    items: list[DocumentResponse] = []
    for item in list_documents(
        status=status,
        department=department,
        assigned_to=assigned_to,
        workspace_id=workspace_id,
        limit=limit,
    ):
        # Keep list endpoint light; full text is available from document detail endpoint.
        item_payload = dict(item)
        item_payload["extracted_text"] = None
        items.append(DocumentResponse(**item_payload))
    return DocumentListResponse(items=items)


@app.get("/api/documents/overdue", response_model=DocumentListResponse)
def get_overdue_documents(
    request: Request = None,
    limit: int = Query(default=100, ge=1, le=500),
) -> DocumentListResponse:
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    items: list[DocumentResponse] = []
    for item in list_overdue_documents(workspace_id=workspace_id, limit=limit):
        item_payload = dict(item)
        item_payload["extracted_text"] = None
        items.append(DocumentResponse(**item_payload))
    return DocumentListResponse(items=items)


@app.get("/api/documents/{document_id}", response_model=DocumentResponse)
def get_document_by_id(document_id: str, request: Request = None) -> DocumentResponse:
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    record = get_document(document_id, workspace_id=workspace_id)
    if not record:
        raise HTTPException(status_code=404, detail="Document not found")
    return DocumentResponse(**record)


@app.get("/api/documents/{document_id}/download")
def download_document(document_id: str, request: Request = None):
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    record = get_document(document_id, workspace_id=workspace_id)
    if not record:
        raise HTTPException(status_code=404, detail="Document not found")
    file_path = Path(record.get("storage_path", ""))
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found on disk")
    if not file_path.resolve().is_relative_to(UPLOAD_DIR.resolve()):
        raise HTTPException(status_code=403, detail="Access denied")
    media_type = (
        mimetypes.guess_type(record["filename"])[0] or "application/octet-stream"
    )
    file_bytes = read_document_bytes(file_path)
    return Response(
        content=file_bytes,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{record["filename"]}"'},
    )


@app.get("/api/documents/{document_id}/preview")
def preview_document(document_id: str, request: Request = None):
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    record = get_document(document_id, workspace_id=workspace_id)
    if not record:
        raise HTTPException(status_code=404, detail="Document not found")
    file_path = Path(record.get("storage_path", ""))
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found on disk")
    if not file_path.resolve().is_relative_to(UPLOAD_DIR.resolve()):
        raise HTTPException(status_code=403, detail="Access denied")
    media_type = (
        record.get("content_type")
        or mimetypes.guess_type(record["filename"])[0]
        or "application/octet-stream"
    )
    file_bytes = read_document_bytes(file_path)
    return Response(
        content=file_bytes,
        media_type=media_type,
        headers={"Content-Disposition": f'inline; filename="{record["filename"]}"'},
    )


@app.post("/api/documents/{document_id}/reupload", response_model=DocumentResponse)
async def reupload_document(
    document_id: str,
    request: Request = None,
    file: UploadFile = File(...),
    reprocess: bool = Form(True),
) -> DocumentResponse:
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)
    document = get_document(document_id, workspace_id=workspace_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")
    contents = await file.read()
    content_type = (
        file.content_type
        or mimetypes.guess_type(file.filename)[0]
        or "application/octet-stream"
    )
    try:
        validate_upload(
            filename=file.filename, content_type=content_type, payload=contents
        )
    except UploadValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    safe_filename = f"{document_id}_{Path(file.filename).name}"
    new_file_path = UPLOAD_DIR / safe_filename
    old_path = Path(document.get("storage_path", ""))
    if old_path.exists():
        old_path.unlink(missing_ok=True)
    write_document_bytes(new_file_path, contents)
    update_document(
        document_id,
        updates={
            "storage_path": str(new_file_path),
            "filename": file.filename,
            "content_type": content_type,
        },
        workspace_id=workspace_id,
    )
    create_audit_event(
        document_id=document_id,
        action="reuploaded",
        actor=str(identity.get("actor", "dashboard_reviewer")),
        details=f"new_file={file.filename}",
        workspace_id=workspace_id,
    )
    if reprocess:
        process_document_by_id(
            document_id, actor=str(identity.get("actor", "manual_reupload"))
        )
    refreshed = get_document(document_id, workspace_id=workspace_id)
    if not refreshed:
        raise HTTPException(status_code=500, detail="Unable to reload document")
    return DocumentResponse(**refreshed)


@app.post("/api/documents/{document_id}/review", response_model=DocumentResponse)
def review_document(
    document_id: str, payload: ReviewRequest, request: Request = None
) -> DocumentResponse:
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)
    document = get_document(document_id, workspace_id=workspace_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")
    if payload.approve and STRICT_APPROVAL_ROLE:
        actor_role = str(identity.get("role", "viewer")).lower()
        if not role_allows(actor_role, STRICT_APPROVAL_ROLE):
            raise HTTPException(
                status_code=403,
                detail=f"Approvals require role '{STRICT_APPROVAL_ROLE}' or higher.",
            )

    updates: dict[str, object] = {
        "reviewer_notes": payload.notes,
    }

    if not payload.approve:
        updates["status"] = "needs_review"
        updates["requires_review"] = True
    else:
        corrected_doc_type = payload.corrected_doc_type or document.get("doc_type")
        corrected_fields = {
            **document.get("extracted_fields", {}),
            **payload.corrected_fields,
        }
        corrected_department = payload.corrected_department or route_document(
            corrected_doc_type or "other"
        )

        updates["doc_type"] = corrected_doc_type
        updates["department"] = corrected_department
        updates["extracted_fields"] = corrected_fields
        updates["requires_review"] = False
        updates["missing_fields"] = []
        updates["validation_errors"] = []
        updates["status"] = (
            "corrected"
            if payload.corrected_doc_type or payload.corrected_fields
            else "approved"
        )

    updated = update_document(document_id, updates=updates, workspace_id=workspace_id)
    if not updated:
        raise HTTPException(status_code=500, detail="Unable to update document")

    create_audit_event(
        document_id=document_id,
        action="reviewed",
        actor=str(identity.get("actor", payload.actor)),
        details=(
            f"approve={payload.approve} corrected_doc_type={payload.corrected_doc_type} "
            f"allowed_types={','.join(sorted(get_active_rules()[0].keys()))}"
        ),
        workspace_id=workspace_id,
    )
    _export_approved_snapshot(
        updated,
        actor=str(identity.get("actor", payload.actor)),
        trigger="review",
    )

    # Send review complete email if document was approved/corrected
    if payload.approve:
        try:
            from .auto_emails import send_review_complete_notification

            send_review_complete_notification(document_id)
        except Exception:
            logger.debug("Review complete email failed (non-blocking)", exc_info=True)

    return DocumentResponse(**updated)


@app.post("/api/documents/{document_id}/reprocess", response_model=DocumentResponse)
def reprocess_document(document_id: str, request: Request = None) -> DocumentResponse:
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)
    document = get_document(document_id, workspace_id=workspace_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    process_document_by_id(
        document_id, actor=str(identity.get("actor", "manual_reprocess"))
    )
    updated = get_document(document_id, workspace_id=workspace_id)
    if not updated:
        raise HTTPException(status_code=500, detail="Unable to reload document")

    return DocumentResponse(**updated)


@app.get("/api/documents/{document_id}/audit", response_model=AuditTrailResponse)
def get_document_audit(
    document_id: str,
    request: Request = None,
    limit: int = Query(default=50, ge=1, le=200),
) -> AuditTrailResponse:
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    document = get_document(document_id, workspace_id=workspace_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    return AuditTrailResponse(
        items=list_audit_events(document_id, workspace_id=workspace_id, limit=limit)
    )


@app.get("/api/config/rules", response_model=RulesConfigResponse)
def get_rules_config(request: Request = None) -> RulesConfigResponse:
    _enforce(request, role="viewer")
    rules, source = get_active_rules()
    return RulesConfigResponse(source=source, path=str(get_rules_path()), rules=rules)


@app.put("/api/config/rules", response_model=RulesConfigResponse)
def update_rules_config(
    payload: RulesConfigUpdate, request: Request = None
) -> RulesConfigResponse:
    _enforce(request, role="operator")
    try:
        normalized = save_rules(
            {key: value.model_dump() for key, value in payload.rules.items()}
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return RulesConfigResponse(
        source="custom", path=str(get_rules_path()), rules=normalized
    )


@app.post("/api/config/rules/reset", response_model=RulesConfigResponse)
def reset_rules_config(request: Request = None) -> RulesConfigResponse:
    _enforce(request, role="operator")
    rules = reset_rules_to_default()
    return RulesConfigResponse(
        source="default", path=str(get_rules_path()), rules=rules
    )


@app.get("/api/queues", response_model=QueueResponse)
def get_queues(request: Request = None) -> QueueResponse:
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    queues = get_queue_snapshot(workspace_id=workspace_id)
    return QueueResponse(queues=queues)


@app.get("/api/analytics", response_model=AnalyticsResponse)
def get_analytics(request: Request = None) -> AnalyticsResponse:
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    snapshot = get_analytics_snapshot(workspace_id=workspace_id)
    return AnalyticsResponse(**snapshot)


@app.get("/api/classifier/info")
def get_classifier_info(request: Request = None) -> dict:
    """Return current classifier provider and email automation status."""
    _enforce(request, role="viewer")
    from .config import (
        AUTO_ACK_EMAIL_ENABLED,
        AUTO_ASSIGN_ENABLED,
        AUTO_MISSING_INFO_EMAIL_ENABLED,
        AUTO_STATUS_EMAIL_ENABLED,
        CLASSIFIER_PROVIDER,
        ESCALATION_DAYS,
        ESCALATION_ENABLED,
    )

    return {
        "classifier_provider": CLASSIFIER_PROVIDER,
        "email_configured": email_configured(),
        "auto_ack_email": AUTO_ACK_EMAIL_ENABLED,
        "auto_status_email": AUTO_STATUS_EMAIL_ENABLED,
        "auto_missing_info_email": AUTO_MISSING_INFO_EMAIL_ENABLED,
        "auto_assign": AUTO_ASSIGN_ENABLED,
        "escalation_enabled": ESCALATION_ENABLED,
        "escalation_days": ESCALATION_DAYS,
    }


@app.get("/api/activity/recent")
def get_recent_activity(
    request: Request = None, limit: int = Query(default=15, ge=1, le=50)
) -> dict:
    """Return recent audit events across all documents for the activity feed."""
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    where_sql = ""
    params: list[object] = []
    if workspace_id is not None:
        where_sql = "WHERE a.workspace_id = ?"
        params.append(workspace_id)
    params.append(limit)
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT a.id, a.document_id, a.action, a.actor, a.details, a.created_at,
                   d.filename
            FROM audit_events a
            LEFT JOIN documents d ON d.id = a.document_id
            """
            + where_sql
            + """
            ORDER BY a.id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/platform/connectivity", response_model=ConnectivityResponse)
def get_platform_connectivity(request: Request = None) -> ConnectivityResponse:
    _enforce(request, role="viewer")
    return _connectivity_snapshot()


@app.post("/api/platform/connectivity/check", response_model=ConnectivityResponse)
def run_platform_connectivity_check(request: Request = None) -> ConnectivityResponse:
    _enforce(request, role="operator")
    return _connectivity_snapshot()


@app.post("/api/platform/deployments/manual", response_model=DeploymentRecord)
def run_manual_deployment(
    payload: ManualDeploymentRequest, request: Request = None
) -> DeploymentRecord:
    identity = _enforce(request, role="operator")
    actor = str(identity.get("actor", payload.actor or "dashboard_admin"))

    snapshot = _connectivity_snapshot()
    health_details = (
        f"database={snapshot.database.status}; "
        f"ocr={snapshot.ocr_provider.status}; "
        f"classifier={snapshot.classifier_provider.status}; "
        f"deploy={snapshot.deployment_provider.status if snapshot.deployment_provider else 'unknown'}"
    )

    try:
        deploy_result = trigger_manual_deployment(
            environment=payload.environment, actor=actor, notes=payload.notes
        )
        status = deploy_result.get("status", "completed")
        details = f"{health_details}; {deploy_result.get('details', '')}".strip("; ")
        external_id = deploy_result.get("external_id")
        provider = str(deploy_result.get("provider", DEPLOY_PROVIDER))
    except Exception as exc:
        status = "failed"
        details = f"{health_details}; deployment_error={exc}"
        external_id = None
        provider = DEPLOY_PROVIDER

    created = create_deployment(
        environment=payload.environment,
        provider=provider,
        actor=actor,
        notes=payload.notes,
        status=status,
        details=details,
        external_id=external_id,
    )
    return DeploymentRecord(**created)


@app.get("/api/platform/deployments", response_model=DeploymentListResponse)
def get_platform_deployments(
    request: Request = None, limit: int = Query(default=20, ge=1, le=100)
) -> DeploymentListResponse:
    _enforce(request, role="viewer")
    items = [DeploymentRecord(**item) for item in list_deployments(limit=limit)]
    return DeploymentListResponse(items=items)


@app.post("/api/platform/invitations", response_model=InvitationCreateResponse)
def create_platform_invitation(
    payload: InvitationCreateRequest, request: Request
) -> InvitationCreateResponse:
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)
    invitation, raw_token = create_invitation(
        workspace_id=workspace_id,
        email=payload.email.strip().lower(),
        role=payload.role.strip().lower(),
        actor=str(identity.get("actor", payload.actor or "dashboard_admin")),
        expires_in_days=payload.expires_in_days,
    )
    invite_link = f"{str(request.base_url).rstrip('/')}/invite/{raw_token}"

    # Send invitation email (fire-and-forget)
    try:
        from .account_emails import send_invitation_email

        inviter = str(identity.get("actor", "An administrator"))
        send_invitation_email(
            payload.email.strip().lower(),
            invite_link,
            inviter,
            workspace_id=workspace_id,
        )
    except Exception:
        logger.debug("Invitation email failed (non-blocking)", exc_info=True)

    return InvitationCreateResponse(
        invitation=invitation,
        invite_token=raw_token,
        invite_link=invite_link,
    )


@app.get("/api/platform/invitations", response_model=InvitationListResponse)
def get_platform_invitations(
    request: Request = None,
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> InvitationListResponse:
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    items = [
        item
        for item in list_invitations(
            workspace_id=workspace_id,
            status=status,
            limit=limit,
        )
    ]
    return InvitationListResponse(items=items)


@app.post("/api/platform/api-keys", response_model=ApiKeyCreateResponse)
def create_platform_api_key(
    payload: ApiKeyCreateRequest, request: Request = None
) -> ApiKeyCreateResponse:
    identity = _enforce(request, role="operator")
    key_name = payload.name.strip()
    if not key_name:
        raise HTTPException(status_code=400, detail="API key name is required.")

    record, raw_key = create_api_key(
        name=key_name,
        actor=str(identity.get("actor", payload.actor or "dashboard_admin")),
    )
    return ApiKeyCreateResponse(api_key=record, raw_key=raw_key)


@app.get("/api/platform/api-keys", response_model=ApiKeyListResponse)
def get_platform_api_keys(
    request: Request = None,
    include_revoked: bool = Query(default=False),
    limit: int = Query(default=100, ge=1, le=500),
) -> ApiKeyListResponse:
    _enforce(request, role="viewer")
    items = [
        item for item in list_api_keys(include_revoked=include_revoked, limit=limit)
    ]
    return ApiKeyListResponse(items=items)


@app.post("/api/platform/api-keys/{key_id}/revoke", response_model=ApiKeyRecord)
def revoke_platform_api_key(key_id: int, request: Request = None) -> ApiKeyRecord:
    _enforce(request, role="operator")
    updated = revoke_api_key(key_id=key_id)
    if not updated:
        raise HTTPException(status_code=404, detail="API key not found.")
    return ApiKeyRecord(**updated)


@app.get("/api/platform/summary", response_model=PlatformSummaryResponse)
def get_platform_summary(request: Request = None) -> PlatformSummaryResponse:
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    connectivity = _connectivity_snapshot()
    active_api_keys = count_api_keys(status="active")
    pending_invitations = count_invitations(
        workspace_id=workspace_id,
        status="pending",
    )
    latest_deployment_raw = get_latest_deployment()
    latest_deployment = (
        DeploymentRecord(**latest_deployment_raw) if latest_deployment_raw else None
    )

    return PlatformSummaryResponse(
        connectivity=connectivity,
        active_api_keys=active_api_keys,
        pending_invitations=pending_invitations,
        latest_deployment=latest_deployment,
    )


@app.get("/api/admin/billing-stats")
def get_admin_billing_stats(request: Request = None) -> dict:
    _enforce(request, role="admin", allow_api_key=False)
    cutoff_30d = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    monthly_price_map = {"pro": 2900, "enterprise": 9900}

    with get_connection() as connection:
        active_row = connection.execute(
            "SELECT COUNT(*) AS total FROM subscriptions WHERE status = 'active'"
        ).fetchone()
        monthly_rows = connection.execute(
            """
            SELECT plan_tier, COUNT(*) AS total
            FROM subscriptions
            WHERE status = 'active' AND billing_type = 'monthly'
            GROUP BY plan_tier
            """
        ).fetchall()
        revenue_row = connection.execute(
            """
            SELECT COALESCE(SUM(amount_cents), 0) AS total
            FROM payment_events
            WHERE created_at >= ?
              AND event_type IN ('checkout.session.completed', 'invoice.paid')
            """,
            (cutoff_30d,),
        ).fetchone()
        plan_rows = connection.execute(
            """
            SELECT plan_tier, COUNT(*) AS total
            FROM users
            GROUP BY plan_tier
            ORDER BY total DESC, plan_tier ASC
            """
        ).fetchall()
        payment_rows = connection.execute(
            """
            SELECT p.id, p.user_id, p.event_type, p.amount_cents, p.currency, p.plan_tier,
                   p.billing_type, p.created_at, u.email AS user_email
            FROM payment_events p
            LEFT JOIN users u ON u.id = p.user_id
            ORDER BY p.id DESC
            LIMIT 20
            """
        ).fetchall()

    mrr_cents = 0
    for row in monthly_rows:
        plan_tier = str(row["plan_tier"] or "").strip().lower()
        mrr_cents += int(row["total"] or 0) * int(monthly_price_map.get(plan_tier, 0))

    plan_distribution = {
        str(row["plan_tier"] or "free"): int(row["total"] or 0) for row in plan_rows
    }
    recent_payments = [dict(row) for row in payment_rows]

    return {
        "active_subscriptions": int(active_row["total"] or 0) if active_row else 0,
        "mrr_cents": int(mrr_cents),
        "revenue_last_30_days_cents": int(revenue_row["total"] or 0)
        if revenue_row
        else 0,
        "plan_distribution": plan_distribution,
        "recent_payments": recent_payments,
    }


@app.get("/api/admin/system-health")
def get_admin_system_health(request: Request = None) -> dict:
    _enforce(request, role="admin", allow_api_key=False)
    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    with get_connection() as connection:
        job_rows = connection.execute(
            """
            SELECT status, COUNT(*) AS total
            FROM jobs
            GROUP BY status
            """
        ).fetchall()
        documents_row = connection.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status IN ('routed', 'approved', 'corrected', 'completed', 'archived')
                          AND requires_review = 0 THEN 1 ELSE 0 END) AS auto_routed,
                SUM(CASE WHEN requires_review = 1 OR status = 'needs_review' THEN 1 ELSE 0 END) AS needs_review,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
            FROM documents
            WHERE created_at >= ?
            """,
            (cutoff_24h,),
        ).fetchone()
        audit_errors_row = connection.execute(
            """
            SELECT COUNT(*) AS total
            FROM audit_events
            WHERE created_at >= ?
              AND LOWER(action) LIKE '%failed%'
            """,
            (cutoff_24h,),
        ).fetchone()
        email_rows = connection.execute(
            """
            SELECT status, COUNT(*) AS total
            FROM outbound_emails
            WHERE created_at >= ?
            GROUP BY status
            """,
            (cutoff_24h,),
        ).fetchall()

    job_queue = {"queued": 0, "running": 0, "failed": 0}
    for row in job_rows:
        status = str(row["status"] or "").strip().lower()
        count = int(row["total"] or 0)
        if status in {"queued"}:
            job_queue["queued"] += count
        elif status in {"running", "in_progress"}:
            job_queue["running"] += count
        elif status in {"failed"}:
            job_queue["failed"] += count

    email_counts = {"sent": 0, "failed": 0}
    for row in email_rows:
        status = str(row["status"] or "").strip().lower()
        if status in email_counts:
            email_counts[status] += int(row["total"] or 0)

    db_status = "ok"
    try:
        with get_connection() as connection:
            connection.execute("SELECT 1").fetchone()
    except Exception:
        db_status = "error"

    storage_ok = bool(UPLOAD_DIR.exists() and os.access(UPLOAD_DIR, os.W_OK))

    return {
        "job_queue": job_queue,
        "documents_last_24h": {
            "total": int(documents_row["total"] or 0) if documents_row else 0,
            "auto_routed": int(documents_row["auto_routed"] or 0)
            if documents_row
            else 0,
            "needs_review": int(documents_row["needs_review"] or 0)
            if documents_row
            else 0,
            "failed": int(documents_row["failed"] or 0) if documents_row else 0,
        },
        "errors_last_24h": int(audit_errors_row["total"] or 0)
        if audit_errors_row
        else 0,
        "emails_last_24h": email_counts,
        "connectivity": {
            "database": {"status": db_status},
            "storage": {
                "status": "ok" if storage_ok else "error",
                "path": str(UPLOAD_DIR),
            },
        },
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/admin/audit-log")
def get_admin_audit_log(
    request: Request = None,
    action: Optional[str] = Query(default=None),
    actor: Optional[str] = Query(default=None),
    document_id: Optional[str] = Query(default=None),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict:
    _enforce(request, role="admin", allow_api_key=False)

    where: list[str] = []
    params: list[object] = []
    if action:
        where.append("a.action = ?")
        params.append(action)
    if actor:
        where.append("a.actor = ?")
        params.append(actor)
    if document_id:
        where.append("a.document_id = ?")
        params.append(document_id)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    with get_connection() as connection:
        total_row = connection.execute(
            f"SELECT COUNT(*) AS total FROM audit_events a {where_sql}",
            tuple(params),
        ).fetchone()
        rows = connection.execute(
            f"""
            SELECT a.id, a.document_id, a.action, a.actor, a.details, a.created_at, d.filename
            FROM audit_events a
            LEFT JOIN documents d ON d.id = a.document_id
            {where_sql}
            ORDER BY a.id DESC
            LIMIT ? OFFSET ?
            """,
            tuple([*params, limit, offset]),
        ).fetchall()

    return {
        "items": [dict(row) for row in rows],
        "total": int(total_row["total"] or 0) if total_row else 0,
        "offset": offset,
        "limit": limit,
    }


@app.get("/api/jobs", response_model=JobListResponse)
def list_worker_jobs(
    request: Request = None,
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> JobListResponse:
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    items = [
        JobRecord(**item)
        for item in get_jobs(status=status, workspace_id=workspace_id, limit=limit)
    ]
    return JobListResponse(items=items)


@app.get("/api/jobs/{job_id}", response_model=JobRecord)
def get_worker_job(job_id: str, request: Request = None) -> JobRecord:
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    record = get_job_by_id(job_id, workspace_id=workspace_id)
    if not record:
        raise HTTPException(status_code=404, detail="Job not found.")
    return JobRecord(**record)


# =====================================================================
# Workflow Transitions (Feature 4)
# =====================================================================


@app.post("/api/documents/{document_id}/transition", response_model=DocumentResponse)
def transition_document(
    document_id: str, payload: TransitionRequest, request: Request = None
) -> DocumentResponse:
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)
    document = get_document(document_id, workspace_id=workspace_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    current = document["status"]
    allowed = ALLOWED_TRANSITIONS.get(current, set())
    if payload.status not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot transition from '{current}' to '{payload.status}'. Allowed: {sorted(allowed)}",
        )

    updates: dict[str, object] = {"status": payload.status}
    if payload.notes:
        updates["reviewer_notes"] = payload.notes

    updated = update_document(document_id, updates=updates, workspace_id=workspace_id)
    create_audit_event(
        document_id=document_id,
        action="status_transition",
        actor=str(identity.get("actor", payload.actor)),
        details=f"from={current} to={payload.status}",
        workspace_id=workspace_id,
    )
    create_notification(
        type="status_change",
        title=f"{document['filename']}: {current} → {payload.status}",
        document_id=document_id,
        workspace_id=workspace_id,
    )
    _export_approved_snapshot(
        updated,
        actor=str(identity.get("actor", payload.actor)),
        trigger="transition",
    )

    # Auto-send status update email to citizen on key transitions.
    try:
        from .auto_emails import send_auto_status_update

        send_auto_status_update(document_id, payload.status)
    except Exception:
        pass  # Never block transition on email failure.

    return DocumentResponse(**updated)


# =====================================================================
# Assignment (Feature 5)
# =====================================================================


@app.post("/api/documents/{document_id}/assign", response_model=DocumentResponse)
def assign_document(
    document_id: str, payload: AssignRequest, request: Request = None
) -> DocumentResponse:
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)
    document = get_document(document_id, workspace_id=workspace_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    updates: dict[str, object] = {"assigned_to": payload.user_id}
    if document["status"] in ("needs_review", "acknowledged"):
        updates["status"] = "assigned"

    updated = update_document(document_id, updates=updates, workspace_id=workspace_id)
    create_audit_event(
        document_id=document_id,
        action="assigned",
        actor=str(identity.get("actor", payload.actor)),
        details=f"assigned_to={payload.user_id}",
        workspace_id=workspace_id,
    )
    create_notification(
        type="assignment",
        title=f"Document assigned to you: {document['filename']}",
        message=f"Type: {document.get('doc_type', '-')}",
        user_id=payload.user_id,
        document_id=document_id,
        workspace_id=workspace_id,
    )

    # Send assignment email (fire-and-forget)
    try:
        from .auto_emails import send_assignment_notification

        send_assignment_notification(document_id, payload.user_id)
    except Exception:
        logger.debug("Assignment email failed (non-blocking)", exc_info=True)

    return DocumentResponse(**updated)


# =====================================================================
# Automation Assistant
# =====================================================================


@app.post("/api/automation/auto-assign", response_model=AutomationAutoAssignResponse)
def auto_assign_manual_documents(
    payload: AutomationAutoAssignRequest,
    request: Request = None,
) -> AutomationAutoAssignResponse:
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)
    user = identity.get("user")
    requested_user = _coerce_optional_text(payload.user_id)
    identity_user_id = user.get("id") if isinstance(user, dict) else None
    assignee = (
        requested_user or _coerce_optional_text(identity_user_id) or "triage_queue"
    )
    actor = str(identity.get("actor", payload.actor))

    assigned_count = 0
    processed_document_ids: list[str] = []
    for document in list_unassigned_manual_documents(
        workspace_id=workspace_id,
        limit=payload.limit,
    ):
        document_id = str(document.get("id", "")).strip()
        if not document_id:
            continue
        updates: dict[str, object] = {"assigned_to": assignee}
        if document.get("status") in ("needs_review", "acknowledged"):
            updates["status"] = "assigned"
        updated = update_document(
            document_id,
            updates=updates,
            workspace_id=workspace_id,
        )
        if not updated:
            continue
        assigned_count += 1
        processed_document_ids.append(document_id)
        create_audit_event(
            document_id=document_id,
            action="auto_assigned",
            actor=actor,
            details=f"assigned_to={assignee}",
            workspace_id=workspace_id,
        )
        create_notification(
            type="assignment",
            title=f"Document assigned: {document.get('filename', 'document')}",
            message="Auto-assigned by Automation Assistant.",
            user_id=assignee,
            document_id=document_id,
            workspace_id=workspace_id,
        )
        try:
            from .auto_emails import send_assignment_notification

            send_assignment_notification(document_id, assignee)
        except Exception:
            logger.debug(
                "Auto-assignment email failed for doc %s (non-blocking)",
                document_id,
                exc_info=True,
            )

    remaining_unassigned = count_unassigned_manual_documents(workspace_id=workspace_id)
    return AutomationAutoAssignResponse(
        assignee=assignee,
        assigned_count=assigned_count,
        remaining_unassigned=remaining_unassigned,
        processed_document_ids=processed_document_ids,
    )


@app.post(
    "/api/automation/anthropic-sweep",
    response_model=AutomationAnthropicSweepResponse,
)
def run_anthropic_automation_sweep(
    payload: AutomationAnthropicSweepRequest,
    request: Request = None,
) -> AutomationAnthropicSweepResponse:
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)

    # Plan enforcement: check AI classifier access
    user = identity.get("user")
    if user and STRIPE_ENABLED:
        enforce_plan_limits(
            user["id"],
            "use_ai_classifier",
            workspace_id=workspace_id,
        )

    if not ANTHROPIC_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="ANTHROPIC_API_KEY is not configured.",
        )

    candidates: list[dict[str, object]] = []
    sweep_statuses = ["needs_review", "assigned", "acknowledged"]
    if payload.include_failed:
        sweep_statuses.append("failed")
    for status in sweep_statuses:
        remaining = max(payload.limit - len(candidates), 0)
        if remaining <= 0:
            break
        candidates.extend(
            list_documents(status=status, workspace_id=workspace_id, limit=remaining)
        )

    seen_ids: set[str] = set()
    unique_ids: list[str] = []
    for item in candidates:
        doc_id = str(item.get("id", "")).strip()
        if not doc_id or doc_id in seen_ids:
            continue
        seen_ids.add(doc_id)
        unique_ids.append(doc_id)

    processed_count = 0
    auto_cleared_count = 0
    still_manual_count = 0
    processed_document_ids: list[str] = []
    for doc_id in unique_ids:
        process_document_by_id(
            doc_id,
            actor=payload.actor,
            force_anthropic_classification=True,
        )
        refreshed = get_document(doc_id, workspace_id=workspace_id)
        processed_count += 1
        processed_document_ids.append(doc_id)
        status = str((refreshed or {}).get("status", "")).strip().lower()
        if status == "routed":
            auto_cleared_count += 1
        else:
            still_manual_count += 1

    return AutomationAnthropicSweepResponse(
        processed_count=processed_count,
        auto_cleared_count=auto_cleared_count,
        still_manual_count=still_manual_count,
        processed_document_ids=processed_document_ids,
    )


# =====================================================================
# Notifications (Feature 2)
# =====================================================================


@app.get("/api/notifications", response_model=NotificationListResponse)
def get_notifications(
    request: Request = None,
    unread_only: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=200),
) -> NotificationListResponse:
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    user = identity.get("user")
    user_id = user.get("id") if isinstance(user, dict) else None
    items = list_notifications(
        user_id=user_id,
        workspace_id=workspace_id,
        unread_only=unread_only,
        limit=limit,
    )
    unread = count_unread(user_id=user_id, workspace_id=workspace_id)
    return NotificationListResponse(
        items=[NotificationRecord(**n) for n in items],
        unread_count=unread,
    )


@app.post("/api/notifications/{notification_id}/read")
def read_notification(notification_id: int, request: Request = None):
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    user = identity.get("user")
    user_id = user.get("id") if isinstance(user, dict) else None
    result = mark_read(notification_id, user_id=user_id, workspace_id=workspace_id)
    if not result:
        raise HTTPException(status_code=404, detail="Notification not found")
    return NotificationRecord(**result)


@app.post("/api/notifications/read-all")
def read_all_notifications(request: Request = None):
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    user = identity.get("user")
    user_id = user.get("id") if isinstance(user, dict) else None
    count = mark_all_read(user_id=user_id, workspace_id=workspace_id)
    return {"marked_read": count}


# =====================================================================
# Watcher Status (Feature 3)
# =====================================================================


@app.get("/api/watcher/status")
def get_watcher_status(request: Request = None):
    _enforce(request, role="viewer")
    from .config import WATCH_DIR, WATCH_ENABLED, WATCH_INTERVAL_SECONDS
    from .db import get_connection as _get_conn

    with _get_conn() as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM watched_files").fetchone()
    return {
        "enabled": WATCH_ENABLED,
        "watch_dir": WATCH_DIR,
        "interval_seconds": WATCH_INTERVAL_SECONDS,
        "files_ingested": row["total"] if row else 0,
    }


# =====================================================================
# Response Templates (Feature 6)
# =====================================================================


@app.get("/api/templates", response_model=TemplateListResponse)
def get_templates(
    request: Request = None,
    doc_type: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> TemplateListResponse:
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    items = [
        TemplateRecord(**t)
        for t in list_templates(
            workspace_id=workspace_id,
            doc_type=doc_type,
            limit=limit,
        )
    ]
    return TemplateListResponse(items=items)


@app.post("/api/templates", response_model=TemplateRecord)
def create_new_template(
    payload: TemplateCreateRequest, request: Request = None
) -> TemplateRecord:
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)
    record = create_template_record(
        workspace_id=workspace_id,
        name=payload.name,
        doc_type=payload.doc_type,
        template_body=payload.template_body,
    )
    return TemplateRecord(**record)


@app.get("/api/templates/{template_id}", response_model=TemplateRecord)
def get_template_by_id(template_id: int, request: Request = None) -> TemplateRecord:
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    record = get_template(template_id, workspace_id=workspace_id)
    if not record:
        raise HTTPException(status_code=404, detail="Template not found")
    return TemplateRecord(**record)


@app.put("/api/templates/{template_id}", response_model=TemplateRecord)
def update_template_by_id(
    template_id: int, payload: TemplateUpdateRequest, request: Request = None
) -> TemplateRecord:
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)
    record = update_template(
        template_id,
        name=payload.name,
        doc_type=payload.doc_type,
        template_body=payload.template_body,
        workspace_id=workspace_id,
    )
    if not record:
        raise HTTPException(status_code=404, detail="Template not found")
    return TemplateRecord(**record)


@app.delete("/api/templates/{template_id}")
def delete_template_by_id(template_id: int, request: Request = None):
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)
    if not delete_template(template_id, workspace_id=workspace_id):
        raise HTTPException(status_code=404, detail="Template not found")
    return {"deleted": True}


@app.post(
    "/api/templates/{template_id}/render/{document_id}",
    response_model=TemplateRenderResponse,
)
def render_template_for_document(
    template_id: int, document_id: str, request: Request = None
) -> TemplateRenderResponse:
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    template_record = get_template(template_id, workspace_id=workspace_id)
    if not template_record:
        raise HTTPException(status_code=404, detail="Template not found")
    try:
        rendered = render_template(
            template_id,
            document_id,
            workspace_id=workspace_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return TemplateRenderResponse(
        rendered=rendered,
        template_name=template_record["name"],
        document_id=document_id,
    )


@app.post(
    "/api/templates/{template_id}/compose/{document_id}",
    response_model=TemplateComposeResponse,
)
def compose_template_for_document(
    template_id: int,
    document_id: str,
    request: Request = None,
) -> TemplateComposeResponse:
    identity = _enforce(request, role="viewer")
    workspace_id = _resolve_workspace_id(identity)
    try:
        composed = compose_template_email(
            template_id,
            document_id,
            workspace_id=workspace_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return TemplateComposeResponse(**composed)


@app.post(
    "/api/documents/{document_id}/send-response",
    response_model=ResponseEmailSendResponse,
)
def send_document_response(
    document_id: str,
    payload: ResponseEmailSendRequest,
    request: Request = None,
) -> ResponseEmailSendResponse:
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)
    document = get_document(document_id, workspace_id=workspace_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    to_email = str(payload.to_email or "").strip()
    subject = str(payload.subject or "").strip()
    body = str(payload.body or "").strip()
    if not EMAIL_RE.match(to_email):
        raise HTTPException(
            status_code=400, detail="A valid recipient email is required."
        )
    if not subject:
        raise HTTPException(status_code=400, detail="Subject is required.")
    if not body:
        raise HTTPException(status_code=400, detail="Message body is required.")

    actor = str(identity.get("actor", payload.actor))
    email_record = create_outbound_email(
        document_id=document_id,
        workspace_id=workspace_id,
        to_email=to_email,
        subject=subject,
        body=body,
        status="pending",
    )

    if not email_configured():
        failed = update_outbound_email(
            int(email_record["id"]),
            status="failed",
            error="Email sending is not configured.",
        )
        create_audit_event(
            document_id=document_id,
            action="response_email_failed",
            actor=actor,
            details=f"to={to_email} reason=email_not_configured",
            workspace_id=workspace_id,
        )
        raise HTTPException(
            status_code=503,
            detail="Email sending is not configured. Set CITYSORT_EMAIL_* and CITYSORT_SMTP_* env vars.",
        )

    try:
        send_email(to_email=to_email, subject=subject, body=body)
        updated = update_outbound_email(
            int(email_record["id"]),
            status="sent",
            sent_at=_utcnow_iso(),
            error=None,
        )
        create_audit_event(
            document_id=document_id,
            action="response_email_sent",
            actor=actor,
            details=f"to={to_email} subject={subject}",
            workspace_id=workspace_id,
        )
        create_notification(
            type="response_sent",
            title=f"Response email sent: {document.get('filename', 'document')}",
            message=f"Sent to {to_email}",
            document_id=document_id,
            workspace_id=workspace_id,
        )
        return ResponseEmailSendResponse(**(updated or email_record))
    except Exception as exc:
        failed = update_outbound_email(
            int(email_record["id"]),
            status="failed",
            error=str(exc),
        )
        create_audit_event(
            document_id=document_id,
            action="response_email_failed",
            actor=actor,
            details=f"to={to_email} error={exc}",
            workspace_id=workspace_id,
        )
        return ResponseEmailSendResponse(**(failed or email_record))


# =====================================================================
# Bulk Operations (Feature 7)
# =====================================================================


@app.post("/api/documents/bulk", response_model=BulkActionResponse)
def bulk_document_action(
    payload: BulkActionRequest, request: Request = None
) -> BulkActionResponse:
    identity = _enforce(request, role="operator")
    workspace_id = _resolve_workspace_id(identity)
    actor = str(identity.get("actor", payload.actor))
    success_count = 0
    errors: list[str] = []

    for doc_id in payload.document_ids:
        try:
            doc = get_document(doc_id, workspace_id=workspace_id)
            if not doc:
                errors.append(f"{doc_id}: not found")
                continue

            if payload.action == "approve":
                updated_doc = update_document(
                    doc_id,
                    updates={
                        "status": "approved",
                        "requires_review": False,
                        "missing_fields": [],
                        "validation_errors": [],
                    },
                    workspace_id=workspace_id,
                )
                create_audit_event(
                    document_id=doc_id,
                    action="bulk_approved",
                    actor=actor,
                    workspace_id=workspace_id,
                )
                if updated_doc:
                    _export_approved_snapshot(
                        updated_doc, actor=actor, trigger="bulk_approve"
                    )

            elif payload.action == "assign":
                user_id = payload.params.get("user_id")
                if not user_id:
                    errors.append(f"{doc_id}: user_id required for assign")
                    continue
                updates_map: dict[str, object] = {"assigned_to": user_id}
                if doc["status"] in ("needs_review", "acknowledged"):
                    updates_map["status"] = "assigned"
                update_document(doc_id, updates=updates_map, workspace_id=workspace_id)
                create_audit_event(
                    document_id=doc_id,
                    action="bulk_assigned",
                    actor=actor,
                    details=f"assigned_to={user_id}",
                    workspace_id=workspace_id,
                )

            elif payload.action == "transition":
                target_status = payload.params.get("status")
                if not target_status:
                    errors.append(f"{doc_id}: status required for transition")
                    continue
                allowed = ALLOWED_TRANSITIONS.get(doc["status"], set())
                if target_status not in allowed:
                    errors.append(
                        f"{doc_id}: invalid transition {doc['status']} → {target_status}"
                    )
                    continue
                updated_doc = update_document(
                    doc_id,
                    updates={"status": target_status},
                    workspace_id=workspace_id,
                )
                create_audit_event(
                    document_id=doc_id,
                    action="bulk_transition",
                    actor=actor,
                    details=f"to={target_status}",
                    workspace_id=workspace_id,
                )
                if updated_doc:
                    _export_approved_snapshot(
                        updated_doc, actor=actor, trigger="bulk_transition"
                    )

            else:
                errors.append(f"{doc_id}: unknown action '{payload.action}'")
                continue

            success_count += 1
        except Exception as exc:
            errors.append(f"{doc_id}: {exc}")

    return BulkActionResponse(
        success_count=success_count,
        error_count=len(errors),
        errors=errors[:50],
    )
