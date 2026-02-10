from __future__ import annotations

import mimetypes
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .auth import (
    authenticate_user,
    authorize_request,
    bootstrap_admin,
    create_user_account,
    get_users,
    set_user_role,
)
from .config import (
    DEPLOY_PROVIDER,
    CLASSIFIER_PROVIDER,
    CONFIDENCE_THRESHOLD,
    FORCE_REVIEW_DOC_TYPES,
    OCR_PROVIDER,
    REQUIRE_AUTH,
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
from .jobs import enqueue_document_processing, get_job_by_id, get_jobs, start_job_worker, stop_job_worker
from .document_tasks import process_document_by_id
from .pipeline import route_document
from .repository import (
    count_api_keys,
    count_invitations,
    create_api_key,
    create_audit_event,
    create_deployment,
    create_document,
    create_outbound_email,
    create_invitation,
    get_latest_deployment,
    get_analytics_snapshot,
    get_document,
    get_queue_snapshot,
    list_api_keys,
    list_audit_events,
    list_deployments,
    list_documents,
    list_invitations,
    list_overdue_documents,
    revoke_api_key,
    update_outbound_email,
    update_document,
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
from .watcher import start_watcher, stop_watcher
from .schemas import (
    AnalyticsResponse,
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
    UserCreateRequest,
    UserListResponse,
    UserRecord,
    UserRoleUpdateRequest,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FRONTEND_DIR = PROJECT_ROOT / "frontend"

app = FastAPI(
    title="CitySort AI MVP",
    description="AI-powered document intake, classification, and routing for local government.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def _coerce_optional_text(value: object | None) -> Optional[str]:
    if value is None:
        return None
    parsed = str(value).strip()
    return parsed or None


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _enforce(
    request: Optional[Request],
    *,
    role: str = "viewer",
    allow_api_key: bool = True,
) -> dict[str, object]:
    return authorize_request(request, required_role=role, allow_api_key=allow_api_key)


def _database_health() -> ServiceHealth:
    try:
        with get_connection() as connection:
            connection.execute("SELECT 1").fetchone()
        return ServiceHealth(name="database", status="ok", configured=True, details="Database connection succeeded.")
    except Exception as exc:  # pragma: no cover - runtime safeguard
        return ServiceHealth(name="database", status="error", configured=False, details=str(exc))


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
            details="OpenAI classifier configured." if configured else "Missing OPENAI_API_KEY.",
        )

    if provider == "anthropic":
        from .config import ANTHROPIC_API_KEY

        configured = bool(ANTHROPIC_API_KEY)
        return ServiceHealth(
            name="classifier",
            status="ok" if configured else "not_configured",
            configured=configured,
            details="Anthropic classifier configured." if configured else "Missing ANTHROPIC_API_KEY.",
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


@app.on_event("startup")
def startup_event() -> None:
    init_db()
    start_job_worker()
    start_watcher()


@app.on_event("shutdown")
def shutdown_event() -> None:
    stop_job_worker()
    stop_watcher()


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


@app.get("/")
def root() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.post("/api/auth/bootstrap", response_model=AuthResponse)
def auth_bootstrap(payload: AuthBootstrapRequest) -> AuthResponse:
    try:
        result = bootstrap_admin(email=payload.email, password=payload.password, full_name=payload.full_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return AuthResponse(access_token=result["access_token"], user=UserRecord(**result["user"]))


@app.post("/api/auth/login", response_model=AuthResponse)
def auth_login(payload: AuthLoginRequest) -> AuthResponse:
    result = authenticate_user(email=payload.email, password=payload.password)
    return AuthResponse(access_token=result["access_token"], user=UserRecord(**result["user"]))


@app.get("/api/auth/me", response_model=UserRecord)
def auth_me(request: Request) -> UserRecord:
    identity = _enforce(request, role="viewer", allow_api_key=False)
    user = identity.get("user")
    if not user:
        raise HTTPException(status_code=401, detail="User session required.")
    return UserRecord(**user)


@app.get("/api/auth/users", response_model=UserListResponse)
def auth_list_users(request: Request, limit: int = Query(default=200, ge=1, le=500)) -> UserListResponse:
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
def auth_update_user_role(request: Request, user_id: str, payload: UserRoleUpdateRequest) -> UserRecord:
    _enforce(request, role="admin", allow_api_key=False)
    try:
        updated = set_user_role(user_id=user_id, role=payload.role)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return UserRecord(**updated)


@app.post("/api/documents/upload", response_model=DocumentResponse)
async def upload_document(
    request: Request = None,
    file: UploadFile = File(...),
    source_channel: str = Form("upload_portal"),
    process_async: bool = Form(False),
) -> DocumentResponse:
    identity = _enforce(request, role="operator")
    actor = str(identity.get("actor", "upload_portal"))

    if not file.filename:
        raise HTTPException(status_code=400, detail="File name is required")

    document_id = str(uuid.uuid4())
    safe_filename = f"{document_id}_{Path(file.filename).name}"
    file_path = UPLOAD_DIR / safe_filename

    contents = await file.read()
    file_path.write_bytes(contents)

    document = create_document(
        document={
            "id": document_id,
            "filename": file.filename,
            "storage_path": str(file_path),
            "source_channel": source_channel,
            "content_type": file.content_type,
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
    )

    if process_async:
        enqueue_document_processing(document_id=document_id, actor=actor)
    else:
        process_document_by_id(document_id, actor=actor)

    refreshed = get_document(document_id)
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
            rows = fetch_import_rows(connection=connection, query=payload.query, limit=payload.limit)
        except ExternalDatabaseError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

        for index, row in enumerate(rows, start=1):
            try:
                raw_filename = _coerce_optional_text(get_row_value(row, payload.filename_column))
                fallback_name = f"db_document_{index}.txt"
                filename = Path(raw_filename or fallback_name).name or fallback_name

                source_path: Optional[Path] = None
                if payload.file_path_column:
                    raw_path_value = _coerce_optional_text(get_row_value(row, payload.file_path_column))
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
                        raw_content_type = get_row_value(row, payload.content_type_column)
                    except KeyError:
                        raw_content_type = None

                content_type = _coerce_optional_text(raw_content_type) or mimetypes.guess_type(filename)[0]

                document_id = str(uuid.uuid4())
                safe_filename = f"{document_id}_{filename}"
                storage_path = UPLOAD_DIR / safe_filename

                if source_path is not None:
                    shutil.copy2(source_path, storage_path)
                else:
                    storage_path.write_bytes(file_bytes or b"")

                create_document(
                    document={
                        "id": document_id,
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
                )

                if payload.process_async:
                    enqueue_document_processing(document_id=document_id, actor=actor)
                    scheduled_async_count += 1
                else:
                    process_document_by_id(document_id, actor=actor)
                    processed_sync_count += 1

                refreshed = get_document(document_id)
                if refreshed:
                    imported_items.append(
                        {
                            "id": refreshed["id"],
                            "filename": refreshed["filename"],
                            "status": refreshed["status"],
                        }
                    )
                else:
                    imported_items.append({"id": document_id, "filename": filename, "status": "ingested"})

            except KeyError as exc:
                missing_column = exc.args[0] if exc.args else "unknown"
                errors.append(f"Row {index}: Missing expected column '{missing_column}'.")
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
    "servicenow", "confluence", "salesforce",
    "google_cloud_storage", "amazon_s3", "jira", "sharepoint",
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
        missing = [k for k, v in config.items() if not str(v).strip()]
        if missing:
            return ConnectorTestResponse(
                success=False,
                message=f"Missing required fields: {', '.join(missing)}",
                connector_type=connector_type,
            )
        friendly_name = connector_type.replace("_", " ").title()
        return ConnectorTestResponse(
            success=False,
            message=f"{friendly_name} integration is coming soon. Your configuration looks valid.",
            connector_type=connector_type,
            details="SaaS connector integration pending.",
        )

    raise HTTPException(status_code=400, detail=f"Unknown connector type: {connector_type}")


@app.get("/api/documents", response_model=DocumentListResponse)
def get_documents(
    request: Request = None,
    status: Optional[str] = Query(default=None),
    department: Optional[str] = Query(default=None),
    assigned_to: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> DocumentListResponse:
    _enforce(request, role="viewer")
    items: list[DocumentResponse] = []
    for item in list_documents(status=status, department=department, assigned_to=assigned_to, limit=limit):
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
    _enforce(request, role="viewer")
    items: list[DocumentResponse] = []
    for item in list_overdue_documents(limit=limit):
        item_payload = dict(item)
        item_payload["extracted_text"] = None
        items.append(DocumentResponse(**item_payload))
    return DocumentListResponse(items=items)


@app.get("/api/documents/{document_id}", response_model=DocumentResponse)
def get_document_by_id(document_id: str, request: Request = None) -> DocumentResponse:
    _enforce(request, role="viewer")
    record = get_document(document_id)
    if not record:
        raise HTTPException(status_code=404, detail="Document not found")
    return DocumentResponse(**record)


@app.get("/api/documents/{document_id}/download")
def download_document(document_id: str, request: Request = None):
    _enforce(request, role="viewer")
    record = get_document(document_id)
    if not record:
        raise HTTPException(status_code=404, detail="Document not found")
    file_path = Path(record.get("storage_path", ""))
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found on disk")
    if not file_path.resolve().is_relative_to(UPLOAD_DIR.resolve()):
        raise HTTPException(status_code=403, detail="Access denied")
    media_type = mimetypes.guess_type(record["filename"])[0] or "application/octet-stream"
    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=record["filename"],
        headers={"Content-Disposition": f'attachment; filename="{record["filename"]}"'},
    )


@app.post("/api/documents/{document_id}/reupload", response_model=DocumentResponse)
async def reupload_document(
    document_id: str,
    request: Request = None,
    file: UploadFile = File(...),
    reprocess: bool = Form(True),
) -> DocumentResponse:
    identity = _enforce(request, role="operator")
    document = get_document(document_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")
    contents = await file.read()
    safe_filename = f"{document_id}_{Path(file.filename).name}"
    new_file_path = UPLOAD_DIR / safe_filename
    old_path = Path(document.get("storage_path", ""))
    if old_path.exists():
        old_path.unlink(missing_ok=True)
    new_file_path.write_bytes(contents)
    content_type = file.content_type or mimetypes.guess_type(file.filename)[0]
    update_document(document_id, updates={
        "storage_path": str(new_file_path),
        "filename": file.filename,
        "content_type": content_type,
    })
    create_audit_event(
        document_id=document_id,
        action="reuploaded",
        actor=str(identity.get("actor", "dashboard_reviewer")),
        details=f"new_file={file.filename}",
    )
    if reprocess:
        process_document_by_id(document_id, actor=str(identity.get("actor", "manual_reupload")))
    refreshed = get_document(document_id)
    if not refreshed:
        raise HTTPException(status_code=500, detail="Unable to reload document")
    return DocumentResponse(**refreshed)


@app.post("/api/documents/{document_id}/review", response_model=DocumentResponse)
def review_document(document_id: str, payload: ReviewRequest, request: Request = None) -> DocumentResponse:
    identity = _enforce(request, role="operator")
    document = get_document(document_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    updates: dict[str, object] = {
        "reviewer_notes": payload.notes,
    }

    if not payload.approve:
        updates["status"] = "needs_review"
        updates["requires_review"] = True
    else:
        corrected_doc_type = payload.corrected_doc_type or document.get("doc_type")
        corrected_fields = {**document.get("extracted_fields", {}), **payload.corrected_fields}
        corrected_department = payload.corrected_department or route_document(corrected_doc_type or "other")

        updates["doc_type"] = corrected_doc_type
        updates["department"] = corrected_department
        updates["extracted_fields"] = corrected_fields
        updates["requires_review"] = False
        updates["missing_fields"] = []
        updates["validation_errors"] = []
        updates["status"] = "corrected" if payload.corrected_doc_type or payload.corrected_fields else "approved"

    updated = update_document(document_id, updates=updates)
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
    )

    return DocumentResponse(**updated)


@app.post("/api/documents/{document_id}/reprocess", response_model=DocumentResponse)
def reprocess_document(document_id: str, request: Request = None) -> DocumentResponse:
    identity = _enforce(request, role="operator")
    document = get_document(document_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    process_document_by_id(document_id, actor=str(identity.get("actor", "manual_reprocess")))
    updated = get_document(document_id)
    if not updated:
        raise HTTPException(status_code=500, detail="Unable to reload document")

    return DocumentResponse(**updated)


@app.get("/api/documents/{document_id}/audit", response_model=AuditTrailResponse)
def get_document_audit(
    document_id: str,
    request: Request = None,
    limit: int = Query(default=50, ge=1, le=200),
) -> AuditTrailResponse:
    _enforce(request, role="viewer")
    document = get_document(document_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    return AuditTrailResponse(items=list_audit_events(document_id, limit=limit))


@app.get("/api/config/rules", response_model=RulesConfigResponse)
def get_rules_config(request: Request = None) -> RulesConfigResponse:
    _enforce(request, role="viewer")
    rules, source = get_active_rules()
    return RulesConfigResponse(source=source, path=str(get_rules_path()), rules=rules)


@app.put("/api/config/rules", response_model=RulesConfigResponse)
def update_rules_config(payload: RulesConfigUpdate, request: Request = None) -> RulesConfigResponse:
    _enforce(request, role="operator")
    try:
        normalized = save_rules({key: value.model_dump() for key, value in payload.rules.items()})
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return RulesConfigResponse(source="custom", path=str(get_rules_path()), rules=normalized)


@app.post("/api/config/rules/reset", response_model=RulesConfigResponse)
def reset_rules_config(request: Request = None) -> RulesConfigResponse:
    _enforce(request, role="operator")
    rules = reset_rules_to_default()
    return RulesConfigResponse(source="default", path=str(get_rules_path()), rules=rules)


@app.get("/api/queues", response_model=QueueResponse)
def get_queues(request: Request = None) -> QueueResponse:
    _enforce(request, role="viewer")
    queues = get_queue_snapshot()
    return QueueResponse(queues=queues)


@app.get("/api/analytics", response_model=AnalyticsResponse)
def get_analytics(request: Request = None) -> AnalyticsResponse:
    _enforce(request, role="viewer")
    snapshot = get_analytics_snapshot()
    return AnalyticsResponse(**snapshot)


@app.get("/api/platform/connectivity", response_model=ConnectivityResponse)
def get_platform_connectivity(request: Request = None) -> ConnectivityResponse:
    _enforce(request, role="viewer")
    return _connectivity_snapshot()


@app.post("/api/platform/connectivity/check", response_model=ConnectivityResponse)
def run_platform_connectivity_check(request: Request = None) -> ConnectivityResponse:
    _enforce(request, role="operator")
    return _connectivity_snapshot()


@app.post("/api/platform/deployments/manual", response_model=DeploymentRecord)
def run_manual_deployment(payload: ManualDeploymentRequest, request: Request = None) -> DeploymentRecord:
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
        deploy_result = trigger_manual_deployment(environment=payload.environment, actor=actor, notes=payload.notes)
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
def get_platform_deployments(request: Request = None, limit: int = Query(default=20, ge=1, le=100)) -> DeploymentListResponse:
    _enforce(request, role="viewer")
    items = [DeploymentRecord(**item) for item in list_deployments(limit=limit)]
    return DeploymentListResponse(items=items)


@app.post("/api/platform/invitations", response_model=InvitationCreateResponse)
def create_platform_invitation(payload: InvitationCreateRequest, request: Request) -> InvitationCreateResponse:
    identity = _enforce(request, role="operator")
    invitation, raw_token = create_invitation(
        email=payload.email.strip().lower(),
        role=payload.role.strip().lower(),
        actor=str(identity.get("actor", payload.actor or "dashboard_admin")),
        expires_in_days=payload.expires_in_days,
    )
    invite_link = f"{str(request.base_url).rstrip('/')}/invite/{raw_token}"
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
    _enforce(request, role="viewer")
    items = [item for item in list_invitations(status=status, limit=limit)]
    return InvitationListResponse(items=items)


@app.post("/api/platform/api-keys", response_model=ApiKeyCreateResponse)
def create_platform_api_key(payload: ApiKeyCreateRequest, request: Request = None) -> ApiKeyCreateResponse:
    identity = _enforce(request, role="operator")
    key_name = payload.name.strip()
    if not key_name:
        raise HTTPException(status_code=400, detail="API key name is required.")

    record, raw_key = create_api_key(name=key_name, actor=str(identity.get("actor", payload.actor or "dashboard_admin")))
    return ApiKeyCreateResponse(api_key=record, raw_key=raw_key)


@app.get("/api/platform/api-keys", response_model=ApiKeyListResponse)
def get_platform_api_keys(
    request: Request = None,
    include_revoked: bool = Query(default=False),
    limit: int = Query(default=100, ge=1, le=500),
) -> ApiKeyListResponse:
    _enforce(request, role="viewer")
    items = [item for item in list_api_keys(include_revoked=include_revoked, limit=limit)]
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
    _enforce(request, role="viewer")
    connectivity = _connectivity_snapshot()
    active_api_keys = count_api_keys(status="active")
    pending_invitations = count_invitations(status="pending")
    latest_deployment_raw = get_latest_deployment()
    latest_deployment = DeploymentRecord(**latest_deployment_raw) if latest_deployment_raw else None

    return PlatformSummaryResponse(
        connectivity=connectivity,
        active_api_keys=active_api_keys,
        pending_invitations=pending_invitations,
        latest_deployment=latest_deployment,
    )


@app.get("/api/jobs", response_model=JobListResponse)
def list_worker_jobs(
    request: Request = None,
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> JobListResponse:
    _enforce(request, role="viewer")
    items = [JobRecord(**item) for item in get_jobs(status=status, limit=limit)]
    return JobListResponse(items=items)


@app.get("/api/jobs/{job_id}", response_model=JobRecord)
def get_worker_job(job_id: str, request: Request = None) -> JobRecord:
    _enforce(request, role="viewer")
    record = get_job_by_id(job_id)
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
    document = get_document(document_id)
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

    updated = update_document(document_id, updates=updates)
    create_audit_event(
        document_id=document_id,
        action="status_transition",
        actor=str(identity.get("actor", payload.actor)),
        details=f"from={current} to={payload.status}",
    )
    create_notification(
        type="status_change",
        title=f"{document['filename']}: {current} → {payload.status}",
        document_id=document_id,
    )
    return DocumentResponse(**updated)


# =====================================================================
# Assignment (Feature 5)
# =====================================================================

@app.post("/api/documents/{document_id}/assign", response_model=DocumentResponse)
def assign_document(
    document_id: str, payload: AssignRequest, request: Request = None
) -> DocumentResponse:
    identity = _enforce(request, role="operator")
    document = get_document(document_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    updates: dict[str, object] = {"assigned_to": payload.user_id}
    if document["status"] in ("needs_review", "acknowledged"):
        updates["status"] = "assigned"

    updated = update_document(document_id, updates=updates)
    create_audit_event(
        document_id=document_id,
        action="assigned",
        actor=str(identity.get("actor", payload.actor)),
        details=f"assigned_to={payload.user_id}",
    )
    create_notification(
        type="assignment",
        title=f"Document assigned to you: {document['filename']}",
        message=f"Type: {document.get('doc_type', '-')}",
        user_id=payload.user_id,
        document_id=document_id,
    )
    return DocumentResponse(**updated)


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
    user = identity.get("user")
    user_id = user.get("id") if isinstance(user, dict) else None
    items = list_notifications(user_id=user_id, unread_only=unread_only, limit=limit)
    unread = count_unread(user_id=user_id)
    return NotificationListResponse(
        items=[NotificationRecord(**n) for n in items],
        unread_count=unread,
    )


@app.post("/api/notifications/{notification_id}/read")
def read_notification(notification_id: int, request: Request = None):
    identity = _enforce(request, role="viewer")
    user = identity.get("user")
    user_id = user.get("id") if isinstance(user, dict) else None
    result = mark_read(notification_id, user_id=user_id)
    if not result:
        raise HTTPException(status_code=404, detail="Notification not found")
    return NotificationRecord(**result)


@app.post("/api/notifications/read-all")
def read_all_notifications(request: Request = None):
    identity = _enforce(request, role="viewer")
    user = identity.get("user")
    user_id = user.get("id") if isinstance(user, dict) else None
    count = mark_all_read(user_id=user_id)
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
    _enforce(request, role="viewer")
    items = [TemplateRecord(**t) for t in list_templates(doc_type=doc_type, limit=limit)]
    return TemplateListResponse(items=items)


@app.post("/api/templates", response_model=TemplateRecord)
def create_new_template(payload: TemplateCreateRequest, request: Request = None) -> TemplateRecord:
    _enforce(request, role="operator")
    record = create_template_record(
        name=payload.name,
        doc_type=payload.doc_type,
        template_body=payload.template_body,
    )
    return TemplateRecord(**record)


@app.get("/api/templates/{template_id}", response_model=TemplateRecord)
def get_template_by_id(template_id: int, request: Request = None) -> TemplateRecord:
    _enforce(request, role="viewer")
    record = get_template(template_id)
    if not record:
        raise HTTPException(status_code=404, detail="Template not found")
    return TemplateRecord(**record)


@app.put("/api/templates/{template_id}", response_model=TemplateRecord)
def update_template_by_id(
    template_id: int, payload: TemplateUpdateRequest, request: Request = None
) -> TemplateRecord:
    _enforce(request, role="operator")
    record = update_template(
        template_id,
        name=payload.name,
        doc_type=payload.doc_type,
        template_body=payload.template_body,
    )
    if not record:
        raise HTTPException(status_code=404, detail="Template not found")
    return TemplateRecord(**record)


@app.delete("/api/templates/{template_id}")
def delete_template_by_id(template_id: int, request: Request = None):
    _enforce(request, role="operator")
    if not delete_template(template_id):
        raise HTTPException(status_code=404, detail="Template not found")
    return {"deleted": True}


@app.post(
    "/api/templates/{template_id}/render/{document_id}",
    response_model=TemplateRenderResponse,
)
def render_template_for_document(
    template_id: int, document_id: str, request: Request = None
) -> TemplateRenderResponse:
    _enforce(request, role="viewer")
    template_record = get_template(template_id)
    if not template_record:
        raise HTTPException(status_code=404, detail="Template not found")
    try:
        rendered = render_template(template_id, document_id)
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
    _enforce(request, role="viewer")
    try:
        composed = compose_template_email(template_id, document_id)
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
    document = get_document(document_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    to_email = str(payload.to_email or "").strip()
    subject = str(payload.subject or "").strip()
    body = str(payload.body or "").strip()
    if not EMAIL_RE.match(to_email):
        raise HTTPException(status_code=400, detail="A valid recipient email is required.")
    if not subject:
        raise HTTPException(status_code=400, detail="Subject is required.")
    if not body:
        raise HTTPException(status_code=400, detail="Message body is required.")

    actor = str(identity.get("actor", payload.actor))
    email_record = create_outbound_email(
        document_id=document_id,
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
        )
        create_notification(
            type="response_sent",
            title=f"Response email sent: {document.get('filename', 'document')}",
            message=f"Sent to {to_email}",
            document_id=document_id,
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
        )
        return ResponseEmailSendResponse(**(failed or email_record))


# =====================================================================
# Bulk Operations (Feature 7)
# =====================================================================

@app.post("/api/documents/bulk", response_model=BulkActionResponse)
def bulk_document_action(payload: BulkActionRequest, request: Request = None) -> BulkActionResponse:
    identity = _enforce(request, role="operator")
    actor = str(identity.get("actor", payload.actor))
    success_count = 0
    errors: list[str] = []

    for doc_id in payload.document_ids:
        try:
            doc = get_document(doc_id)
            if not doc:
                errors.append(f"{doc_id}: not found")
                continue

            if payload.action == "approve":
                update_document(doc_id, updates={
                    "status": "approved",
                    "requires_review": False,
                    "missing_fields": [],
                    "validation_errors": [],
                })
                create_audit_event(
                    document_id=doc_id, action="bulk_approved", actor=actor,
                )

            elif payload.action == "assign":
                user_id = payload.params.get("user_id")
                if not user_id:
                    errors.append(f"{doc_id}: user_id required for assign")
                    continue
                updates_map: dict[str, object] = {"assigned_to": user_id}
                if doc["status"] in ("needs_review", "acknowledged"):
                    updates_map["status"] = "assigned"
                update_document(doc_id, updates=updates_map)
                create_audit_event(
                    document_id=doc_id,
                    action="bulk_assigned",
                    actor=actor,
                    details=f"assigned_to={user_id}",
                )

            elif payload.action == "transition":
                target_status = payload.params.get("status")
                if not target_status:
                    errors.append(f"{doc_id}: status required for transition")
                    continue
                allowed = ALLOWED_TRANSITIONS.get(doc["status"], set())
                if target_status not in allowed:
                    errors.append(f"{doc_id}: invalid transition {doc['status']} → {target_status}")
                    continue
                update_document(doc_id, updates={"status": target_status})
                create_audit_event(
                    document_id=doc_id,
                    action="bulk_transition",
                    actor=actor,
                    details=f"to={target_status}",
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
