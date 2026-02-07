from __future__ import annotations

import mimetypes
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import (
    CLASSIFIER_PROVIDER,
    CONFIDENCE_THRESHOLD,
    FORCE_REVIEW_DOC_TYPES,
    OCR_PROVIDER,
    PROCESSED_DIR,
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
from .pipeline import process_document, route_document
from .repository import (
    count_api_keys,
    count_invitations,
    create_api_key,
    create_audit_event,
    create_deployment,
    create_document,
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
    revoke_api_key,
    update_document,
)
from .rules import get_active_rules, get_rules_path, reset_rules_to_default, save_rules
from .schemas import (
    AnalyticsResponse,
    ApiKeyCreateRequest,
    ApiKeyCreateResponse,
    ApiKeyListResponse,
    ApiKeyRecord,
    AuditTrailResponse,
    ConnectivityResponse,
    DatabaseImportRequest,
    DatabaseImportResponse,
    DeploymentListResponse,
    DeploymentRecord,
    DocumentListResponse,
    DocumentResponse,
    InvitationCreateRequest,
    InvitationCreateResponse,
    InvitationListResponse,
    ManualDeploymentRequest,
    PlatformSummaryResponse,
    QueueResponse,
    RulesConfigResponse,
    RulesConfigUpdate,
    ReviewRequest,
    ServiceHealth,
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


def _process_document_by_id(document_id: str, actor: str = "system") -> None:
    document = get_document(document_id)
    if not document:
        return

    try:
        result = process_document(file_path=document["storage_path"], content_type=document.get("content_type"))
        final_status = "needs_review" if result["requires_review"] else "routed"

        update_document(
            document_id,
            updates={
                "status": final_status,
                "doc_type": result["doc_type"],
                "department": result["department"],
                "urgency": result["urgency"],
                "confidence": result["confidence"],
                "requires_review": result["requires_review"],
                "extracted_text": result["extracted_text"],
                "extracted_fields": result["extracted_fields"],
                "missing_fields": result["missing_fields"],
                "validation_errors": result["validation_errors"],
            },
        )

        source_path = Path(document["storage_path"])
        target_path = PROCESSED_DIR / source_path.name
        if source_path.exists():
            shutil.copy2(source_path, target_path)

        create_audit_event(
            document_id=document_id,
            action="pipeline_processed",
            actor=actor,
            details=(
                f"doc_type={result['doc_type']} confidence={result['confidence']} "
                f"requires_review={result['requires_review']} threshold={CONFIDENCE_THRESHOLD}"
            ),
        )

    except Exception as exc:  # pragma: no cover - runtime safeguard
        update_document(document_id, updates={"status": "failed", "requires_review": True})
        create_audit_event(
            document_id=document_id,
            action="pipeline_failed",
            actor=actor,
            details=str(exc),
        )


def _coerce_optional_text(value: object | None) -> Optional[str]:
    if value is None:
        return None
    parsed = str(value).strip()
    return parsed or None


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    return ConnectivityResponse(
        database=_database_health(),
        ocr_provider=_ocr_provider_health(),
        classifier_provider=_classifier_provider_health(),
        checked_at=_utcnow_iso(),
    )


@app.on_event("startup")
def startup_event() -> None:
    init_db()


@app.get("/health")
def health_check() -> dict[str, str]:
    return {
        "status": "ok",
        "ocr_provider": OCR_PROVIDER,
        "classifier_provider": CLASSIFIER_PROVIDER,
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


@app.post("/api/documents/upload", response_model=DocumentResponse)
async def upload_document(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    source_channel: str = Form("upload_portal"),
    process_async: bool = Form(False),
) -> DocumentResponse:
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
        actor="upload_portal",
        details=f"source_channel={source_channel} file={file.filename}",
    )

    if process_async:
        background_tasks.add_task(_process_document_by_id, document_id)
    else:
        _process_document_by_id(document_id)

    refreshed = get_document(document_id)
    if not refreshed:
        raise HTTPException(status_code=500, detail="Unable to load processed document")

    return DocumentResponse(**refreshed)


@app.post("/api/documents/import/database", response_model=DatabaseImportResponse)
def import_documents_from_database(
    payload: DatabaseImportRequest,
    background_tasks: BackgroundTasks,
) -> DatabaseImportResponse:
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
                    actor=payload.actor,
                    details=f"source_channel={payload.source_channel} row={index}",
                )

                if payload.process_async:
                    background_tasks.add_task(_process_document_by_id, document_id, payload.actor)
                    scheduled_async_count += 1
                else:
                    _process_document_by_id(document_id, actor=payload.actor)
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


@app.get("/api/documents", response_model=DocumentListResponse)
def get_documents(
    status: Optional[str] = Query(default=None),
    department: Optional[str] = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> DocumentListResponse:
    items: list[DocumentResponse] = []
    for item in list_documents(status=status, department=department, limit=limit):
        # Keep list endpoint light; full text is available from document detail endpoint.
        item_payload = dict(item)
        item_payload["extracted_text"] = None
        items.append(DocumentResponse(**item_payload))
    return DocumentListResponse(items=items)


@app.get("/api/documents/{document_id}", response_model=DocumentResponse)
def get_document_by_id(document_id: str) -> DocumentResponse:
    record = get_document(document_id)
    if not record:
        raise HTTPException(status_code=404, detail="Document not found")
    return DocumentResponse(**record)


@app.post("/api/documents/{document_id}/review", response_model=DocumentResponse)
def review_document(document_id: str, payload: ReviewRequest) -> DocumentResponse:
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
        actor=payload.actor,
        details=(
            f"approve={payload.approve} corrected_doc_type={payload.corrected_doc_type} "
            f"allowed_types={','.join(sorted(get_active_rules()[0].keys()))}"
        ),
    )

    return DocumentResponse(**updated)


@app.post("/api/documents/{document_id}/reprocess", response_model=DocumentResponse)
def reprocess_document(document_id: str) -> DocumentResponse:
    document = get_document(document_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    _process_document_by_id(document_id, actor="manual_reprocess")
    updated = get_document(document_id)
    if not updated:
        raise HTTPException(status_code=500, detail="Unable to reload document")

    return DocumentResponse(**updated)


@app.get("/api/documents/{document_id}/audit", response_model=AuditTrailResponse)
def get_document_audit(document_id: str, limit: int = Query(default=50, ge=1, le=200)) -> AuditTrailResponse:
    document = get_document(document_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    return AuditTrailResponse(items=list_audit_events(document_id, limit=limit))


@app.get("/api/config/rules", response_model=RulesConfigResponse)
def get_rules_config() -> RulesConfigResponse:
    rules, source = get_active_rules()
    return RulesConfigResponse(source=source, path=str(get_rules_path()), rules=rules)


@app.put("/api/config/rules", response_model=RulesConfigResponse)
def update_rules_config(payload: RulesConfigUpdate) -> RulesConfigResponse:
    try:
        normalized = save_rules({key: value.model_dump() for key, value in payload.rules.items()})
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return RulesConfigResponse(source="custom", path=str(get_rules_path()), rules=normalized)


@app.post("/api/config/rules/reset", response_model=RulesConfigResponse)
def reset_rules_config() -> RulesConfigResponse:
    rules = reset_rules_to_default()
    return RulesConfigResponse(source="default", path=str(get_rules_path()), rules=rules)


@app.get("/api/queues", response_model=QueueResponse)
def get_queues() -> QueueResponse:
    queues = get_queue_snapshot()
    return QueueResponse(queues=queues)


@app.get("/api/analytics", response_model=AnalyticsResponse)
def get_analytics() -> AnalyticsResponse:
    snapshot = get_analytics_snapshot()
    return AnalyticsResponse(**snapshot)


@app.get("/api/platform/connectivity", response_model=ConnectivityResponse)
def get_platform_connectivity() -> ConnectivityResponse:
    return _connectivity_snapshot()


@app.post("/api/platform/connectivity/check", response_model=ConnectivityResponse)
def run_platform_connectivity_check() -> ConnectivityResponse:
    return _connectivity_snapshot()


@app.post("/api/platform/deployments/manual", response_model=DeploymentRecord)
def run_manual_deployment(payload: ManualDeploymentRequest) -> DeploymentRecord:
    snapshot = _connectivity_snapshot()
    is_ready = snapshot.database.status == "ok"
    status = "completed" if is_ready else "failed"
    details = (
        f"database={snapshot.database.status}; "
        f"ocr={snapshot.ocr_provider.status}; "
        f"classifier={snapshot.classifier_provider.status}"
    )
    created = create_deployment(
        environment=payload.environment,
        actor=payload.actor,
        notes=payload.notes,
        status=status,
        details=details,
    )
    return DeploymentRecord(**created)


@app.get("/api/platform/deployments", response_model=DeploymentListResponse)
def get_platform_deployments(limit: int = Query(default=20, ge=1, le=100)) -> DeploymentListResponse:
    items = [DeploymentRecord(**item) for item in list_deployments(limit=limit)]
    return DeploymentListResponse(items=items)


@app.post("/api/platform/invitations", response_model=InvitationCreateResponse)
def create_platform_invitation(payload: InvitationCreateRequest, request: Request) -> InvitationCreateResponse:
    invitation, raw_token = create_invitation(
        email=payload.email.strip().lower(),
        role=payload.role.strip().lower(),
        actor=payload.actor,
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
    status: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> InvitationListResponse:
    items = [item for item in list_invitations(status=status, limit=limit)]
    return InvitationListResponse(items=items)


@app.post("/api/platform/api-keys", response_model=ApiKeyCreateResponse)
def create_platform_api_key(payload: ApiKeyCreateRequest) -> ApiKeyCreateResponse:
    key_name = payload.name.strip()
    if not key_name:
        raise HTTPException(status_code=400, detail="API key name is required.")

    record, raw_key = create_api_key(name=key_name, actor=payload.actor)
    return ApiKeyCreateResponse(api_key=record, raw_key=raw_key)


@app.get("/api/platform/api-keys", response_model=ApiKeyListResponse)
def get_platform_api_keys(
    include_revoked: bool = Query(default=False),
    limit: int = Query(default=100, ge=1, le=500),
) -> ApiKeyListResponse:
    items = [item for item in list_api_keys(include_revoked=include_revoked, limit=limit)]
    return ApiKeyListResponse(items=items)


@app.post("/api/platform/api-keys/{key_id}/revoke", response_model=ApiKeyRecord)
def revoke_platform_api_key(key_id: int) -> ApiKeyRecord:
    updated = revoke_api_key(key_id=key_id)
    if not updated:
        raise HTTPException(status_code=404, detail="API key not found.")
    return ApiKeyRecord(**updated)


@app.get("/api/platform/summary", response_model=PlatformSummaryResponse)
def get_platform_summary() -> PlatformSummaryResponse:
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
