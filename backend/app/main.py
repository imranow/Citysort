from __future__ import annotations

import mimetypes
import shutil
import uuid
from pathlib import Path
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, Query, UploadFile
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
from .db import init_db
from .pipeline import process_document, route_document
from .repository import (
    create_audit_event,
    create_document,
    get_analytics_snapshot,
    get_document,
    get_queue_snapshot,
    list_audit_events,
    list_documents,
    update_document,
)
from .rules import get_active_rules, get_rules_path, reset_rules_to_default, save_rules
from .schemas import (
    AnalyticsResponse,
    AuditTrailResponse,
    DatabaseImportRequest,
    DatabaseImportResponse,
    DocumentListResponse,
    DocumentResponse,
    QueueResponse,
    RulesConfigResponse,
    RulesConfigUpdate,
    ReviewRequest,
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
