from __future__ import annotations

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
