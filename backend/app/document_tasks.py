from __future__ import annotations

import shutil
from pathlib import Path

from .config import CONFIDENCE_THRESHOLD, PROCESSED_DIR
from .pipeline import process_document
from .repository import create_audit_event, get_document, update_document


def process_document_by_id(document_id: str, actor: str = "system") -> None:
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
