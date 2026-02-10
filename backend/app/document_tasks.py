from __future__ import annotations

import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import CONFIDENCE_THRESHOLD, PROCESSED_DIR
from .pipeline import process_document
from .repository import create_audit_event, get_document, update_document
from .rules import get_active_rules


def process_document_by_id(document_id: str, actor: str = "system") -> None:
    document = get_document(document_id)
    if not document:
        return

    try:
        result = process_document(file_path=document["storage_path"], content_type=document.get("content_type"))
        final_status = "needs_review" if result["requires_review"] else "routed"

        # Compute SLA due_date from the matched rule.
        active_rules = get_active_rules()[0]
        rule = active_rules.get(result["doc_type"], active_rules.get("other", {}))
        sla_days = rule.get("sla_days")
        due_date = None
        if sla_days is not None:
            created_at = datetime.now(timezone.utc)
            created_at_raw = document.get("created_at")
            if created_at_raw:
                try:
                    created_at = datetime.fromisoformat(str(created_at_raw).replace("Z", "+00:00"))
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=timezone.utc)
                except ValueError:
                    pass
            due_date = (created_at + timedelta(days=int(sla_days))).isoformat()

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
                "sla_days": sla_days,
                "due_date": due_date,
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

        # Create notification when document needs human review.
        if result["requires_review"]:
            try:
                from .notifications import create_notification
                create_notification(
                    type="needs_review",
                    title=f"Review needed: {document.get('filename', 'Unknown')}",
                    message=f"Type: {result['doc_type']}, Confidence: {result['confidence']}",
                    document_id=document_id,
                )
            except Exception:
                pass  # Notification failure should not block pipeline.

    except Exception as exc:  # pragma: no cover - runtime safeguard
        update_document(document_id, updates={"status": "failed", "requires_review": True})
        create_audit_event(
            document_id=document_id,
            action="pipeline_failed",
            actor=actor,
            details=str(exc),
        )
