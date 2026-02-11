"""Shared import orchestration — ingest documents from any connector into CitySort pipeline."""
from __future__ import annotations

import json
import logging
import mimetypes
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

from .base import BaseConnector, ConnectorError, ExternalDocument, get_connector
from ..security import UploadValidationError, validate_upload
from ..storage import write_document_bytes

logger = logging.getLogger(__name__)


def _is_already_synced(connector_type: str, external_id: str) -> bool:
    """Check if an external document has already been imported."""
    from ..db import get_connection

    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM connector_sync_log WHERE connector_type = ? AND external_id = ?",
            (connector_type, external_id),
        ).fetchone()
    return row is not None


def _record_sync(
    connector_type: str,
    external_id: str,
    filename: str,
    document_id: str,
    metadata: Optional[dict] = None,
) -> None:
    """Record a successful import in the sync log for deduplication."""
    from ..db import get_connection
    from ..repository import utcnow_iso

    with get_connection() as conn:
        conn.execute(
            """INSERT INTO connector_sync_log
               (connector_type, external_id, filename, document_id, metadata_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(connector_type, external_id) DO NOTHING""",
            (
                connector_type,
                external_id,
                filename,
                document_id,
                json.dumps(metadata or {}),
                utcnow_iso(),
            ),
        )


def _update_last_sync(connector_type: str) -> None:
    """Update the last_sync_at timestamp in connector_configs."""
    from ..db import get_connection
    from ..repository import utcnow_iso

    with get_connection() as conn:
        conn.execute(
            "UPDATE connector_configs SET last_sync_at = ?, updated_at = ? WHERE connector_type = ?",
            (utcnow_iso(), utcnow_iso(), connector_type),
        )


def import_from_connector(
    connector_type: str,
    config: dict[str, Any],
    *,
    limit: int = 50,
    process_async: bool = True,
    actor: str = "connector_import",
) -> dict[str, Any]:
    """
    Run a full import cycle for any connector:
    1. List documents from the external system
    2. Skip already-imported docs (dedup via sync log)
    3. Download & ingest new documents
    4. Enqueue for AI processing

    Returns summary dict with imported_count, skipped_count, failed_count, errors.
    """
    from ..config import UPLOAD_DIR
    from ..jobs import enqueue_document_processing
    from ..repository import create_audit_event, create_document, utcnow_iso

    connector = get_connector(connector_type)
    source_channel = f"connector_{connector_type}"

    # 1. List documents
    try:
        external_docs = connector.list_documents(config, limit=limit)
    except ConnectorError as exc:
        return {
            "imported_count": 0,
            "skipped_count": 0,
            "failed_count": 1,
            "errors": [f"Failed to list documents: {exc}"],
            "documents": [],
        }

    imported: list[dict[str, str]] = []
    skipped = 0
    errors: list[str] = []

    for doc in external_docs:
        # 2. Dedup check
        if _is_already_synced(connector_type, doc.external_id):
            skipped += 1
            continue

        # 3. Download
        try:
            filename, file_bytes, content_type = connector.download_document(config, doc)
        except ConnectorError as exc:
            errors.append(f"{doc.filename}: Download failed — {exc}")
            continue
        except Exception as exc:
            errors.append(f"{doc.filename}: Unexpected error — {exc}")
            continue

        if not file_bytes:
            errors.append(f"{doc.filename}: Empty file content.")
            continue

        # 4. Save to disk
        document_id = str(uuid4())
        safe_filename = f"{document_id}_{filename}"
        storage_path = UPLOAD_DIR / safe_filename

        # Infer content type if not provided
        if not content_type:
            content_type = mimetypes.guess_type(filename)[0]
        content_type = content_type or "application/octet-stream"
        try:
            validate_upload(filename=filename, content_type=content_type, payload=file_bytes)
            write_document_bytes(storage_path, file_bytes)
        except UploadValidationError as exc:
            errors.append(f"{filename}: Validation failed — {exc}")
            continue
        except OSError as exc:
            errors.append(f"{filename}: Failed to write file — {exc}")
            continue

        # 5. Create document record
        try:
            create_document(
                document={
                    "id": document_id,
                    "filename": filename,
                    "storage_path": str(storage_path),
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
        except Exception as exc:
            errors.append(f"{filename}: Failed to create document — {exc}")
            # Clean up file
            try:
                storage_path.unlink(missing_ok=True)
            except OSError:
                pass
            continue

        # 6. Record in sync log
        _record_sync(
            connector_type=connector_type,
            external_id=doc.external_id,
            filename=filename,
            document_id=document_id,
            metadata=doc.metadata,
        )

        # 7. Audit event
        create_audit_event(
            document_id=document_id,
            action="connector_imported",
            actor=actor,
            details=f"source={source_channel} external_id={doc.external_id}",
        )

        # 8. Enqueue processing
        if process_async:
            enqueue_document_processing(document_id=document_id, actor=actor)

        imported.append({"id": document_id, "filename": filename, "status": "ingested"})
        logger.info("Connector [%s] imported: %s -> %s", connector_type, doc.external_id, document_id)

    # Update last sync timestamp
    _update_last_sync(connector_type)

    return {
        "imported_count": len(imported),
        "skipped_count": skipped,
        "failed_count": len(errors),
        "errors": errors[:50],
        "documents": imported,
    }


def get_sync_count(connector_type: str) -> int:
    """Return total documents previously imported for this connector type."""
    from ..db import get_connection

    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM connector_sync_log WHERE connector_type = ?",
            (connector_type,),
        ).fetchone()
    return int(row["cnt"]) if row else 0
