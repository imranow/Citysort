"""Watched folder ingestion service."""

from __future__ import annotations

import hashlib
import logging
import mimetypes
import threading
import time
from pathlib import Path
from typing import Optional
from uuid import uuid4

from .db import get_connection
from .jobs import enqueue_document_processing
from .repository import create_audit_event, create_document, utcnow_iso
from .security import UploadValidationError, validate_upload
from .storage import write_document_bytes

logger = logging.getLogger(__name__)


def _file_hash(path: Path) -> str:
    h = hashlib.sha256()
    h.update(str(path.resolve()).encode("utf-8"))
    h.update(str(path.stat().st_size).encode("utf-8"))
    h.update(str(path.stat().st_mtime).encode("utf-8"))
    return h.hexdigest()


def _is_already_watched(file_hash: str) -> bool:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT id FROM watched_files WHERE file_hash = ?", (file_hash,)
        ).fetchone()
    return row is not None


def _record_watched_file(
    *, filename: str, file_hash: str, source_path: str, document_id: str
) -> None:
    with get_connection() as conn:
        conn.execute(
            """INSERT INTO watched_files (filename, file_hash, source_path, document_id, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (filename, file_hash, source_path, document_id, utcnow_iso()),
        )


class FolderWatcher:
    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        from .config import WATCH_DIR, WATCH_ENABLED

        if not WATCH_ENABLED or not WATCH_DIR:
            logger.info("Folder watcher disabled")
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="folder-watcher", daemon=True
        )
        self._thread.start()
        logger.info("Started folder watcher on %s", WATCH_DIR)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        logger.info("Stopped folder watcher")

    def _run_loop(self) -> None:
        from .config import UPLOAD_DIR, WATCH_DIR, WATCH_INTERVAL_SECONDS

        watch_path = Path(WATCH_DIR)
        while not self._stop_event.is_set():
            if watch_path.is_dir():
                for file_path in sorted(watch_path.iterdir()):
                    if self._stop_event.is_set():
                        break
                    if not file_path.is_file():
                        continue
                    # Skip hidden files.
                    if file_path.name.startswith("."):
                        continue
                    try:
                        fhash = _file_hash(file_path)
                        if _is_already_watched(fhash):
                            continue
                        self._ingest_file(file_path, fhash, UPLOAD_DIR)
                    except Exception as exc:
                        logger.exception("Watcher error for %s: %s", file_path, exc)
            time.sleep(WATCH_INTERVAL_SECONDS)

    def _ingest_file(self, file_path: Path, fhash: str, upload_dir: Path) -> None:
        document_id = str(uuid4())
        safe_filename = f"{document_id}_{file_path.name}"
        dest = upload_dir / safe_filename
        payload = file_path.read_bytes()
        content_type = (
            mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
        )
        try:
            validate_upload(
                filename=file_path.name, content_type=content_type, payload=payload
            )
        except UploadValidationError as exc:
            logger.warning(
                "Watcher skipped %s due to validation failure: %s", file_path, exc
            )
            return
        write_document_bytes(dest, payload)

        create_document(
            document={
                "id": document_id,
                "filename": file_path.name,
                "storage_path": str(dest),
                "source_channel": "watched_folder",
                "content_type": content_type,
                "status": "ingested",
                "requires_review": False,
                "confidence": 0.0,
                "doc_type": None,
                "department": None,
                "urgency": "normal",
            }
        )
        _record_watched_file(
            filename=file_path.name,
            file_hash=fhash,
            source_path=str(file_path),
            document_id=document_id,
        )
        create_audit_event(
            document_id=document_id,
            action="watched_folder_ingested",
            actor="folder_watcher",
            details=f"source={file_path}",
        )
        try:
            from .workflows import run_workflows_for_document

            run_workflows_for_document(
                trigger_event="document_ingested",
                document_id=document_id,
                actor="folder_watcher",
                workspace_id=None,
            )
        except Exception:
            pass
        enqueue_document_processing(document_id=document_id, actor="folder_watcher")
        logger.info("Watcher ingested: %s -> %s", file_path.name, document_id)


_watcher = FolderWatcher()


def start_watcher() -> None:
    _watcher.start()


def stop_watcher() -> None:
    _watcher.stop()
