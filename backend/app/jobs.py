from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from uuid import uuid4

from .config import (
    ESCALATION_DAYS,
    ESCALATION_ENABLED,
    ESCALATION_FALLBACK_USER,
    WORKER_ENABLED,
    WORKER_MAX_ATTEMPTS,
    WORKER_POLL_INTERVAL_SECONDS,
)
from .db import get_connection
from .document_tasks import process_document_by_id
from .notifications import create_notification
from .repository import (
    claim_next_job,
    complete_job,
    create_audit_event,
    create_job,
    fail_job,
    get_job,
    list_jobs,
    list_overdue_documents,
    update_document,
)

logger = logging.getLogger(__name__)

JobHandler = Callable[[dict[str, Any]], dict[str, Any]]
SLA_CHECK_INTERVAL_SECONDS = 15 * 60
OVERDUE_NOTIFICATION_LOOKBACK_MINUTES = 24 * 60


def _has_recent_overdue_notification(document_id: str) -> bool:
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=OVERDUE_NOTIFICATION_LOOKBACK_MINUTES)).isoformat()
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id FROM notifications
            WHERE type = 'overdue'
              AND document_id = ?
              AND created_at >= ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (document_id, cutoff),
        ).fetchone()
    return row is not None


def _days_overdue(due_date_str: str) -> int:
    """Return how many whole days a document is past its due date."""
    try:
        due_dt = datetime.fromisoformat(str(due_date_str).replace("Z", "+00:00"))
        if due_dt.tzinfo is None:
            due_dt = due_dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - due_dt
        return max(0, delta.days)
    except (ValueError, TypeError):
        return 0


def _run_overdue_sla_scan() -> None:
    overdue_documents = list_overdue_documents(limit=500)
    for document in overdue_documents:
        document_id = str(document.get("id") or "").strip()
        if not document_id or _has_recent_overdue_notification(document_id):
            continue
        filename = str(document.get("filename") or "Document")
        due_date = str(document.get("due_date") or "unknown")
        assigned_to = document.get("assigned_to")
        days_late = _days_overdue(due_date)

        create_notification(
            type="overdue",
            title=f"Overdue: {filename}",
            message=f"SLA due date passed ({due_date}). {days_late}d overdue.",
            user_id=str(assigned_to) if assigned_to else None,
            document_id=document_id,
        )

        # --- Escalation: auto-reassign if overdue beyond threshold ---
        if (
            ESCALATION_ENABLED
            and ESCALATION_FALLBACK_USER
            and days_late >= ESCALATION_DAYS
            and str(assigned_to or "") != ESCALATION_FALLBACK_USER
        ):
            try:
                update_document(document_id, updates={"assigned_to": ESCALATION_FALLBACK_USER})
                create_audit_event(
                    document_id=document_id,
                    action="auto_escalated",
                    actor="system_escalation",
                    details=f"Reassigned from {assigned_to or 'unassigned'} to {ESCALATION_FALLBACK_USER} ({days_late}d overdue)",
                )
                create_notification(
                    type="assignment",
                    title=f"Escalated: {filename}",
                    message=f"Auto-reassigned after {days_late}d overdue.",
                    user_id=ESCALATION_FALLBACK_USER,
                    document_id=document_id,
                )
                logger.info("Auto-escalated doc %s to %s (%dd overdue)", document_id, ESCALATION_FALLBACK_USER, days_late)
            except Exception as exc:
                logger.warning("Escalation failed for %s: %s", document_id, exc)


class DurableJobWorker:
    def __init__(self) -> None:
        self.worker_id = f"citysort-worker-{uuid4().hex[:8]}"
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._handlers: dict[str, JobHandler] = {}

    def register_handler(self, job_type: str, handler: JobHandler) -> None:
        self._handlers[job_type] = handler

    def start(self) -> None:
        if not WORKER_ENABLED:
            logger.info("Durable worker disabled by CITYSORT_WORKER_ENABLED=false")
            return
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name=self.worker_id, daemon=True)
        self._thread.start()
        logger.info("Started durable job worker %s", self.worker_id)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        logger.info("Stopped durable job worker %s", self.worker_id)

    def _run_loop(self) -> None:
        next_sla_check_at = 0.0
        while not self._stop_event.is_set():
            now = time.monotonic()
            if now >= next_sla_check_at:
                try:
                    _run_overdue_sla_scan()
                except Exception as exc:  # pragma: no cover - runtime safeguard
                    logger.exception("Overdue SLA scan failed: %s", exc)
                next_sla_check_at = now + SLA_CHECK_INTERVAL_SECONDS

            job = claim_next_job(worker_id=self.worker_id)
            if not job:
                time.sleep(WORKER_POLL_INTERVAL_SECONDS)
                continue

            job_id = job["id"]
            job_type = job["job_type"]
            payload = job.get("payload", {}) or {}
            handler = self._handlers.get(job_type)
            if not handler:
                fail_job(job_id=job_id, error=f"No registered handler for job_type='{job_type}'")
                continue

            try:
                result = handler(payload)
                complete_job(job_id=job_id, result=result or {"ok": True})
            except Exception as exc:  # pragma: no cover - runtime safeguard
                logger.exception("Job %s failed: %s", job_id, exc)
                fail_job(job_id=job_id, error=str(exc))


_worker = DurableJobWorker()


def _handle_process_document_job(payload: dict[str, Any]) -> dict[str, Any]:
    document_id = str(payload.get("document_id", "")).strip()
    if not document_id:
        raise ValueError("payload.document_id is required")
    actor = str(payload.get("actor", "system")).strip() or "system"
    process_document_by_id(document_id, actor=actor)
    return {"document_id": document_id, "actor": actor, "processed": True}


_worker.register_handler("process_document", _handle_process_document_job)


def start_job_worker() -> None:
    _worker.start()


def stop_job_worker() -> None:
    _worker.stop()


def enqueue_document_processing(*, document_id: str, actor: str, max_attempts: int = WORKER_MAX_ATTEMPTS) -> dict[str, Any]:
    return create_job(
        job_type="process_document",
        payload={"document_id": document_id, "actor": actor},
        actor=actor,
        max_attempts=max_attempts,
    )


def get_job_by_id(job_id: str) -> dict[str, Any] | None:
    return get_job(job_id)


def get_jobs(*, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    return list_jobs(status=status, limit=limit)
