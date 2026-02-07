from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable
from uuid import uuid4

from .config import WORKER_ENABLED, WORKER_MAX_ATTEMPTS, WORKER_POLL_INTERVAL_SECONDS
from .document_tasks import process_document_by_id
from .repository import claim_next_job, complete_job, create_job, fail_job, get_job, list_jobs

logger = logging.getLogger(__name__)

JobHandler = Callable[[dict[str, Any]], dict[str, Any]]


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
        while not self._stop_event.is_set():
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
