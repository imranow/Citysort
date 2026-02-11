from __future__ import annotations

import logging
from time import perf_counter
from typing import Optional

from fastapi import Response

from .config import PROMETHEUS_ENABLED, SENTRY_DSN, SENTRY_TRACES_SAMPLE_RATE

logger = logging.getLogger(__name__)

_REQUEST_COUNTER = None
_REQUEST_LATENCY = None
_METRICS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
_PROMETHEUS_READY = False
_SENTRY_READY = False


def init_observability() -> None:
    _init_prometheus()
    _init_sentry()


def _init_prometheus() -> None:
    global _REQUEST_COUNTER, _REQUEST_LATENCY, _PROMETHEUS_READY, _METRICS_CONTENT_TYPE
    if _PROMETHEUS_READY or not PROMETHEUS_ENABLED:
        return
    _PROMETHEUS_READY = True
    try:
        from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram
    except Exception:
        logger.info("Prometheus client not installed; /metrics will be unavailable.")
        return

    _REQUEST_COUNTER = Counter(
        "citysort_http_requests_total",
        "Total number of HTTP requests",
        ["method", "path", "status"],
    )
    _REQUEST_LATENCY = Histogram(
        "citysort_http_request_duration_seconds",
        "HTTP request latency",
        ["method", "path"],
        buckets=(0.01, 0.03, 0.1, 0.3, 1.0, 3.0, 10.0),
    )
    _METRICS_CONTENT_TYPE = CONTENT_TYPE_LATEST


def _init_sentry() -> None:
    global _SENTRY_READY
    if _SENTRY_READY:
        return
    _SENTRY_READY = True
    if not SENTRY_DSN:
        return
    try:
        import sentry_sdk
    except Exception:
        logger.warning("SENTRY_DSN is set but sentry-sdk is not installed.")
        return

    sentry_sdk.init(
        dsn=SENTRY_DSN,
        traces_sample_rate=float(SENTRY_TRACES_SAMPLE_RATE or 0.0),
    )
    logger.info("Sentry initialized.")


def start_timer() -> float:
    return perf_counter()


def observe_request(*, method: str, path: str, status_code: int, started_at: Optional[float]) -> None:
    if started_at is None:
        return
    duration = max(perf_counter() - started_at, 0.0)
    if _REQUEST_COUNTER is not None:
        _REQUEST_COUNTER.labels(method=method, path=path, status=str(status_code)).inc()
    if _REQUEST_LATENCY is not None:
        _REQUEST_LATENCY.labels(method=method, path=path).observe(duration)


def metrics_response() -> Response:
    if _REQUEST_COUNTER is None:
        return Response(status_code=503, content="Prometheus metrics are disabled.")
    from prometheus_client import generate_latest

    return Response(content=generate_latest(), media_type=_METRICS_CONTENT_TYPE)
