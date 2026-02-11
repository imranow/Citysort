"""Base connector class and shared types for all CitySort connectors."""

from __future__ import annotations

import base64
import json
import logging
import ssl
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Optional
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)


class ConnectorError(Exception):
    """Raised when a connector operation fails."""


@dataclass
class ExternalDocument:
    """Represents a document discovered in an external system."""

    external_id: str
    filename: str
    content_type: Optional[str] = None
    download_url: Optional[str] = None
    size_bytes: Optional[int] = None
    metadata: dict[str, Any] = field(default_factory=dict)


class BaseConnector:
    """Abstract base class for all connectors."""

    connector_type: str = "base"

    def test_connection(self, config: dict[str, Any]) -> tuple[bool, str]:
        """Validate credentials and connectivity. Returns (success, message)."""
        raise NotImplementedError

    def list_documents(
        self, config: dict[str, Any], limit: int = 50
    ) -> list[ExternalDocument]:
        """List available documents/attachments from the external system."""
        raise NotImplementedError

    def download_document(
        self, config: dict[str, Any], doc: ExternalDocument
    ) -> tuple[str, bytes, Optional[str]]:
        """Download a single document. Returns (filename, file_bytes, content_type)."""
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Shared HTTP helpers (all connectors use urllib — no extra deps)
# ---------------------------------------------------------------------------

_SSL_CTX = ssl.create_default_context()


def http_request(
    url: str,
    *,
    method: str = "GET",
    headers: Optional[dict[str, str]] = None,
    body: Optional[bytes] = None,
    timeout: int = 30,
) -> tuple[int, bytes, dict[str, str]]:
    """Perform an HTTP request. Returns (status_code, body_bytes, response_headers)."""
    req = urllib.request.Request(url, method=method, headers=headers or {}, data=body)
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except HTTPError as exc:
        body_bytes = b""
        try:
            body_bytes = exc.read()
        except Exception:
            pass
        raise ConnectorError(
            f"HTTP {exc.code} from {url}: {body_bytes[:500].decode('utf-8', errors='replace')}"
        ) from exc
    except URLError as exc:
        raise ConnectorError(f"Connection error for {url}: {exc.reason}") from exc
    except TimeoutError:
        raise ConnectorError(f"Request timed out: {url}")


def http_json(
    url: str,
    *,
    method: str = "GET",
    headers: Optional[dict[str, str]] = None,
    body: Optional[bytes] = None,
    timeout: int = 30,
) -> Any:
    """Perform an HTTP request and parse JSON response."""
    _status, resp_bytes, _hdrs = http_request(
        url, method=method, headers=headers, body=body, timeout=timeout
    )
    try:
        return json.loads(resp_bytes)
    except json.JSONDecodeError as exc:
        raise ConnectorError(f"Invalid JSON from {url}: {exc}") from exc


def basic_auth_header(username: str, password: str) -> str:
    """Return Base64-encoded Basic auth header value."""
    creds = f"{username}:{password}".encode("utf-8")
    return "Basic " + base64.b64encode(creds).decode("ascii")


def bearer_auth_header(token: str) -> str:
    """Return Bearer auth header value."""
    return f"Bearer {token}"


# ---------------------------------------------------------------------------
# Connector registry
# ---------------------------------------------------------------------------

_REGISTRY: dict[str, type[BaseConnector]] = {}


def register_connector(cls: type[BaseConnector]) -> type[BaseConnector]:
    """Decorator to register a connector class."""
    _REGISTRY[cls.connector_type] = cls
    return cls


def get_connector(connector_type: str) -> BaseConnector:
    """Instantiate and return the connector for the given type."""
    cls = _REGISTRY.get(connector_type)
    if cls is None:
        raise ConnectorError(f"Unknown connector type: {connector_type}")
    return cls()


def list_connector_types() -> list[str]:
    """Return all registered connector type names."""
    return sorted(_REGISTRY.keys())
