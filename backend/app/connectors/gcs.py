"""Google Cloud Storage connector — pull files from a GCS bucket using service account JWT auth."""

from __future__ import annotations

import base64
import json
import time
from typing import Any, Optional
from urllib.parse import quote

from .base import (
    BaseConnector,
    ConnectorError,
    ExternalDocument,
    bearer_auth_header,
    http_json,
    http_request,
    register_connector,
)

_TOKEN_URL = "https://oauth2.googleapis.com/token"
_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only"


def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _get_access_token(service_account_key: str) -> str:
    """Create a signed JWT and exchange it for an access token."""
    try:
        sa = json.loads(service_account_key)
    except json.JSONDecodeError as exc:
        raise ConnectorError(f"Invalid service account JSON key: {exc}")

    private_key_pem = sa.get("private_key", "")
    client_email = sa.get("client_email", "")
    if not private_key_pem or not client_email:
        raise ConnectorError(
            "Service account key must contain 'private_key' and 'client_email'."
        )

    now = int(time.time())
    header = _base64url_encode(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    claims = _base64url_encode(
        json.dumps(
            {
                "iss": client_email,
                "scope": _SCOPE,
                "aud": _TOKEN_URL,
                "iat": now,
                "exp": now + 3600,
            }
        ).encode()
    )
    signing_input = f"{header}.{claims}".encode("ascii")

    # Sign with RSA-SHA256 using the private key
    try:
        # Use cryptography or ssl module for RSA signing
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding

        private_key = serialization.load_pem_private_key(
            private_key_pem.encode(), password=None
        )
        signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    except ImportError:
        # Fallback: try using subprocess with openssl
        import subprocess
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=True) as f:
            f.write(private_key_pem)
            f.flush()
            try:
                proc = subprocess.run(
                    ["openssl", "dgst", "-sha256", "-sign", f.name],
                    input=signing_input,
                    capture_output=True,
                    timeout=10,
                )
                if proc.returncode != 0:
                    raise ConnectorError("Failed to sign JWT with openssl.")
                signature = proc.stdout
            except FileNotFoundError:
                raise ConnectorError(
                    "GCS connector requires either the 'cryptography' Python package or openssl CLI."
                )

    jwt_token = f"{header}.{claims}.{_base64url_encode(signature)}"

    # Exchange JWT for access token
    body = f"grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion={jwt_token}"
    data = http_json(
        _TOKEN_URL,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        body=body.encode(),
        timeout=15,
    )
    token = data.get("access_token")
    if not token:
        raise ConnectorError("GCS OAuth token exchange failed.")
    return token


@register_connector
class GCSConnector(BaseConnector):
    connector_type = "google_cloud_storage"

    def test_connection(self, config: dict[str, Any]) -> tuple[bool, str]:
        for key in ("bucket_name", "service_account_key"):
            if not config.get(key, "").strip():
                return False, f"Missing required field: {key}"
        try:
            token = _get_access_token(config["service_account_key"])
            bucket = config["bucket_name"]
            url = f"https://storage.googleapis.com/storage/v1/b/{bucket}?fields=name"
            http_json(
                url, headers={"Authorization": bearer_auth_header(token)}, timeout=15
            )
            return True, f"Successfully connected to GCS bucket '{bucket}'."
        except ConnectorError as exc:
            return False, f"Connection failed: {exc}"

    def list_documents(
        self, config: dict[str, Any], limit: int = 50
    ) -> list[ExternalDocument]:
        token = _get_access_token(config["service_account_key"])
        bucket = config["bucket_name"]
        prefix = config.get("prefix", "").strip()
        headers = {"Authorization": bearer_auth_header(token)}

        url = (
            f"https://storage.googleapis.com/storage/v1/b/{bucket}/o"
            f"?maxResults={limit}&fields=items(name,size,contentType,md5Hash)"
        )
        if prefix:
            url += f"&prefix={quote(prefix, safe='/')}"

        data = http_json(url, headers=headers)

        docs: list[ExternalDocument] = []
        for item in data.get("items", []):
            name = item.get("name", "")
            # Skip "directory" markers
            if name.endswith("/"):
                continue
            # Use the basename as filename
            filename = name.rsplit("/", 1)[-1] if "/" in name else name
            docs.append(
                ExternalDocument(
                    external_id=f"gcs_{bucket}_{name}",
                    filename=filename or name,
                    content_type=item.get("contentType"),
                    download_url=f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{quote(name, safe='')}?alt=media",
                    size_bytes=int(item.get("size", 0)) or None,
                    metadata={"bucket": bucket, "object_name": name},
                )
            )

        return docs[:limit]

    def download_document(
        self, config: dict[str, Any], doc: ExternalDocument
    ) -> tuple[str, bytes, Optional[str]]:
        if not doc.download_url:
            raise ConnectorError("No download URL for GCS object.")
        token = _get_access_token(config["service_account_key"])
        headers = {"Authorization": bearer_auth_header(token)}
        _status, file_bytes, _hdrs = http_request(doc.download_url, headers=headers)
        return doc.filename, file_bytes, doc.content_type
