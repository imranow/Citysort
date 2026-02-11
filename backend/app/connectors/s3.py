"""Amazon S3 connector — pull files from an S3 bucket using AWS Signature V4 auth."""

from __future__ import annotations

import hashlib
import hmac
import mimetypes
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import quote
from xml.etree import ElementTree

from .base import (
    BaseConnector,
    ConnectorError,
    ExternalDocument,
    http_request,
    register_connector,
)


# ---------------------------------------------------------------------------
# AWS Signature V4 implementation (stdlib only)
# ---------------------------------------------------------------------------


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _signing_key(
    secret_key: str, date_stamp: str, region: str, service: str = "s3"
) -> bytes:
    k_date = _hmac_sha256(f"AWS4{secret_key}".encode("utf-8"), date_stamp)
    k_region = _hmac_sha256(k_date, region)
    k_service = _hmac_sha256(k_region, service)
    k_signing = _hmac_sha256(k_service, "aws4_request")
    return k_signing


def _sign_request(
    method: str,
    url: str,
    region: str,
    access_key: str,
    secret_key: str,
    *,
    headers: Optional[dict[str, str]] = None,
    payload: bytes = b"",
) -> dict[str, str]:
    """Add AWS Signature V4 auth headers to a request. Returns full headers dict."""
    from urllib.parse import urlparse

    parsed = urlparse(url)
    host = parsed.netloc
    path = parsed.path or "/"
    query = parsed.query

    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    hdrs = dict(headers or {})
    hdrs["Host"] = host
    hdrs["x-amz-date"] = amz_date
    hdrs["x-amz-content-sha256"] = _sha256(payload)

    signed_header_keys = sorted(k.lower() for k in hdrs)
    signed_headers = ";".join(signed_header_keys)
    canonical_headers = "".join(f"{k}:{hdrs[k]}\n" for k in sorted(hdrs, key=str.lower))

    canonical_request = "\n".join(
        [method, path, query, canonical_headers, signed_headers, _sha256(payload)]
    )

    credential_scope = f"{date_stamp}/{region}/s3/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            _sha256(canonical_request.encode()),
        ]
    )

    key = _signing_key(secret_key, date_stamp, region)
    signature = hmac.new(
        key, string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    hdrs["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return hdrs


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


@register_connector
class S3Connector(BaseConnector):
    connector_type = "amazon_s3"

    def _endpoint(self, config: dict[str, Any]) -> str:
        bucket = config["bucket_name"]
        region = config.get("region", "us-east-1")
        return f"https://{bucket}.s3.{region}.amazonaws.com"

    def _region(self, config: dict[str, Any]) -> str:
        return config.get("region", "us-east-1")

    # ------------------------------------------------------------------

    def test_connection(self, config: dict[str, Any]) -> tuple[bool, str]:
        for key in ("bucket_name", "region", "access_key_id", "secret_access_key"):
            if not config.get(key, "").strip():
                return False, f"Missing required field: {key}"
        try:
            url = f"{self._endpoint(config)}/?list-type=2&max-keys=1"
            headers = _sign_request(
                "GET",
                url,
                self._region(config),
                config["access_key_id"],
                config["secret_access_key"],
            )
            _status, body, _hdrs = http_request(url, headers=headers, timeout=15)
            return (
                True,
                f"Successfully connected to S3 bucket '{config['bucket_name']}'.",
            )
        except ConnectorError as exc:
            return False, f"Connection failed: {exc}"

    def list_documents(
        self, config: dict[str, Any], limit: int = 50
    ) -> list[ExternalDocument]:
        endpoint = self._endpoint(config)
        region = self._region(config)
        access_key = config["access_key_id"]
        secret_key = config["secret_access_key"]
        prefix = config.get("prefix", "").strip()
        bucket = config["bucket_name"]

        url = f"{endpoint}/?list-type=2&max-keys={limit}"
        if prefix:
            url += f"&prefix={quote(prefix, safe='/')}"

        headers = _sign_request("GET", url, region, access_key, secret_key)
        _status, body, _hdrs = http_request(url, headers=headers)

        # Parse XML response
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        try:
            root = ElementTree.fromstring(body)
        except ElementTree.ParseError:
            raise ConnectorError("Failed to parse S3 ListObjectsV2 XML response.")

        docs: list[ExternalDocument] = []
        for item in root.findall(".//s3:Contents", ns):
            key_el = item.find("s3:Key", ns)
            size_el = item.find("s3:Size", ns)
            if key_el is None or not key_el.text:
                continue
            key = key_el.text
            # Skip "directory" markers
            if key.endswith("/"):
                continue
            filename = key.rsplit("/", 1)[-1] if "/" in key else key
            content_type = mimetypes.guess_type(filename)[0]
            docs.append(
                ExternalDocument(
                    external_id=f"s3_{bucket}_{key}",
                    filename=filename,
                    content_type=content_type,
                    download_url=f"{endpoint}/{quote(key, safe='/')}",
                    size_bytes=int(size_el.text)
                    if size_el is not None and size_el.text
                    else None,
                    metadata={"bucket": bucket, "key": key},
                )
            )

        return docs[:limit]

    def download_document(
        self, config: dict[str, Any], doc: ExternalDocument
    ) -> tuple[str, bytes, Optional[str]]:
        if not doc.download_url:
            raise ConnectorError("No download URL for S3 object.")
        headers = _sign_request(
            "GET",
            doc.download_url,
            self._region(config),
            config["access_key_id"],
            config["secret_access_key"],
        )
        _status, file_bytes, _hdrs = http_request(doc.download_url, headers=headers)
        return doc.filename, file_bytes, doc.content_type
