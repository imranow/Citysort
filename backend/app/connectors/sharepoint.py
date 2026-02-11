"""SharePoint Online connector — pull files from a SharePoint document library via OAuth 2.0."""
from __future__ import annotations

import json
from typing import Any, Optional
from urllib.parse import quote, urlencode

from .base import (
    BaseConnector,
    ConnectorError,
    ExternalDocument,
    bearer_auth_header,
    http_json,
    http_request,
    register_connector,
)


def _get_access_token(config: dict[str, Any]) -> str:
    """Authenticate via OAuth 2.0 client_credentials flow."""
    tenant_id = config["tenant_id"]
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    site_url = config["site_url"].rstrip("/")
    # Derive the SharePoint resource scope
    # For Graph API use: https://graph.microsoft.com/.default
    # For SharePoint REST API use: {site_host}/.default
    from urllib.parse import urlparse

    parsed = urlparse(site_url)
    scope = f"https://{parsed.netloc}/.default"

    body = urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "scope": scope,
        }
    ).encode("utf-8")
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
    data = http_json(token_url, method="POST", headers=headers, body=body, timeout=15)
    token = data.get("access_token")
    if not token:
        error_desc = data.get("error_description", data.get("error", "unknown error"))
        raise ConnectorError(f"SharePoint OAuth failed: {error_desc}")
    return token


@register_connector
class SharePointConnector(BaseConnector):
    connector_type = "sharepoint"

    def _site_url(self, config: dict[str, Any]) -> str:
        return config["site_url"].rstrip("/")

    # ------------------------------------------------------------------

    def test_connection(self, config: dict[str, Any]) -> tuple[bool, str]:
        for key in ("site_url", "client_id", "client_secret", "tenant_id"):
            if not config.get(key, "").strip():
                return False, f"Missing required field: {key}"
        try:
            token = _get_access_token(config)
            site = self._site_url(config)
            url = f"{site}/_api/web?$select=Title"
            headers = {
                "Authorization": bearer_auth_header(token),
                "Accept": "application/json;odata=verbose",
            }
            data = http_json(url, headers=headers, timeout=15)
            title = data.get("d", {}).get("Title", "site")
            return True, f"Successfully connected to SharePoint site '{title}'."
        except ConnectorError as exc:
            return False, f"Connection failed: {exc}"

    def list_documents(
        self, config: dict[str, Any], limit: int = 50
    ) -> list[ExternalDocument]:
        token = _get_access_token(config)
        site = self._site_url(config)
        library = config.get("library_name", "Documents").strip() or "Documents"
        headers = {
            "Authorization": bearer_auth_header(token),
            "Accept": "application/json;odata=verbose",
        }

        # Fetch items from the document library
        list_url = (
            f"{site}/_api/web/lists/getbytitle('{quote(library)}')/items"
            f"?$select=Id,FileLeafRef,FileRef,File/Length"
            f"&$expand=File"
            f"&$top={limit}"
            f"&$orderby=Created desc"
        )
        data = http_json(list_url, headers=headers)
        results = data.get("d", {}).get("results", [])

        docs: list[ExternalDocument] = []
        for item in results:
            file_name = item.get("FileLeafRef", "")
            file_ref = item.get("FileRef", "")
            item_id = str(item.get("Id", ""))

            if not file_name or not file_ref:
                continue

            import mimetypes as mt

            content_type = mt.guess_type(file_name)[0]
            file_length = None
            file_data = item.get("File", {})
            if isinstance(file_data, dict):
                file_length = file_data.get("Length")
                if file_length is not None:
                    try:
                        file_length = int(file_length)
                    except (ValueError, TypeError):
                        file_length = None

            docs.append(
                ExternalDocument(
                    external_id=f"sp_{item_id}_{file_name}",
                    filename=file_name,
                    content_type=content_type,
                    download_url=f"{site}/_api/web/getfilebyserverrelativeurl('{quote(file_ref, safe='/')}')/$value",
                    size_bytes=file_length,
                    metadata={
                        "library": library,
                        "file_ref": file_ref,
                        "item_id": item_id,
                    },
                )
            )

        return docs[:limit]

    def download_document(
        self, config: dict[str, Any], doc: ExternalDocument
    ) -> tuple[str, bytes, Optional[str]]:
        if not doc.download_url:
            raise ConnectorError("No download URL for SharePoint file.")
        token = _get_access_token(config)
        headers = {
            "Authorization": bearer_auth_header(token),
            "Accept": "*/*",
        }
        _status, file_bytes, _hdrs = http_request(doc.download_url, headers=headers)
        return doc.filename, file_bytes, doc.content_type
