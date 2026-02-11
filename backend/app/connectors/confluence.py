"""Confluence Cloud connector — pull attachments from wiki pages."""

from __future__ import annotations

from typing import Any, Optional

from .base import (
    BaseConnector,
    ConnectorError,
    ExternalDocument,
    basic_auth_header,
    http_json,
    http_request,
    register_connector,
)


@register_connector
class ConfluenceConnector(BaseConnector):
    connector_type = "confluence"

    def _auth_header(self, config: dict[str, Any]) -> dict[str, str]:
        return {
            "Authorization": basic_auth_header(config["email"], config["api_token"]),
            "Accept": "application/json",
        }

    def _base(self, config: dict[str, Any]) -> str:
        url = config["base_url"].rstrip("/")
        if not url.startswith("http"):
            url = "https://" + url
        return url

    # ------------------------------------------------------------------

    def test_connection(self, config: dict[str, Any]) -> tuple[bool, str]:
        for key in ("base_url", "email", "api_token", "space_key"):
            if not config.get(key, "").strip():
                return False, f"Missing required field: {key}"
        try:
            url = f"{self._base(config)}/rest/api/space/{config['space_key']}"
            http_json(url, headers=self._auth_header(config), timeout=15)
            return True, "Successfully connected to Confluence."
        except ConnectorError as exc:
            return False, f"Connection failed: {exc}"

    def list_documents(
        self, config: dict[str, Any], limit: int = 50
    ) -> list[ExternalDocument]:
        base = self._base(config)
        space_key = config["space_key"]
        headers = self._auth_header(config)

        # Fetch pages in the space
        pages_url = (
            f"{base}/rest/api/content"
            f"?spaceKey={space_key}&type=page&limit={min(limit, 100)}"
            f"&expand=children.attachment"
        )
        data = http_json(pages_url, headers=headers)
        pages = data.get("results", [])

        docs: list[ExternalDocument] = []
        for page in pages:
            page_title = page.get("title", "")
            attachments = (
                page.get("children", {}).get("attachment", {}).get("results", [])
            )

            # If no inline attachments, fetch explicitly
            if not attachments:
                att_url = (
                    f"{base}/rest/api/content/{page['id']}/child/attachment?limit=50"
                )
                try:
                    att_data = http_json(att_url, headers=headers)
                    attachments = att_data.get("results", [])
                except ConnectorError:
                    continue

            for att in attachments:
                download_path = att.get("_links", {}).get("download", "")
                docs.append(
                    ExternalDocument(
                        external_id=f"conf_{att.get('id', '')}",
                        filename=att.get("title", "attachment"),
                        content_type=att.get("mediaType"),
                        download_url=f"{base}{download_path}"
                        if download_path
                        else None,
                        size_bytes=att.get("extensions", {}).get("fileSize"),
                        metadata={
                            "page_id": page.get("id", ""),
                            "page_title": page_title,
                        },
                    )
                )

            if len(docs) >= limit:
                break

        return docs[:limit]

    def download_document(
        self, config: dict[str, Any], doc: ExternalDocument
    ) -> tuple[str, bytes, Optional[str]]:
        if not doc.download_url:
            raise ConnectorError("No download URL for Confluence attachment.")
        headers = self._auth_header(config)
        headers["Accept"] = "*/*"
        _status, file_bytes, _hdrs = http_request(doc.download_url, headers=headers)
        return doc.filename, file_bytes, doc.content_type
