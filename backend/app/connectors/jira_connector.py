"""Jira Cloud connector — pull attachments from Jira issues."""
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
class JiraConnector(BaseConnector):
    connector_type = "jira"

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
        for key in ("base_url", "email", "api_token"):
            if not config.get(key, "").strip():
                return False, f"Missing required field: {key}"
        try:
            url = f"{self._base(config)}/rest/api/3/myself"
            data = http_json(url, headers=self._auth_header(config), timeout=15)
            display = data.get("displayName", data.get("emailAddress", "user"))
            return True, f"Successfully connected to Jira as {display}."
        except ConnectorError as exc:
            return False, f"Connection failed: {exc}"

    def list_documents(
        self, config: dict[str, Any], limit: int = 50
    ) -> list[ExternalDocument]:
        base = self._base(config)
        headers = self._auth_header(config)

        # Build JQL — default to recent issues with attachments
        jql = config.get("jql_query", "").strip()
        if not jql:
            project_key = config.get("project_key", "").strip()
            if project_key:
                jql = f"project = {project_key} AND attachments IS NOT EMPTY ORDER BY created DESC"
            else:
                jql = "attachments IS NOT EMPTY ORDER BY created DESC"

        search_url = (
            f"{base}/rest/api/3/search"
            f"?jql={jql.replace(' ', '+')}"
            f"&fields=attachment,summary,key"
            f"&maxResults={min(limit, 100)}"
        )
        data = http_json(search_url, headers=headers)

        docs: list[ExternalDocument] = []
        for issue in data.get("issues", []):
            issue_key = issue.get("key", "")
            summary = issue.get("fields", {}).get("summary", "")
            attachments = issue.get("fields", {}).get("attachment", []) or []

            for att in attachments:
                docs.append(
                    ExternalDocument(
                        external_id=f"jira_{att.get('id', '')}",
                        filename=att.get("filename", "attachment"),
                        content_type=att.get("mimeType"),
                        download_url=att.get("content"),  # Direct download URL
                        size_bytes=att.get("size"),
                        metadata={
                            "issue_key": issue_key,
                            "issue_summary": summary,
                            "author": att.get("author", {}).get("displayName", ""),
                            "created": att.get("created", ""),
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
            raise ConnectorError("No download URL for Jira attachment.")
        headers = self._auth_header(config)
        headers["Accept"] = "*/*"
        _status, file_bytes, _hdrs = http_request(doc.download_url, headers=headers)
        return doc.filename, file_bytes, doc.content_type
