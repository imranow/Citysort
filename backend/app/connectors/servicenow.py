"""ServiceNow connector — pull attachments from incident/request records."""

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
class ServiceNowConnector(BaseConnector):
    connector_type = "servicenow"

    def _auth_header(self, config: dict[str, Any]) -> dict[str, str]:
        return {
            "Authorization": basic_auth_header(config["username"], config["password"]),
            "Accept": "application/json",
        }

    def _base(self, config: dict[str, Any]) -> str:
        url = config["instance_url"].rstrip("/")
        if not url.startswith("http"):
            url = "https://" + url
        return url

    # ------------------------------------------------------------------

    def test_connection(self, config: dict[str, Any]) -> tuple[bool, str]:
        for key in ("instance_url", "username", "password", "table_name"):
            if not config.get(key, "").strip():
                return False, f"Missing required field: {key}"
        try:
            url = f"{self._base(config)}/api/now/table/{config['table_name']}?sysparm_limit=1"
            http_json(url, headers=self._auth_header(config), timeout=15)
            return True, "Successfully connected to ServiceNow."
        except ConnectorError as exc:
            return False, f"Connection failed: {exc}"

    def list_documents(
        self, config: dict[str, Any], limit: int = 50
    ) -> list[ExternalDocument]:
        base = self._base(config)
        table = config["table_name"]
        headers = self._auth_header(config)

        # 1) Fetch records from the table
        records_url = (
            f"{base}/api/now/table/{table}"
            f"?sysparm_limit={limit}&sysparm_fields=sys_id,number,short_description"
            f"&sysparm_orderby=sys_created_on"
        )
        data = http_json(records_url, headers=headers)
        records = data.get("result", [])

        docs: list[ExternalDocument] = []
        for record in records:
            sys_id = record.get("sys_id", "")
            if not sys_id:
                continue

            # 2) Fetch attachments for each record
            attach_url = (
                f"{base}/api/now/attachment"
                f"?sysparm_query=table_sys_id={sys_id}"
                f"&sysparm_fields=sys_id,file_name,content_type,size_bytes"
            )
            try:
                attach_data = http_json(attach_url, headers=headers)
            except ConnectorError:
                continue

            for att in attach_data.get("result", []):
                docs.append(
                    ExternalDocument(
                        external_id=f"sn_{att.get('sys_id', '')}",
                        filename=att.get(
                            "file_name", f"attachment_{att.get('sys_id', '')[:8]}"
                        ),
                        content_type=att.get("content_type"),
                        download_url=f"{base}/api/now/attachment/{att['sys_id']}/file",
                        size_bytes=int(att.get("size_bytes", 0)) or None,
                        metadata={
                            "record_number": record.get("number", ""),
                            "record_description": record.get("short_description", ""),
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
            raise ConnectorError("No download URL for ServiceNow attachment.")
        headers = self._auth_header(config)
        headers["Accept"] = "*/*"
        _status, file_bytes, _hdrs = http_request(doc.download_url, headers=headers)
        return doc.filename, file_bytes, doc.content_type
