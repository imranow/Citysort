"""Salesforce connector — pull attachments from Salesforce records via OAuth 2.0."""

from __future__ import annotations

from typing import Any, Optional
from urllib.parse import urlencode

from .base import (
    BaseConnector,
    ConnectorError,
    ExternalDocument,
    bearer_auth_header,
    http_json,
    http_request,
    register_connector,
)

_API_VERSION = "v59.0"


def _get_access_token(config: dict[str, Any]) -> tuple[str, str]:
    """Authenticate via OAuth 2.0 password grant. Returns (access_token, instance_url)."""
    token_url = f"{config['instance_url'].rstrip('/')}/services/oauth2/token"
    body = urlencode(
        {
            "grant_type": "password",
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
            "username": config["username"],
            "password": config["password"],
        }
    ).encode("utf-8")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }
    data = http_json(token_url, method="POST", headers=headers, body=body, timeout=15)
    access_token = data.get("access_token")
    instance_url = data.get("instance_url", config["instance_url"].rstrip("/"))
    if not access_token:
        raise ConnectorError("Salesforce OAuth did not return an access_token.")
    return access_token, instance_url


@register_connector
class SalesforceConnector(BaseConnector):
    connector_type = "salesforce"

    def test_connection(self, config: dict[str, Any]) -> tuple[bool, str]:
        for key in (
            "instance_url",
            "client_id",
            "client_secret",
            "username",
            "password",
        ):
            if not config.get(key, "").strip():
                return False, f"Missing required field: {key}"
        try:
            token, inst = _get_access_token(config)
            # Test query
            url = f"{inst}/services/data/{_API_VERSION}/query?q=SELECT+Id+FROM+Account+LIMIT+1"
            http_json(
                url,
                headers={
                    "Authorization": bearer_auth_header(token),
                    "Accept": "application/json",
                },
                timeout=15,
            )
            return True, "Successfully connected to Salesforce."
        except ConnectorError as exc:
            return False, f"Connection failed: {exc}"

    def list_documents(
        self, config: dict[str, Any], limit: int = 50
    ) -> list[ExternalDocument]:
        token, inst = _get_access_token(config)
        headers = {
            "Authorization": bearer_auth_header(token),
            "Accept": "application/json",
        }

        # Query recent attachments (classic Attachment object)
        soql = (
            f"SELECT Id, Name, ContentType, BodyLength, ParentId, CreatedDate "
            f"FROM Attachment ORDER BY CreatedDate DESC LIMIT {limit}"
        )
        url = f"{inst}/services/data/{_API_VERSION}/query?q={soql.replace(' ', '+')}"
        data = http_json(url, headers=headers)

        docs: list[ExternalDocument] = []
        for record in data.get("records", []):
            docs.append(
                ExternalDocument(
                    external_id=f"sf_{record.get('Id', '')}",
                    filename=record.get("Name", "attachment"),
                    content_type=record.get("ContentType"),
                    download_url=f"{inst}/services/data/{_API_VERSION}/sobjects/Attachment/{record['Id']}/Body",
                    size_bytes=record.get("BodyLength"),
                    metadata={
                        "parent_id": record.get("ParentId", ""),
                        "created_date": record.get("CreatedDate", ""),
                        "sf_instance": inst,
                        "access_token": token,  # Needed for download
                    },
                )
            )

        return docs[:limit]

    def download_document(
        self, config: dict[str, Any], doc: ExternalDocument
    ) -> tuple[str, bytes, Optional[str]]:
        if not doc.download_url:
            raise ConnectorError("No download URL for Salesforce attachment.")

        # Re-authenticate to ensure fresh token
        token, _inst = _get_access_token(config)
        headers = {"Authorization": bearer_auth_header(token), "Accept": "*/*"}
        _status, file_bytes, _hdrs = http_request(doc.download_url, headers=headers)
        return doc.filename, file_bytes, doc.content_type
