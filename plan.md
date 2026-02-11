# CitySort AI — All Connectors Implementation Plan

## Overview
Build out all 10 connectors (3 database + 7 SaaS) so documents can be imported from external systems into the CitySort pipeline. Each connector: authenticates, lists/fetches documents, downloads files, ingests them, and deduplicates.

---

## Phase 1: Backend Foundation — Connector Framework & Database Tables

### 1a. New database tables in `db.py`
- **`connector_configs`** — Persistent storage for connector credentials/settings
  - id, connector_type, config_json (encrypted-at-rest later), enabled, last_sync_at, created_at, updated_at
- **`connector_sync_log`** — Deduplication tracking (what's been imported)
  - id, connector_type, external_id (unique per connector), filename, document_id, created_at
  - UNIQUE constraint on (connector_type, external_id) to prevent re-imports

### 1b. New endpoints in `main.py`
- `POST /api/connectors/{type}/import` — Trigger a one-click import from any connector
- `GET /api/connectors/{type}/config` — Retrieve saved config
- `PUT /api/connectors/{type}/config` — Save/update config (persists to DB instead of localStorage)
- Update existing `POST /api/connectors/{type}/test` — Wire up real API calls for SaaS

### 1c. New schemas in `schemas.py`
- `ConnectorConfigSaveRequest` — config dict + connector_type
- `ConnectorImportRequest` — connector_type + config override + limit
- `ConnectorImportResponse` — imported_count, skipped_count (dedup), failed_count, errors

### 1d. Base connector module: `backend/app/connectors/__init__.py` + `base.py`
- `BaseConnector` class with methods:
  - `test_connection(config) -> (success, message)` — Validate credentials
  - `list_documents(config, limit) -> list[ExternalDocument]` — List available docs
  - `download_document(config, external_doc) -> (filename, bytes, content_type)` — Download one doc
- `ExternalDocument` dataclass: external_id, filename, content_type, metadata
- `get_connector(connector_type) -> BaseConnector` — Factory function

---

## Phase 2: SaaS Connector Implementations (7 files)

All use `urllib.request` for HTTP calls (no new dependencies except `boto3` for S3).

### 2a. `connectors/servicenow.py` — ServiceNow
- Auth: Basic Auth (username:password)
- List: `GET {instance_url}/api/now/table/{table_name}?sysparm_fields=sys_id,number,short_description&sysparm_limit={limit}`
- Get attachments per record: `GET {instance_url}/api/now/attachment?sysparm_query=table_sys_id={sys_id}`
- Download: `GET {instance_url}/api/now/attachment/{attach_sys_id}/file`
- External ID: attachment sys_id

### 2b. `connectors/confluence.py` — Confluence Cloud
- Auth: Email + API Token (Basic Auth)
- List pages: `GET {base_url}/rest/api/content?spaceKey={space_key}&type=page&limit={limit}`
- Get attachments: `GET {base_url}/rest/api/content/{page_id}/child/attachment`
- Download: `GET {base_url}{attachment._links.download}`
- External ID: attachment id

### 2c. `connectors/salesforce.py` — Salesforce
- Auth: OAuth 2.0 password grant → `POST {instance_url}/services/oauth2/token`
- List: `GET {instance_url}/services/data/v59.0/query?q=SELECT+Id,Name,Body,ContentType+FROM+Attachment+ORDER+BY+CreatedDate+DESC+LIMIT+{limit}`
- Download: `GET {instance_url}/services/data/v59.0/sobjects/Attachment/{id}/Body`
- External ID: Attachment Id

### 2d. `connectors/gcs.py` — Google Cloud Storage
- Auth: Parse service account JSON key → sign JWT → exchange for access token
- List: `GET https://storage.googleapis.com/storage/v1/b/{bucket}/o?prefix={prefix}&maxResults={limit}`
- Download: `GET https://storage.googleapis.com/storage/v1/b/{bucket}/o/{object}?alt=media`
- External ID: object name (full path)
- Note: JWT signing uses Python's `hmac`/`hashlib` + `base64` — no external library needed

### 2e. `connectors/s3.py` — Amazon S3
- Auth: AWS Signature V4 (HMAC-SHA256)
- List: `GET https://{bucket}.s3.{region}.amazonaws.com/?list-type=2&prefix={prefix}&max-keys={limit}`
- Download: `GET https://{bucket}.s3.{region}.amazonaws.com/{key}`
- External ID: object key
- Note: Implement AWS Sig V4 signing with `hmac`/`hashlib` — no boto3 needed, keeping deps minimal

### 2f. `connectors/jira_connector.py` — Jira Cloud
- Auth: Email + API Token (Basic Auth)
- Search: `GET {base_url}/rest/api/3/search?jql={jql}&fields=attachment,summary&maxResults={limit}`
- Download: `GET {attachment.content}` with auth header
- External ID: attachment id

### 2g. `connectors/sharepoint.py` — SharePoint Online
- Auth: OAuth 2.0 client_credentials → `POST https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token`
- List files: `GET {site_url}/_api/web/lists/getbytitle('{library}')/items?$select=FileLeafRef,FileRef,Id&$top={limit}`
- Download: `GET {site_url}/_api/web/getfilebyserverrelativeurl('{file_ref}')/$value`
- External ID: item Id

---

## Phase 3: Import Orchestration — `backend/app/connectors/importer.py`

Shared import logic used by all connectors:
1. Call `connector.list_documents(config, limit)`
2. For each doc, check `connector_sync_log` — skip if external_id already exists
3. Call `connector.download_document(config, doc)`
4. Save file to UPLOAD_DIR with UUID prefix
5. Create document record (status: ingested, source_channel: connector_{type})
6. Record in `connector_sync_log`
7. Create audit event
8. Enqueue async processing
9. Return summary (imported, skipped, failed)

---

## Phase 4: Frontend Updates

### 4a. Update `CONNECTOR_REGISTRY` in `app.v2.js`
- Change all 7 SaaS connectors from `status: "coming_soon"` to `status: "available"`

### 4b. Add SaaS import handler in `app.v2.js`
- New function `_handleSaaSImport(connectorId, values)`:
  - POST to `/api/connectors/{connectorId}/import` with config + limit
  - Show progress toast, then results toast
- Update `bindConnectors()` submit handler to call `_handleSaaSImport()` for saas category

### 4c. Update connector config UI
- For SaaS connectors: add a "Limit" field (default 50) in the import section
- Show import count after successful import
- Remove "Coming Soon" badges and disabled state

### 4d. Update cache busters in `index.html`

---

## Phase 5: Testing & Verification

- Test each connector's `test_connection()` via the Test button
- Test import flow end-to-end (ServiceNow, Jira, etc.)
- Verify deduplication (import twice → second time skips)
- Verify documents flow through AI pipeline after import
- Browser testing of updated UI

---

## File Summary

### New Files (9):
- `backend/app/connectors/__init__.py`
- `backend/app/connectors/base.py`
- `backend/app/connectors/importer.py`
- `backend/app/connectors/servicenow.py`
- `backend/app/connectors/confluence.py`
- `backend/app/connectors/salesforce.py`
- `backend/app/connectors/gcs.py`
- `backend/app/connectors/s3.py`
- `backend/app/connectors/jira_connector.py`
- `backend/app/connectors/sharepoint.py`

### Modified Files (6):
- `backend/app/db.py` — connector_configs + connector_sync_log tables
- `backend/app/main.py` — import/config/test endpoints
- `backend/app/schemas.py` — new request/response models
- `frontend/app.v2.js` — SaaS import handler, status updates
- `frontend/index.html` — cache busters, minor UI tweaks
- `backend/requirements.txt` — no new deps (all using urllib)

### Dependencies:
- **No new Python packages** — all SaaS connectors use `urllib.request` + stdlib (`hmac`, `hashlib`, `base64`, `json`)
- GCS JWT signing uses Python's built-in `hmac` + `base64`
- S3 AWS Sig V4 uses Python's built-in `hmac` + `hashlib`
