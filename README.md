# CitySort AI MVP

Runnable MVP for the CitySort plan: ingest, extract, classify, validate, and route city documents with human-in-the-loop review.

## Implemented capabilities

- FastAPI backend with SQLite persistence and audit events
- Upload pipeline with document lifecycle states (`ingested`, `routed`, `needs_review`, `approved`, `corrected`, `failed`)
- Bulk database import API to ingest documents from SQLite/PostgreSQL/MySQL using a SELECT query
- OCR provider switch:
  - `local` (native text + PDF parsing)
  - `azure_di` (Azure Document Intelligence)
- Classification provider switch:
  - `rules` (local keyword model)
  - `openai` (JSON classification)
  - `anthropic` (JSON classification)
- Automatic fallback to local processing when provider credentials/calls are unavailable
- Department queues and analytics APIs
- Human review API and dashboard workflow
- Audit trail API per document (`/api/documents/{id}/audit`)
- Rules config APIs (`GET/PUT /api/config/rules`, `POST /api/config/rules/reset`)
- Platform operations APIs for enterprise-style controls:
  - Connectivity checks (`GET /api/platform/connectivity`, `POST /api/platform/connectivity/check`)
  - Manual deployments + history (`POST /api/platform/deployments/manual`, `GET /api/platform/deployments`)
  - Team invitations (`POST /api/platform/invitations`, `GET /api/platform/invitations`)
  - API key lifecycle (`POST /api/platform/api-keys`, `GET /api/platform/api-keys`, `POST /api/platform/api-keys/{id}/revoke`)
  - Platform summary (`GET /api/platform/summary`)
- Web dashboard for upload, queue monitoring, analytics, and review actions
- Enhanced review pane with extracted fields, validation issues, corrected JSON fields, text preview, and audit history
- Reprocess action on selected documents to apply latest rules/providers without re-upload
- Rules editor panel in dashboard to update doc types, keywords, required fields, and routing without code changes
- Form-based rules builder (add/remove types, comma-separated keywords/required fields) so most users never need JSON
- Dashboard topbar actions wired end-to-end:
  - `Connect`: runs provider/database readiness checks
  - `Manual Deploy`: records and returns deploy result
  - `Invite`: creates invitation token/link
  - `New API Key`: issues a new key (shown once)
- Unit tests for core pipeline logic

## Key files

- `backend/app/main.py`: API routes and orchestration
- `backend/app/pipeline.py`: pipeline core
- `backend/app/providers.py`: Azure/OpenAI/Anthropic integrations
- `backend/app/rules.py`: runtime rule loading/validation/persistence
- `backend/app/config.py`: environment-based config
- `backend/tests/test_platform_api.py`: platform operations API tests
- `frontend/index.html`: dashboard shell
- `frontend/app.v2.js`: dashboard behavior
- `deploy/k8s/`: Kubernetes manifests (namespace, deployment, service, ingress, HPA, config)
- `docker-compose.yml`: local container orchestration
- `scripts/run_demo.sh`: end-to-end demo runner

## Setup

```bash
cd citysort
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
```

## Configuration

```bash
cd citysort
cp .env.example .env
```

Default `.env` values run fully local.

To enable external providers:

- Azure OCR: set `CITYSORT_OCR_PROVIDER=azure_di` and fill `AZURE_DOCUMENT_INTELLIGENCE_*`
- OpenAI classifier: set `CITYSORT_CLASSIFIER_PROVIDER=openai` and `OPENAI_API_KEY`
- Anthropic classifier: set `CITYSORT_CLASSIFIER_PROVIDER=anthropic` and `ANTHROPIC_API_KEY`
- Optional custom rules file path: `CITYSORT_RULES_PATH` (defaults to `data/document_rules.json`)
- Confidence gate for auto-routing: `CITYSORT_CONFIDENCE_THRESHOLD` (default `0.82`)
- Always-human-review types: `CITYSORT_FORCE_REVIEW_DOC_TYPES` (comma-separated)

### Safer AI rollout profile (recommended)

```env
CITYSORT_OCR_PROVIDER=azure_di
CITYSORT_CLASSIFIER_PROVIDER=openai
OPENAI_MODEL=gpt-4o-mini
CITYSORT_CONFIDENCE_THRESHOLD=0.92
CITYSORT_FORCE_REVIEW_DOC_TYPES=other,benefits_application,court_filing
```

## Run server

```bash
cd citysort
source .venv/bin/activate
uvicorn backend.app.main:app --reload --port 8000
```

Dashboard: [http://localhost:8000](http://localhost:8000)
Health: [http://localhost:8000/health](http://localhost:8000/health)
Readiness: [http://localhost:8000/readyz](http://localhost:8000/readyz)
Liveness: [http://localhost:8000/livez](http://localhost:8000/livez)

## Run tests

```bash
cd citysort
source .venv/bin/activate
PYTHONPATH=backend pytest backend/tests -q
```

## End-to-end demo run

This starts the API, uploads sample documents, prints results/analytics/queues, and shuts down the server.

```bash
cd citysort
./scripts/run_demo.sh
```

Sample docs used by the demo are in `assets/samples`.

## Bulk import from a database

Use the dashboard **Database Import** panel, or call the API directly:

```bash
curl -X POST http://localhost:8000/api/documents/import/database \
  -H "Content-Type: application/json" \
  -d '{
    "database_url": "postgresql://user:pass@localhost:5432/files_db",
    "query": "SELECT filename, content, content_type FROM incoming_files",
    "filename_column": "filename",
    "content_column": "content",
    "content_type_column": "content_type",
    "source_channel": "database_import",
    "actor": "ops_user",
    "process_async": false,
    "limit": 500
  }'
```

Notes:
- `database_url` supports:
  - SQLite: `sqlite:///absolute/path/to/files.db` or `/absolute/path/to/files.db`
  - PostgreSQL: `postgresql://user:pass@host:5432/dbname`
  - MySQL: `mysql://user:pass@host:3306/dbname`
- Query must be a single `SELECT`/`WITH ... SELECT` statement.
- Provide either `content_column` (BLOB/text) or `file_path_column` (path on server).

## Push to GitHub

```bash
cd citysort
git remote add origin https://github.com/imranow/Citysort.git
git push -u origin main
```

`.env` is intentionally excluded from git. Only commit `.env.example`.

## Deploy as a website (Render)

This repo includes `render.yaml`, so you can deploy with a Render Blueprint.

1. In Render, choose **New +** -> **Blueprint**.
2. Connect `imranow/Citysort`.
3. Render will detect `render.yaml`.
4. Set any required secret env vars in Render dashboard:
   - `OPENAI_API_KEY` (if using OpenAI classification)
   - `ANTHROPIC_API_KEY` (if using Anthropic classification)
   - `AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT` and `AZURE_DOCUMENT_INTELLIGENCE_API_KEY` (if using Azure OCR)
5. Deploy. Render provides a public URL.

Default deployment uses local rule-based classification:
- `CITYSORT_OCR_PROVIDER=local`
- `CITYSORT_CLASSIFIER_PROVIDER=rules`

## Docker deploy

This repo includes a production Dockerfile at `Dockerfile`.

```bash
cd citysort
docker build -t citysort:latest .
docker run -p 8000:8000 --env-file .env citysort:latest
```

Or run with compose (includes volume + healthcheck):

```bash
cd citysort
docker compose up --build
```

## Kubernetes deploy

Kubernetes manifests are provided in `deploy/k8s`.

1. Update the image in `deploy/k8s/deployment.yaml` to your published image.
2. Set real secrets in `deploy/k8s/secret.example.yaml` (or create your own `Secret`).
3. Update the host in `deploy/k8s/ingress.yaml`.
4. Apply:

```bash
cd citysort
kubectl apply -k deploy/k8s
```
