# CitySort AI MVP

Runnable MVP for the CitySort plan: ingest, extract, classify, validate, and route city documents with human-in-the-loop review.

## Implemented capabilities

- FastAPI backend with SQLite persistence and audit events
- Upload pipeline with document lifecycle states (`ingested`, `routed`, `needs_review`, `approved`, `corrected`, `failed`)
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
- Web dashboard for upload, queue monitoring, analytics, and review actions
- Enhanced review pane with extracted fields, validation issues, corrected JSON fields, text preview, and audit history
- Reprocess action on selected documents to apply latest rules/providers without re-upload
- Rules editor panel in dashboard to update doc types, keywords, required fields, and routing without code changes
- Form-based rules builder (add/remove types, comma-separated keywords/required fields) so most users never need JSON
- Unit tests for core pipeline logic

## Key files

- `/Users/imran/Documents/Projects/citysort/backend/app/main.py`: API routes and orchestration
- `/Users/imran/Documents/Projects/citysort/backend/app/pipeline.py`: pipeline core
- `/Users/imran/Documents/Projects/citysort/backend/app/providers.py`: Azure/OpenAI/Anthropic integrations
- `/Users/imran/Documents/Projects/citysort/backend/app/rules.py`: runtime rule loading/validation/persistence
- `/Users/imran/Documents/Projects/citysort/backend/app/config.py`: environment-based config
- `/Users/imran/Documents/Projects/citysort/frontend/index.html`: dashboard shell
- `/Users/imran/Documents/Projects/citysort/frontend/app.js`: dashboard behavior
- `/Users/imran/Documents/Projects/citysort/scripts/run_demo.sh`: end-to-end demo runner

## Setup

```bash
cd /Users/imran/Documents/Projects/citysort
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
```

## Configuration

```bash
cd /Users/imran/Documents/Projects/citysort
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
cd /Users/imran/Documents/Projects/citysort
source .venv/bin/activate
uvicorn backend.app.main:app --reload --port 8000
```

Dashboard: [http://localhost:8000](http://localhost:8000)
Health: [http://localhost:8000/health](http://localhost:8000/health)

## Run tests

```bash
cd /Users/imran/Documents/Projects/citysort
source .venv/bin/activate
PYTHONPATH=backend pytest backend/tests -q
```

## End-to-end demo run

This starts the API, uploads sample documents, prints results/analytics/queues, and shuts down the server.

```bash
cd /Users/imran/Documents/Projects/citysort
./scripts/run_demo.sh
```

Sample docs used by the demo are in `/Users/imran/Documents/Projects/citysort/assets/samples`.

## Push to GitHub

```bash
cd /Users/imran/Documents/Projects/citysort
git remote add origin https://github.com/imranow/Citysort.git
git push -u origin main
```

`.env` is intentionally excluded from git. Only commit `.env.example`.

## Deploy as a website (Render)

This repo includes `/Users/imran/Documents/Projects/citysort/render.yaml`, so you can deploy with a Render Blueprint.

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

This repo includes a production Dockerfile at `/Users/imran/Documents/Projects/citysort/Dockerfile`.

```bash
cd /Users/imran/Documents/Projects/citysort
docker build -t citysort:latest .
docker run -p 8000:8000 --env-file .env citysort:latest
```
