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
