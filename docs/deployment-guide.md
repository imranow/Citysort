# Deployment Guide

## 1. Environment

Required baseline for production:

- `CITYSORT_ENV=production`
- `CITYSORT_DATABASE_URL=postgresql://...`
- `CITYSORT_REQUIRE_AUTH=true`
- `CITYSORT_AUTH_SECRET=<strong-random-secret>`
- `CITYSORT_ENFORCE_HTTPS=true`
- `CITYSORT_CORS_ALLOWED_ORIGINS=https://<your-ui-domain>`

Recommended:

- `CITYSORT_ENCRYPTION_AT_REST_ENABLED=true`
- `CITYSORT_ENCRYPTION_KEY=<fernet-key>`
- `CITYSORT_PROMETHEUS_ENABLED=true`
- `CITYSORT_SENTRY_DSN=<dsn>`

## 2. Provisioning

1. Create PostgreSQL database and user.
2. Provision object/file storage or persistent disk for `data/uploads`.
3. Configure TLS termination at ingress/load balancer.
4. Install dependencies and run API service.
5. Run backup cron:
   - `scripts/backup.sh`

## 3. Start Service

```bash
source .venv/bin/activate
uvicorn backend.app.main:app --host 0.0.0.0 --port 8000
```

## 4. Verify

- `GET /health`
- `GET /readyz`
- `GET /metrics` (if enabled)
- Upload + process a sample document.

## 5. Backup & Restore

- Backup:
  - `scripts/backup.sh`
- Restore:
  - `scripts/restore.sh <db_backup_file> [uploads_archive]`
