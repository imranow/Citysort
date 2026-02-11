# Operations Runbook

## Daily Checks

1. `GET /readyz` is `200`.
2. Error logs in last 24h are below threshold.
3. Queue backlog (`/api/jobs`) is not growing.
4. Backup artifacts created in `data/backups/`.

## Incident Triage

1. Determine blast radius:
   - Single endpoint vs all traffic.
2. Pull request logs using `X-Request-ID`.
3. Check:
   - DB connectivity (`/readyz`)
   - Worker health (job status progression)
   - External provider errors (OpenAI/Anthropic/Azure)

## Recovery Actions

1. Restart API process.
2. If DB issue:
   - Fail over to PostgreSQL standby (if configured)
   - Restore latest backup if corruption is detected.
3. Requeue failed jobs from `jobs` table/API.

## Security Response

1. Rotate credentials:
   - `CITYSORT_AUTH_SECRET`
   - API keys
   - SMTP/API provider keys
2. Audit suspicious activity:
   - `audit_events`
   - request logs by source IP / request ID
3. Apply temporary stricter rate limits.

## Retention & Compliance

- Audit events retention: `CITYSORT_AUDIT_RETENTION_DAYS`
- Notifications retention: `CITYSORT_NOTIFICATION_RETENTION_DAYS`
- Outbound email log retention: `CITYSORT_OUTBOUND_EMAIL_RETENTION_DAYS`
