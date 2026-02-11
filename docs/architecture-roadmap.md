# Architecture Roadmap

## Completed Foundations (this phase)

- PostgreSQL-ready database backend support via `CITYSORT_DATABASE_URL`
- Security middleware (rate limiting, secure headers, trusted hosts, HTTPS gate)
- Upload validation + optional ClamAV scan
- Optional at-rest encryption for uploaded files
- Prometheus metrics endpoint and structured logging
- Backup/restore scripts and CI workflow baseline

## Next Phases

1. Queue reliability
   - Redis-backed queue (RQ/Celery)
   - Dead-letter queue and retry policy dashboard
2. Storage scalability
   - S3/Azure Blob as primary document store
   - lifecycle and archival rules
3. Compliance hardening
   - PII redaction in logs
   - tenant-aware retention policies
   - exportable compliance reports
4. Access control
   - fine-grained roles: intake, reviewer, approver
   - SSO (OIDC/SAML)
5. Product evolution
   - Frontend migration to React/Vue with typed state management
   - multi-tenant data isolation model
