# Incident Response Playbook

## Severity Levels

- `SEV-1`: data breach, full outage, or inability to process documents.
- `SEV-2`: degraded processing, queue delays, partial API outage.
- `SEV-3`: non-critical feature outage.

## Steps

1. **Detect**
   - Alert from monitoring/Sentry or user report.
2. **Contain**
   - Block abusive IPs, tighten rate limits, disable affected connector.
3. **Eradicate**
   - Patch root cause and rotate compromised credentials.
4. **Recover**
   - Restore service, replay failed jobs, validate data integrity.
5. **Postmortem**
   - Timeline, root cause, corrective actions, owner and due date.

## Evidence Sources

- API logs (request IDs)
- `audit_events` table
- worker/job logs
- reverse proxy/load balancer logs

## Required Notifications

- Internal engineering + operations
- Security owner
- External stakeholders per policy (for PII/security incidents)
