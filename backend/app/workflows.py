"""Workspace-scoped workflow automations triggered by document lifecycle events."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Optional

from .auto_emails import send_assignment_notification
from .emailer import email_configured, send_email
from .notifications import create_notification
from .repository import (
    create_audit_event,
    create_outbound_email,
    get_document,
    get_workspace,
    list_workflow_rules,
    update_document,
    update_outbound_email,
    utcnow_iso,
)
from .templates import compose_template_email, list_templates

logger = logging.getLogger(__name__)

# Keep in sync with backend/app/main.py ALLOWED_TRANSITIONS.
ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "ingested": {"needs_review", "routed"},
    "needs_review": {"acknowledged", "approved", "corrected"},
    "routed": {"acknowledged", "approved"},
    "acknowledged": {"assigned", "approved", "in_progress"},
    "assigned": {"in_progress", "approved"},
    "in_progress": {"completed", "approved"},
    "completed": {"archived"},
    "approved": {"archived"},
    "corrected": {"archived"},
    "failed": {"needs_review", "ingested"},
}


def _document_context(document: dict[str, Any]) -> dict[str, str]:
    ctx: dict[str, str] = {
        "id": str(document.get("id", "")),
        "workspace_id": str(document.get("workspace_id") or ""),
        "filename": str(document.get("filename", "")),
        "doc_type": str(document.get("doc_type") or ""),
        "department": str(document.get("department") or ""),
        "status": str(document.get("status") or ""),
        "urgency": str(document.get("urgency") or ""),
        "confidence": str(document.get("confidence") or ""),
        "assigned_to": str(document.get("assigned_to") or ""),
        "due_date": str(document.get("due_date") or ""),
        "source_channel": str(document.get("source_channel") or ""),
    }
    extracted_fields = document.get("extracted_fields")
    if isinstance(extracted_fields, dict):
        for key, value in extracted_fields.items():
            if not key:
                continue
            ctx[str(key)] = "" if value is None else str(value)
    return ctx


def _render(template: str, context: dict[str, str]) -> str:
    rendered = str(template or "")
    for key, value in context.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", value)
    return rendered


def _matches_filters(filters: dict[str, Any], document: dict[str, Any]) -> bool:
    if not filters:
        return True

    def _matches_value(field_name: str) -> bool:
        raw = filters.get(field_name)
        if raw is None or raw == "":
            return True
        actual = document.get(field_name)
        if isinstance(raw, list):
            return str(actual or "") in {str(item) for item in raw}
        return str(actual or "") == str(raw)

    for field in ("doc_type", "department", "status", "urgency", "source_channel"):
        if not _matches_value(field):
            return False

    try:
        min_conf = filters.get("min_confidence")
        if min_conf is not None:
            if float(document.get("confidence") or 0.0) < float(min_conf):
                return False
        max_conf = filters.get("max_confidence")
        if max_conf is not None:
            if float(document.get("confidence") or 0.0) > float(max_conf):
                return False
    except Exception:
        # Ignore confidence filter parsing issues (treat as non-match safe).
        return False

    return True


def _resolve_assignee(
    config: dict[str, Any],
    *,
    workspace_id: Optional[str],
) -> Optional[str]:
    user_id = str(config.get("user_id") or "").strip()
    if user_id:
        return user_id
    assignee = str(config.get("assignee") or "").strip().lower()
    if assignee == "workspace_owner" and workspace_id:
        workspace = get_workspace(workspace_id)
        if workspace and workspace.get("owner_id"):
            return str(workspace["owner_id"])
    return None


def _action_assign(
    *,
    rule_name: str,
    document: dict[str, Any],
    actor: str,
    workspace_id: Optional[str],
    config: dict[str, Any],
) -> None:
    document_id = str(document.get("id") or "").strip()
    if not document_id:
        return

    only_if_unassigned = bool(config.get("only_if_unassigned", True))
    current_assignee = str(document.get("assigned_to") or "").strip()
    if only_if_unassigned and current_assignee:
        return

    assignee_id = _resolve_assignee(config, workspace_id=workspace_id)
    if not assignee_id:
        return

    updates: dict[str, Any] = {"assigned_to": assignee_id}
    if bool(config.get("set_status_assigned", True)):
        current_status = str(document.get("status") or "").strip().lower()
        if current_status in {"needs_review", "acknowledged"}:
            updates["status"] = "assigned"

    updated = update_document(
        document_id,
        updates=updates,
        workspace_id=workspace_id,
    )
    if not updated:
        return

    create_audit_event(
        document_id=document_id,
        action="workflow_assigned",
        actor=actor,
        details=f"rule={rule_name} assigned_to={assignee_id}",
        workspace_id=workspace_id,
    )
    try:
        create_notification(
            type="assignment",
            title=f"Assigned by workflow: {updated.get('filename', 'Document')}",
            message=f"Rule: {rule_name}",
            user_id=assignee_id,
            document_id=document_id,
            workspace_id=workspace_id,
        )
    except Exception:
        pass

    try:
        send_assignment_notification(document_id, assignee_id)
    except Exception:
        logger.debug("Workflow assignment email failed (non-blocking)", exc_info=True)


def _find_template_id_by_name_hint(
    *, workspace_id: Optional[str], name_hint: str
) -> Optional[int]:
    hint = (name_hint or "").strip().lower()
    if not hint:
        return None
    candidates = list_templates(workspace_id=workspace_id, limit=200)
    for item in candidates:
        name = str(item.get("name") or "").lower()
        if hint in name and item.get("id") is not None:
            try:
                return int(item["id"])
            except Exception:
                continue
    return None


def _action_send_template_email(
    *,
    rule_name: str,
    document: dict[str, Any],
    actor: str,
    workspace_id: Optional[str],
    config: dict[str, Any],
) -> None:
    if not email_configured():
        return

    document_id = str(document.get("id") or "").strip()
    if not document_id:
        return

    template_id: Optional[int] = None
    raw_template_id = config.get("template_id")
    if raw_template_id is not None:
        try:
            template_id = int(raw_template_id)
        except Exception:
            template_id = None
    if template_id is None and config.get("template_name_hint"):
        template_id = _find_template_id_by_name_hint(
            workspace_id=workspace_id,
            name_hint=str(config.get("template_name_hint") or ""),
        )
    if template_id is None:
        return

    try:
        composed = compose_template_email(
            int(template_id),
            document_id,
            workspace_id=workspace_id,
        )
    except Exception:
        logger.debug("Workflow template compose failed (non-blocking)", exc_info=True)
        return

    to_email = str(composed.get("to_email") or "").strip()
    if not to_email:
        return

    subject = str(composed.get("subject") or "CitySort Update")
    body = str(composed.get("body") or "")
    if not body:
        return

    record = create_outbound_email(
        document_id=document_id,
        workspace_id=workspace_id,
        to_email=to_email,
        subject=subject,
        body=body,
        status="pending",
    )

    try:
        send_email(to_email=to_email, subject=subject, body=body)
        update_outbound_email(
            int(record["id"]),
            status="sent",
            sent_at=datetime.now(timezone.utc).isoformat(),
        )
        create_audit_event(
            document_id=document_id,
            action="workflow_email_sent",
            actor=actor,
            details=f"rule={rule_name} to={to_email} template_id={template_id}",
            workspace_id=workspace_id,
        )
    except Exception as exc:
        update_outbound_email(int(record["id"]), status="failed", error=str(exc))
        create_audit_event(
            document_id=document_id,
            action="workflow_email_failed",
            actor=actor,
            details=f"rule={rule_name} to={to_email} error={exc}",
            workspace_id=workspace_id,
        )


def _action_webhook_post(
    *,
    rule_name: str,
    document: dict[str, Any],
    actor: str,
    workspace_id: Optional[str],
    trigger_event: str,
    config: dict[str, Any],
) -> None:
    url = str(config.get("url") or "").strip()
    if not url:
        return

    payload = {
        "event": trigger_event,
        "rule": rule_name,
        "actor": actor,
        "workspace_id": workspace_id,
        "document": {
            "id": document.get("id"),
            "filename": document.get("filename"),
            "status": document.get("status"),
            "doc_type": document.get("doc_type"),
            "department": document.get("department"),
            "urgency": document.get("urgency"),
            "confidence": document.get("confidence"),
            "assigned_to": document.get("assigned_to"),
            "due_date": document.get("due_date"),
            "source_channel": document.get("source_channel"),
        },
        "extracted_fields": document.get("extracted_fields") or {},
        "missing_fields": document.get("missing_fields") or [],
        "validation_errors": document.get("validation_errors") or [],
        "sent_at": utcnow_iso(),
    }
    data = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    request = urllib.request.Request(
        url=url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(request, timeout=10).read()
        create_audit_event(
            document_id=str(document.get("id") or ""),
            action="workflow_webhook_sent",
            actor=actor,
            details=f"rule={rule_name} event={trigger_event} url={url}",
            workspace_id=workspace_id,
        )
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        logger.debug("Workflow webhook failed: %s", exc)
        try:
            create_audit_event(
                document_id=str(document.get("id") or ""),
                action="workflow_webhook_failed",
                actor=actor,
                details=f"rule={rule_name} event={trigger_event} url={url} error={exc}",
                workspace_id=workspace_id,
            )
        except Exception:
            pass


def _action_create_notification(
    *,
    rule_name: str,
    document: dict[str, Any],
    actor: str,
    workspace_id: Optional[str],
    trigger_event: str,
    config: dict[str, Any],
) -> None:
    ctx = _document_context(document)
    notif_type = str(config.get("type") or "workflow").strip() or "workflow"
    title_tmpl = str(config.get("title") or "Workflow event").strip()
    message_tmpl = str(config.get("message") or "").strip()
    user_id = str(config.get("user_id") or "").strip() or None
    title = _render(title_tmpl, ctx)
    message = _render(message_tmpl, ctx) if message_tmpl else f"Event: {trigger_event}"
    try:
        create_notification(
            type=notif_type,
            title=title,
            message=message,
            user_id=user_id,
            document_id=str(document.get("id") or ""),
            workspace_id=workspace_id,
        )
        create_audit_event(
            document_id=str(document.get("id") or ""),
            action="workflow_notification_created",
            actor=actor,
            details=f"rule={rule_name} event={trigger_event} type={notif_type}",
            workspace_id=workspace_id,
        )
    except Exception:
        logger.debug("Workflow notification failed (non-blocking)", exc_info=True)


def _action_transition(
    *,
    rule_name: str,
    document: dict[str, Any],
    actor: str,
    workspace_id: Optional[str],
    config: dict[str, Any],
) -> None:
    document_id = str(document.get("id") or "").strip()
    if not document_id:
        return

    target = str(config.get("status") or config.get("to_status") or "").strip().lower()
    if not target:
        return

    current = str(document.get("status") or "").strip().lower()
    allowed = ALLOWED_TRANSITIONS.get(current, set())
    if target not in allowed:
        return

    updates: dict[str, Any] = {"status": target}
    notes = config.get("notes")
    if notes is not None and str(notes).strip():
        updates["reviewer_notes"] = str(notes).strip()

    updated = update_document(
        document_id,
        updates=updates,
        workspace_id=workspace_id,
    )
    if not updated:
        return

    create_audit_event(
        document_id=document_id,
        action="workflow_transition",
        actor=actor,
        details=f"rule={rule_name} from={current} to={target}",
        workspace_id=workspace_id,
    )
    try:
        create_notification(
            type="status_change",
            title=f"{updated.get('filename', 'Document')}: {current} -> {target}",
            message=f"Rule: {rule_name}",
            document_id=document_id,
            workspace_id=workspace_id,
        )
    except Exception:
        pass


def run_workflows(
    *,
    trigger_event: str,
    document: dict[str, Any],
    actor: str,
    workspace_id: Optional[str],
) -> None:
    """Evaluate enabled workflow rules and apply actions (never raises)."""
    try:
        rules = list_workflow_rules(
            workspace_id=workspace_id,
            trigger_event=trigger_event,
            enabled_only=True,
            include_global=True,
            limit=200,
        )
    except Exception:
        logger.debug("Workflow rule load failed (non-blocking)", exc_info=True)
        return

    for rule in rules:
        try:
            if not rule.get("enabled", True):
                continue
            filters = rule.get("filters", {})
            if not isinstance(filters, dict):
                filters = {}
            if not _matches_filters(filters, document):
                continue
            actions = rule.get("actions", [])
            if not isinstance(actions, list):
                continue
            rule_name = str(rule.get("name") or f"workflow-{rule.get('id', '')}")
            for action in actions:
                if not isinstance(action, dict):
                    continue
                action_type = str(action.get("type") or "").strip().lower()
                config = action.get("config", {})
                if not isinstance(config, dict):
                    config = {}
                if action_type == "assign":
                    _action_assign(
                        rule_name=rule_name,
                        document=document,
                        actor=actor,
                        workspace_id=workspace_id,
                        config=config,
                    )
                elif action_type == "send_template_email":
                    _action_send_template_email(
                        rule_name=rule_name,
                        document=document,
                        actor=actor,
                        workspace_id=workspace_id,
                        config=config,
                    )
                elif action_type == "webhook_post":
                    _action_webhook_post(
                        rule_name=rule_name,
                        document=document,
                        actor=actor,
                        workspace_id=workspace_id,
                        trigger_event=trigger_event,
                        config=config,
                    )
                elif action_type == "create_notification":
                    _action_create_notification(
                        rule_name=rule_name,
                        document=document,
                        actor=actor,
                        workspace_id=workspace_id,
                        trigger_event=trigger_event,
                        config=config,
                    )
                elif action_type == "transition":
                    _action_transition(
                        rule_name=rule_name,
                        document=document,
                        actor=actor,
                        workspace_id=workspace_id,
                        config=config,
                    )
        except Exception:
            logger.debug("Workflow execution failed (non-blocking)", exc_info=True)


def run_workflows_for_document(
    *,
    trigger_event: str,
    document_id: str,
    actor: str,
    workspace_id: Optional[str] = None,
) -> None:
    """Convenience wrapper for event points that only have document_id."""
    doc = get_document(document_id, workspace_id=workspace_id)
    if not doc:
        return
    resolved_workspace_id = workspace_id or doc.get("workspace_id")
    run_workflows(
        trigger_event=trigger_event,
        document=doc,
        actor=actor,
        workspace_id=str(resolved_workspace_id) if resolved_workspace_id else None,
    )
