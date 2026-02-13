"""Curated workflow automation presets (government + business starter packs)."""

from __future__ import annotations

from typing import Any, Optional

from .repository import create_workflow_rule, list_workflow_rules
from .templates import create_template, list_templates


def _preset_catalog() -> list[dict[str, Any]]:
    """Return the full preset catalog.

    Notes:
    - Presets are intentionally conservative and meant as starting points.
    - Most presets assume doc_type keys like "foia_request" or "invoice" exist in your routing rules.
      City teams should adapt doc_type/department filters to match their environment.
    """

    return [
        {
            "id": "gov-intake-triage",
            "name": "Government Intake Triage",
            "category": "government",
            "description": (
                "Auto-assign anything that lands in needs_review and broadcast a notification so a clerk can triage."
            ),
            "templates": [],
            "rules": [
                {
                    "name": "Triage: Auto-assign needs_review",
                    "enabled": True,
                    "trigger_event": "document_needs_review",
                    "filters": {},
                    "actions": [
                        {
                            "type": "assign",
                            "config": {
                                "assignee": "workspace_owner",
                                "only_if_unassigned": True,
                            },
                        }
                    ],
                },
                {
                    "name": "Triage: Broadcast needs_review alert",
                    "enabled": True,
                    "trigger_event": "document_needs_review",
                    "filters": {},
                    "actions": [
                        {
                            "type": "create_notification",
                            "config": {
                                "type": "triage",
                                "title": "Needs review: {{filename}}",
                                "message": "New document requires review. Status: {{status}}. Doc type: {{doc_type}}. Dept: {{department}}.",
                            },
                        }
                    ],
                },
            ],
        },
        {
            "id": "gov-foia",
            "name": "FOIA / Public Records Requests",
            "category": "government",
            "description": (
                "Assign FOIA requests on ingestion, notify on overdue SLA, and email the requester when approved."
            ),
            "templates": [
                {
                    "name": "FOIA Status Update",
                    "doc_type": "foia_request",
                    "template_body": (
                        "Hello {{applicant_name}},\n\n"
                        "We have an update on your public records request.\n\n"
                        "Document: {{filename}}\n"
                        "Current status: {{status}}\n"
                        "Reference: {{id}}\n\n"
                        "Thank you,\n"
                        "City Records Office\n"
                    ),
                }
            ],
            "rules": [
                {
                    "name": "FOIA: Auto-assign on ingestion",
                    "enabled": True,
                    "trigger_event": "document_ingested",
                    "filters": {"doc_type": ["foia_request", "foi_request"]},
                    "actions": [
                        {"type": "assign", "config": {"assignee": "workspace_owner"}}
                    ],
                },
                {
                    "name": "FOIA: Notify when overdue",
                    "enabled": True,
                    "trigger_event": "document_overdue",
                    "filters": {"doc_type": ["foia_request", "foi_request"]},
                    "actions": [
                        {
                            "type": "create_notification",
                            "config": {
                                "type": "sla",
                                "title": "FOIA overdue: {{filename}}",
                                "message": "FOIA request is overdue. Due: {{due_date}}. Assigned: {{assigned_to}}.",
                            },
                        }
                    ],
                },
                {
                    "name": "FOIA: Email requester on approval",
                    "enabled": True,
                    "trigger_event": "document_approved",
                    "filters": {"doc_type": ["foia_request", "foi_request"]},
                    "actions": [
                        {
                            "type": "send_template_email",
                            "config": {"template_name_hint": "FOIA Status Update"},
                        }
                    ],
                },
            ],
        },
        {
            "id": "gov-permits",
            "name": "Permits & Licensing",
            "category": "government",
            "description": (
                "Assign permit applications for review and email applicants when approved."
            ),
            "templates": [
                {
                    "name": "Permit Decision Notice",
                    "doc_type": "permit_application",
                    "template_body": (
                        "Hello {{applicant_name}},\n\n"
                        "Your permit application has been updated.\n\n"
                        "Document: {{filename}}\n"
                        "Status: {{status}}\n"
                        "Reference: {{id}}\n\n"
                        "Thank you,\n"
                        "Permitting Office\n"
                    ),
                }
            ],
            "rules": [
                {
                    "name": "Permits: Auto-assign on needs_review",
                    "enabled": True,
                    "trigger_event": "document_needs_review",
                    "filters": {"doc_type": ["permit_application", "business_license"]},
                    "actions": [
                        {"type": "assign", "config": {"assignee": "workspace_owner"}}
                    ],
                },
                {
                    "name": "Permits: Email applicant on approval",
                    "enabled": True,
                    "trigger_event": "document_approved",
                    "filters": {"doc_type": ["permit_application", "business_license"]},
                    "actions": [
                        {
                            "type": "send_template_email",
                            "config": {"template_name_hint": "Permit Decision Notice"},
                        }
                    ],
                },
            ],
        },
        {
            "id": "biz-ap-invoices",
            "name": "Accounts Payable: Invoices",
            "category": "business",
            "description": (
                "Auto-assign invoices that need review and broadcast a notification on high-confidence processed invoices."
            ),
            "templates": [],
            "rules": [
                {
                    "name": "AP: Auto-assign invoices needing review",
                    "enabled": True,
                    "trigger_event": "document_needs_review",
                    "filters": {"doc_type": ["invoice", "purchase_order"]},
                    "actions": [
                        {
                            "type": "assign",
                            "config": {
                                "assignee": "workspace_owner",
                                "only_if_unassigned": True,
                            },
                        }
                    ],
                },
                {
                    "name": "AP: Notify on high-confidence processed invoices",
                    "enabled": True,
                    "trigger_event": "document_processed",
                    "filters": {"doc_type": ["invoice"], "min_confidence": 0.92},
                    "actions": [
                        {
                            "type": "create_notification",
                            "config": {
                                "type": "ap",
                                "title": "Invoice ready: {{filename}}",
                                "message": "Invoice processed at confidence {{confidence}}. Review fields and approve if ready.",
                            },
                        }
                    ],
                },
            ],
        },
        {
            "id": "biz-contracts",
            "name": "Contract Review",
            "category": "business",
            "description": (
                "Assign contracts on ingestion and broadcast an overdue notification for SLA enforcement."
            ),
            "templates": [],
            "rules": [
                {
                    "name": "Contracts: Auto-assign on ingestion",
                    "enabled": True,
                    "trigger_event": "document_ingested",
                    "filters": {"doc_type": ["contract"]},
                    "actions": [
                        {"type": "assign", "config": {"assignee": "workspace_owner"}}
                    ],
                },
                {
                    "name": "Contracts: Notify when overdue",
                    "enabled": True,
                    "trigger_event": "document_overdue",
                    "filters": {"doc_type": ["contract"]},
                    "actions": [
                        {
                            "type": "create_notification",
                            "config": {
                                "type": "sla",
                                "title": "Contract overdue: {{filename}}",
                                "message": "Contract is overdue. Due: {{due_date}}. Assigned: {{assigned_to}}.",
                            },
                        }
                    ],
                },
            ],
        },
    ]


def list_workflow_presets() -> list[dict[str, Any]]:
    presets = _preset_catalog()
    items: list[dict[str, Any]] = []
    for preset in presets:
        items.append(
            {
                "id": preset["id"],
                "name": preset["name"],
                "category": preset.get("category", "general"),
                "description": preset.get("description", ""),
                "rules_count": len(preset.get("rules") or []),
                "templates_count": len(preset.get("templates") or []),
            }
        )
    return items


def get_workflow_preset(preset_id: str) -> Optional[dict[str, Any]]:
    normalized = str(preset_id or "").strip().lower()
    for preset in _preset_catalog():
        if str(preset.get("id") or "").strip().lower() == normalized:
            return preset
    return None


def apply_workflow_preset(
    *,
    preset_id: str,
    workspace_id: str,
    overwrite: bool = False,
) -> dict[str, Any]:
    preset = get_workflow_preset(preset_id)
    if not preset:
        raise ValueError("Workflow preset not found.")

    existing_templates = list_templates(workspace_id=workspace_id, limit=400)
    existing_templates_by_name = {
        str(t.get("name") or "").strip().lower(): t for t in existing_templates
    }

    created_templates: list[dict[str, Any]] = []
    skipped_templates = 0
    for template in preset.get("templates") or []:
        name = str(template.get("name") or "").strip()
        if not name:
            continue
        key = name.lower()
        if key in existing_templates_by_name and not overwrite:
            skipped_templates += 1
            continue
        if key in existing_templates_by_name and overwrite:
            # We intentionally do not overwrite existing templates; teams usually customize copy.
            skipped_templates += 1
            continue
        created = create_template(
            workspace_id=workspace_id,
            name=name,
            doc_type=template.get("doc_type"),
            template_body=str(template.get("template_body") or ""),
        )
        created_templates.append(created)
        existing_templates_by_name[key] = created

    existing_rules = list_workflow_rules(
        workspace_id=workspace_id, include_global=False, limit=400
    )
    existing_rule_keys = {
        (
            str(r.get("name") or "").strip().lower(),
            str(r.get("trigger_event") or "").strip().lower(),
        )
        for r in existing_rules
    }

    created_rules: list[dict[str, Any]] = []
    skipped_rules = 0
    for rule in preset.get("rules") or []:
        name = str(rule.get("name") or "").strip()
        trigger_event = str(rule.get("trigger_event") or "").strip()
        if not name or not trigger_event:
            continue
        key = (name.lower(), trigger_event.lower())
        if key in existing_rule_keys and not overwrite:
            skipped_rules += 1
            continue
        if key in existing_rule_keys and overwrite:
            # We intentionally do not overwrite rules automatically; edits are safer in the UI.
            skipped_rules += 1
            continue
        created = create_workflow_rule(
            workspace_id=workspace_id,
            name=name,
            enabled=bool(rule.get("enabled", True)),
            trigger_event=trigger_event,
            filters=rule.get("filters")
            if isinstance(rule.get("filters"), dict)
            else {},
            actions=rule.get("actions")
            if isinstance(rule.get("actions"), list)
            else [],
        )
        created_rules.append(created)
        existing_rule_keys.add(key)

    return {
        "preset_id": str(preset["id"]),
        "created_rules": created_rules,
        "created_templates": created_templates,
        "skipped_rules": skipped_rules,
        "skipped_templates": skipped_templates,
    }
