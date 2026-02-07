from __future__ import annotations

import json
import subprocess
import urllib.error
import urllib.request
from typing import Any, Optional

from .config import (
    DEPLOY_COMMAND,
    DEPLOY_COMMAND_TIMEOUT_SECONDS,
    DEPLOY_PROVIDER,
    GITHUB_OWNER,
    GITHUB_REF,
    GITHUB_REPO,
    GITHUB_TOKEN,
    GITHUB_WORKFLOW_ID,
    RENDER_API_TOKEN,
    RENDER_DEPLOY_HOOK_URL,
    RENDER_SERVICE_ID,
)


class DeploymentTriggerError(RuntimeError):
    pass


def deployment_provider_health() -> dict[str, Any]:
    provider = DEPLOY_PROVIDER
    if provider == "local":
        if not DEPLOY_COMMAND:
            return {
                "provider": "local",
                "status": "not_configured",
                "configured": False,
                "details": "Set CITYSORT_DEPLOY_COMMAND to execute real deploys.",
            }
        return {
            "provider": "local",
            "status": "ok",
            "configured": True,
            "details": "Local deployment command configured.",
        }

    if provider == "render":
        configured = bool(RENDER_DEPLOY_HOOK_URL or (RENDER_API_TOKEN and RENDER_SERVICE_ID))
        return {
            "provider": "render",
            "status": "ok" if configured else "not_configured",
            "configured": configured,
            "details": (
                "Render deployment hook/API is configured."
                if configured
                else "Set CITYSORT_RENDER_DEPLOY_HOOK_URL or CITYSORT_RENDER_API_TOKEN + CITYSORT_RENDER_SERVICE_ID."
            ),
        }

    if provider == "github":
        configured = bool(GITHUB_TOKEN and GITHUB_OWNER and GITHUB_REPO and GITHUB_WORKFLOW_ID and GITHUB_REF)
        return {
            "provider": "github",
            "status": "ok" if configured else "not_configured",
            "configured": configured,
            "details": (
                "GitHub Actions deployment configuration is complete."
                if configured
                else "Set CITYSORT_GITHUB_TOKEN/OWNER/REPO/WORKFLOW_ID/REF."
            ),
        }

    return {
        "provider": provider,
        "status": "unsupported",
        "configured": False,
        "details": f"Unsupported deploy provider: {provider}",
    }


def trigger_manual_deployment(*, environment: str, actor: str, notes: Optional[str] = None) -> dict[str, Any]:
    provider = DEPLOY_PROVIDER

    if provider == "local":
        return _trigger_local(environment=environment, actor=actor, notes=notes)
    if provider == "render":
        return _trigger_render(environment=environment, actor=actor, notes=notes)
    if provider == "github":
        return _trigger_github(environment=environment, actor=actor, notes=notes)

    raise DeploymentTriggerError(f"Unsupported deploy provider: {provider}")


def _trigger_local(*, environment: str, actor: str, notes: Optional[str]) -> dict[str, Any]:
    if not DEPLOY_COMMAND:
        return {
            "provider": "local",
            "status": "completed",
            "details": "No CITYSORT_DEPLOY_COMMAND set; recorded as no-op local deployment.",
            "external_id": None,
        }

    command = DEPLOY_COMMAND.replace("{environment}", environment).replace("{actor}", actor)
    if notes:
        command = command.replace("{notes}", notes)

    completed = subprocess.run(
        ["sh", "-lc", command],
        capture_output=True,
        text=True,
        timeout=DEPLOY_COMMAND_TIMEOUT_SECONDS,
        check=False,
    )

    output = (completed.stdout or "").strip()
    if completed.stderr:
        output = f"{output}\n{completed.stderr.strip()}".strip()

    status = "completed" if completed.returncode == 0 else "failed"
    return {
        "provider": "local",
        "status": status,
        "details": output or f"Local deploy command exited with code {completed.returncode}.",
        "external_id": None,
    }


def _trigger_render(*, environment: str, actor: str, notes: Optional[str]) -> dict[str, Any]:
    if RENDER_DEPLOY_HOOK_URL:
        request = urllib.request.Request(url=RENDER_DEPLOY_HOOK_URL, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                status_code = response.getcode()
                body = response.read().decode("utf-8", errors="ignore")
        except (urllib.error.HTTPError, urllib.error.URLError) as exc:
            raise DeploymentTriggerError(f"Render hook request failed: {exc}")

        if status_code >= 300:
            raise DeploymentTriggerError(f"Render hook responded with status {status_code}.")

        return {
            "provider": "render",
            "status": "completed",
            "details": body[:500] or "Render deploy hook triggered successfully.",
            "external_id": None,
        }

    if not (RENDER_API_TOKEN and RENDER_SERVICE_ID):
        raise DeploymentTriggerError(
            "Render provider requires CITYSORT_RENDER_DEPLOY_HOOK_URL or CITYSORT_RENDER_API_TOKEN + CITYSORT_RENDER_SERVICE_ID."
        )

    payload = {"clearCache": "do_not_clear"}
    request = urllib.request.Request(
        url=f"https://api.render.com/v1/services/{RENDER_SERVICE_ID}/deploys",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {RENDER_API_TOKEN}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw_body = response.read().decode("utf-8", errors="ignore")
            status_code = response.getcode()
    except (urllib.error.HTTPError, urllib.error.URLError) as exc:
        raise DeploymentTriggerError(f"Render API request failed: {exc}")

    if status_code >= 300:
        raise DeploymentTriggerError(f"Render API responded with status {status_code}.")

    external_id = None
    details = "Render deployment triggered."
    if raw_body.strip():
        try:
            parsed = json.loads(raw_body)
            external_id = parsed.get("id")
            details = parsed.get("status") or details
        except json.JSONDecodeError:
            details = raw_body[:500]

    return {
        "provider": "render",
        "status": "completed",
        "details": details,
        "external_id": external_id,
    }


def _trigger_github(*, environment: str, actor: str, notes: Optional[str]) -> dict[str, Any]:
    if not (GITHUB_TOKEN and GITHUB_OWNER and GITHUB_REPO and GITHUB_WORKFLOW_ID and GITHUB_REF):
        raise DeploymentTriggerError(
            "GitHub provider requires CITYSORT_GITHUB_TOKEN/OWNER/REPO/WORKFLOW_ID/REF."
        )

    payload = {
        "ref": GITHUB_REF,
        "inputs": {
            "environment": environment,
            "actor": actor,
            "notes": notes or "",
        },
    }

    request = urllib.request.Request(
        url=f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/actions/workflows/{GITHUB_WORKFLOW_ID}/dispatches",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status_code = response.getcode()
            body = response.read().decode("utf-8", errors="ignore")
    except (urllib.error.HTTPError, urllib.error.URLError) as exc:
        raise DeploymentTriggerError(f"GitHub workflow dispatch failed: {exc}")

    if status_code not in {200, 201, 202, 204}:
        raise DeploymentTriggerError(f"GitHub API responded with status {status_code}.")

    return {
        "provider": "github",
        "status": "completed",
        "details": body[:500] or "GitHub workflow dispatch submitted.",
        "external_id": None,
    }
