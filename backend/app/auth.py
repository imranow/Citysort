from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import HTTPException, Request

from .config import ACCESS_TOKEN_TTL_MINUTES, AUTH_SECRET, REQUIRE_AUTH
from .repository import (
    count_users,
    create_workspace,
    create_user,
    get_api_key_by_hash,
    get_default_workspace_for_user,
    get_user_by_email,
    get_user_by_id,
    get_workspace_role as repository_get_workspace_role,
    list_users,
    update_user_login,
    update_user_role,
)

ROLE_ORDER = {"viewer": 1, "operator": 2, "admin": 3}
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PASSWORD_MIN_LENGTH = 10


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _sign(value: str) -> str:
    digest = hmac.new(
        AUTH_SECRET.encode("utf-8"), value.encode("utf-8"), hashlib.sha256
    ).digest()
    return _b64url_encode(digest)


def _hash_api_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def hash_password(password: str) -> str:
    if len(password) < PASSWORD_MIN_LENGTH:
        raise ValueError(f"Password must be at least {PASSWORD_MIN_LENGTH} characters.")
    salt = hashlib.sha256(f"{_now().timestamp()}:{password}".encode("utf-8")).digest()[
        :16
    ]
    iterations = 240000
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return (
        f"pbkdf2_sha256${iterations}${_b64url_encode(salt)}${_b64url_encode(derived)}"
    )


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algorithm, raw_iterations, raw_salt, raw_hash = stored_hash.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        iterations = int(raw_iterations)
        salt = _b64url_decode(raw_salt)
        expected = _b64url_decode(raw_hash)
    except Exception:
        return False

    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(derived, expected)


def create_access_token(
    *,
    user_id: str,
    role: str,
    workspace_id: Optional[str] = None,
    ttl_minutes: int = ACCESS_TOKEN_TTL_MINUTES,
) -> str:
    payload = {
        "sub": user_id,
        "role": role,
        "exp": int((_now() + timedelta(minutes=ttl_minutes)).timestamp()),
    }
    if workspace_id:
        payload["wid"] = workspace_id
    raw_payload = _b64url_encode(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    signature = _sign(raw_payload)
    return f"{raw_payload}.{signature}"


def decode_access_token(token: str) -> dict[str, Any]:
    try:
        raw_payload, raw_sig = token.split(".", 1)
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid token format.")

    expected_sig = _sign(raw_payload)
    if not hmac.compare_digest(raw_sig, expected_sig):
        raise HTTPException(status_code=401, detail="Invalid token signature.")

    try:
        payload = json.loads(_b64url_decode(raw_payload).decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token payload.")

    exp = int(payload.get("exp", 0))
    if exp <= int(_now().timestamp()):
        raise HTTPException(status_code=401, detail="Token has expired.")
    return payload


def _normalize_role(role: str) -> str:
    normalized = (role or "").strip().lower()
    if normalized not in ROLE_ORDER:
        raise ValueError("Role must be one of: viewer, operator, admin.")
    return normalized


def _validate_email(email: str) -> str:
    normalized = (email or "").strip().lower()
    if not EMAIL_RE.match(normalized):
        raise ValueError("A valid email address is required.")
    return normalized


def bootstrap_admin(
    *, email: str, password: str, full_name: Optional[str] = None
) -> dict[str, Any]:
    if count_users() > 0:
        raise ValueError("Bootstrap is only allowed when no users exist.")

    normalized_email = _validate_email(email)
    password_hash = hash_password(password)
    user = create_user(
        email=normalized_email,
        full_name=(full_name or "").strip() or None,
        password_hash=password_hash,
        role="admin",
    )
    workspace_name = (full_name or "Admin").strip() or "Admin"
    workspace = create_workspace(
        name=f"{workspace_name}'s Workspace",
        owner_id=user["id"],
        plan_tier=user.get("plan_tier", "free"),
    )
    token = create_access_token(
        user_id=user["id"], role=user["role"], workspace_id=workspace["id"]
    )
    user["workspace_id"] = workspace["id"]
    return {"user": user, "access_token": token}


def create_user_account(
    *, email: str, password: str, role: str, full_name: Optional[str] = None
) -> dict[str, Any]:
    normalized_email = _validate_email(email)
    normalized_role = _normalize_role(role)
    if get_user_by_email(normalized_email):
        raise ValueError("User with this email already exists.")

    user = create_user(
        email=normalized_email,
        full_name=(full_name or "").strip() or None,
        password_hash=hash_password(password),
        role=normalized_role,
    )
    workspace_label = (
        full_name or normalized_email.split("@")[0]
    ).strip() or "Personal"
    workspace = create_workspace(
        name=f"{workspace_label}'s Workspace",
        owner_id=user["id"],
        plan_tier=user.get("plan_tier", "free"),
    )
    user["workspace_id"] = workspace["id"]
    return user


def authenticate_user(*, email: str, password: str) -> dict[str, Any]:
    normalized_email = _validate_email(email)
    record = get_user_by_email(normalized_email, include_password_hash=True)
    if not record or record.get("status") != "active":
        raise HTTPException(status_code=401, detail="Invalid credentials.")

    if not verify_password(password, record.get("password_hash", "")):
        raise HTTPException(status_code=401, detail="Invalid credentials.")

    update_user_login(record["id"])
    user = get_user_by_id(record["id"])
    if not user:
        raise HTTPException(status_code=401, detail="User not found.")

    default_workspace = get_default_workspace_for_user(user["id"])
    workspace_id = default_workspace["id"] if default_workspace else None
    token = create_access_token(
        user_id=user["id"], role=user["role"], workspace_id=workspace_id
    )
    if workspace_id:
        user["workspace_id"] = workspace_id
    return {"user": user, "access_token": token}


def role_allows(user_role: str, required_role: str) -> bool:
    current = ROLE_ORDER.get((user_role or "").lower(), 0)
    required = ROLE_ORDER.get((required_role or "").lower(), 0)
    return current >= required


def _authenticate_from_api_key(raw_key: str) -> dict[str, Any]:
    key_hash = _hash_api_key(raw_key)
    key_record = get_api_key_by_hash(key_hash)
    if not key_record or key_record.get("status") != "active":
        raise HTTPException(status_code=401, detail="Invalid API key.")
    return {
        "auth_type": "api_key",
        "actor": key_record.get("name") or "api_key",
        "role": "operator",
    }


def _authenticate_from_token(raw_token: str) -> dict[str, Any]:
    payload = decode_access_token(raw_token)
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token subject.")
    user = get_user_by_id(str(user_id))
    if not user or user.get("status") != "active":
        raise HTTPException(status_code=401, detail="User no longer active.")
    workspace_id = payload.get("wid")
    if not workspace_id:
        default_workspace = get_default_workspace_for_user(str(user_id))
        workspace_id = default_workspace.get("id") if default_workspace else None
    workspace_role = None
    if workspace_id:
        workspace_role = repository_get_workspace_role(str(user_id), str(workspace_id))
        if workspace_role is None:
            raise HTTPException(status_code=401, detail="Workspace access denied.")
    return {
        "auth_type": "user",
        "actor": user.get("email", "user"),
        "role": user.get("role", "viewer"),
        "user": user,
        "workspace_id": workspace_id,
        "workspace_role": workspace_role or "member",
    }


def authorize_request(
    request: Optional[Request],
    *,
    required_role: str = "viewer",
    allow_api_key: bool = True,
) -> dict[str, Any]:
    normalized_required_role = _normalize_role(required_role)

    if not REQUIRE_AUTH:
        return {
            "auth_type": "disabled",
            "actor": "system",
            "role": "admin",
        }

    if request is None:
        raise HTTPException(status_code=401, detail="Authentication required.")

    auth_header = request.headers.get("authorization", "").strip()
    api_key_header = request.headers.get("x-api-key", "").strip()

    identity: Optional[dict[str, Any]] = None
    if auth_header.lower().startswith("bearer "):
        token = auth_header.split(" ", 1)[1].strip()
        if allow_api_key and token.startswith("cs_"):
            identity = _authenticate_from_api_key(token)
        else:
            identity = _authenticate_from_token(token)
    elif allow_api_key and api_key_header:
        identity = _authenticate_from_api_key(api_key_header)

    if not identity:
        raise HTTPException(
            status_code=401, detail="Missing or invalid authentication credentials."
        )

    if not role_allows(identity.get("role", "viewer"), normalized_required_role):
        raise HTTPException(status_code=403, detail="Insufficient role permissions.")

    return identity


def get_users(limit: int = 200) -> list[dict[str, Any]]:
    return list_users(limit=limit)


def set_user_role(*, user_id: str, role: str) -> dict[str, Any]:
    normalized_role = _normalize_role(role)
    updated = update_user_role(user_id, role=normalized_role)
    if not updated:
        raise ValueError("User not found.")
    return updated


def get_workspace_role(user_id: str, workspace_id: str) -> Optional[str]:
    return repository_get_workspace_role(user_id, workspace_id)
