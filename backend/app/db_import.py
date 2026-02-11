from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse


class ExternalDatabaseError(ValueError):
    """Raised when an external database import request is invalid."""


_DISALLOWED_QUERY_KEYWORDS = (
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "replace",
    "truncate",
    "attach",
    "detach",
    "pragma",
    "vacuum",
)

_POSTGRES_SCHEMES = {"postgres", "postgresql", "postgresql+psycopg2"}
_MYSQL_SCHEMES = {"mysql", "mysql+pymysql"}


def _strip_sql_comments(query: str) -> str:
    without_block_comments = re.sub(r"/\*.*?\*/", " ", query, flags=re.DOTALL)
    without_line_comments = re.sub(
        r"--.*?$", " ", without_block_comments, flags=re.MULTILINE
    )
    return without_line_comments


def validate_readonly_query(query: str) -> str:
    candidate = str(query or "").strip()
    if not candidate:
        raise ExternalDatabaseError("Query is required.")

    normalized = _strip_sql_comments(candidate).strip().lower()
    if not normalized:
        raise ExternalDatabaseError("Query is empty after removing SQL comments.")

    if ";" in normalized.rstrip(";"):
        raise ExternalDatabaseError("Only one SELECT statement is allowed.")

    if not (normalized.startswith("select") or normalized.startswith("with")):
        raise ExternalDatabaseError(
            "Only SELECT queries are allowed for database imports."
        )

    for keyword in _DISALLOWED_QUERY_KEYWORDS:
        if re.search(rf"\b{keyword}\b", normalized):
            raise ExternalDatabaseError(
                f"Query contains a disallowed keyword: '{keyword}'."
            )

    return candidate


def _normalize_sqlite_target(database_url: str) -> str:
    raw = str(database_url or "").strip()
    if not raw:
        raise ExternalDatabaseError("Database URL is required.")

    lowered = raw.lower()
    if lowered.startswith("sqlite:///"):
        target = raw[len("sqlite:///") :]
    elif lowered.startswith("sqlite://"):
        target = raw[len("sqlite://") :]
    elif "://" in raw:
        raise ExternalDatabaseError(
            "Only SQLite database URLs are currently supported."
        )
    else:
        target = raw

    target = unquote(target).strip()
    if not target:
        raise ExternalDatabaseError("SQLite target path cannot be empty.")

    if target.startswith("file:"):
        return target

    if target == ":memory:":
        raise ExternalDatabaseError(
            "In-memory SQLite databases are not supported for imports."
        )

    resolved = Path(target).expanduser().resolve()
    if not resolved.exists():
        raise ExternalDatabaseError(f"SQLite file does not exist: {resolved}")

    return str(resolved)


def _connect_sqlite(database_url: str) -> sqlite3.Connection:
    target = _normalize_sqlite_target(database_url)
    uri_mode = target.startswith("file:")

    try:
        connection = sqlite3.connect(target, check_same_thread=False, uri=uri_mode)
    except sqlite3.Error as exc:
        raise ExternalDatabaseError(f"Could not connect to SQLite database: {exc}")

    connection.row_factory = sqlite3.Row
    return connection


def _connect_postgres(database_url: str) -> Any:
    try:
        import psycopg2
    except Exception:
        raise ExternalDatabaseError(
            "PostgreSQL driver is not installed. Install dependencies from backend/requirements.txt."
        )

    try:
        connection = psycopg2.connect(database_url, connect_timeout=8)
    except Exception as exc:
        raise ExternalDatabaseError(f"Could not connect to PostgreSQL database: {exc}")

    connection.autocommit = True
    return connection


def _connect_mysql(database_url: str) -> Any:
    try:
        import pymysql
    except Exception:
        raise ExternalDatabaseError(
            "MySQL driver is not installed. Install dependencies from backend/requirements.txt."
        )

    parsed = urlparse(database_url)
    if not parsed.hostname or not parsed.path.strip("/"):
        raise ExternalDatabaseError("MySQL URL must include host and database name.")

    database_name = parsed.path.strip("/")
    username = unquote(parsed.username) if parsed.username else None
    password = unquote(parsed.password) if parsed.password else None

    try:
        connection = pymysql.connect(
            host=parsed.hostname,
            port=parsed.port or 3306,
            user=username,
            password=password,
            database=database_name,
            connect_timeout=8,
            autocommit=True,
            charset="utf8mb4",
        )
    except Exception as exc:
        raise ExternalDatabaseError(f"Could not connect to MySQL database: {exc}")

    return connection


def connect_external_database(database_url: str) -> Any:
    raw = str(database_url or "").strip()
    if not raw:
        raise ExternalDatabaseError("Database URL is required.")

    parsed = urlparse(raw)
    scheme = parsed.scheme.lower()

    if not scheme:
        return _connect_sqlite(raw)

    if scheme in _POSTGRES_SCHEMES:
        return _connect_postgres(raw)

    if scheme in _MYSQL_SCHEMES:
        return _connect_mysql(raw)

    if scheme == "sqlite":
        return _connect_sqlite(raw)

    raise ExternalDatabaseError(
        "Unsupported database URL scheme. Supported: sqlite, postgresql, mysql."
    )


def fetch_import_rows(
    *, connection: Any, query: str, limit: int
) -> list[dict[str, Any]]:
    safe_query = validate_readonly_query(query)
    row_limit = max(1, int(limit))
    cursor = None

    try:
        module_name = connection.__class__.__module__.lower()
        if module_name.startswith("psycopg2"):
            from psycopg2.extras import RealDictCursor

            cursor = connection.cursor(cursor_factory=RealDictCursor)
        else:
            cursor = connection.cursor()

        cursor.execute(safe_query)
        rows = cursor.fetchmany(row_limit)
        column_names = [column[0] for column in (cursor.description or [])]
    except Exception as exc:
        raise ExternalDatabaseError(f"Failed to execute import query: {exc}")
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass

    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, sqlite3.Row):
            normalized_rows.append(dict(row))
            continue

        if isinstance(row, dict):
            normalized_rows.append(dict(row))
            continue

        if isinstance(row, (tuple, list)):
            normalized_rows.append(
                {name: value for name, value in zip(column_names, row)}
            )
            continue

        raise ExternalDatabaseError(
            f"Unsupported row type from database driver: {type(row).__name__}"
        )

    return normalized_rows


def get_row_value(row: dict[str, Any], column_name: str) -> Any:
    keys = list(row.keys())
    if column_name in keys:
        return row[column_name]

    lower_map = {key.lower(): key for key in keys}
    matched = lower_map.get(column_name.lower())
    if matched is None:
        raise KeyError(column_name)
    return row[matched]


def coerce_row_content_to_bytes(value: Any, *, encoding: str = "utf-8") -> bytes:
    if value is None:
        raise ValueError("Content value is empty.")

    if isinstance(value, bytes):
        return value

    if isinstance(value, bytearray):
        return bytes(value)

    if isinstance(value, memoryview):
        return value.tobytes()

    if isinstance(value, str):
        return value.encode(encoding)

    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False).encode(encoding)

    if isinstance(value, (int, float, bool)):
        return str(value).encode(encoding)

    raise ValueError(f"Unsupported content type: {type(value).__name__}")
