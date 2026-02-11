#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path
from typing import Iterable

import psycopg2
from psycopg2.extras import execute_batch


TABLES_IN_ORDER = [
    "documents",
    "audit_events",
    "deployments",
    "invitations",
    "api_keys",
    "users",
    "jobs",
    "notifications",
    "watched_files",
    "templates",
    "outbound_emails",
    "connector_configs",
    "connector_sync_log",
]

SERIAL_ID_TABLES = {
    "audit_events",
    "deployments",
    "invitations",
    "api_keys",
    "notifications",
    "watched_files",
    "templates",
    "outbound_emails",
    "connector_configs",
    "connector_sync_log",
}


def _chunks(rows: Iterable[tuple], size: int = 500) -> Iterable[list[tuple]]:
    batch: list[tuple] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def migrate(sqlite_path: Path, postgres_url: str, truncate: bool = True) -> None:
    if not sqlite_path.exists():
        raise FileNotFoundError(f"SQLite DB not found: {sqlite_path}")
    if not postgres_url.strip():
        raise ValueError("PostgreSQL URL is required.")

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = psycopg2.connect(postgres_url)
    pg_conn.autocommit = False

    try:
        with pg_conn.cursor() as cur:
            if truncate:
                cur.execute("SET session_replication_role = replica")
                for table in reversed(TABLES_IN_ORDER):
                    cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
                cur.execute("SET session_replication_role = DEFAULT")

        for table in TABLES_IN_ORDER:
            col_rows = sqlite_conn.execute(f"PRAGMA table_info({table})").fetchall()
            columns = [str(row["name"]) for row in col_rows]
            if not columns:
                print(f"[skip] {table}: no columns found")
                continue

            select_sql = f"SELECT {', '.join(columns)} FROM {table}"
            source_rows = sqlite_conn.execute(select_sql)
            placeholders = ", ".join(["%s"] * len(columns))
            insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

            row_count = 0
            with pg_conn.cursor() as cur:
                for batch in _chunks((tuple(row[col] for col in columns) for row in source_rows), size=500):
                    execute_batch(cur, insert_sql, batch, page_size=500)
                    row_count += len(batch)
            print(f"[ok] {table}: migrated {row_count} row(s)")

        with pg_conn.cursor() as cur:
            for table in sorted(SERIAL_ID_TABLES):
                cur.execute(
                    f"""
                    SELECT setval(
                        pg_get_serial_sequence(%s, 'id'),
                        COALESCE((SELECT MAX(id) FROM {table}), 1),
                        (SELECT COUNT(*) > 0 FROM {table})
                    )
                    """,
                    (table,),
                )
        pg_conn.commit()
        print("[done] migration completed")
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        sqlite_conn.close()
        pg_conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate CitySort data from SQLite to PostgreSQL.")
    parser.add_argument(
        "--sqlite-path",
        default=os.getenv("CITYSORT_SQLITE_PATH", "data/citysort.db"),
        help="Path to SQLite database file.",
    )
    parser.add_argument(
        "--postgres-url",
        default=os.getenv("CITYSORT_POSTGRES_URL", ""),
        help="PostgreSQL connection URL.",
    )
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Append into target tables without truncating existing data.",
    )
    args = parser.parse_args()

    migrate(
        sqlite_path=Path(args.sqlite_path).expanduser().resolve(),
        postgres_url=args.postgres_url,
        truncate=not args.no_truncate,
    )


if __name__ == "__main__":
    main()
