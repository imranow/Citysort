#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: scripts/restore.sh <db_backup_file> [uploads_archive]" >&2
  exit 1
fi

DB_BACKUP_FILE="$1"
UPLOADS_ARCHIVE="${2:-}"

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATA_DIR="${CITYSORT_DATA_DIR:-$ROOT_DIR/data}"
DB_URL="${CITYSORT_DATABASE_URL:-sqlite:///$DATA_DIR/citysort.db}"

if [[ ! -f "$DB_BACKUP_FILE" ]]; then
  echo "[restore] backup file not found: $DB_BACKUP_FILE" >&2
  exit 1
fi

echo "[restore] db_url=$DB_URL"

if [[ "$DB_URL" == postgres* || "$DB_URL" == postgresql* ]]; then
  echo "[restore] restoring postgres database from $DB_BACKUP_FILE"
  pg_restore --clean --if-exists --no-owner --no-privileges --dbname="$DB_URL" "$DB_BACKUP_FILE"
else
  sqlite_path="${DB_URL#sqlite:///}"
  mkdir -p "$(dirname "$sqlite_path")"
  echo "[restore] restoring sqlite database to $sqlite_path"
  cp "$DB_BACKUP_FILE" "$sqlite_path"
fi

if [[ -n "$UPLOADS_ARCHIVE" ]]; then
  if [[ ! -f "$UPLOADS_ARCHIVE" ]]; then
    echo "[restore] uploads archive not found: $UPLOADS_ARCHIVE" >&2
    exit 1
  fi
  echo "[restore] restoring uploads from $UPLOADS_ARCHIVE"
  mkdir -p "$DATA_DIR"
  tar -C "$DATA_DIR" -xzf "$UPLOADS_ARCHIVE"
fi

echo "[restore] completed"
