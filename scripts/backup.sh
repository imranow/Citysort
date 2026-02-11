#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATA_DIR="${CITYSORT_DATA_DIR:-$ROOT_DIR/data}"
BACKUP_DIR="${CITYSORT_BACKUP_DIR:-$DATA_DIR/backups}"
DB_URL="${CITYSORT_DATABASE_URL:-sqlite:///$DATA_DIR/citysort.db}"
RETENTION_DAYS="${CITYSORT_BACKUP_RETENTION_DAYS:-14}"

mkdir -p "$BACKUP_DIR"
ts="$(date -u +%Y%m%dT%H%M%SZ)"

echo "[backup] root=$ROOT_DIR"
echo "[backup] backup_dir=$BACKUP_DIR"

if [[ "$DB_URL" == postgres* || "$DB_URL" == postgresql* ]]; then
  db_file="$BACKUP_DIR/citysort_db_${ts}.dump"
  echo "[backup] dumping postgres -> $db_file"
  pg_dump --format=custom --no-owner --no-privileges "$DB_URL" >"$db_file"
else
  sqlite_path="${DB_URL#sqlite:///}"
  if [[ ! -f "$sqlite_path" ]]; then
    echo "[backup] sqlite file not found: $sqlite_path" >&2
    exit 1
  fi
  db_file="$BACKUP_DIR/citysort_db_${ts}.sqlite3"
  echo "[backup] copying sqlite -> $db_file"
  cp "$sqlite_path" "$db_file"
fi

uploads_dir="$DATA_DIR/uploads"
if [[ -d "$uploads_dir" ]]; then
  uploads_archive="$BACKUP_DIR/citysort_uploads_${ts}.tar.gz"
  echo "[backup] archiving uploads -> $uploads_archive"
  tar -C "$DATA_DIR" -czf "$uploads_archive" uploads
fi

echo "[backup] pruning files older than $RETENTION_DAYS day(s)"
find "$BACKUP_DIR" -type f -mtime +"$RETENTION_DAYS" -delete

echo "[backup] completed at $ts"
