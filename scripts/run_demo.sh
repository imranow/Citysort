#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="$ROOT_DIR/.venv/bin/python"
UVICORN="$ROOT_DIR/.venv/bin/uvicorn"
HOST="127.0.0.1"
PORT="8000"
BASE_URL="http://$HOST:$PORT"
SERVER_LOG="$ROOT_DIR/data/demo_server.log"

if [[ ! -x "$UVICORN" ]]; then
  echo "Missing uvicorn in .venv. Run dependency install first."
  exit 1
fi

mkdir -p "$ROOT_DIR/data"

"$UVICORN" backend.app.main:app --host "$HOST" --port "$PORT" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

cleanup() {
  if kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

for _ in {1..30}; do
  if curl -sf "$BASE_URL/health" >/dev/null; then
    break
  fi
  sleep 1
 done

if ! curl -sf "$BASE_URL/health" >/dev/null; then
  echo "Server failed to start. See $SERVER_LOG"
  exit 1
fi

echo "Server started at $BASE_URL"

for file in "$ROOT_DIR"/assets/samples/*.txt; do
  response=$(curl -sS -X POST "$BASE_URL/api/documents/upload" \
    -F "file=@${file}" \
    -F "source_channel=upload_portal" \
    -F "process_async=false")

  "$VENV_PY" - <<'PY' "$response" "$file"
import json
import sys
payload = json.loads(sys.argv[1])
path = sys.argv[2]
print(f"uploaded={path.split('/')[-1]} id={payload['id']} type={payload.get('doc_type')} status={payload.get('status')} confidence={payload.get('confidence')}")
PY
 done

echo ""
echo "Analytics snapshot:"
curl -sS "$BASE_URL/api/analytics"
echo ""
echo ""
echo "Queue snapshot:"
curl -sS "$BASE_URL/api/queues"
echo ""
