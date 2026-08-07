#!/usr/bin/env bash
set -uo pipefail

REAL_SCRIPT="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$REAL_SCRIPT")" && pwd)"
PERF_MATRIX="${SCRIPT_DIR}/perf_matrix.sh"
INPUT="${SCRIPT_DIR}/workers/benchmark.json"

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

run_mock() {
  local mode="$1" port_file="$2" records="$3"
  python3 - "$port_file" "$records" "$mode" <<'PY' >/dev/null 2>&1 &
import json
import pathlib
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

port_file, records, mode = sys.argv[1:]
class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if mode == "ok" and (self.path != "/api/v1/performance/benchmarks/runs" or self.headers.get("Authorization") != "Bearer test-admin"):
            self.send_response(401)
            self.end_headers()
            return
        if mode == "fail":
            self.send_response(500)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length))
        with open(records, "a", encoding="utf-8") as output:
            output.write(json.dumps(payload, separators=(",", ":")) + "\n")
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"status":"recorded"}')

    def log_message(self, *_args):
        pass

server = HTTPServer(("127.0.0.1", 0), Handler)
pathlib.Path(port_file).write_text(str(server.server_port), encoding="utf-8")
server.serve_forever()
PY
  MOCK_PID=$!
}

TMP_DIR=$(mktemp -d)
SERVER_PIDS=()
MOCK_PID=""
cleanup() {
  for pid in "${SERVER_PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

bash -n "$PERF_MATRIX" || fail "perf_matrix.sh has invalid shell syntax"

port_file="$TMP_DIR/ok.port"
records="$TMP_DIR/ok.records"
: > "$records"
run_mock ok "$port_file" "$records"
SERVER_PIDS+=("$MOCK_PID")
for _ in $(seq 1 50); do [[ -s "$port_file" ]] && break; sleep 0.1; done
[[ -s "$port_file" ]] || fail "success mock did not start"
port=$(cat "$port_file")

VELOX_MASTER_URL="http://127.0.0.1:${port}" \
VELOX_ADMIN_TOKEN=test-admin \
REGISTER_BENCHMARK_RUNS=true \
bash "$PERF_MATRIX" --input "$INPUT" --cache-mode both --csv-out "$TMP_DIR/out.csv" >/dev/null 2>"$TMP_DIR/success.log" \
  || fail "both-mode registration unexpectedly failed"

python3 - "$records" <<'PY'
import json
import sys
rows = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8")]
assert len(rows) == 24, len(rows)
assert sum(row["cache_mode"] == "cold_cache" for row in rows) == 12
assert sum(row["cache_mode"] == "warm_cache" for row in rows) == 12
assert all(row["benchmark_case_id"] == "gervais-final-v1" for row in rows)
assert len({row["run_id"] for row in rows}) == 24
assert all(row["job_id"] and row["task_id"] and row["attempt_id"] for row in rows)
PY

fail_port_file="$TMP_DIR/fail.port"
fail_records="$TMP_DIR/fail.records"
: > "$fail_records"
run_mock fail "$fail_port_file" "$fail_records"
SERVER_PIDS+=("$MOCK_PID")
for _ in $(seq 1 50); do [[ -s "$fail_port_file" ]] && break; sleep 0.1; done
[[ -s "$fail_port_file" ]] || fail "failure mock did not start"
fail_port=$(cat "$fail_port_file")

if VELOX_MASTER_URL="http://127.0.0.1:${fail_port}" \
   VELOX_ADMIN_TOKEN=test-admin \
   REGISTER_BENCHMARK_RUNS=true \
   bash "$PERF_MATRIX" --input "$INPUT" --cache-mode cold_cache --csv-out "$TMP_DIR/fail.csv" >/dev/null 2>"$TMP_DIR/failure.log"; then
  fail "HTTP 500 registration unexpectedly returned success"
fi

printf 'perf_matrix benchmark registration test passed: both modes and HTTP failure propagation\n'
