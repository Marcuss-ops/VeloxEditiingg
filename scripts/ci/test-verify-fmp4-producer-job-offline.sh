#!/usr/bin/env bash
# Offline tests for scripts/ops/verify-fmp4-producer-job.sh.
#
# A local Python mock Master stands in for the real one so the harness can be
# exercised end-to-end without touching production: it accepts the producer
# submit, serves job status transitions, exposes the admin live metrics that
# carry artifact.safe_offset_bytes / artifact.finalized, and serves an artifact
# body with (or without) a moof box.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HARNESS="$ROOT/scripts/ops/verify-fmp4-producer-job.sh"

fail() {
  printf 'verify-fmp4-producer-job-offline: FAIL: %s\n' "$*" >&2
  exit 1
}
pass() {
  printf 'verify-fmp4-producer-job-offline: %s\n' "$*"
}

command -v python3 >/dev/null 2>&1 || { echo "SKIP: python3 not available" >&2; exit 0; }
[[ -f "$HARNESS" ]] || fail "harness is missing: $HARNESS"

TMP="$(mktemp -d -t velox-fmp4-producer-offline.XXXXXX)"
SERVER_PID=""
cleanup() {
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
  rm -rf "$TMP"
}
trap cleanup EXIT

PLAN="$TMP/plan.json"
cat >"$PLAN" <<'JSON'
{"plan_version":2,"output":{"profile_id":"velox-h264-fmp4-stream-v1","container":"mp4","video_codec":"h264","width":1920,"height":1080,"fps_num":24,"fps_den":1,"pixel_format":"yuv420p"}}
JSON

PORTFILE="$TMP/port"
SUBMIT_FILE="$TMP/submitted.json"

cat >"$TMP/mock_master.py" <<'PY'
import http.server, json, os, sys

SCENARIO = os.environ.get("MOCK_SCENARIO", "happy")
ARTIFACT = os.environ.get("MOCK_ARTIFACT", "fmp4")
PORTFILE = os.environ["MOCK_PORTFILE"]
SUBMIT_FILE = os.environ["MOCK_SUBMIT_FILE"]
STATUS = os.environ.get("MOCK_JOB_STATUS", "")

state = {"polls": 0}


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _send(self, code, body=b"", ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        with open(SUBMIT_FILE, "wb") as fh:
            fh.write(self.rfile.read(length))
        self._send(202, json.dumps({"ok": True, "job_id": "job_test_1"}).encode())

    def do_GET(self):
        port = self.server.server_address[1]
        if self.path == "/api/v1/jobs/job_test_1":
            state["polls"] += 1
            if STATUS:
                status = STATUS
            else:
                status = "RUNNING" if state["polls"] <= 2 else "SUCCEEDED"
            body = {"ok": True, "job_id": "job_test_1", "status": status}
            if status == "SUCCEEDED":
                body["artifact_url"] = "http://127.0.0.1:%d/artifact.mp4" % port
                body["artifact_size_bytes"] = 4096
            self._send(200, json.dumps(body).encode())
        elif self.path == "/api/v1/admin/jobs/job_test_1":
            finalized = 0 if state["polls"] <= 2 else 1
            doc = {"job": {"job_id": "job_test_1", "attempts": [{
                "attempt_id": "attempt-1",
                "status": "RUNNING",
                "cumulative_metrics": {
                    "artifact.safe_offset_bytes": 16777216,
                    "artifact.high_watermark_bytes": 25165824,
                    "artifact.finalized": finalized,
                },
            }]}}
            self._send(200, json.dumps(doc).encode())
        elif self.path == "/artifact.mp4":
            payload = b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom"
            payload += b"moof" + b"\x00" * 16 if ARTIFACT == "fmp4" else b"moov" + b"\x00" * 16
            payload += b"mdat" + b"\x00" * 64
            self._send(200, payload, "video/mp4")
        else:
            self._send(404, b"{}")


if __name__ == "__main__":
    socketserver = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    with open(PORTFILE, "w") as fh:
        fh.write(str(socketserver.server_address[1]))
    socketserver.serve_forever()
PY

start_mock() {
  : >"$PORTFILE"
  : >"$SUBMIT_FILE"
  MOCK_PORTFILE="$PORTFILE" MOCK_SUBMIT_FILE="$SUBMIT_FILE" \
    MOCK_SCENARIO="${MOCK_SCENARIO:-happy}" MOCK_ARTIFACT="${MOCK_ARTIFACT:-fmp4}" \
    MOCK_JOB_STATUS="${MOCK_JOB_STATUS:-}" \
    python3 "$TMP/mock_master.py" >"$TMP/mock.log" 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 50); do
    [[ -s "$PORTFILE" ]] && return 0
    sleep 0.1
  done
  fail "mock master did not start: $(cat "$TMP/mock.log")"
}

stop_mock() {
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

checks=0
run_case() {
  # run_case <want_rc> <extra env assignments as "K=V K=V"> [harness args...]
  local want="$1" envs="$2"
  shift 2
  set +e
  out="$(env $envs \
    "VELOX_MASTER_URL=http://127.0.0.1:$(cat "$PORTFILE")" \
    "VELOX_M2M_SECRET=test-secret" \
    "VELOX_CLIENT_ID=computer-editor-test" \
    bash "$HARNESS" --plan "$PLAN" --interval 1 --wait 30 "$@" 2>&1)"
  rc=$?
  set -e
  [[ "$rc" == "$want" ]] || fail "exit code = $rc, want $want (args='$*'): $out"
  checks=$((checks + 1))
}

# ── 1. Happy path: fMP4 plan, live safe_offset before finalize, moof artifact.
start_mock
run_case 0 "VELOX_ADMIN_TOKEN=admin-test" --json
printf '%s' "$out" | grep -q 'Check B  OK' || fail "happy path did not observe check B: $out"
printf '%s' "$out" | grep -q 'Check C  OK' || fail "happy path did not observe check C: $out"
printf '%s' "$out" | grep -q '"verdict":"PASS"' || fail "happy path verdict: $out"
jq -e --rawfile plan "$PLAN" '.compiled_render_plan_json == $plan' "$SUBMIT_FILE" >/dev/null \
  || fail "producer plan bytes were not passed through verbatim: $(head -c 200 "$SUBMIT_FILE")"
jq -e '.compiled_render_plan_sha256 | test("^[0-9a-f]{64}$")' "$SUBMIT_FILE" >/dev/null \
  || fail "submit body is missing a sha256 of the plan"
jq -e '.delivery_plan[0].destination_id == "local-fallback"' "$SUBMIT_FILE" >/dev/null \
  || fail "submit body is missing the delivery plan"
pass 'happy path: profile + safe_offset-before-finalize + moof artifact'

# ── 2. Progressive artifact (no moof) fails check C.
stop_mock
MOCK_ARTIFACT=progressive start_mock
set +e
out="$(env "VELOX_MASTER_URL=http://127.0.0.1:$(cat "$PORTFILE")" VELOX_M2M_SECRET=test-secret \
  VELOX_CLIENT_ID=computer-editor-test VELOX_ADMIN_TOKEN=admin-test \
  bash "$HARNESS" --plan "$PLAN" --interval 1 --wait 30 2>&1)"
rc=$?
set -e
[[ "$rc" == 1 ]] || fail "progressive artifact exit code = $rc, want 1: $out"
printf '%s' "$out" | grep -q 'no moof box' || fail "progressive artifact message: $out"
checks=$((checks + 1))
pass 'progressive artifact (no moof): check C fails with exit 1'

# ── 3. Missing admin token: check B is not silently waived (exit 3).
stop_mock
start_mock
run_case 3 ""
printf '%s' "$out" | grep -q 'INCONCLUSIVE' || fail "missing-token case message: $out"
pass 'no admin token: check B INCONCLUSIVE / exit 3 (never silent)'

# ── 4. --allow-artifact-only downgrades the missing evidence to an explicit waiver.
run_case 0 "" --allow-artifact-only --json
printf '%s' "$out" | grep -q '"verdict":"PASS-WAIVED"' || fail "waiver verdict: $out"
pass '--allow-artifact-only: explicit waiver, exit 0'

# ── 5. Failed job fails the acceptance.
stop_mock
MOCK_JOB_STATUS=FAILED start_mock
run_case 1 ""
printf '%s' "$out" | grep -q 'ended status=FAILED' || fail "failed-job message: $out"
pass 'failed job: exit 1'

stop_mock
printf 'verify-fmp4-producer-job-offline: OK (%d checks green)\n' "$checks"
