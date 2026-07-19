#!/usr/bin/env bash
# Computer B: register and run the worker.  No master directory is mounted or
# read; assets arrive through the authenticated master HTTP bridge.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
MASTER_URL="${MASTER_URL:?set MASTER_URL, e.g. http://velox-master.test:8180}"
GRPC_URL="${GRPC_URL:?set GRPC_URL, e.g. velox-master.test:51851}"
WORKER_ID="${WORKER_ID:-worker-pc-b-01}"
WORKER_NAME="${WORKER_NAME:-physical-pc-b}"
WORKER_BIN="${VELOX_WORKER_BIN:-/opt/velox/bin/velox-worker-agent}"
ENGINE_BIN="${VELOX_ENGINE_BIN:-/opt/velox/bin/velox_video_engine}"
CERT_DIR="${VELOX_WORKER_CERT_DIR:-/opt/velox/certs/worker}"
STATE_DIR="${VELOX_STATE_DIR:-/var/lib/velox/worker/state}"
OUTPUT_DIR="${VELOX_OUTPUT_DIR:-/var/lib/velox/worker/output}"
TEMP_DIR="${VELOX_TEMP_DIR:-/var/lib/velox/worker/tmp}"
CONFIG="${VELOX_WORKER_CONFIG:-/opt/velox/worker.json}"
LOG="${VELOX_WORKER_LOG:-/var/log/velox/worker.log}"
BUNDLE_HASH="${VELOX_BUNDLE_HASH:?set VELOX_BUNDLE_HASH from write-local-bundle-identity.sh}"
VERSION="$(tr -d '[:space:]' < "$ROOT/VERSION.txt")"

[[ -x "$WORKER_BIN" && -x "$ENGINE_BIN" ]] || { echo "worker/engine binary missing" >&2; exit 3; }
WORKER_CERT="${VELOX_WORKER_CERT_FILE:-$CERT_DIR/$WORKER_ID.crt}"
WORKER_KEY="${VELOX_WORKER_KEY_FILE:-$CERT_DIR/$WORKER_ID.key}"
[[ -f "$WORKER_CERT" && -f "$WORKER_KEY" && -f "$CERT_DIR/ca.crt" ]] || { echo "worker PKI missing" >&2; exit 3; }
mkdir -p "$STATE_DIR" "$OUTPUT_DIR" "$TEMP_DIR" "$(dirname "$CONFIG")" "$(dirname "$LOG")"

REGISTER="$(curl -fsS -X POST -H 'Content-Type: application/json' --data "{\"worker_id\":\"$WORKER_ID\",\"worker_name\":\"$WORKER_NAME\",\"version\":\"$VERSION\",\"bundle_hash\":\"$BUNDLE_HASH\",\"protocol_version\":\"v3\"}" "$MASTER_URL/api/v1/workers/register")"
TOKEN="$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("session_id",""))' <<<"$REGISTER")"
[[ -n "$TOKEN" ]] || { echo "registration returned no session_id" >&2; exit 1; }
cat > "$CONFIG" <<JSON
{
  "master_url":"$MASTER_URL", "control_grpc_url":"$GRPC_URL",
  "worker_id":"$WORKER_ID", "worker_name":"$WORKER_NAME", "work_dir":"/opt/velox",
  "state_dir":"$STATE_DIR", "output_dir":"$OUTPUT_DIR", "temp_dir":"$TEMP_DIR",
  "log_level":"info", "job_delivery":"push",
  "tls_cert_file":"$WORKER_CERT", "tls_key_file":"$WORKER_KEY", "tls_ca_file":"$CERT_DIR/ca.crt",
  "video_engine_cpp_bin":"$ENGINE_BIN", "bundle_hash":"$BUNDLE_HASH", "max_active_jobs":1,
  "health_port":8181, "prometheus_port":9090, "protocol_version":"v3"
}
JSON
export VELOX_WORKER_TOKEN="$TOKEN" VELOX_BUNDLE_HASH="$BUNDLE_HASH" VELOX_VIDEO_ENGINE_CPP_BIN="$ENGINE_BIN" VELOX_STATE_DIR="$STATE_DIR"
setsid nohup "$WORKER_BIN" -config "$CONFIG" </dev/null >"$LOG" 2>&1 &
echo $! > "${VELOX_WORKER_PIDFILE:-/tmp/velox-worker.pid}"
echo "[two-host-worker][OK] worker $WORKER_ID started; engine=$ENGINE_BIN"
