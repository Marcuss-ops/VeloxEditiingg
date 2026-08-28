# shellcheck shell=bash
# worker.sh — Phase 5: start worker agent, register, wait for connection.

phase_worker_start() {
  info "Phase 5: starting worker"

  # Ensure worker runtime directories exist before the agent starts.
  # The C++ engine and the Go agent write scratch data here and can fail
  # fast if the directories are missing.
  mkdir -p "$WORKDIR/runtime-temp" "$WORKDIR/runtime-output" "$WORKDIR/state"

  local bundle_hash
  bundle_hash="$(scripts/e2e/write-local-bundle-identity.sh "$WORKDIR" "$WORKER_BIN" "$ENGINE_BIN")"

  cat > "$WORKER_CFG" <<JSON
{
  "master_url": "http://127.0.0.1:${MASTER_PORT}",
  "worker_id": "${WORKER_ID}",
  "work_dir": "${WORKDIR}",
  "control_grpc_url": "127.0.0.1:${GRPC_PORT}",
  "job_delivery": "push",
  "environment": "dev",
  "allow_insecure_grpc_dev": true,
  "bundle_hash": "${bundle_hash}",
  "video_engine_cpp_bin": "${ENGINE_BIN}",
  "output_dir": "${WORKDIR}/runtime-output",
  "temp_dir": "${WORKDIR}/runtime-temp",
  "data_dir": "${WORKDIR}",
  "state_dir": "${WORKDIR}/state",
  "max_active_jobs": 1,
  "health_port": ${WORKER_HEALTH_PORT},
  "prometheus_port": ${WORKER_PROMETHEUS_PORT},
  "protocol_version": "v3"
}
JSON

  mkdir -p "$WORKDIR/tests/fixtures"
  cp "$REPO_ROOT/RemoteCodex/native/worker-agent-go/tests/fixtures/engine_selftest_baseline.sha256" \
    "$WORKDIR/tests/fixtures/engine_selftest_baseline.sha256"

  local worker_token
  worker_token="$(curl -fsS -m 10 -X POST \
    -H "Content-Type: application/json" \
    --data "{\"worker_id\":\"${WORKER_ID}\",\"worker_name\":\"e2e-worker\",\"protocol_version\":\"v3\",\"bundle_hash\":\"${bundle_hash}\"}" \
    "http://127.0.0.1:${MASTER_PORT}/api/v1/agent/register" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["session_id"])')" \
    || { fail "worker HTTP registration/token bootstrap failed"; exit 1; }
  [[ -n "$worker_token" ]] || { fail "worker HTTP registration returned an empty token"; exit 1; }

  rm -f "$WORKER_LOG"
  # Isolate the C++ engine's temp directory so that a stale /tmp path
  # owned by another user (e.g. /tmp/velox_video_engine_plan) cannot
  # make the self-render bootstrap fail.
  setsid env TMPDIR="$WORKDIR/runtime-temp" VELOX_ENV=dev VELOX_ALLOW_INSECURE_GRPC_DEV=true WORKER_TOKEN="$worker_token" "$WORKER_BIN" --config "$WORKER_CFG" \
    >"$WORKER_LOG" 2>&1 &
  local pid=$!
  echo "$pid" > "$WORKER_PIDFILE"
  push_pid "$pid"
  info "worker PID=$pid"

  wait_for_worker_registration "$pid"
}
