# shellcheck shell=bash
# master.sh — Phase 3: start master server, seed DB, wait for health.

phase_master_start() {
  info "Phase 3: starting master"
  mkdir -p "$DATA_DIR" "$STORAGE_DIR" "$LOG_DIR"

  assert_port_free "$MASTER_PORT"
  assert_port_free "$GRPC_PORT"
  [[ ! -e "$DATA_DIR/velox.db" ]] || {
    fail "refusing to reuse existing database $DATA_DIR/velox.db; choose a fresh E2E_WORKDIR"
    exit 3
  }
  (cd "$REPO_ROOT/DataServer" && go run ./cmd/seed-velox-db-fixture "$DATA_DIR/velox.db") >/dev/null
  sqlite3 "$DATA_DIR/velox.db" \
    "INSERT INTO delivery_destinations (destination_id, provider, name, enabled, configuration_json, created_at, updated_at) VALUES ('$DESTINATION_ID', 'google_drive', 'Local E2E', 1, '{}', datetime('now'), datetime('now'));"

  # Control-plane endpoints required by validateBootstrapEndpoints (REST public + gRPC control).
  cat > "$MASTER_ENV" <<ENV
VELOX_MASTER_PORT=$MASTER_PORT
VELOX_GRPC_PORT=$GRPC_PORT
VELOX_CONTROL_PLANE_REST_PUBLIC_URL=http://127.0.0.1:${MASTER_PORT}
VELOX_CONTROL_PLANE_GRPC_URL=127.0.0.1:${GRPC_PORT}
VELOX_DB_PATH=$DATA_DIR/velox.db
VELOX_DATA_DIR=$DATA_DIR
VELOX_STAGING_DIR=$STAGING_DIR
VELOX_STORAGE_DIR=$STORAGE_DIR
VELOX_ADMIN_TOKEN=$ADMIN_TOKEN
# Required by the typed artifact publication protocol (artifact.commit.v1):
# without VELOX_COMMIT_HMAC_KEY the completion coordinator is disabled at
# bootstrap, TaskOutputDeclared is rejected, and the worker waits forever
# for an ArtifactUploadPlan (ARTIFACT_UPLOAD_PLAN_WAIT_FAILED after ~4min).
# 64 hex chars = 32 raw bytes (validator minimum). Same value golden-e2e.sh
# pins, so the workload run exercises the identical wire path.
VELOX_COMMIT_HMAC_KEY=00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff
VELOX_ALLOWED_WORKERS=$WORKER_ID
VELOX_CODE_VERSION=$VERSION
VELOX_GRPC_ALLOW_INSECURE_DEV=true
VELOX_ASSET_REWRITE_DEV_BYPASS=true
# Fail-closed Level-D smoke (2026-08-10 capability audit): production wiring
# requires a real Drive service + asset resolver, neither of which exists on
# this dev sandbox. Development mode wires the documented local fakes
# (LocalShellWorker / LocalFileDriveUploader / StubAssetResolver) so the
# master boots without mTLS certs or production asset wiring.
VELOX_ENVIRONMENT=development
VELOX_SMOKE_MODE=development
GIN_MODE=release
ENV

  set -a
  # shellcheck disable=SC1090
  source "$MASTER_ENV"
  set +a
  rm -f "$MASTER_LOG"

  setsid "$MASTER_BIN" serve >"$MASTER_LOG" 2>&1 &
  local pid=$!
  echo "$pid" > "$MASTER_PIDFILE"
  push_pid "$pid"
  info "master PID=$pid"

  # Wait for healthy
  for i in $(seq 1 20); do
    if curl -fsS -o /dev/null "http://127.0.0.1:${MASTER_PORT}/health" 2>/dev/null; then
      kill -0 "$pid" 2>/dev/null || { fail "health answered after master PID exited"; exit 1; }
      ss -ltnp | grep -q "pid=${pid}," || { fail "health port is not owned by master PID=$pid"; exit 1; }
      grep -q "Velox master listening on :${MASTER_PORT}" "$MASTER_LOG" || { fail "master listener identity missing"; exit 1; }
      pass "master healthy after ${i}s"
      return 0
    fi
    sleep 1
  done
  fail "master did not become healthy"
  tail -40 "$MASTER_LOG" 2>/dev/null || true
  exit 1
}
