#!/usr/bin/env bash
# Sourced by scripts/pilot.sh; definitions only.
# shellcheck disable=SC1090,SC2015,SC2164

assert_port_free() {
  local port="$1"
  if ss -ltn "sport = :${port}" | grep -q LISTEN; then
    die "required pilot port ${port} is occupied; set PILOT_*_PORT or stop its owner" 3
  fi
}

init_database() {
  [[ ! -e "${DATA_DIR}/velox.db" ]] ||
    die "refusing to reuse existing database ${DATA_DIR}/velox.db; choose a new PILOT_DIR or remove it explicitly" 3
  mkdir -p "$DATA_DIR"
  log "  → applying canonical SQLite migrations"
  (cd "$REPO_ROOT/DataServer" && go run ./cmd/seed-velox-db-fixture "${DATA_DIR}/velox.db")
  sqlite3 "${DATA_DIR}/velox.db" \
    "INSERT INTO delivery_destinations (destination_id, provider, name, enabled, configuration_json, created_at, updated_at) VALUES ('${DESTINATION_ID}', 'google_drive', 'Local E2E', 1, '{}', datetime('now'), datetime('now'));"
}

cmd_start() {
  banner "START: master"

  assert_port_free "$MASTER_PORT"
  assert_port_free "$GRPC_PORT"
  init_database

  # Build if binaries don't exist
  if [[ ! -x "$MASTER_BIN" ]]; then
    warn "master binary not found — building first"
    cmd_build
  fi

  # Ensure clean state
  mkdir -p "$LOGDIR" "$DATA_DIR" "$STAGING_DIR" "$STORAGE_DIR"
  rm -f "$MASTER_LOG" "$MASTER_PIDFILE"

  # Write master env file (dev bypasses are auto-set at script top)
  cat > "$MASTER_ENV" <<ENV
VELOX_MASTER_PORT=${MASTER_PORT}
VELOX_GRPC_PORT=${GRPC_PORT}
VELOX_DB_PATH=${DATA_DIR}/velox.db
VELOX_DATA_DIR=${DATA_DIR}
VELOX_STAGING_DIR=${STAGING_DIR}
VELOX_STORAGE_DIR=${STORAGE_DIR}
VELOX_ADMIN_TOKEN=${ADMIN_TOKEN}
VELOX_ALLOWED_WORKERS=${WORKER_ID}
VELOX_CODE_VERSION=${VERSION}
GIN_MODE=release
ENV

  # Source the env file so the master inherits VELOX_MASTER_PORT, VELOX_DB_PATH, etc.
  set -a; source "$MASTER_ENV"; set +a

  # Dev bypasses are scoped to this function so they do NOT leak into the
  # calling shell on `./scripts/pilot.sh status` / `./scripts/pilot.sh stop`.
  export VELOX_GRPC_ALLOW_INSECURE_DEV=true
  export VELOX_ASSET_REWRITE_DEV_BYPASS=true

  cd "$PILOT_DIR"
  setsid "$MASTER_BIN" serve </dev/null >"$MASTER_LOG" 2>&1 &
  local MPID=$!
  echo "$MPID" > "$MASTER_PIDFILE"
  disown "$MPID" 2>/dev/null
  log "master PID=${MPID}"

  # Wait for healthy
  for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
    if curl -fsS -o /dev/null "http://127.0.0.1:${MASTER_PORT}/health" 2>/dev/null; then
      kill -0 "$MPID" 2>/dev/null || die "health answered after master PID exited" 1
      ss -ltnp | grep -q "pid=${MPID}," || die "port ${MASTER_PORT} is not owned by master PID ${MPID}" 1
      grep -q "Velox master listening on :${MASTER_PORT}" "$MASTER_LOG" || die "master listener identity missing" 1
      ok "master healthy (${i}s)"
      return 0
    fi
    sleep 1
  done

  tail -40 "$MASTER_LOG" 2>/dev/null || true
  die "master did not become healthy within 15s" 1
}

cmd_work() {
  banner "WORK: start worker"

  if [[ ! -x "$WORKER_BIN" ]]; then
    warn "worker binary not found — building first"
    cmd_build
  fi

  mkdir -p "$LOGDIR"
  rm -f "$WORKER_LOG" "$WORKER_PIDFILE"

  # Write worker config (dev bypass: allow_insecure_grpc_dev: true)
  local BUNDLE_HASH
  BUNDLE_HASH="$("${REPO_ROOT}/scripts/e2e/write-local-bundle-identity.sh" "$PILOT_DIR" "$WORKER_BIN" "$ENGINE_BIN")"
  mkdir -p "${PILOT_DIR}/tests/fixtures"
  cp "${REPO_ROOT}/RemoteCodex/native/worker-agent-go/tests/fixtures/engine_selftest_baseline.sha256" \
    "${PILOT_DIR}/tests/fixtures/engine_selftest_baseline.sha256"
  cat > "$WORKER_CONFIG" <<JSON
{
  "master_url": "http://127.0.0.1:${MASTER_PORT}",
  "admin_token": "${ADMIN_TOKEN}",
  "worker_id": "${WORKER_ID}",
  "work_dir": "${PILOT_DIR}",
  "control_grpc_url": "127.0.0.1:${GRPC_PORT}",
  "job_delivery": "push",
  "allow_insecure_grpc_dev": true,
  "bundle_hash": "${BUNDLE_HASH}",
  "video_engine_cpp_bin": "${ENGINE_BIN}",
  "output_dir": "${PILOT_DIR}/runtime-output",
  "temp_dir": "${PILOT_DIR}/runtime-temp",
  "data_dir": "${PILOT_DIR}",
  "state_dir": "${PILOT_DIR}/state",
  "max_active_jobs": 1,
  "health_port": ${WORKER_HEALTH_PORT},
  "protocol_version": "v3"
}
JSON

  # Worker has its OWN separate env var (VELOX_ALLOW_INSECURE_GRPC_DEV) that
  # transport_factory.go enforces — it's NOT the same var as the master's
  # VELOX_GRPC_ALLOW_INSECURE_DEV. Must pass it explicitly. Scoped to this
  # function so it does not leak into the calling shell on other subcommands.
  cd "$PILOT_DIR"
  local WORKER_TOKEN
  WORKER_TOKEN="$(curl -fsS -m 10 -X POST \
    -H "Content-Type: application/json" \
    --data "{\"worker_id\":\"${WORKER_ID}\",\"worker_name\":\"pilot-worker\",\"protocol_version\":\"v3\",\"bundle_hash\":\"${BUNDLE_HASH}\"}" \
    "http://127.0.0.1:${MASTER_PORT}/api/v1/agent/register" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["session_id"])')" \
    || die "worker HTTP registration/token bootstrap failed" 1
  [[ -n "$WORKER_TOKEN" ]] || die "worker HTTP registration returned an empty token" 1
  setsid env \
    VELOX_ENV=dev \
    VELOX_ALLOW_INSECURE_GRPC_DEV=true \
    WORKER_TOKEN="$WORKER_TOKEN" \
    "$WORKER_BIN" -config "$WORKER_CONFIG" \
    </dev/null >"$WORKER_LOG" 2>&1 &
  local WPID=$!
  echo "$WPID" > "$WORKER_PIDFILE"
  disown "$WPID" 2>/dev/null
  log "worker PID=${WPID}"

  # Wait for registration signal in master log
  for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
    if grep -qE "${WORKER_ID}.*(hello_ack|HelloAck)|Worker ${WORKER_ID} connected" "$MASTER_LOG" 2>/dev/null \
      || grep -q "Registration successful" "$WORKER_LOG" 2>/dev/null; then
      ok "worker registered (${i}s)"
      return 0
    fi
    if ! kill -0 "$WPID" 2>/dev/null; then
      warn "worker process died — dumping worker log"
      tail -60 "$WORKER_LOG" 2>/dev/null || true
      die "worker crashed during registration" 1
    fi
    sleep 2
  done

  tail -40 "$MASTER_LOG" 2>/dev/null || true
  tail -40 "$WORKER_LOG" 2>/dev/null || true
  die "worker did not register within 30s" 126
}

cmd_status() {
  banner "STATUS"

  # Master
  if [[ -f "$MASTER_PIDFILE" ]]; then
    local MPID
    MPID="$(cat "$MASTER_PIDFILE")"
    if ps -p "$MPID" >/dev/null 2>&1; then
      ok "master running (PID=${MPID})"
      # Health check
      curl -fsS -m 3 -o /dev/null "http://127.0.0.1:${MASTER_PORT}/health" 2>/dev/null && \
        ok "master health: OK" || warn "master health: FAIL"
    else
      warn "master PID=${MPID} NOT running (stale PID file)"
    fi
  else
    warn "master NOT running (no PID file)"
  fi

  # Worker
  if [[ -f "$WORKER_PIDFILE" ]]; then
    local WPID
    WPID="$(cat "$WORKER_PIDFILE")"
    if ps -p "$WPID" >/dev/null 2>&1; then
      ok "worker running (PID=${WPID})"
    else
      warn "worker PID=${WPID} NOT running (stale PID file)"
    fi
  else
    warn "worker NOT running (no PID file)"
  fi

  # Jobs
  if [[ -f "${DATA_DIR}/velox.db" ]]; then
    banner "JOBS in DB"
    sqlite3 "${DATA_DIR}/velox.db" \
      "SELECT job_id, status, video_name, updated_at FROM jobs ORDER BY updated_at DESC LIMIT 5;" \
      2>/dev/null || true
  fi

  # Log tails
  banner "MASTER LOG (tail 10)"
  tail -10 "$MASTER_LOG" 2>/dev/null || true
  banner "WORKER LOG (tail 10)"
  tail -10 "$WORKER_LOG" 2>/dev/null || true
}

cmd_stop() {
  banner "STOP"

  # Worker first (de-register cleanly)
  if [[ -f "$WORKER_PIDFILE" ]]; then
    local WPID
    WPID="$(cat "$WORKER_PIDFILE")"
    kill -- -"$WPID" 2>/dev/null && log "worker process-group TERM sent to PGID=${WPID}" || true
    sleep 2
    kill -- -"$WPID" 2>/dev/null && log "worker process-group KILL sent" || true
    rm -f "$WORKER_PIDFILE"
  fi

  # Master
  if [[ -f "$MASTER_PIDFILE" ]]; then
    local MPID
    MPID="$(cat "$MASTER_PIDFILE")"
    kill -- -"$MPID" 2>/dev/null && log "master process-group TERM sent to PGID=${MPID}" || true
    sleep 2
    kill -- -"$MPID" 2>/dev/null && log "master process-group KILL sent" || true
    rm -f "$MASTER_PIDFILE"
  fi

  ok "processes stopped"
}

cmd_log() {
  if [[ ! -f "$MASTER_LOG" ]]; then
    die "master log not found at ${MASTER_LOG} — start master first" 1
  fi
  tail -n 200 -F "$MASTER_LOG"
}
