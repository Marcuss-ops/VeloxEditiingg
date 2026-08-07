#!/usr/bin/env bash
# =============================================================================
# master-worker-lifecycle/run.sh — master/worker lifecycle E2E
# =============================================================================
# Runs the real velox-server and the real dev-hello-client against an isolated
# SQLite database. The client performs the typed Hello/HelloAck handshake and
# sends real gRPC heartbeats; it does not mock the control plane.
#
# Covered lifecycle:
#   1. clean master bootstrap + readiness
#   2. worker registration + fresh heartbeat
#   3. persisted admin operation + terminal ledger status
#   4. lost heartbeat (SIGSTOP) -> stale/disconnected read model
#   5. master restart against the SAME database
#   6. same WorkerID reconnect + fresh heartbeat
#   7. operation remains readable after master restart
#
# This is intentionally a local deterministic harness. It does not reboot the
# host, use sudo, modify firewall rules, or touch a production database.
# =============================================================================

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
WORKDIR="${E2E_WORKDIR:-/tmp/velox-e2e-master-worker-lifecycle}"
BIN_DIR="$WORKDIR/bin"
DATA_DIR="$WORKDIR/data"
LOG_DIR="$WORKDIR/logs"
DB="$DATA_DIR/velox.db"
MASTER_ENV="$WORKDIR/master.env"
MASTER_LOG="$LOG_DIR/master.log"
WORKER_LOG="$LOG_DIR/worker.log"
MASTER_PIDFILE="$WORKDIR/master.pid"
WORKER_PIDFILE="$WORKDIR/worker.pid"

MASTER_PORT="${E2E_MASTER_PORT:-}"
GRPC_PORT="${E2E_GRPC_PORT:-}"
MASTER_URL=""
ADMIN_TOKEN="${E2E_ADMIN_TOKEN:-e2e-master-worker-admin}"
WORKER_ID="${E2E_WORKER_ID:-e2e-master-worker-1}"
WORKER_NAME="${E2E_WORKER_NAME:-e2e-master-worker}"
WORKER_SECRET="${E2E_WORKER_SECRET:-e2e-lifecycle-secret}"
HEARTBEAT_INTERVAL="${E2E_HEARTBEAT_INTERVAL:-1s}"
STALE_WAIT_SECONDS="${E2E_STALE_WAIT_SECONDS:-180}"
HEARTBEAT_WINDOW="${E2E_HEARTBEAT_WINDOW:-30m}"
POLL_SECONDS="${E2E_POLL_SECONDS:-1}"
MASTER_READY_TIMEOUT="${E2E_MASTER_READY_TIMEOUT:-60}"
OPERATION_TIMEOUT="${E2E_OPERATION_TIMEOUT:-30}"
KEEP_WORKDIR="${E2E_KEEP_WORKDIR:-1}"

MASTER_BIN="${VELOX_SERVER_BIN:-$BIN_DIR/velox-server}"
HELLO_BIN="${VELOX_DEV_HELLO_BIN:-$BIN_DIR/dev-hello-client}"
SEED_BIN="${VELOX_SEED_BIN:-$BIN_DIR/seed-velox-db-fixture}"

CHILD_PIDS=()

info() { printf '[master-worker-e2e][INFO] %s\n' "$*"; }
pass() { printf '[master-worker-e2e][PASS] %s\n' "$*"; }
fail() { printf '[master-worker-e2e][FAIL] %s\n' "$*" >&2; exit 1; }

wait_pid_exit() {
  local pid="$1" timeout_s="${2:-10}"
  local deadline=$(( $(date +%s) + timeout_s ))
  while kill -0 "$pid" 2>/dev/null && (( $(date +%s) < deadline )); do
    sleep 1
  done
  wait "$pid" 2>/dev/null || true
  ! kill -0 "$pid" 2>/dev/null
}

forget_child_pid() {
  local pid="$1" kept=()
  for child in "${CHILD_PIDS[@]:-}"; do
    [[ "$child" == "$pid" ]] || kept+=("$child")
  done
  CHILD_PIDS=("${kept[@]:-}")
}

stop_child() {
  local pid="$1"
  [[ -n "$pid" ]] || return 0
  # Every suite child is started with setsid, so signal its process group.
  kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
  if ! wait_pid_exit "$pid" 10; then
    kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
    wait_pid_exit "$pid" 5 || true
  fi
  forget_child_pid "$pid"
}

cleanup() {
  set +e
  if [[ -f "$WORKER_PIDFILE" ]]; then
    worker_pid="$(cat "$WORKER_PIDFILE" 2>/dev/null || true)"
    [[ -z "$worker_pid" ]] || kill -CONT -- "-$worker_pid" 2>/dev/null || kill -CONT "$worker_pid" 2>/dev/null || true
  fi
  for pid in "${CHILD_PIDS[@]:-}"; do
    stop_child "$pid"
  done
  if [[ "$KEEP_WORKDIR" != "1" ]]; then
    rm -rf "$WORKDIR"
  else
    info "evidence preserved at $WORKDIR"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

pick_free_port() {
  python3 - <<'PY'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

wait_http() {
  local url="$1" timeout_s="$2"
  local deadline=$(( $(date +%s) + timeout_s ))
  while (( $(date +%s) < deadline )); do
    if curl -fsS --max-time 2 "$url" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  return 1
}

assert_master_fail_fast() {
  local bad_workdir="$WORKDIR/fail-fast"
  local bad_env="$bad_workdir/master.env"
  local bad_log="$bad_workdir/master.log"
  mkdir -p "$bad_workdir"
  cat >"$bad_env" <<ENV
GIN_MODE=release
VELOX_MASTER_PORT=$(pick_free_port)
VELOX_GRPC_PORT=$(pick_free_port)
VELOX_RUNTIME_DIR=$bad_workdir/runtime
VELOX_DATA_DIR=$bad_workdir/data
VELOX_DB_PATH=$bad_workdir/data/velox.db
VELOX_DB_DRIVER=postgres
VELOX_ADMIN_TOKEN=$ADMIN_TOKEN
VELOX_ALLOWED_WORKERS=$WORKER_ID
VELOX_GRPC_ALLOW_INSECURE_DEV=true
ENV
  info "probing invalid bootstrap dependency for fail-fast behavior"
  set +e
  (
    set -a
    # shellcheck disable=SC1090
    source "$bad_env"
    set +a
    timeout 15s "$MASTER_BIN" serve
  ) >"$bad_log" 2>&1
  local rc=$?
  set -e
  [[ "$rc" -ne 0 && "$rc" -ne 124 ]] || {
    tail -100 "$bad_log" >&2 || true
    fail "invalid database driver did not fail fast (rc=$rc)"
  }
  if grep -Eq 'Bootstrap complete|listening|health/ready' "$bad_log"; then
    tail -100 "$bad_log" >&2 || true
    fail "invalid bootstrap reached runtime readiness"
  fi
  pass "invalid bootstrap dependency failed before readiness (rc=$rc)"
}

admin_get() {
  curl -fsS --max-time 5 \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    "$MASTER_URL$1"
}

wait_worker_status() {
  local expected_re="$1" timeout_s="$2"
  local deadline=$(( $(date +%s) + timeout_s )) body status
  while (( $(date +%s) < deadline )); do
    body="$(admin_get "/api/v1/admin/workers/${WORKER_ID}" 2>/dev/null || true)"
    status="$(jq -r '.status // .connection_status // ""' <<<"$body" 2>/dev/null || true)"
    if [[ "$status" =~ $expected_re ]]; then
      printf '%s\n' "$body"
      return 0
    fi
    sleep "$POLL_SECONDS"
  done
  info "last worker response: ${body:-<none>}"
  return 1
}

wait_operation() {
  local operation_id="$1" timeout_s="$2"
  local deadline=$(( $(date +%s) + timeout_s )) body status
  while (( $(date +%s) < deadline )); do
    body="$(admin_get "/api/v1/admin/operations/${operation_id}" 2>/dev/null || true)"
    status="$(jq -r '.status // ""' <<<"$body" 2>/dev/null || true)"
    case "$status" in
      SUCCEEDED)
        printf '%s\n' "$body"
        return 0
        ;;
      FAILED|CANCELLED|ROLLED_BACK)
        info "terminal operation response: ${body:-<none>}"
        return 1
        ;;
    esac
    sleep "$POLL_SECONDS"
  done
  info "operation timeout response: ${body:-<none>}"
  return 1
}

build_binaries() {
  mkdir -p "$BIN_DIR"
  if [[ -n "${VELOX_SERVER_BIN:-}" ]]; then
    [[ -x "$MASTER_BIN" ]] || fail "VELOX_SERVER_BIN is not executable: $MASTER_BIN"
  else
    info "building velox-server from current source"
    (cd "$ROOT/DataServer" && go build -o "$MASTER_BIN" ./cmd/server)
  fi
  if [[ -n "${VELOX_DEV_HELLO_BIN:-}" ]]; then
    [[ -x "$HELLO_BIN" ]] || fail "VELOX_DEV_HELLO_BIN is not executable: $HELLO_BIN"
  else
    info "building dev-hello-client from current source"
    (cd "$ROOT/DataServer" && go build -o "$HELLO_BIN" ./cmd/dev-hello-client)
  fi
  if [[ -n "${VELOX_SEED_BIN:-}" ]]; then
    [[ -x "$SEED_BIN" ]] || fail "VELOX_SEED_BIN is not executable: $SEED_BIN"
  else
    info "building SQLite fixture seeder from current source"
    (cd "$ROOT/DataServer" && go build -o "$SEED_BIN" ./cmd/seed-velox-db-fixture)
  fi
}

write_master_env() {
  cat >"$MASTER_ENV" <<ENV
GIN_MODE=release
VELOX_MASTER_PORT=$MASTER_PORT
VELOX_GRPC_PORT=$GRPC_PORT
VELOX_GRPC_PUSH_MODE=true
VELOX_RUNTIME_DIR=$WORKDIR/runtime
VELOX_DATA_DIR=$DATA_DIR
VELOX_DB_PATH=$DB
VELOX_STAGING_DIR=$WORKDIR/staging
VELOX_STORAGE_DIR=$WORKDIR/storage
VELOX_ADMIN_TOKEN=$ADMIN_TOKEN
VELOX_ALLOWED_WORKERS=$WORKER_ID
VELOX_GRPC_ALLOW_INSECURE_DEV=true
VELOX_WORKER_HEARTBEAT_TIMEOUT=20
VELOX_CODE_VERSION=e2e-lifecycle
ENV
}

start_master() {
  : >"$MASTER_LOG"
  info "starting master on REST :$MASTER_PORT / gRPC :$GRPC_PORT"
  set -a
  # shellcheck disable=SC1090
  source "$MASTER_ENV"
  set +a
  setsid "$MASTER_BIN" serve >"$MASTER_LOG" 2>&1 &
  local pid=$!
  CHILD_PIDS+=("$pid")
  printf '%s\n' "$pid" >"$MASTER_PIDFILE"
  if ! wait_http "$MASTER_URL/health/ready" "$MASTER_READY_TIMEOUT"; then
    tail -100 "$MASTER_LOG" >&2 || true
    fail "master did not become ready"
  fi
  local ready
  ready="$(curl -fsS --max-time 5 "$MASTER_URL/health/ready")"
  jq -e '(.ready == true) or (.status == "ready") or (.ok == true)' <<<"$ready" >/dev/null 2>&1 || {
    info "readiness body: $ready"
    fail "master readiness response is not ready"
  }
  pass "master bootstrap/readiness verified (pid=$pid)"
}

register_worker() {
  local credential response registered_id
  credential="$(printf '%s:%s' "$WORKER_ID" "$WORKER_SECRET" | sha256sum | awk '{print $1}')"
  info "registering worker identity through POST /api/v1/agent/register"
  response="$(curl -fsS --max-time 5 -X POST \
    -H 'Content-Type: application/json' \
    --data "$(jq -nc \
      --arg worker_id "$WORKER_ID" \
      --arg credential "$credential" \
      --arg worker_name "$WORKER_NAME" \
      --arg hostname "e2e-master-worker" \
      --arg ip "127.0.0.1" \
      --arg protocol_version "v3" \
      '{worker_id:$worker_id,credential:$credential,worker_name:$worker_name,hostname:$hostname,ip:$ip,protocol_version:$protocol_version}')" \
    "$MASTER_URL/api/v1/agent/register")"
  registered_id="$(jq -er '.worker_id' <<<"$response")"
  [[ "$registered_id" == "$WORKER_ID" ]] || fail "HTTP registration returned unexpected worker_id: $registered_id"
  pass "worker identity registration persisted (worker_id=$registered_id)"
}

start_worker() {
  local expected_status="${1:-^CONNECTED$}" body
  : >"$WORKER_LOG"
  info "starting real dev-hello-client for $WORKER_ID"
  setsid "$HELLO_BIN" \
    --master "127.0.0.1:$GRPC_PORT" \
    --worker-id "$WORKER_ID" \
    --worker-name "$WORKER_NAME" \
    --worker-secret "$WORKER_SECRET" \
    --heartbeat-window "$HEARTBEAT_WINDOW" \
    --heartbeat-interval "$HEARTBEAT_INTERVAL" \
    >"$WORKER_LOG" 2>&1 &
  local pid=$!
  CHILD_PIDS+=("$pid")
  printf '%s\n' "$pid" >"$WORKER_PIDFILE"
  if ! body="$(wait_worker_status "$expected_status" 30)"; then
    tail -100 "$WORKER_LOG" >&2 || true
    tail -100 "$MASTER_LOG" >&2 || true
    fail "worker did not reach expected status ${expected_status}"
  fi
  [[ "$(jq -r '.session_active // false' <<<"$body")" == "true" ]] || {
    fail "worker reached ${expected_status} without an active persisted session"
  }
  pass "worker registration and fresh heartbeat verified (pid=$pid, status=$(jq -r '.status' <<<"$body"))"
}

verify_persisted_operation() {
  info "publishing a real admin drain operation"
  local response operation_id db_status api_status
  response="$(curl -fsS --max-time 5 -X POST \
    -H "Authorization: Bearer ${ADMIN_TOKEN}" \
    -H 'Content-Type: application/json' \
    --data '{"reason":"master-worker lifecycle persistence E2E"}' \
    "$MASTER_URL/api/v1/admin/workers/${WORKER_ID}/drain")"
  operation_id="$(jq -er '.operation_id' <<<"$response")"
  [[ -n "$operation_id" ]] || fail "drain response did not contain operation_id"
  [[ "$(jq -r '.worker_id // ""' <<<"$response")" == "$WORKER_ID" ]] || fail "drain response worker_id mismatch"
  [[ "$(jq -r '.op // ""' <<<"$response")" == "drain" ]] || fail "drain response op mismatch"
  [[ "$(jq -r '.status // ""' <<<"$response")" =~ ^(QUEUED|RUNNING|SUCCEEDED)$ ]] || fail "drain response has invalid initial status"
  if ! wait_operation "$operation_id" "$OPERATION_TIMEOUT" >/dev/null; then
    fail "operation $operation_id did not reach SUCCEEDED"
  fi
  api_status="$(admin_get "/api/v1/admin/operations/${operation_id}" | jq -r '.status')"
  db_status="$(sqlite3 -noheader "$DB" "SELECT status FROM fleet_operations WHERE operation_id='${operation_id}';")"
  [[ "$api_status" == "SUCCEEDED" && "$db_status" == "SUCCEEDED" ]] || {
    fail "operation status mismatch before restart: api=$api_status db=$db_status"
  }
  local drain_status
  drain_status="$(admin_get "/api/v1/admin/workers/${WORKER_ID}" | jq -r '.status // ""')"
  [[ "$drain_status" == "DRAINING" ]] || fail "drain operation completed without DRAINING read-model state: $drain_status"
  printf '%s\n' "$operation_id" >"$WORKDIR/operation.id"
  pass "operation persisted and reached SUCCEEDED (operation_id=$operation_id)"
}

stop_worker_for_partition() {
  local pid body status connection deadline
  pid="$(cat "$WORKER_PIDFILE")"
  info "freezing worker pid=$pid to stop heartbeats"
  kill -STOP -- "-$pid" 2>/dev/null || kill -STOP "$pid"
  deadline=$(( $(date +%s) + STALE_WAIT_SECONDS ))
  while (( $(date +%s) < deadline )); do
    body="$(admin_get "/api/v1/admin/workers/${WORKER_ID}" 2>/dev/null || true)"
    status="$(jq -r '.status // .connection_status // ""' <<<"$body" 2>/dev/null || true)"
    connection="$(jq -r '.connection_state // ""' <<<"$body" 2>/dev/null || true)"
    # DRAINING is intentionally higher precedence in the operator status
    # projection. The placement admission signal is the canonical connection
    # dimension, so assert that dimension directly while preserving the prior
    # drain operation.
    if [[ "$connection" == "STALE" || "$connection" == "OFFLINE" || "$status" == "STALE" || "$status" == "DISCONNECTED" ]]; then
      break
    fi
    sleep "$POLL_SECONDS"
  done
  [[ "$connection" == "STALE" || "$connection" == "OFFLINE" || "$status" == "STALE" || "$status" == "DISCONNECTED" ]] || {
    info "last worker response: ${body:-<none>}"
    fail "worker did not become stale/disconnected after heartbeat loss"
  }
  [[ "$connection" == "STALE" || "$connection" == "OFFLINE" ]] || fail "heartbeat-loss API response omitted canonical connection state"
  [[ "$(jq -r '.session_active // false' <<<"$body")" == "true" ]] || fail "heartbeat-loss worker unexpectedly lost persisted session before stale classification"
  [[ "$(jq -r '.last_heartbeat_at // ""' <<<"$body")" != "" ]] || fail "heartbeat-loss response omitted last_heartbeat_at"
  pass "lost heartbeat excluded worker from live API status (status=$status, connection=$connection)"
}

verify_worker_id_collision() {
  local collision_secret="e2e-collision-secret"
  local collision_credential original_credential collision_log collision_rc
  collision_credential="$(printf '%s:%s' "$WORKER_ID" "$collision_secret" | sha256sum | awk '{print $1}')"
  original_credential="$(printf '%s:%s' "$WORKER_ID" "$WORKER_SECRET" | sha256sum | awk '{print $1}')"
  collision_log="$WORKDIR/worker-collision.log"
  info "attempting a real second gRPC session with the same WorkerID and a different credential"

  # The gRPC stream validates the declared credential before its session
  # collision gate. Rotate only the isolated test DB credential so the second
  # client reaches InsertSession; a subshell EXIT trap restores it even when
  # the client probe fails.
  set +e
  (
    set -Eeuo pipefail
    restore_collision_credential() {
      sqlite3 "$DB" "UPDATE worker_credentials SET credential_hash='${original_credential}' WHERE worker_id='${WORKER_ID}';" >/dev/null 2>&1 || true
    }
    trap restore_collision_credential EXIT
    sqlite3 "$DB" "UPDATE worker_credentials SET credential_hash='${collision_credential}' WHERE worker_id='${WORKER_ID}';"
    : >"$collision_log"
    timeout 20s "$HELLO_BIN" \
      --master "127.0.0.1:$GRPC_PORT" \
      --worker-id "$WORKER_ID" \
      --worker-name "e2e-collision-worker" \
      --credential-hash "$collision_credential" \
      >"$collision_log" 2>&1
  )
  collision_rc=$?
  set -e

  [[ "$collision_rc" -ne 0 && "$collision_rc" -ne 124 ]] || {
    cat "$collision_log" >&2 || true
    fail "duplicate WorkerID gRPC session did not fail fast (rc=$collision_rc)"
  }
  grep -Eq 'AlreadyExists|already connected on a different credential|COLLISION' "$collision_log" || {
    cat "$collision_log" >&2 || true
    fail "duplicate WorkerID gRPC session did not report the typed collision error"
  }
  [[ "$(sqlite3 -noheader "$DB" "SELECT COUNT(*) FROM worker_sessions WHERE worker_id='${WORKER_ID}' AND session_type='control' AND status='ACTIVE' AND revoked=0;")" == "1" ]] || fail "WorkerID collision did not preserve exactly one active session"
  pass "real duplicate WorkerID gRPC session rejected with collision and first session preserved"
}

restart_master_and_reconnect() {
  local old_pid new_body operation_id persisted_status session_count
  old_pid="$(cat "$MASTER_PIDFILE")"
  info "restarting master pid=$old_pid against unchanged DB=$DB"
  stop_child "$old_pid"
  start_master

  operation_id="$(cat "$WORKDIR/operation.id")"
  new_body="$(admin_get "/api/v1/admin/operations/${operation_id}")"
  [[ "$(jq -r '.status' <<<"$new_body")" == "SUCCEEDED" ]] || {
    fail "persisted operation $operation_id was not readable after master restart"
  }
  persisted_status="$(sqlite3 -noheader "$DB" "SELECT status FROM fleet_operations WHERE operation_id='${operation_id}';")"
  [[ "$persisted_status" == "SUCCEEDED" ]] || fail "SQLite operation status changed after restart: $persisted_status"
  pass "persisted operation remains readable after master restart"

  local frozen_pid
  frozen_pid="$(cat "$WORKER_PIDFILE")"
  kill -CONT -- "-$frozen_pid" 2>/dev/null || kill -CONT "$frozen_pid" 2>/dev/null || true
  stop_child "$frozen_pid"
  start_worker '^DRAINING$'
  new_body="$(wait_worker_status '^DRAINING$' 30)" || fail "worker did not reconnect after master restart"
  [[ "$(jq -r '.worker_id' <<<"$new_body")" == "$WORKER_ID" ]] || fail "reconnected worker identity changed"
  [[ "$(jq -r '.session_active // false' <<<"$new_body")" == "true" ]] || fail "reconnected worker is not session_active"
  session_count="$(sqlite3 -noheader "$DB" "SELECT COUNT(*) FROM worker_sessions WHERE worker_id='${WORKER_ID}' AND status='ACTIVE' AND revoked=0;")"
  [[ "$session_count" == "1" ]] || fail "expected exactly one active session after reconnect, got $session_count"
  pass "worker reconnected with the same WorkerID and one active session"
}

main() {
  command -v go >/dev/null || fail "go is required"
  command -v curl >/dev/null || fail "curl is required"
  command -v jq >/dev/null || fail "jq is required"
  command -v sqlite3 >/dev/null || fail "sqlite3 is required"
  command -v setsid >/dev/null || fail "setsid is required"
  command -v timeout >/dev/null || fail "timeout is required"
  command -v sha256sum >/dev/null || fail "sha256sum is required"

  [[ -n "$MASTER_PORT" ]] || MASTER_PORT="$(pick_free_port)"
  [[ -n "$GRPC_PORT" ]] || GRPC_PORT="$(pick_free_port)"
  MASTER_URL="http://127.0.0.1:$MASTER_PORT"
  mkdir -p "$DATA_DIR" "$LOG_DIR" "$WORKDIR/runtime" "$WORKDIR/staging" "$WORKDIR/storage"
  rm -f "$DB" "$MASTER_PIDFILE" "$WORKER_PIDFILE" "$WORKDIR/operation.id"

  build_binaries
  assert_master_fail_fast
  info "initializing isolated SQLite schema"
  if ! timeout 120s "$SEED_BIN" "$DB" >"$WORKDIR/seed.log" 2>&1; then
    tail -100 "$WORKDIR/seed.log" >&2 || true
    fail "SQLite fixture seeder failed or timed out"
  fi
  write_master_env
  start_master
  register_worker
  start_worker
  verify_worker_id_collision
  verify_persisted_operation
  stop_worker_for_partition
  restart_master_and_reconnect

  pass "master-worker lifecycle E2E completed"
}

main "$@"
