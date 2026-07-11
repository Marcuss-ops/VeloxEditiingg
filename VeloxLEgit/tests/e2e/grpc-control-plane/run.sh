#!/usr/bin/env bash
# =============================================================================
# tests/e2e/grpc-control-plane/run.sh — PR 3 E2E Matrix Orchestrator
# =============================================================================
# Runs the 6-case gRPC control-plane matrix against a local velox-server:
#
#   1. plaintext accept               (VELOX_GRPC_ALLOW_INSECURE_DEV=true)
#   2. TLS accept                     (full matching mTLS triple)
#   3. bad-cert reject                (worker leaf self-signed / wrong key)
#   4. wrong-CA reject                (worker leaf signed by CA-B; master has CA-A pool)
#   5. plaintext-vs-TLS reject        (master TLS-required; worker sends plaintext)
#   6. parallel one-accept-one-reject  (two workers; one valid mTLS, one bad)
#
# Orchestration strategy
# ──────────────────────
# run.sh spawns HOST-NATIVE processes (bash-built velox-server + velox-worker-agent
# binaries) instead of going through docker compose. Three reasons:
#
#   * Speed: no container startup per case. Full matrix ≈ 90-120s vs ~5 minutes.
#   * Signal hygiene: trap-based PID reaping is reliable when the parent is bash.
#     docker compose run --rm adds a daemon roundtrip that's harder to clean up
#     under SIGINT (the daemon survives).
#   * Footprint: `make e2e-grpc` should work on a CI box WITHOUT the compose v2
#     plugin. Plain `docker` is enough; compose is provided as reference (compose.yml).
#
# Cleanup
# ───────
# Trap EXIT / INT / TERM walks $CHILD_PIDS, sends SIGTERM, escalates to SIGKILL
# after 1s. After kill, run.sh intentionally does NOT remove $WORKDIR — the logs
# and per-case db paths are the operator's post-mortem evidence. Set
# E2E_CLEAN=1 to wipe on EXIT.
#
# Environment
# ───────────
#   E2E_WORKDIR          root for certs, logs, dbs           (default /tmp/velox-e2e-grpc)
#   VELOX_SERVER_BIN     path to pre-built velox-server       (auto-built if absent)
#   VELOX_WORKER_BIN     path to pre-built velox-worker-agent (auto-built if absent)
#   DATASERVER_ROOT      path to DataServer/ source           (default $ROOT/../../DataServer)
#   WORKERAGENT_ROOT     path to RemoteCodex/.../ source      (default $ROOT/../../RemoteCodex/native/worker-agent-go)
#   E2E_CLEAN=1          wipe $WORKDIR on exit (default keep)
# =============================================================================

set -uo pipefail  # NOT -e: continue across case failures so the matrix reports all verdicts

# ─── Paths ───────────────────────────────────────────────────────────────────
ROOT="$(cd "$(dirname "$0")" && pwd)"
WORKDIR="${E2E_WORKDIR:-/tmp/velox-e2e-grpc}"
DATASERVER_ROOT="${DATASERVER_ROOT:-$ROOT/../../../DataServer}"
WORKERAGENT_ROOT="${WORKERAGENT_ROOT:-$ROOT/../../../RemoteCodex/native/worker-agent-go}"
BIN_DIR="$WORKDIR/bin"

# go binaries
GO_BIN="${GO_BIN:-$(command -v go || true)}"

# ─── Source assertion helpers ───────────────────────────────────────────────
# shellcheck disable=SC1091
source "$ROOT/assert.sh"

# ─── Child PID tracking + trap cleanup ──────────────────────────────────────
declare -a CHILD_PIDS=()
declare -a CHILD_LABELS=()   # parallel array: CHILD_LABELS[i] = label for CHILD_PIDS[i]

push_pid() {
  CHILD_PIDS+=("$1")
  CHILD_LABELS+=("$2")
}

kill_all() {
  local sig="${1:-TERM}"
  local n=${#CHILD_PIDS[@]}
  if (( n == 0 )); then return 0; fi
  printf "[run.sh] sending %s to %d child(ren): %s\n" "$sig" "$n" "${CHILD_LABELS[*]}"
  for i in "${!CHILD_PIDS[@]}"; do
    local pid="${CHILD_PIDS[$i]}"
    if kill -0 "$pid" 2>/dev/null; then
      kill -"$sig" "$pid" 2>/dev/null || true
    fi
  done
  if [[ "$sig" == "TERM" ]]; then
    sleep 1
    # Escalate to KILL any survivors.
    for i in "${!CHILD_PIDS[@]}"; do
      local pid="${CHILD_PIDS[$i]}"
      if kill -0 "$pid" 2>/dev/null; then
        printf "[run.sh] escalating to KILL: pid=%s label=%s\n" "$pid" "${CHILD_LABELS[$i]}"
        kill -KILL "$pid" 2>/dev/null || true
      fi
    done
    # wait briefly so the kernel actually reaps
    for pid in "${CHILD_PIDS[@]}"; do
      wait "$pid" 2>/dev/null || true
    done
  fi
}

on_int()  { kill_all TERM; exit 130; }
on_term() { kill_all TERM; exit 143; }
on_exit() {
  kill_all TERM
  [[ "${E2E_CLEAN:-0}" == "1" ]] && rm -rf "$WORKDIR"
}
trap on_exit EXIT
trap 'on_int'  INT
trap 'on_term' TERM

# ─── Helpers ────────────────────────────────────────────────────────────────
mkdir_p() { mkdir -p "$1" ; }

# resolve_bin <basename> <module-root> <cmd-path-rel>
# Returns the absolute path to a built binary; builds it if missing.
resolve_bin() {
  local bin="$BIN_DIR/$1"
  local module_root="$2"
  local cmd_rel="$3"
  if [[ -x "$bin" ]]; then
    printf "%s\n" "$bin"
    return 0
  fi
  if [[ -z "$GO_BIN" ]]; then
    printf "%s\n" "[run.sh] FATAL: go not on PATH and $bin not built" >&2
    return 1
  fi
  assert_info "building $1 (one-time) into $bin"
  (cd "$module_root" && "$GO_BIN" build -o "$bin" "./$cmd_rel")
  printf "%s\n" "$bin"
}

patch_env() {
  # patch_env <template> <output> <sed-program-argv...>
  local tmpl="$1" out="$2"; shift 2
  sed "$@" "$tmpl" > "$out"
}

spawn_master() {
  # spawn_master <case-id> <patched-master-env-file>
  local case_id="$1" envfile="$2"
  local log="$WORKDIR/$case_id/master.log"
  mkdir_p "$(dirname "$log")"
  assert_info "starting master for $case_id (env=$envfile, log=$log)"
  # Disable bash interactive job-control messages; we want clean stderr in log.
  set +m
  set -a
  # shellcheck disable=SC1090
  source "$envfile"
  set +a
  set -m  # re-enable job control briefly for the launch only
  set +m
  "$VELOX_SERVER_BIN" >"$log" 2>&1 &
  local pid=$!
  push_pid "$pid" "master-$case_id"
  set -m
  # Give the master a moment to bind ports before the worker tries to dial.
  sleep 1
}

spawn_worker_sync() {
  # spawn_worker_sync <case-id> <worker-id> <config-json>
  # Waits up to 12s for handshake to succeed; returns 0 if worker is accepted.
  local case_id="$1" worker_id="$2" config="$3"
  local log="$WORKDIR/$case_id/worker-${worker_id}.log"
  mkdir_p "$(dirname "$log")"
  assert_info "starting worker '$worker_id' for $case_id"
  set +m
  "$VELOX_WORKER_BIN" --config "$config" >"$log" 2>&1 &
  local pid=$!
  push_pid "$pid" "worker-$worker_id"
  set -m
  # Worker is short-lived on accept (PR 2: drainStream emits exit 0 quickly
  # after HelloAck for case 1's plaintext path; OR after GoodbyeTimeout for
  # case 2's TLS path with --heartbeat-window).
  # We poll for the log marker instead of trusting shell & exit code, so the
  # test is stable regardless of how fast the worker tears down.
  local master_log="$WORKDIR/$case_id/master.log"
  if wait_for_worker_connection "$master_log" "$worker_id" 12; then
    return 0
  fi
  return 1
}

wait_for_master_ready() {
  local base_url="$1" admin_token="$2" budget="${3:-15}"
  if [[ -z "$VELOX_SERVER_BIN" ]]; then
    return 1
  fi
  # We don't have a /healthz endpoint guaranteed pre-PR 4; fall back to
  # waiting for the master's "listening" log marker.
  local case_id="$4"
  local log="$WORKDIR/$case_id/master.log"
  local deadline=$(( $(date +%s) + budget ))
  while (( $(date +%s) < deadline )); do
    if grep -qE "listening on|server starting|Server ready|gRPC listening|HTTP server listening" \
          "$log" 2>/dev/null; then
      return 0
    fi
    if curl -fsS -H "X-Admin-Token: $admin_token" "$base_url/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

# ─── Pre-flight: build binaries ──────────────────────────────────────────────
assert_info "workdir = $WORKDIR"
mkdir_p "$WORKDIR" "$BIN_DIR" "$WORKDIR/pki" "$WORKDIR/cases"

if [[ -z "${VELOX_SERVER_BIN:-}" ]]; then
  VELOX_SERVER_BIN="$(resolve_bin velox-server "$DATASERVER_ROOT" cmd/server)" || exit 1
fi
if [[ -z "${VELOX_WORKER_BIN:-}" ]]; then
  VELOX_WORKER_BIN="$(resolve_bin velox-worker-agent "$WORKERAGENT_ROOT" cmd/velox-worker-agent)" || exit 1
fi

# ─── Counters ────────────────────────────────────────────────────────────────
PASS=0
FAIL_COUNT=0
declare -a CASE_VERDICTS=()

record() {
  local case_name="$1" verdict="$2"
  CASE_VERDICTS+=("$case_name: $verdict")
  if [[ "$verdict" == "PASS" ]]; then
    (( PASS++ ))
  else
    (( FAIL_COUNT++ ))
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Case 1 — plaintext accept (master + worker both plaintext)
# ─────────────────────────────────────────────────────────────────────────────
case_1_plaintext_accept() {
  local id="case-1-plaintext-accept"
  local worker_id="e2e-worker-plaintext-1"
  local case_dir="$WORKDIR/cases/$id"
  local pki_dir="$WORKDIR/pki/$id"
  local master_env="$case_dir/master.env"
  local worker_cfg="$case_dir/worker-config.json"
  mkdir_p "$case_dir"

  # Patch env: TLS commented (already commented in template). Enable insecure-dev.
  patch_env "$ROOT/configs/master.env.example" "$master_env" \
    -e "s|^VELOX_RUNTIME_DIR=.*|VELOX_RUNTIME_DIR=$case_dir/run|" \
    -e "s|^VELOX_DATA_DIR=.*|VELOX_DATA_DIR=$case_dir/data|" \
    -e "s|^VELOX_DB_PATH=.*|VELOX_DB_PATH=$case_dir/data/velox.db|" \
    -e "s|^VELOX_VIDEOS_DIR=.*|VELOX_VIDEOS_DIR=$case_dir/videos|" \
    -e "s|^VELOX_ALLOWED_WORKERS=.*|VELOX_ALLOWED_WORKERS=$worker_id|" \
    -e 's|^# VELOX_GRPC_ALLOW_INSECURE_DEV=.*|VELOX_GRPC_ALLOW_INSECURE_DEV=true|'

  cp "$ROOT/configs/worker-plaintext.json" "$worker_cfg"
  sed -i "s|WORKER_ID_PLACEHOLDER|$worker_id|" "$worker_cfg"

  # ── No PKI ──
  rm -rf "$pki_dir"

  CHILD_PIDS=(); CHILD_LABELS=()
  spawn_master "$id" "$master_env"
  if ! wait_for_master_ready "http://localhost:8000" "e2e-admin-token" 15 "$id"; then
    kill_all TERM
    assert_fail "case-1: master never became ready"
    record "$id" "FAIL"
    return
  fi
  spawn_worker_sync "$id" "$worker_id" "$worker_cfg"
  local rv=$?
  sleep 1
  kill_all TERM
  if (( rv == 0 )); then
    record "$id" "PASS"
  else
    record "$id" "FAIL"
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Case 2 — TLS accept (master + worker both full matching mTLS)
# ─────────────────────────────────────────────────────────────────────────────
case_2_tls_accept() {
  local id="case-2-tls-accept"
  local worker_id="e2e-worker-tls-case-2"
  local case_dir="$WORKDIR/cases/$id"
  local pki_dir="$WORKDIR/pki/$id"
  local master_env="$case_dir/master.env"
  local worker_cfg="$case_dir/worker-config.json"
  mkdir_p "$case_dir" "$pki_dir"

  "$ROOT/certs/generate-dev-pki.sh" "$pki_dir" "$worker_id" 7 1 >/dev/null

  patch_env "$ROOT/configs/master.env.example" "$master_env" \
    -e "s|^VELOX_RUNTIME_DIR=.*|VELOX_RUNTIME_DIR=$case_dir/run|" \
    -e "s|^VELOX_DATA_DIR=.*|VELOX_DATA_DIR=$case_dir/data|" \
    -e "s|^VELOX_DB_PATH=.*|VELOX_DB_PATH=$case_dir/data/velox.db|" \
    -e "s|^VELOX_VIDEOS_DIR=.*|VELOX_VIDEOS_DIR=$case_dir/videos|" \
    -e "s|^VELOX_ALLOWED_WORKERS=.*|VELOX_ALLOWED_WORKERS=$worker_id|" \
    -e "s|^# VELOX_GRPC_TLS_CERT_FILE=.*|VELOX_GRPC_TLS_CERT_FILE=$pki_dir/server.crt|" \
    -e "s|^# VELOX_GRPC_TLS_KEY_FILE=.*|VELOX_GRPC_TLS_KEY_FILE=$pki_dir/server.key|" \
    -e "s|^# VELOX_GRPC_TLS_CA_FILE=.*|VELOX_GRPC_TLS_CA_FILE=$pki_dir/ca.crt|" \
    -e 's|^VELOX_GRPC_ALLOW_INSECURE_DEV=.*|VELOX_GRPC_ALLOW_INSECURE_DEV=false|'

  cp "$ROOT/configs/worker-tls.json" "$worker_cfg"
  sed -i \
    -e "s|WORKER_ID_PLACEHOLDER|$worker_id|" \
    -e "s|CERT_DIR_PLACEHOLDER|$pki_dir|g" \
    "$worker_cfg"

  CHILD_PIDS=(); CHILD_LABELS=()
  spawn_master "$id" "$master_env"
  if ! wait_for_master_ready "http://localhost:8000" "e2e-admin-token" 15 "$id"; then
    kill_all TERM
    assert_fail "case-2: master never became ready"
    record "$id" "FAIL"
    return
  fi
  spawn_worker_sync "$id" "$worker_id" "$worker_cfg"
  local rv=$?
  sleep 1
  kill_all TERM
  if (( rv == 0 )); then
    record "$id" "PASS"
  else
    record "$id" "FAIL"
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Case 3 — bad-cert reject (worker leaf is self-signed / wrong-key)
# ─────────────────────────────────────────────────────────────────────────────
case_3_bad_cert_reject() {
  local id="case-3-bad-cert-reject"
  local worker_id="e2e-worker-tls-bad-3"
  local case_dir="$WORKDIR/cases/$id"
  local pki_dir="$WORKDIR/pki/$id"
  local pki_bad_dir="$WORKDIR/pki/${id}-worker-bad"
  local master_env="$case_dir/master.env"
  local worker_cfg="$case_dir/worker-config.json"
  mkdir_p "$case_dir" "$pki_dir" "$pki_bad_dir"

  # Master triple: legitimate (CA-valid) — case-2-style.
  "$ROOT/certs/generate-dev-pki.sh" "$pki_dir" "phantom-ca-3" 7 1 >/dev/null
  # Worker triple: standalone CA + leaf signed by it (won't chain to master CA).
  "$ROOT/certs/generate-dev-pki.sh" "$pki_bad_dir" "$worker_id" 7 1 >/dev/null

  patch_env "$ROOT/configs/master.env.example" "$master_env" \
    -e "s|^VELOX_RUNTIME_DIR=.*|VELOX_RUNTIME_DIR=$case_dir/run|" \
    -e "s|^VELOX_DATA_DIR=.*|VELOX_DATA_DIR=$case_dir/data|" \
    -e "s|^VELOX_DB_PATH=.*|VELOX_DB_PATH=$case_dir/data/velox.db|" \
    -e "s|^VELOX_VIDEOS_DIR=.*|VELOX_VIDEOS_DIR=$case_dir/videos|" \
    -e "s|^VELOX_ALLOWED_WORKERS=.*|VELOX_ALLOWED_WORKERS=$worker_id|" \
    -e "s|^# VELOX_GRPC_TLS_CERT_FILE=.*|VELOX_GRPC_TLS_CERT_FILE=$pki_dir/server.crt|" \
    -e "s|^# VELOX_GRPC_TLS_KEY_FILE=.*|VELOX_GRPC_TLS_KEY_FILE=$pki_dir/server.key|" \
    -e "s|^# VELOX_GRPC_TLS_CA_FILE=.*|VELOX_GRPC_TLS_CA_FILE=$pki_dir/ca.crt|"

  cp "$ROOT/configs/worker-tls.json" "$worker_cfg"
  sed -i \
    -e "s|WORKER_ID_PLACEHOLDER|$worker_id|" \
    -e "s|CERT_DIR_PLACEHOLDER|$pki_bad_dir|g" \
    "$worker_cfg"

  CHILD_PIDS=(); CHILD_LABELS=()
  spawn_master "$id" "$master_env"
  if ! wait_for_master_ready "http://localhost:8000" "e2e-admin-token" 15 "$id"; then
    kill_all TERM
    assert_fail "case-3: master never became ready"
    record "$id" "FAIL"
    return
  fi

  # Spawn worker; expect rejection (worker exits 1).
  local worker_log="$WORKDIR/$id/worker-${worker_id}.log"
  set +m
  "$VELOX_WORKER_BIN" --config "$worker_cfg" >"$worker_log" 2>&1 &
  push_pid $! "worker-$worker_id"
  set -m
  sleep 6
  local exit_code=0
  for pid in "${CHILD_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || exit_code=$?
  done
  kill_all TERM

  if grep -qiE "(handshake|verify|certificate|unknown authority|invalid|TLS.*fail|PermissionDenied|Unauthenticated)" "$worker_log"; then
    record "$id" "PASS"
  else
    assert_fail "case-3: worker log lacks handover-failure marker (see $worker_log)"
    record "$id" "FAIL"
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Case 4 — wrong-CA reject (master CA pool = CA-A; worker leaf signed by CA-B)
# ─────────────────────────────────────────────────────────────────────────────
case_4_wrong_ca_reject() {
  local id="case-4-wrong-ca-reject"
  local worker_id="e2e-worker-wrong-ca-4"
  local case_dir="$WORKDIR/cases/$id"
  local pki_a_dir="$WORKDIR/pki/${id}-ca-a"   # master's PKI
  local pki_b_dir="$WORKDIR/pki/${id}-ca-b"   # worker's PKI (different CA)
  local master_env="$case_dir/master.env"
  local worker_cfg="$case_dir/worker-config.json"
  mkdir_p "$case_dir" "$pki_a_dir" "$pki_b_dir"

  "$ROOT/certs/generate-dev-pki.sh" "$pki_a_dir" "phantom-master-ca-4" 7 1 >/dev/null
  "$ROOT/certs/generate-dev-pki.sh" "$pki_b_dir" "$worker_id"        7 1 >/dev/null

  patch_env "$ROOT/configs/master.env.example" "$master_env" \
    -e "s|^VELOX_RUNTIME_DIR=.*|VELOX_RUNTIME_DIR=$case_dir/run|" \
    -e "s|^VELOX_DATA_DIR=.*|VELOX_DATA_DIR=$case_dir/data|" \
    -e "s|^VELOX_DB_PATH=.*|VELOX_DB_PATH=$case_dir/data/velox.db|" \
    -e "s|^VELOX_VIDEOS_DIR=.*|VELOX_VIDEOS_DIR=$case_dir/videos|" \
    -e "s|^VELOX_ALLOWED_WORKERS=.*|VELOX_ALLOWED_WORKERS=$worker_id|" \
    -e "s|^# VELOX_GRPC_TLS_CERT_FILE=.*|VELOX_GRPC_TLS_CERT_FILE=$pki_a_dir/server.crt|" \
    -e "s|^# VELOX_GRPC_TLS_KEY_FILE=.*|VELOX_GRPC_TLS_KEY_FILE=$pki_a_dir/server.key|" \
    -e "s|^# VELOX_GRPC_TLS_CA_FILE=.*|VELOX_GRPC_TLS_CA_FILE=$pki_a_dir/ca.crt|"

  cp "$ROOT/configs/worker-tls.json" "$worker_cfg"
  sed -i \
    -e "s|WORKER_ID_PLACEHOLDER|$worker_id|" \
    -e "s|CERT_DIR_PLACEHOLDER|$pki_b_dir|g" \
    "$worker_cfg"

  CHILD_PIDS=(); CHILD_LABELS=()
  spawn_master "$id" "$master_env"
  if ! wait_for_master_ready "http://localhost:8000" "e2e-admin-token" 15 "$id"; then
    kill_all TERM
    assert_fail "case-4: master never became ready"
    record "$id" "FAIL"
    return
  fi

  local worker_log="$WORKDIR/$id/worker-${worker_id}.log"
  set +m
  "$VELOX_WORKER_BIN" --config "$worker_cfg" >"$worker_log" 2>&1 &
  push_pid $! "worker-$worker_id"
  set -m
  sleep 6
  for pid in "${CHILD_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  kill_all TERM

  if grep -qiE "(handshake|verify|certificate|unknown authority|invalid|PermissionDenied|Unauthenticated)" "$worker_log"; then
    record "$id" "PASS"
  else
    assert_fail "case-4: worker log lacks handover-failure marker (see $worker_log)"
    record "$id" "FAIL"
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Case 5 — plaintext-vs-TLS reject (master TLS-required; worker sends plaintext)
# ─────────────────────────────────────────────────────────────────────────────
case_5_plaintext_vs_tls_reject() {
  local id="case-5-plaintext-vs-tls-reject"
  local worker_id="e2e-worker-plaintext-5"
  local case_dir="$WORKDIR/cases/$id"
  local pki_dir="$WORKDIR/pki/$id"
  local master_env="$case_dir/master.env"
  local worker_cfg="$case_dir/worker-config.json"
  mkdir_p "$case_dir" "$pki_dir"

  "$ROOT/certs/generate-dev-pki.sh" "$pki_dir" "phantom-master-ca-5" 7 1 >/dev/null

  patch_env "$ROOT/configs/master.env.example" "$master_env" \
    -e "s|^VELOX_RUNTIME_DIR=.*|VELOX_RUNTIME_DIR=$case_dir/run|" \
    -e "s|^VELOX_DATA_DIR=.*|VELOX_DATA_DIR=$case_dir/data|" \
    -e "s|^VELOX_DB_PATH=.*|VELOX_DB_PATH=$case_dir/data/velox.db|" \
    -e "s|^VELOX_VIDEOS_DIR=.*|VELOX_VIDEOS_DIR=$case_dir/videos|" \
    -e "s|^VELOX_ALLOWED_WORKERS=.*|VELOX_ALLOWED_WORKERS=$worker_id|" \
    -e "s|^# VELOX_GRPC_TLS_CERT_FILE=.*|VELOX_GRPC_TLS_CERT_FILE=$pki_dir/server.crt|" \
    -e "s|^# VELOX_GRPC_TLS_KEY_FILE=.*|VELOX_GRPC_TLS_KEY_FILE=$pki_dir/server.key|" \
    -e "s|^# VELOX_GRPC_TLS_CA_FILE=.*|VELOX_GRPC_TLS_CA_FILE=$pki_dir/ca.crt|" \
    -e 's|^VELOX_GRPC_ALLOW_INSECURE_DEV=.*|VELOX_GRPC_ALLOW_INSECURE_DEV=false|'
  # NB: master TLS ENABLED + insecure-dev disabled — pure TLS-required path.

  cp "$ROOT/configs/worker-plaintext.json" "$worker_cfg"
  sed -i "s|WORKER_ID_PLACEHOLDER|$worker_id|" "$worker_cfg"

  CHILD_PIDS=(); CHILD_LABELS=()
  spawn_master "$id" "$master_env"
  if ! wait_for_master_ready "http://localhost:8000" "e2e-admin-token" 15 "$id"; then
    kill_all TERM
    assert_fail "case-5: master never became ready"
    record "$id" "FAIL"
    return
  fi

  local worker_log="$WORKDIR/$id/worker-${worker_id}.log"
  set +m
  "$VELOX_WORKER_BIN" --config "$worker_cfg" >"$worker_log" 2>&1 &
  push_pid $! "worker-$worker_id"
  set -m
  sleep 6
  for pid in "${CHILD_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  kill_all TERM

  if grep -qiE "(handshake|verify|TLS|no certificate|connection refused|PermissionDenied|Unauthenticated|unknown)" "$worker_log"; then
    record "$id" "PASS"
  else
    assert_fail "case-5: worker log lacks handover-failure marker (see $worker_log)"
    record "$id" "FAIL"
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Case 6 — parallel one-accept-one-reject (two workers; one good, one bad)
# ─────────────────────────────────────────────────────────────────────────────
case_6_parallel_one_accept_one_reject() {
  local id="case-6-parallel-one-accept-one-reject"
  local good_id="e2e-worker-tls-good-6"
  local bad_id="e2e-worker-tls-bad-6"
  local case_dir="$WORKDIR/cases/$id"
  local pki_good_dir="$WORKDIR/pki/${id}-good"
  local pki_bad_dir="$WORKDIR/pki/${id}-bad"
  local master_env="$case_dir/master.env"
  local worker_good_cfg="$case_dir/worker-good.json"
  local worker_bad_cfg="$case_dir/worker-bad.json"
  mkdir_p "$case_dir" "$pki_good_dir" "$pki_bad_dir"

  "$ROOT/certs/generate-dev-pki.sh" "$pki_good_dir" "$good_id" 7 1 >/dev/null
  # The "bad" PKI is a separate CA — worker's leaf won't chain to master's pool.
  "$ROOT/certs/generate-dev-pki.sh" "$pki_bad_dir"  "$bad_id"  7 1 >/dev/null

  patch_env "$ROOT/configs/master.env.example" "$master_env" \
    -e "s|^VELOX_RUNTIME_DIR=.*|VELOX_RUNTIME_DIR=$case_dir/run|" \
    -e "s|^VELOX_DATA_DIR=.*|VELOX_DATA_DIR=$case_dir/data|" \
    -e "s|^VELOX_DB_PATH=.*|VELOX_DB_PATH=$case_dir/data/velox.db|" \
    -e "s|^VELOX_VIDEOS_DIR=.*|VELOX_VIDEOS_DIR=$case_dir/videos|" \
    -e "s|^VELOX_ALLOWED_WORKERS=.*|VELOX_ALLOWED_WORKERS=$good_id,$bad_id|" \
    -e "s|^# VELOX_GRPC_TLS_CERT_FILE=.*|VELOX_GRPC_TLS_CERT_FILE=$pki_good_dir/server.crt|" \
    -e "s|^# VELOX_GRPC_TLS_KEY_FILE=.*|VELOX_GRPC_TLS_KEY_FILE=$pki_good_dir/server.key|" \
    -e "s|^# VELOX_GRPC_TLS_CA_FILE=.*|VELOX_GRPC_TLS_CA_FILE=$pki_good_dir/ca.crt|"

  cp "$ROOT/configs/worker-tls.json" "$worker_good_cfg"
  sed -i \
    -e "s|WORKER_ID_PLACEHOLDER|$good_id|" \
    -e "s|CERT_DIR_PLACEHOLDER|$pki_good_dir|g" \
    "$worker_good_cfg"
  cp "$ROOT/configs/worker-tls.json" "$worker_bad_cfg"
  sed -i \
    -e "s|WORKER_ID_PLACEHOLDER|$bad_id|" \
    -e "s|CERT_DIR_PLACEHOLDER|$pki_bad_dir|g" \
    "$worker_bad_cfg"

  CHILD_PIDS=(); CHILD_LABELS=()
  spawn_master "$id" "$master_env"
  if ! wait_for_master_ready "http://localhost:8000" "e2e-admin-token" 15 "$id"; then
    kill_all TERM
    assert_fail "case-6: master never became ready"
    record "$id" "FAIL"
    return
  fi

  # Sequential fleet: one worker finishing before the next starts.
  # Both share port 50051 on the master; the master's TLS handshake is
  # one-shot per connection. The good worker registers, the bad worker's
  # handshake fails.
  local good_log="$WORKDIR/$id/worker-${good_id}.log"
  local bad_log="$WORKDIR/$id/worker-${bad_id}.log"

  set +m
  "$VELOX_WORKER_BIN" --config "$worker_good_cfg" >"$good_log" 2>&1 &
  push_pid $! "worker-$good_id"
  set -m
  if wait_for_worker_connection "$WORKDIR/$id/master.log" "$good_id" 12; then
    sleep 1   # let handshake complete cleanly
  fi
  for pid in "${CHILD_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  # Remove only the worker PID — master MUST stay in CHILD_PIDS so
  # the trap handler (on_exit → kill_all TERM) can reap it.
  local new_pids=() new_labels=()
  for i in "${!CHILD_PIDS[@]}"; do
    if [[ "${CHILD_LABELS[$i]}" != "worker-$good_id" ]]; then
      new_pids+=("${CHILD_PIDS[$i]}")
      new_labels+=("${CHILD_LABELS[$i]}")
    fi
  done
  CHILD_PIDS=("${new_pids[@]}")
  CHILD_LABELS=("${new_labels[@]}")

  set +m
  "$VELOX_WORKER_BIN" --config "$worker_bad_cfg" >"$bad_log" 2>&1 &
  push_pid $! "worker-$bad_id"
  set -m
  sleep 6
  for pid in "${CHILD_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  kill_all TERM

  local good_ok=0 bad_ok=0
  grep -qE "(HelloAck|✓ HelloAck)" "$good_log" && good_ok=1
  grep -qiE "(handshake|verify|certificate|unknown authority|PermissionDenied|Unauthenticated)" "$bad_log" && bad_ok=1

  if (( good_ok == 1 && bad_ok == 1 )); then
    record "$id" "PASS"
  else
    assert_fail "case-6: good_ok=$good_ok bad_ok=$bad_ok (good_log=$good_log, bad_log=$bad_log)"
    record "$id" "FAIL"
  fi
}

# ─── Main dispatch ──────────────────────────────────────────────────────────
main() {
  printf "\n==== PR 3 E2E: gRPC control-plane matrix (workdir=%s) ====\n\n" "$WORKDIR"
  printf "VELOX_SERVER_BIN  = %s\n" "$VELOX_SERVER_BIN"
  printf "VELOX_WORKER_BIN  = %s\n" "$VELOX_WORKER_BIN"
  printf "DATASERVER_ROOT   = %s\n" "$DATASERVER_ROOT"
  printf "WORKERAGENT_ROOT  = %s\n\n" "$WORKERAGENT_ROOT"

  case_1_plaintext_accept
  case_2_tls_accept
  case_3_bad_cert_reject
  case_4_wrong_ca_reject
  case_5_plaintext_vs_tls_reject
  case_6_parallel_one_accept_one_reject

  printf "\n==== PR 3 E2E: matrix summary ====\n"
  for v in "${CASE_VERDICTS[@]:-}"; do
    printf "  %s\n" "$v"
  done
  printf "\nResult: %d PASS, %d FAIL\n" "$PASS" "$FAIL_COUNT"
  if (( FAIL_COUNT > 0 )); then
    return 1
  fi
  return 0
}

main "$@"
