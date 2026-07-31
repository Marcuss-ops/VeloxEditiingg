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
# Trap EXIT / INT / TERM calls lib_kill_all (TERM with 1s KILL escalation) on
# $LIB_CHILD_PIDS. After kill, run.sh intentionally does NOT remove $WORKDIR —
# the logs and per-case db paths are the operator's post-mortem evidence. Set
# E2E_CLEAN=1 to wipe on EXIT.
#
# Helpers
# ───────
# Cross-test helpers live in tests/_lib/sh/ (logging, pid-trap, ensure, check,
# asset-bootstrap, exitcode-aggregation). The 6 case_N_*() functions below own
# the per-case scenario flow; they reference helpers by the same names they
# had pre-Refactor 6/N, just sourced from _lib.sh.
#
# Environment
# ───────────
#   E2E_WORKDIR          root for certs, logs, dbs           (default /tmp/velox-e2e-grpc)
#   VELOX_SERVER_BIN     path to pre-built velox-server       (auto-built if absent)
#   VELOX_WORKER_BIN     path to pre-built velox-worker-agent (auto-built if absent)
#   DATASERVER_ROOT      path to DataServer/ source           (default $ROOT/../../DataServer)
#   WORKERAGENT_ROOT     path to RemoteCodex/.../ source      (default $ROOT/../../RemoteCodex/native/worker-agent-go)
#   E2E_CLEAN=1          wipe $WORKDIR on exit (default keep)
#   BUNDLE_HASH          bundle hash written to $WORKDIR/work/BUNDLE_HASH.txt
# =============================================================================

set -uo pipefail  # NOT -e: continue across case failures so the matrix reports all verdicts

# ─── Paths ───────────────────────────────────────────────────────────────────
ROOT="$(cd "$(dirname "$0")" && pwd)"
WORKDIR="${E2E_WORKDIR:-/tmp/velox-e2e-grpc}"
DATASERVER_ROOT="${DATASERVER_ROOT:-$ROOT/../../../DataServer}"
WORKERAGENT_ROOT="${WORKERAGENT_ROOT:-$ROOT/../../../RemoteCodex/native/worker-agent-go}"
BIN_DIR="$WORKDIR/bin"
GO_BIN="${GO_BIN:-$(command -v go || true)}"

# ─── Source helpers ─────────────────────────────────────────────────────────
# shellcheck source=tests/_lib/sh/_lib.sh
source "$ROOT/../../_lib/sh/_lib.sh"

# assert.sh lives next to run.sh (test-script-local) and is NOT a cross-test
# helper — it only asserts in the gRPC matrix context. Keep local.
# shellcheck source=assert.sh
source "$ROOT/assert.sh"

# ─── Per-scenario cleanup trap (calls lib_kill_all on exit) ─────────────────
on_int()  { lib_kill_all TERM; exit 130; }
on_term() { lib_kill_all TERM; exit 143; }
on_exit() {
  lib_kill_all TERM
  [[ "${E2E_CLEAN:-0}" == "1" ]] && rm -rf "$WORKDIR"
}
trap on_exit EXIT
trap 'on_int'  INT
trap 'on_term' TERM

# ─── Verdict counter init (sourced from _lib.sh) ────────────────────────────
aggregate_init

# ─── Scenario-specific helpers (only used by case_N_* below) ────────────────

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
    printf "[run.sh] FATAL: go not on PATH and $bin not built\n" >&2
    return 1
  fi
  assert_info "building $1 (one-time) into $bin" >&2
  (cd "$module_root" && "$GO_BIN" build -o "$bin" "./$cmd_rel")
  printf "%s\n" "$bin"
}

# patch_env <template> <output> <sed-program-argv...>
patch_env() {
  local tmpl="$1" out="$2"; shift 2
  sed "$@" "$tmpl" > "$out"
}

spawn_master() {
  local case_id="$1" envfile="$2"
  local log="$WORKDIR/$case_id/master.log"
  mkdir_p "$(dirname "$log")"
  assert_info "starting master for $case_id (env=$envfile, log=$log)"
  set +m
  set -a
  # shellcheck disable=SC1090
  source "$envfile"
  set +a
  set -m
  set +m
  "$VELOX_SERVER_BIN" >"$log" 2>&1 &
  local pid=$!
  lib_push_pid "$pid" "master-$case_id"
  set -m
  sleep 1
}

spawn_worker_sync() {
  local case_id="$1" worker_id="$2" config="$3"
  local log="$WORKDIR/$case_id/worker-${worker_id}.log"
  mkdir_p "$(dirname "$log")"
  assert_info "starting worker '$worker_id' for $case_id"
  set +m
  "$VELOX_WORKER_BIN" --config "$config" >"$log" 2>&1 &
  local pid=$!
  lib_push_pid "$pid" "worker-$worker_id"
  set -m
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

# Wait for master log to see an Accept (HelloAck) marker for $worker_id,
# up to $budget seconds. Returns 0 if marker seen, 1 on timeout.
wait_for_worker_connection() {
  local master_log="$1" worker_id="$2" budget="${3:-12}"
  local deadline=$(( $(date +%s) + budget ))
  while (( $(date +%s) < deadline )); do
    if grep -qE "(${worker_id}.*HelloAck|${worker_id}.*accepted|${worker_id}.*registered)" \
          "$master_log" 2>/dev/null; then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

# Ensure the previous case's listeners are fully released before the next
# master starts. Even after lib_kill_all, a socket may briefly linger; poll
# for up to 20s before giving up and letting the next case surface its own
# error.
wait_for_ports_free() {
  local deadline=$(( $(date +%s) + 20 ))
  while (( $(date +%s) < deadline )); do
    local busy=0
    if command -v ss >/dev/null 2>&1; then
      if ss -ltn 2>/dev/null | grep -qE ':(8000|50051)\b'; then
        busy=1
      fi
    elif command -v netstat >/dev/null 2>&1; then
      if netstat -ltn 2>/dev/null | grep -qE ':(8000|50051)\b'; then
        busy=1
      fi
    fi
    if (( busy == 0 )); then
      return 0
    fi
    sleep 0.5
  done
  return 0
}

# ─────────────────────────────────────────────────────────────────────────────
# Case 1 — plaintext accept (master + worker both plaintext)
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Case 2 — TLS accept (master + worker both full matching mTLS)
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Case 3 — bad-cert reject (worker leaf is self-signed / wrong-key)
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Case 4 — wrong-CA reject (master CA pool = CA-A; worker leaf signed by CA-B)
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Case 5 — plaintext-vs-TLS reject (master TLS-required; worker sends plaintext)
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Case 6 — parallel one-accept-one-reject (two workers; one good, one bad)
# ─────────────────────────────────────────────────────────────────────────────

# ─── Pre-flight: build binaries ──────────────────────────────────────────────
assert_info "workdir = $WORKDIR"
mkdir_p "$WORKDIR" "$BIN_DIR" "$WORKDIR/pki" "$WORKDIR/cases"

if [[ -z "${VELOX_SERVER_BIN:-}" ]]; then
  VELOX_SERVER_BIN="$(resolve_bin velox-server "$DATASERVER_ROOT" cmd/server)" || exit 1
fi
if [[ -z "${VELOX_WORKER_BIN:-}" ]]; then
  VELOX_WORKER_BIN="$(resolve_bin velox-worker-agent "$WORKERAGENT_ROOT" cmd/velox-worker-agent)" || exit 1
fi

# ─── Shared E2E runtime setup (helpers from _lib.sh) ───────────────────────
setup_shared() {
  : "${BUNDLE_HASH:=e2e-bundle-hash}"
  bootstrap_workdir "$WORKDIR/work" "${BUNDLE_HASH}"

  # Stub the velox_video_engine binary — minimal Python|JSON→output_path
  # echo satisfying the runtime contract without a real engine build.
  local stub_body
  stub_body="$(cat <<'STUB_EOF'
#!/usr/bin/env bash
set -euo pipefail
PLAN=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --plan) PLAN="$2"; shift 2 ;;
    *) shift ;;
  esac
done
if [[ -z "$PLAN" ]]; then
  echo "velox_video_engine stub: --plan required" >&2
  exit 1
fi
OUT="$(python3 -c "import json,sys; print(json.load(open('$PLAN'))['output_path'])" 2>/dev/null || jq -r '.output_path' "$PLAN")"
mkdir -p "$(dirname "$OUT")"
printf 'velox-e2e-stub-output' > "$OUT"
STUB_EOF
)"
  write_stub_binary "$BIN_DIR/velox_video_engine" "$stub_body"
}
setup_shared
export VELOX_VIDEO_ENGINE_CPP_BIN="$BIN_DIR/velox_video_engine"


# ─── Case implementations (sourced in-process; shared globals/PIDs preserved) ──
source "$ROOT/cases/case_1_plaintext_accept.sh"
source "$ROOT/cases/case_2_tls_accept.sh"
source "$ROOT/cases/case_3_bad_cert_reject.sh"
source "$ROOT/cases/case_4_wrong_ca_reject.sh"
source "$ROOT/cases/case_5_plaintext_vs_tls_reject.sh"
source "$ROOT/cases/case_6_parallel_one_accept_one_reject.sh"

# ─── Main dispatch ──────────────────────────────────────────────────────────
main() {
  printf "\n==== PR 3 E2E: gRPC control-plane matrix (workdir=%s) ====\n\n" "$WORKDIR"
  printf "VELOX_SERVER_BIN  = %s\n" "$VELOX_SERVER_BIN"
  printf "VELOX_WORKER_BIN  = %s\n" "$VELOX_WORKER_BIN"
  printf "DATASERVER_ROOT   = %s\n" "$DATASERVER_ROOT"
  printf "WORKERAGENT_ROOT  = %s\n\n" "$WORKERAGENT_ROOT"

  case_1_plaintext_accept
  wait_for_ports_free
  case_2_tls_accept
  wait_for_ports_free
  case_3_bad_cert_reject
  wait_for_ports_free
  case_4_wrong_ca_reject
  wait_for_ports_free
  case_5_plaintext_vs_tls_reject
  wait_for_ports_free
  case_6_parallel_one_accept_one_reject

  aggregate_summary_and_exit
}

main "$@"
