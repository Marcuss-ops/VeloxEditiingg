#!/usr/bin/env bash
# Offline tests for scripts/ops/verify-fmp4-rollout.sh.
#
# No SSH, Docker, or worker host is touched: `ssh` is replaced by a stub that
# runs the remote heredoc locally, and `sudo`/`docker` are stubbed so the probe
# sees a deterministic worker.env + container env. The classification matrix
# (READY / DISABLED / MISCONFIGURED, drift, unknown value, no container) and
# the exit codes are what this test pins.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
VERIFIER="$ROOT/scripts/ops/verify-fmp4-rollout.sh"

fail() {
  printf 'verify-fmp4-rollout-offline: FAIL: %s\n' "$*" >&2
  exit 1
}
pass() {
  printf 'verify-fmp4-rollout-offline: %s\n' "$*"
}

[[ -f "$VERIFIER" ]] || fail "verifier is missing: $VERIFIER"

TMP="$(mktemp -d -t velox-fmp4-rollout-offline.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT
BIN="$TMP/bin"
mkdir -p "$BIN"
LOG="$TMP/calls.log"
ENV_FILE="$TMP/worker.env"
: >"$LOG"

# ssh stub: strip options + target, then run the remote command locally with
# the heredoc on stdin.
cat >"$BIN/ssh" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
while [[ $# -gt 0 ]]; do
  case "$1" in
    -i|-o) shift 2 ;;
    *) break ;;
  esac
done
[[ $# -gt 0 ]] || exit 99
target="$1"; shift
printf 'ssh %s\n' "$target" >>"${MOCK_LOG:?}"
[[ "${MOCK_SSH_FAIL:-0}" == 1 ]] && exit 255
exec "$@"
MOCK

cat >"$BIN/sudo" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
[[ "${1:-}" == "-n" ]] && shift
exec "$@"
MOCK

cat >"$BIN/docker" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
  ps)
    [[ "${MOCK_NO_CONTAINER:-0}" == 1 ]] && exit 0
    printf 'cid-offline velox-worker\n'
    ;;
  inspect) printf '2026-09-16T10:00:00Z\n' ;;
  exec)
    [[ "${MOCK_NO_CONTAINER:-0}" == 1 ]] && exit 1
    printf '%s' "${MOCK_CONTAINER_VALUE:-}"
    ;;
  *) exit 43 ;;
esac
MOCK

chmod 0755 "$BIN/ssh" "$BIN/sudo" "$BIN/docker"

run_verifier() {
  env \
    "PATH=$BIN:/usr/bin:/bin" \
    "MOCK_LOG=$LOG" \
    "MOCK_CONTAINER_VALUE=${MOCK_CONTAINER_VALUE-}" \
    "MOCK_NO_CONTAINER=${MOCK_NO_CONTAINER:-0}" \
    "MOCK_SSH_FAIL=${MOCK_SSH_FAIL:-0}" \
    "VELOX_FMP4_ENV_FILE=$ENV_FILE" \
    "VELOX_FMP4_WORKERS=${MOCK_FMP4_WORKERS-w1:host1.test:user1}" \
    bash "$VERIFIER" "$@"
}

checks=0
# run_case <want_rc> <container_value> [verifier args...] -> stdout in $out, rc in $rc
run_case() {
  local want="$1" container="$2"
  shift 2
  set +e
  out="$(MOCK_CONTAINER_VALUE="$container" run_verifier "$@")"
  rc=$?
  set -e
  [[ "$rc" == "$want" ]] || fail "exit code = $rc, want $want (container='$container' args='$*'): $out"
  checks=$((checks + 1))
}

# ── 1. Gate live on both sides -> READY, exit 0.
printf 'VELOX_WORKER_ID=w1\nVELOX_FMP4_STREAM_PROFILE=1\n' >"$ENV_FILE"
: >"$LOG"
run_case 0 1 --json
printf '%s' "$out" | grep -q '"state":"READY"' || fail "READY case state: $out"
printf '%s' "$out" | grep -q '"ready":1' || fail "READY summary: $out"
grep -Fxq 'ssh user1@host1.test' "$LOG" || fail "verifier did not SSH to the requested worker"
pass 'live gate: READY / exit 0'

# ── 2. Template-only rollout: file says 1, the running container never saw it.
run_case 1 '' --json
printf '%s' "$out" | grep -q '"state":"MISCONFIGURED"' || fail "drift case state: $out"
printf '%s' "$out" | grep -q 'drift' || fail "drift case detail: $out"
pass 'file-only flag (already-installed worker): MISCONFIGURED drift / exit 1'

# ── 3. Gate closed on both sides -> DISABLED, exit 1 (rollout incomplete).
printf 'VELOX_WORKER_ID=w1\n' >"$ENV_FILE"
run_case 1 '' --json
printf '%s' "$out" | grep -q '"state":"DISABLED"' || fail "disabled case state: $out"
pass 'gate closed: DISABLED / exit 1'

# ── 4. Unrecognised value -> MISCONFIGURED. The Go gate silently treats an
#       unknown value as disabled, so only this check surfaces the typo.
printf 'VELOX_WORKER_ID=w1\nVELOX_FMP4_STREAM_PROFILE=yes-please\n' >"$ENV_FILE"
run_case 1 'yes-please' --json
printf '%s' "$out" | grep -q '"state":"MISCONFIGURED"' || fail "invalid case state: $out"
printf '%s' "$out" | grep -q 'unrecognised value' || fail "invalid case detail: $out"
pass 'unknown value: MISCONFIGURED / exit 1'

# ── 5. No running container -> MISCONFIGURED.
printf 'VELOX_WORKER_ID=w1\nVELOX_FMP4_STREAM_PROFILE=1\n' >"$ENV_FILE"
set +e
out="$(MOCK_NO_CONTAINER=1 MOCK_CONTAINER_VALUE='' run_verifier --json)"
rc=$?
set -e
[[ "$rc" == 1 ]] || fail "no-container exit code = $rc, want 1"
printf '%s' "$out" | grep -q 'no running velox-worker container' || fail "no-container case: $out"
checks=$((checks + 1))
pass 'no running container: MISCONFIGURED'

# ── 6. SSH transport failure -> exit 2.
set +e
out="$(MOCK_SSH_FAIL=1 run_verifier --json)"
rc=$?
set -e
[[ "$rc" == 2 ]] || fail "SSH-failure exit code = $rc, want 2"
checks=$((checks + 1))
pass 'SSH failure: exit 2'

# ── 7. Multi-worker selection: one SSH per worker, aggregate summary.
run_case 0 1 --json
: >"$LOG"
set +e
out="$(MOCK_CONTAINER_VALUE=1 MOCK_FMP4_WORKERS='w1:host1.test:user1 w2:host2.test:user2' run_verifier --json)"
rc=$?
set -e
[[ "$rc" == 0 ]] || fail "multi-worker exit code = $rc, want 0"
printf '%s' "$out" | grep -q '"ready":2' || fail "multi-worker summary: $out"
[[ "$(grep -c '^ssh ' "$LOG")" == 2 ]] || fail "expected one SSH probe per worker: $(cat "$LOG")"
checks=$((checks + 1))
pass 'multi-worker selection: one probe per worker'

# ── 8. Usage guards.
set +e
out="$(MOCK_FMP4_WORKERS='' run_verifier)" 2>&1
rc=$?
set -e
[[ "$rc" == 2 ]] || fail "no-selection exit code = $rc, want 2"
set +e
out="$(MOCK_FMP4_WORKERS='broken-entry' run_verifier)" 2>&1
rc=$?
set -e
[[ "$rc" == 2 ]] || fail "malformed entry exit code = $rc, want 2"
checks=$((checks + 1))
pass 'usage guards: no selection and malformed entry exit 2'

printf 'verify-fmp4-rollout-offline: OK (%d checks green)\n' "$checks"
