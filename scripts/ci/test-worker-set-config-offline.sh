#!/usr/bin/env bash
# Offline contract tests for deploy/runtime/velox-worker-set-config.
#
# No Docker daemon, systemd, network, or real worker host is touched: systemctl
# and curl are mocked on PATH, and the helper is pointed at a temp copy of
# worker.env. This is the regression pin for the canonical rollout path of the
# fMP4 admission gate (VELOX_FMP4_STREAM_PROFILE) and for the audio-mix knobs
# it rides beside.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HELPER="$ROOT/deploy/runtime/velox-worker-set-config"

fail() {
  printf 'worker-set-config-offline: FAIL: %s\n' "$*" >&2
  exit 1
}
pass() {
  printf 'worker-set-config-offline: %s\n' "$*"
}

[[ -f "$HELPER" ]] || fail "config helper is missing: $HELPER"

TMP="$(mktemp -d -t velox-worker-set-config-offline.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT
BIN="$TMP/bin"
mkdir -p "$BIN"
LOG="$TMP/calls.log"
ENV_FILE="$TMP/worker.env"
LOCK_FILE="$TMP/set-config.lock"

cat >"$BIN/systemctl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf 'systemctl %s\n' "$*" >>"${MOCK_LOG:?}"
[[ "${MOCK_SYSTEMCTL_FAIL:-0}" == 1 ]] && exit 44
case "${1:-}" in
  restart) [[ "${2:-}" == velox-worker.service ]] || exit 45 ;;
  is-active) [[ "${2:-}" == --quiet && "${3:-}" == velox-worker.service ]] || exit 45 ;;
  *) exit 45 ;;
esac
MOCK

cat >"$BIN/curl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf 'curl %s\n' "$*" >>"${MOCK_LOG:?}"
[[ "${MOCK_CURL_FAIL:-0}" == 1 ]] && exit 46
[[ "$*" == *"http://127.0.0.1:8081/health/ready"* ]] || exit 47
printf '{"status":"ready"}\n'
MOCK

chmod 0755 "$BIN/systemctl" "$BIN/curl"
export MOCK_LOG="$LOG"

# baseline_env writes a worker.env that already carries the audio-mix rollout
# plus unrelated settings that must survive every invocation.
baseline_env() {
  cat >"$ENV_FILE" <<EOF
VELOX_WORKER_ID=offline-worker
VELOX_WORKER_IMAGE=ghcr.io/marcuss-ops/velox-worker@sha256:$(printf 'a%.0s' {1..64})
VELOX_AUDIO_MIX_STRATEGY=optimized
VELOX_AUDIO_MIX_PROFILE=1
OTHER_SETTING=preserve-me
EOF
}

run_helper() {
  env \
    "PATH=$BIN:/usr/bin:/bin" \
    "ENV_FILE=$ENV_FILE" \
    "LOCK_FILE=$LOCK_FILE" \
    "POLL_INTERVAL=0.01" \
    "MOCK_LOG=$LOG" \
    ${MOCK_CURL_FAIL:+MOCK_CURL_FAIL=$MOCK_CURL_FAIL} \
    bash "$HELPER" "$@"
}

env_value() {
  sed -n "s/^$1=//p" "$ENV_FILE"
}

count_key() {
  grep -c "^$1=" "$ENV_FILE" || true
}

# ── 1. Fresh rollout: the gate is written for the first time, nothing else
#       is dropped, and the service converges.
baseline_env
: >"$LOG"
run_helper --fmp4-stream-profile 1 || fail "fresh fMP4 rollout failed"
grep -Fxq 'VELOX_FMP4_STREAM_PROFILE=1' "$ENV_FILE" || fail "fMP4 gate was not written"
grep -Fxq 'VELOX_AUDIO_MIX_STRATEGY=optimized' "$ENV_FILE" || fail "audio mix strategy was lost"
grep -Fxq 'VELOX_AUDIO_MIX_PROFILE=1' "$ENV_FILE" || fail "audio mix profile was lost"
grep -Fxq 'OTHER_SETTING=preserve-me' "$ENV_FILE" || fail "unrelated settings were lost"
grep -Fxq 'VELOX_WORKER_ID=offline-worker' "$ENV_FILE" || fail "worker identity was lost"
[[ "$(count_key VELOX_FMP4_STREAM_PROFILE)" == 1 ]] || fail "fMP4 gate key is not unique"
grep -Fxq 'systemctl restart velox-worker.service' "$LOG" || fail "service restart was not requested"
grep -Fq 'curl -fsS --max-time 5 http://127.0.0.1:8081/health/ready' "$LOG" || fail "readiness was not probed"
[[ ! -e "${ENV_FILE}.config-prev" ]] || fail "happy-path backup not cleaned up"
pass 'fresh rollout: gate written, sibling settings and identity preserved'

# ── 2. Closing the gate on an already-installed worker rewrites in place.
run_helper --fmp4-stream-profile 0 || fail "gate close failed"
[[ "$(env_value VELOX_FMP4_STREAM_PROFILE)" == 0 ]] || fail "gate was not closed"
[[ "$(count_key VELOX_FMP4_STREAM_PROFILE)" == 1 ]] || fail "gate key was duplicated on rewrite"
pass 'gate close: in-place rewrite, no duplicate key'

# ── 3. An audio-mix-only mutation must not erase the fMP4 gate.
run_helper --fmp4-stream-profile 1 || fail "gate reopen failed"
run_helper --audio-mix-strategy legacy || fail "audio-mix mutation failed"
[[ "$(env_value VELOX_FMP4_STREAM_PROFILE)" == 1 ]] || fail "audio-mix mutation erased the fMP4 gate"
[[ "$(env_value VELOX_AUDIO_MIX_STRATEGY)" == legacy ]] || fail "audio mix strategy was not updated"
[[ "$(env_value VELOX_AUDIO_MIX_PROFILE)" == 1 ]] || fail "unrequested audio mix profile was erased"
pass 'cross-knob isolation: audio-mix mutation preserves the fMP4 gate'

# ── 4. Invalid toggle fails closed before any write or restart.
baseline_env
cp "$ENV_FILE" "$TMP/env-before-invalid"
: >"$LOG"
set +e
run_helper --fmp4-stream-profile 2 >/dev/null 2>&1
rc=$?
set -e
[[ "$rc" == 2 ]] || fail "invalid toggle exit code = $rc, want 2"
cmp -s "$ENV_FILE" "$TMP/env-before-invalid" || fail "invalid toggle mutated worker.env"
[[ ! -s "$LOG" ]] || fail "invalid toggle shelled out: $(cat "$LOG")"
pass 'invalid toggle: rejected before any write or restart'

# ── 5. Empty invocation is a usage error.
set +e
run_helper >/dev/null 2>&1
rc=$?
set -e
[[ "$rc" == 2 ]] || fail "no-argument exit code = $rc, want 2"
pass 'no-argument invocation: usage error'

# ── 6. Readiness failure rolls the file back, so a bad rollout cannot leave
#       the gate open on an unhealthy worker.
baseline_env
cp "$ENV_FILE" "$TMP/env-before-rollback"
: >"$LOG"
set +e
MOCK_CURL_FAIL=1 run_helper --fmp4-stream-profile 1 >/dev/null 2>&1
rc=$?
set -e
[[ "$rc" == 1 ]] || fail "readiness-failure exit code = $rc, want 1"
cmp -s "$ENV_FILE" "$TMP/env-before-rollback" || fail "worker.env was not rolled back"
[[ -z "$(env_value VELOX_FMP4_STREAM_PROFILE)" ]] || fail "rollback left the fMP4 gate open"
grep -Fxq 'VELOX_AUDIO_MIX_STRATEGY=optimized' "$ENV_FILE" || fail "rollback lost the audio mix strategy"
[[ "$(grep -c 'systemctl restart velox-worker.service' "$LOG")" -ge 2 ]] || fail "rollback did not restart the service"
pass 'readiness failure: worker.env rolled back and gate left closed'

# ── 7. Service restart failure also rolls back.
baseline_env
cp "$ENV_FILE" "$TMP/env-before-restart-fail"
set +e
MOCK_SYSTEMCTL_FAIL=1 run_helper --fmp4-stream-profile 1 >/dev/null 2>&1
rc=$?
set -e
[[ "$rc" == 1 ]] || fail "restart-failure exit code = $rc, want 1"
cmp -s "$ENV_FILE" "$TMP/env-before-restart-fail" || fail "worker.env was not rolled back after restart failure"
pass 'restart failure: worker.env rolled back'

printf 'worker-set-config-offline: OK (7/7 checks green)\n'
