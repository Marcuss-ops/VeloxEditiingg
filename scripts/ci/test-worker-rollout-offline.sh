#!/usr/bin/env bash
# Offline contract tests for deploy/runtime/velox-worker-activate-image.
# No Docker daemon, SSH connection, systemd, registry, or network is used.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HELPER="$ROOT/deploy/runtime/velox-worker-activate-image"

fail() {
  printf 'worker-rollout-offline: FAIL: %s\n' "$*" >&2
  exit 1
}
pass() {
  printf 'worker-rollout-offline: %s\n' "$*"
}

[[ -f "$HELPER" ]] || fail "activation helper is missing: $HELPER"

TMP="$(mktemp -d -t velox-worker-rollout-offline.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT
BIN="$TMP/bin"
mkdir -p "$BIN"
LOG="$TMP/calls.log"
ENV_FILE="$TMP/worker.env"
LOCK_FILE="$TMP/activate.lock"
BACKUP_FILE="$TMP/worker.env.prev"
IMAGE='ghcr.io/marcuss-ops/velox-worker@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
PREV='ghcr.io/marcuss-ops/velox-worker@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'

reset_env() {
  cat >"$ENV_FILE" <<EOF
VELOX_WORKER_ID=offline-worker
VELOX_WORKER_IMAGE=$PREV
VELOX_MASTER_URL=https://master.invalid
OTHER_SETTING=preserve-me
EOF
}
reset_env

cat >"$BIN/docker" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf 'docker %s\n' "$*" >>"${MOCK_LOG:?}"
case "${1:-}" in
  pull)
    [[ "${MOCK_DOCKER_PULL_FAIL:-0}" == 1 ]] && exit 41
    exit 0
    ;;
  inspect)
    [[ "${MOCK_DOCKER_INSPECT_FAIL:-0}" == 1 ]] && exit 42
    printf '%s true\n' "${MOCK_INSPECT_IMAGE:?}"
    ;;
  *)
    exit 43
    ;;
esac
MOCK

cat >"$BIN/systemctl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf 'systemctl %s\n' "$*" >>"${MOCK_LOG:?}"
[[ "${MOCK_SYSTEMCTL_FAIL:-0}" == 1 ]] && exit 44
case "${1:-}" in
  restart) [[ "${2:-}" == velox-worker.service ]] ;;
  is-active) [[ "${2:-}" == --quiet && "${3:-}" == velox-worker.service ]] ;;
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

chmod 0755 "$BIN/docker" "$BIN/systemctl" "$BIN/curl"
export MOCK_LOG="$LOG" MOCK_INSPECT_IMAGE="$IMAGE"

run_helper() {
  local inspect_image="${MOCK_INSPECT_IMAGE:-$IMAGE}"
  env \
    "PATH=$BIN:/usr/bin:/bin" \
    "ENV_FILE=$ENV_FILE" \
    "LOCK_FILE=$LOCK_FILE" \
    "BACKUP_FILE=$BACKUP_FILE" \
    "POLL_INTERVAL=0.01" \
    "MOCK_LOG=$LOG" \
    "MOCK_INSPECT_IMAGE=$inspect_image" \
    bash "$HELPER" "$IMAGE"
}

# Happy path: pull, atomic env replacement, systemd restart, running check,
# image assertion, and readiness probe all happen in order.
reset_env
: >"$LOG"
run_helper || fail "happy-path activation failed"
grep -Fxq "docker pull $IMAGE" "$LOG" || fail "docker pull did not receive expected digest"
grep -Fxq 'systemctl restart velox-worker.service' "$LOG" || fail "systemd restart was not requested"
grep -Fxq 'systemctl is-active --quiet velox-worker.service' "$LOG" || fail "systemd active check was not requested"
grep -Fxq "docker inspect --format {{.Config.Image}} {{.State.Running}} velox-worker" "$LOG" || fail "container digest/running check was not requested"
grep -Fq 'curl -fsS --max-time 10 http://127.0.0.1:8081/health/ready' "$LOG" || fail "health/ready was not requested"
grep -Fxq "VELOX_WORKER_IMAGE=$IMAGE" "$ENV_FILE" || fail "worker.env does not contain the target digest"
grep -Fxq 'VELOX_WORKER_ID=offline-worker' "$ENV_FILE" || fail "worker.env lost worker identity"
grep -Fxq 'OTHER_SETTING=preserve-me' "$ENV_FILE" || fail "worker.env lost unrelated settings"
[[ ! -e "$BACKUP_FILE" ]] || fail "happy-path backup not cleaned up"
pass 'happy path: pull/env/restart/container/health checks passed'

# Fail closed on a mutable image ref before any external command or env write.
reset_env
cp "$ENV_FILE" "$TMP/env-before-invalid"
: >"$LOG"
if env PATH="$BIN:/usr/bin:/bin" ENV_FILE="$ENV_FILE" LOCK_FILE="$LOCK_FILE" MOCK_LOG="$LOG" bash "$HELPER" 'ghcr.io/acme/worker:latest' >/dev/null 2>&1; then
  fail 'mutable image reference was accepted'
fi
cmp -s "$ENV_FILE" "$TMP/env-before-invalid" || fail 'invalid digest changed worker.env'
[[ ! -s "$LOG" ]] || fail 'external command ran before mutable digest was rejected'
pass 'mutable digest rejected fail-closed before pull/restart/health'

# Fail closed when pull fails; restart, health, and env mutation must not occur.
reset_env
cp "$ENV_FILE" "$TMP/env-before-pull-fail"
: >"$LOG"
if MOCK_DOCKER_PULL_FAIL=1 MOCK_INSPECT_IMAGE="$IMAGE" run_helper >/dev/null 2>&1; then
  fail 'docker pull failure was ignored'
fi
cmp -s "$ENV_FILE" "$TMP/env-before-pull-fail" || fail 'pull failure changed worker.env'
! grep -q '^systemctl ' "$LOG" || fail 'systemd ran after pull failure'
! grep -q '^curl ' "$LOG" || fail 'health check ran after pull failure'
pass 'pull failure stopped rollout before restart/health'

# Fail closed when the restarted container reports a different image digest,
# and roll back to the previous digest (env restored + service restarted).
reset_env
cp "$ENV_FILE" "$TMP/env-before-digest-mismatch"
: >"$LOG"
if MOCK_INSPECT_IMAGE='ghcr.io/marcuss-ops/velox-worker@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc' run_helper >/dev/null 2>&1; then
  fail 'container digest mismatch was accepted'
fi
grep -q '^docker inspect ' "$LOG" || fail 'container digest was not inspected'
grep -Fxq "VELOX_WORKER_IMAGE=$PREV" "$ENV_FILE" || fail 'rollback did not restore previous worker.env after digest mismatch'
restart_count="$(grep -c '^systemctl restart velox-worker.service$' "$LOG" || true)"
[[ "$restart_count" == 2 ]] || fail "expected 2 restarts (forward + rollback), got $restart_count"
pass 'container digest mismatch rolled back to previous digest'

# Fail closed when worker.env is missing the canonical image setting.
ENV_MISSING="$TMP/worker-missing-image.env"
printf 'VELOX_WORKER_ID=offline-worker\nOTHER_SETTING=preserve-me\n' >"$ENV_MISSING"
: >"$LOG"
if env PATH="$BIN:/usr/bin:/bin" ENV_FILE="$ENV_MISSING" LOCK_FILE="$LOCK_FILE" MOCK_LOG="$LOG" MOCK_INSPECT_IMAGE="$IMAGE" bash "$HELPER" "$IMAGE" >/dev/null 2>&1; then
  fail 'env without VELOX_WORKER_IMAGE was accepted'
fi
! grep -q '^docker pull ' "$LOG" || fail 'pull ran with incomplete worker.env'
! grep -q '^systemctl ' "$LOG" || fail 'restart ran with incomplete worker.env'
pass 'missing VELOX_WORKER_IMAGE rejected before pull'

# Fail closed after restart if readiness is unavailable. The env is restored
# to the previous digest and the service restarted (rollback contract).
reset_env
: >"$LOG"
if MOCK_CURL_FAIL=1 MOCK_INSPECT_IMAGE="$IMAGE" run_helper >/dev/null 2>&1; then
  fail 'health failure was ignored'
fi
grep -Fxq "systemctl restart velox-worker.service" "$LOG" || fail "restart was not reached before health failure"
grep -Fxq "VELOX_WORKER_IMAGE=$PREV" "$ENV_FILE" || fail 'rollback did not restore previous worker.env after health failure'
restart_count="$(grep -c '^systemctl restart velox-worker.service$' "$LOG" || true)"
[[ "$restart_count" == 2 ]] || fail "expected 2 restarts (forward + rollback), got $restart_count"
pass 'health/ready failure returned non-zero and rolled back'

# Rollback convergence: when the forward container never converges, the
# helper restores the previous digest and verifies it is running.
reset_env
: >"$LOG"
if MOCK_INSPECT_IMAGE="$PREV" run_helper >/dev/null 2>&1; then
  fail 'non-convergent forward activation was accepted'
fi
grep -Fxq "VELOX_WORKER_IMAGE=$PREV" "$ENV_FILE" || fail 'rollback restore lost after non-convergent activation'
grep -Fxq "docker inspect --format {{.Config.Image}} {{.State.Running}} velox-worker" "$LOG" || fail 'rollback did not verify container digest'
restart_count="$(grep -c '^systemctl restart velox-worker.service$' "$LOG" || true)"
[[ "$restart_count" == 2 ]] || fail "expected 2 restarts (forward + rollback), got $restart_count"
pass 'non-convergent activation rolled back and verified previous digest'

pass 'ALL OFFLINE SELF-TESTS PASSED'
