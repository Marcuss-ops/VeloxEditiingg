#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT="$ROOT_DIR/scripts/ops/canary-worker-rollout.sh"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf -- "$TMP_DIR"' EXIT

MOCK_BIN="$TMP_DIR/bin"
LOG="$TMP_DIR/commands.log"
mkdir -p "$MOCK_BIN"
: >"$LOG"

cat >"$MOCK_BIN/fleetctl" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >>"${MOCK_LOG:?}"
worker=''
for arg in "$@"; do
  case "$arg" in
    --worker-id=*) worker="${arg#*=}" ;;
    velox-worker-*) worker="$arg" ;;
  esac
done
worker="${worker:-offline-worker}"
case "${1:-}" in
  inspect)
    if [[ -f "${MOCK_ROLLBACK_MARKER:-}" ]]; then
      jq -n --arg worker "$worker" '{worker_id:$worker,status:"CONNECTED",health:"HEALTHY",active_jobs:0,image_digest:"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",software_version:"v1.2.27"}'
    elif [[ "${MOCK_BAD_INSPECT:-0}" == 1 ]]; then
      jq -n --arg worker "$worker" '{worker_id:$worker,status:"CONNECTED",health:"DEGRADED",active_jobs:0,image_digest:"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",software_version:"v1.2.27"}'
    elif [[ "${MOCK_ROLLED_BACK:-0}" == 1 ]]; then
      jq -n --arg worker "$worker" '{worker_id:$worker,status:"CONNECTED",health:"HEALTHY",active_jobs:0,image_digest:"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",software_version:"v1.2.27"}'
    else
      jq -n --arg worker "$worker" '{worker_id:$worker,status:"CONNECTED",health:"HEALTHY",active_jobs:0,image_digest:"sha256:beb1cfc48d4ffb591e954cff0572ede8b9bf36fdd215239f05c5a403b8278415",software_version:"v1.2.28-canonical"}'
    fi
    ;;
  update|smoke)
    [[ "${MOCK_FAIL_MUTATION:-0}" != 1 ]] || exit 1
    ;;
  rollback)
    [[ "${MOCK_FAIL_MUTATION:-0}" != 1 ]] || exit 1
    : >"${MOCK_ROLLBACK_MARKER:?}"
    ;;
  *) exit 2 ;;
esac
MOCK
chmod +x "$MOCK_BIN/fleetctl"

export PATH="$MOCK_BIN:/usr/bin:/bin"
export MOCK_LOG="$LOG"
export MOCK_ROLLBACK_MARKER="$TMP_DIR/rolled-back"
WORKER='offline-worker'
IMAGE='ghcr.io/marcuss-ops/velox-worker@sha256:beb1cfc48d4ffb591e954cff0572ede8b9bf36fdd215239f05c5a403b8278415'
DIGEST='sha256:beb1cfc48d4ffb591e954cff0572ede8b9bf36fdd215239f05c5a403b8278415'

output="$(bash "$SCRIPT" --worker-id "$WORKER" --dry-run)"
grep -Fq "target_image:  $IMAGE" <<<"$output"
grep -Fq '[DRY-RUN] no fleetctl command will be executed' <<<"$output"
[[ ! -s "$LOG" ]] || { echo 'FAIL: dry-run invoked fleetctl' >&2; exit 1; }
printf 'PASS: dry-run is read-only and pins canonical image/version\n'

output="$(bash "$SCRIPT" --worker-id "$WORKER" --fleetctl "$MOCK_BIN/fleetctl" --apply)"
grep -Fq 'CANARY SUCCEEDED' <<<"$output"
grep -Fq "update $WORKER --digest $DIGEST" "$LOG" || { echo 'FAIL: update was not single-worker/digest-pinned' >&2; exit 1; }
grep -Fq "smoke $WORKER" "$LOG" || { echo 'FAIL: smoke was not single-worker' >&2; exit 1; }
mapfile -t apply_commands <"$LOG"
[[ "${#apply_commands[@]}" -eq 5 ]] || { echo "FAIL: expected 5 ordered commands, got ${#apply_commands[@]}" >&2; exit 1; }
[[ "${apply_commands[0]}" == "inspect $WORKER" ]] || { echo 'FAIL: apply did not inspect before update' >&2; exit 1; }
[[ "${apply_commands[1]}" == "update $WORKER --digest $DIGEST --reason canary v1.2.28-canonical" ]] || { echo 'FAIL: apply update command contract drifted' >&2; exit 1; }
[[ "${apply_commands[2]}" == "inspect $WORKER" ]] || { echo 'FAIL: apply did not inspect after update' >&2; exit 1; }
[[ "${apply_commands[3]}" == "smoke $WORKER" ]] || { echo 'FAIL: apply did not smoke the selected worker' >&2; exit 1; }
[[ "${apply_commands[4]}" == "inspect $WORKER" ]] || { echo 'FAIL: apply did not inspect after smoke/reconnect' >&2; exit 1; }
! grep -Eq '(^| )status( |$)|,|worker-[^ ]+ ' "$LOG" || { echo 'FAIL: apply touched fleet status/implicit workers' >&2; exit 1; }
printf 'PASS: apply is serial and single-worker\n'

: >"$LOG"
output="$(bash "$SCRIPT" --worker-id "$WORKER" --fleetctl "$MOCK_BIN/fleetctl" --rollback)"
grep -Fq 'ROLLBACK SUCCEEDED' <<<"$output"
grep -Fq "rollback $WORKER" "$LOG" || { echo 'FAIL: explicit rollback was not invoked' >&2; exit 1; }
mapfile -t rollback_commands <"$LOG"
[[ "${#rollback_commands[@]}" -eq 3 ]] || { echo "FAIL: expected inspect/rollback/inspect, got ${#rollback_commands[@]}" >&2; exit 1; }
[[ "${rollback_commands[0]}" == "inspect $WORKER" && "${rollback_commands[1]}" == "rollback $WORKER --reason rollback v1.2.28-canonical canary" && "${rollback_commands[2]}" == "inspect $WORKER" ]] || { echo 'FAIL: rollback command order/target drifted' >&2; exit 1; }
printf 'PASS: rollback is explicit and single-worker\n'

if bash "$SCRIPT" --apply >/dev/null 2>&1; then
  echo 'FAIL: missing worker ID was accepted' >&2
  exit 1
fi
if MOCK_BAD_INSPECT=1 bash "$SCRIPT" --worker-id "$WORKER" --fleetctl "$MOCK_BIN/fleetctl" --apply >/dev/null 2>&1; then
  echo 'FAIL: unhealthy worker was accepted' >&2
  exit 1
fi
if MOCK_FAIL_MUTATION=1 bash "$SCRIPT" --worker-id "$WORKER" --fleetctl "$MOCK_BIN/fleetctl" --apply >/dev/null 2>&1; then
  echo 'FAIL: failed mutation was accepted' >&2
  exit 1
fi
printf 'PASS: missing worker, unhealthy preflight, and mutation failure fail closed\n'
printf 'PASS: canary worker rollout offline self-test\n'
