#!/usr/bin/env bash
# Canonical worker-host runtime provisioning/convergence.
#
# This is the bootstrap bridge for an already registered worker that has the
# canonical container but is missing a host-side runtime helper. It stages only
# repository-owned runtime inputs into a short-lived remote directory and then
# invokes the repository's root-owned prepare-host.sh. It never writes
# worker.env directly and never receives or prints credentials.
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SSH_KEY="${SSH_KEY:-/etc/velox/ssh/id_ed25519_velox}"
KNOWN_HOSTS="${KNOWN_HOSTS:-/etc/velox/ssh/known_hosts}"
LOG_FILE="${VELOX_WORKER_RUNTIME_LOG:-/tmp/velox-worker-runtime-provision-$(date -u +%Y%m%dT%H%M%SZ).log}"

RUNTIME_FILES=(
  prepare-host.sh
  compose.yml
  openbao-fetch-worker-secrets.sh
  velox-worker-activate-image
  velox-worker-set-config
  velox-worker-mtls-renew.service
  velox-worker-mtls-renew.sh
  velox-worker-mtls-renew.timer
  velox-worker-maintenance.service
  velox-worker-maintenance.sh
  velox-worker-maintenance.timer
  velox-worker-resource-monitor.service
  velox-worker-resource-monitor.sh
  velox-worker-resource-monitor.timer
  journald-velox-worker.conf
  velox-worker.service
)

WORKERS=()

log() {
  local line
  line="[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"
  printf '%s\n' "$line" | tee -a "$LOG_FILE"
}

die() {
  log "FAIL: $*"
  exit 1
}

redact_remote_output() {
  sed -E 's/((TOKEN|PASSWORD|SECRET|CREDENTIAL|PRIVATE_KEY)[A-Za-z0-9_:-]*)[[:space:]]*=[[:space:]]*[^[:space:]]+/\1=[REDACTED]/Ig'
}

usage() {
  local rc="${1:-2}"
  cat <<'EOF'
Usage:
  scripts/operator/provision-worker-runtime.sh --worker <id> <host> <ssh_user>
  scripts/operator/provision-worker-runtime.sh --fleet

This command bootstraps the canonical host runtime only. Configuration knobs
are applied afterwards through scripts/fleetctl worker-config set.
EOF
  exit "$rc"
}

valid_word() { [[ "$1" =~ ^[A-Za-z0-9._-]+$ ]]; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --fleet)
      WORKERS+=(
        "host_57_131_20_173:57.131.20.173:pierone"
        "velox-worker-13197:149.56.131.97:pierone"
        "velox-worker-523925eb:51.222.204.158:ubuntu"
      )
      shift
      ;;
    --worker)
      [[ $# -ge 4 ]] || usage
      WORKERS+=("$2:$3:$4")
      shift 4
      ;;
    -h|--help) usage 0 ;;
    *) usage ;;
  esac
done

(( ${#WORKERS[@]} > 0 )) || usage
[[ $EUID -eq 0 ]] || die "run as root (the registry SSH key is root-managed)"
[[ -r "$SSH_KEY" ]] || die "SSH key is not readable: $SSH_KEY"
[[ -r "$KNOWN_HOSTS" ]] || die "known_hosts is not readable: $KNOWN_HOSTS"
install -d -m 0755 "$(dirname "$LOG_FILE")"

RUNTIME_DIR="$ROOT/deploy/runtime"
for name in "${RUNTIME_FILES[@]}"; do
  [[ -f "$RUNTIME_DIR/$name" ]] || die "runtime input missing: $RUNTIME_DIR/$name"
done

SSH_COMMON=(-i "$SSH_KEY" -o StrictHostKeyChecking=yes
  -o UserKnownHostsFile="$KNOWN_HOSTS" -o BatchMode=yes
  -o ConnectTimeout=10 -o LogLevel=ERROR)

CURRENT_TARGET=""
CURRENT_REMOTE_DIR=""
cleanup() {
  [[ -n "$CURRENT_TARGET" && -n "$CURRENT_REMOTE_DIR" ]] || return 0
  # The path is validated before assignment; the quoted command is deliberate
  # because cleanup runs on the remote shell.
  # shellcheck disable=SC2029
  ssh "${SSH_COMMON[@]}" "$CURRENT_TARGET" "case '$CURRENT_REMOTE_DIR' in /tmp/velox-worker-runtime.[A-Za-z0-9]*) rm -rf -- '$CURRENT_REMOTE_DIR' ;; esac" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for entry in "${WORKERS[@]}"; do
  IFS=: read -r worker_id host user <<<"$entry"
  if ! valid_word "$worker_id" || ! valid_word "$host" || ! valid_word "$user"; then
    die "invalid worker entry: $entry"
  fi
  target="$user@$host"
  CURRENT_TARGET="$target"
  CURRENT_REMOTE_DIR=""

  log "START worker=$worker_id host=$host user=$user"
  if ! CURRENT_REMOTE_DIR="$(ssh "${SSH_COMMON[@]}" "$target" 'mktemp -d /tmp/velox-worker-runtime.XXXXXX')"; then
    die "SSH bootstrap directory creation failed for $worker_id"
  fi
  [[ "$CURRENT_REMOTE_DIR" =~ ^/tmp/velox-worker-runtime\.[A-Za-z0-9]+$ ]] \
    || die "unexpected remote staging path for $worker_id"

  if ! scp "${SSH_COMMON[@]}" -q \
      "${RUNTIME_FILES[@]/#/$RUNTIME_DIR/}" "$target:$CURRENT_REMOTE_DIR/" >/dev/null 2>&1; then
    die "runtime input staging failed for $worker_id"
  fi

  # prepare-host.sh owns all privileged mutations, OpenBao provisioning,
  # service convergence, and helper/sudoers installation. Keep its output out
  # of the operational log: it may contain environment-dependent diagnostics.
  remote_output=""
  # shellcheck disable=SC2029
  if ! remote_output="$(ssh "${SSH_COMMON[@]}" "$target" \
      "sudo -n env VELOX_SSH_USER='$user' bash '$CURRENT_REMOTE_DIR/prepare-host.sh'" 2>&1)"; then
    log "prepare-host diagnostic for worker=$worker_id (redacted):"
    printf '%s\n' "$remote_output" | redact_remote_output | tail -20 | tee -a "$LOG_FILE"
    die "prepare-host.sh failed for $worker_id"
  fi

  if ! ssh "${SSH_COMMON[@]}" "$target" \
      'sudo -n test -x /usr/local/sbin/velox-worker-set-config && sudo -n stat -c "%U:%G:%a" /usr/local/sbin/velox-worker-set-config | grep -Fxq "root:root:755" && sudo -n test -r /etc/sudoers.d/velox-worker-set-config' >/dev/null 2>&1; then
    die "runtime helper verification failed for $worker_id"
  fi
  log "READY worker=$worker_id helper=/usr/local/sbin/velox-worker-set-config ownership=root:root mode=755"
  cleanup
  CURRENT_REMOTE_DIR=""
done

log "COMPLETE workers=${#WORKERS[@]} log=$LOG_FILE"
