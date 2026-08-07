#!/usr/bin/env bash
# Persistent user-service entrypoint for a worker OpenBao reverse tunnel.
# Configuration is supplied by the systemd EnvironmentFile.

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROBE_SCRIPT="${VELOX_WORKER_OPENBAO_PROBE_SCRIPT:-$SCRIPT_DIR/probe-worker-openbao.sh}"
WORKER_HOST="${VELOX_WORKER_SSH_HOST:-}"
WORKER_USER="${VELOX_WORKER_SSH_USER:-}"
SSH_PORT="${VELOX_WORKER_SSH_PORT:-22}"
WORKER_PORT="${VELOX_WORKER_OPENBAO_PORT:-8200}"
LOCAL_PORT="${VELOX_TUNNEL_OPENBAO_PORT:-18200}"
REMOTE_CA_FILE="${VELOX_WORKER_OPENBAO_CA_FILE:-/etc/velox-worker/certs/openbao-ca.crt}"

fail() { printf 'worker-openbao-service: %s\n' "$*" >&2; exit 1; }
valid_component() { [[ "$1" =~ ^[A-Za-z0-9._-]+$ ]]; }
valid_port() { [[ "$1" =~ ^[0-9]+$ ]] && (( 1 <= 10#$1 && 10#$1 <= 65535 )); }
valid_path() { [[ "$1" =~ ^/[A-Za-z0-9._/-]+$ ]]; }

valid_component "$WORKER_HOST" || fail 'invalid worker SSH host'
valid_component "$WORKER_USER" || fail 'invalid worker SSH user'
valid_port "$SSH_PORT" || fail 'invalid SSH port'
valid_port "$WORKER_PORT" || fail 'invalid worker OpenBao port'
valid_port "$LOCAL_PORT" || fail 'invalid local OpenBao port'
valid_path "$REMOTE_CA_FILE" || fail 'invalid remote CA path'
[[ -x "$PROBE_SCRIPT" ]] || fail "probe script missing or not executable: $PROBE_SCRIPT"
command -v ssh >/dev/null 2>&1 || fail 'ssh is required'

cleanup() {
  if [[ -n "${SSH_PID:-}" ]]; then
    kill "$SSH_PID" 2>/dev/null || true
    wait "$SSH_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

/usr/bin/ssh -N -T \
  -p "$SSH_PORT" \
  -o BatchMode=yes \
  -o ControlMaster=no \
  -o ControlPath=none \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -o TCPKeepAlive=yes \
  -o StrictHostKeyChecking=yes \
  -R "127.0.0.1:${WORKER_PORT}:127.0.0.1:${LOCAL_PORT}" \
  "$WORKER_USER@$WORKER_HOST" &
SSH_PID=$!

if ! "$PROBE_SCRIPT" "$WORKER_HOST" "$WORKER_USER" "$SSH_PORT" "$WORKER_PORT" "$REMOTE_CA_FILE"; then
  fail 'worker OpenBao TLS health probe failed'
fi

wait "$SSH_PID"
