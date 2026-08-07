#!/usr/bin/env bash
# Maintain a loopback-only reverse SSH forward for one Velox worker.
#
# The operator's remote-velox-tunnel.sh must already forward:
#   operator 127.0.0.1:18200 -> master 127.0.0.1:8200
# This helper then forwards, over the operator->worker SSH session:
#   worker 127.0.0.1:8200 -> operator 127.0.0.1:18200
#
# OpenBao therefore remains private and the worker talks to https://127.0.0.1:8200,
# which matches the SAN in the OpenBao CA certificate. Strict host-key checking is
# mandatory; no password or private key is accepted on the command line.

set -Eeuo pipefail

CONFIG_FILE="${VELOX_TUNNEL_ENV_FILE:-${XDG_CONFIG_HOME:-$HOME/.config}/velox/remote-tunnel.env}"
if [[ -f "$CONFIG_FILE" ]]; then
  [[ "$(stat -c '%a' "$CONFIG_FILE" 2>/dev/null || true)" == 600 ]] || {
    echo "worker-openbao-tunnel: refusing insecure config permissions on $CONFIG_FILE (expected 600)" >&2
    exit 1
  }
  set -a
  # shellcheck disable=SC1090
  source "$CONFIG_FILE"
  set +a
fi

LOCAL_OPENBAO_PORT="${VELOX_TUNNEL_OPENBAO_PORT:-18200}"
REMOTE_WORKER_SSH_PORT="${VELOX_REMOTE_SSH_PORT:-22}"
WORKER_OPENBAO_PORT="${VELOX_WORKER_OPENBAO_PORT:-8200}"
REMOTE_WORKER_CA_FILE="${VELOX_WORKER_OPENBAO_CA_FILE:-/etc/velox-worker/certs/openbao-ca.crt}"
STATE_DIR="${VELOX_OPENBAO_TUNNEL_STATE_DIR:-${XDG_RUNTIME_DIR:-/tmp}/velox-worker-openbao-tunnel}"
PROBE_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/probe-worker-openbao.sh"
CA_FILE="${VELOX_OPENBAO_CA_FILE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/.velox/openbao/tls/server.crt}"

valid_port() {
  [[ "$1" =~ ^[0-9]+$ ]] && (( 1 <= 10#$1 && 10#$1 <= 65535 ))
}
valid_worker_id() {
  [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]*$ ]]
}
valid_ssh_component() {
  [[ "$1" =~ ^[A-Za-z0-9._-]+$ ]]
}
valid_remote_path() {
  [[ "$1" =~ ^/[A-Za-z0-9._/-]+$ ]]
}
usage() {
  cat >&2 <<'USAGE'
Usage:
  remote-worker-openbao-tunnel.sh start <worker_id> <worker_ssh_host> <worker_ssh_user>
  remote-worker-openbao-tunnel.sh stop <worker_id>
  remote-worker-openbao-tunnel.sh status <worker_id>

The operator-side OpenBao forward must already be active on
127.0.0.1:${VELOX_TUNNEL_OPENBAO_PORT:-18200}. The worker receives a
loopback-only reverse forward on port ${VELOX_WORKER_OPENBAO_PORT:-8200}.
USAGE
  exit 2
}
die() { echo "worker-openbao-tunnel: $*" >&2; exit 1; }

command -v ssh >/dev/null 2>&1 || die "ssh is required"
valid_port "$LOCAL_OPENBAO_PORT" || die "invalid local OpenBao port"
valid_port "$REMOTE_WORKER_SSH_PORT" || die "invalid SSH port"
valid_port "$WORKER_OPENBAO_PORT" || die "invalid worker OpenBao port"

[[ -n "${VELOX_REMOTE_SSH_PASSWORD:-}" ]] && die "password authentication is not supported; use an SSH key/agent"

if [[ -z "${1:-}" ]]; then usage; fi
ACTION="$1"
WORKER_ID="${2:-}"
valid_worker_id "$WORKER_ID" || die "invalid or missing worker_id"
PID_FILE="$STATE_DIR/${WORKER_ID}.pid"
LOG_FILE="$STATE_DIR/${WORKER_ID}.log"

is_running() {
  [[ -s "$PID_FILE" ]] || return 1
  local pid
  pid="$(<"$PID_FILE")"
  [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null
}

case "$ACTION" in
  start)
    WORKER_HOST="${3:-}"
    WORKER_USER="${4:-}"
    [[ -n "$WORKER_HOST" && -n "$WORKER_USER" ]] || usage
    valid_ssh_component "$WORKER_HOST" || die "invalid worker SSH host"
    valid_ssh_component "$WORKER_USER" || die "invalid worker SSH user"
    if is_running; then
      echo "worker-openbao-tunnel: already running worker=$WORKER_ID pid=$(<"$PID_FILE")"
      exit 0
    fi
    if ! timeout 3 bash -c "</dev/tcp/127.0.0.1/$LOCAL_OPENBAO_PORT" 2>/dev/null; then
      die "operator OpenBao forward is not listening on 127.0.0.1:$LOCAL_OPENBAO_PORT; start remote-velox-tunnel.sh first"
    fi
    mkdir -p "$STATE_DIR"
    rm -f "$PID_FILE"
    : >"$LOG_FILE"
    setsid ssh -N -T \
      -p "$REMOTE_WORKER_SSH_PORT" \
      -o BatchMode=yes \
      -o ControlMaster=no \
      -o ControlPath=none \
      -o ExitOnForwardFailure=yes \
      -o ServerAliveInterval=30 \
      -o ServerAliveCountMax=3 \
      -o TCPKeepAlive=yes \
      -o StrictHostKeyChecking=yes \
      -R "127.0.0.1:${WORKER_OPENBAO_PORT}:127.0.0.1:${LOCAL_OPENBAO_PORT}" \
      "${WORKER_USER}@${WORKER_HOST}" >>"$LOG_FILE" 2>&1 &
    echo "$!" >"$PID_FILE"
    for _ in 1 2 3 4 5; do
      is_running && break
      sleep 1
    done
    is_running || {
      tail -20 "$LOG_FILE" >&2 || true
      rm -f "$PID_FILE"
      die "reverse OpenBao tunnel failed to start"
    }
    [[ -x "$PROBE_SCRIPT" ]] || {
      kill "$(<"$PID_FILE")" 2>/dev/null || true
      rm -f "$PID_FILE"
      die "worker OpenBao probe script is missing or not executable: $PROBE_SCRIPT"
    }
    [[ -s "$CA_FILE" ]] || {
      kill "$(<"$PID_FILE")" 2>/dev/null || true
      rm -f "$PID_FILE"
      die "OpenBao CA file missing: $CA_FILE"
    }
    if ! "$PROBE_SCRIPT" "$WORKER_HOST" "$WORKER_USER" "$REMOTE_WORKER_SSH_PORT" "$WORKER_OPENBAO_PORT" "$REMOTE_WORKER_CA_FILE"; then
      kill "$(<"$PID_FILE")" 2>/dev/null || true
      rm -f "$PID_FILE"
      die "reverse tunnel started but worker OpenBao TLS health probe failed"
    fi
    echo "worker-openbao-tunnel: started and TLS-verified worker=$WORKER_ID pid=$(<"$PID_FILE")"
    ;;
  stop)
    if ! is_running; then rm -f "$PID_FILE"; echo "worker-openbao-tunnel: stopped worker=$WORKER_ID"; exit 0; fi
    pid="$(<"$PID_FILE")"
    kill "$pid" 2>/dev/null || true
    for _ in 1 2 3 4 5; do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
    kill -KILL "$pid" 2>/dev/null || true
    rm -f "$PID_FILE"
    echo "worker-openbao-tunnel: stopped worker=$WORKER_ID"
    ;;
  status)
    if is_running; then echo "worker-openbao-tunnel: running worker=$WORKER_ID pid=$(<"$PID_FILE")"; else echo "worker-openbao-tunnel: stopped worker=$WORKER_ID"; exit 1; fi
    ;;
  *) usage ;;
esac
