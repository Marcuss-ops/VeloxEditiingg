#!/usr/bin/env bash
# Maintain the operator-side SSH forwards to a remote Velox pilot/master.
#
# The worker control plane is gRPC and the worker dials it; this tunnel is for
# operator/API access and for creator requests.  All forwards bind to loopback
# only.  Authentication is deliberately delegated to the user's SSH config or
# agent; passwords must never be placed in an env file or command line.

set -Eeuo pipefail

REMOTE_HOST="${VELOX_REMOTE_SSH_HOST:-}"
REMOTE_USER="${VELOX_REMOTE_SSH_USER:-}"
REMOTE_PORT="${VELOX_REMOTE_SSH_PORT:-22}"
LOCAL_MASTER_PORT="${VELOX_TUNNEL_MASTER_PORT:-18080}"
LOCAL_CREATOR_PORT="${VELOX_TUNNEL_CREATOR_PORT:-18000}"
LOCAL_GRPC_PORT="${VELOX_TUNNEL_GRPC_PORT:-18505}"
REMOTE_MASTER_PORT="${VELOX_REMOTE_MASTER_PORT:-8080}"
REMOTE_CREATOR_PORT="${VELOX_REMOTE_CREATOR_PORT:-8000}"
REMOTE_GRPC_PORT="${VELOX_REMOTE_GRPC_PORT:-50051}"
STATE_DIR="${VELOX_TUNNEL_STATE_DIR:-${XDG_RUNTIME_DIR:-/tmp}/velox-remote-tunnel}"
PID_FILE="${STATE_DIR}/tunnel.pid"
LOG_FILE="${STATE_DIR}/tunnel.log"

die() { echo "remote-tunnel: $*" >&2; exit 1; }
log() { echo "remote-tunnel: $*"; }

valid_port() {
  [[ "$1" =~ ^[0-9]+$ ]] && (( 1 <= 10#$1 && 10#$1 <= 65535 ))
}

check_config() {
  [[ -n "$REMOTE_HOST" ]] || die "VELOX_REMOTE_SSH_HOST is required"
  [[ -n "$REMOTE_USER" ]] || die "VELOX_REMOTE_SSH_USER is required"
  [[ "$REMOTE_HOST" != *:* ]] || die "IPv6 hosts must be supplied via SSH config alias"
  for port in "$REMOTE_PORT" "$LOCAL_MASTER_PORT" "$LOCAL_CREATOR_PORT" "$LOCAL_GRPC_PORT" \
    "$REMOTE_MASTER_PORT" "$REMOTE_CREATOR_PORT" "$REMOTE_GRPC_PORT"; do
    valid_port "$port" || die "invalid port: $port"
  done
  [[ -z "${VELOX_REMOTE_SSH_PASSWORD:-}" ]] || die "password authentication is not supported; use an SSH key/agent"
  command -v ssh >/dev/null || die "ssh is required"
}

is_running() {
  [[ -s "$PID_FILE" ]] || return 1
  local pid
  pid="$(<"$PID_FILE")"
  [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null
}

start() {
  check_config
  if is_running; then
    log "already running (pid=$(<"$PID_FILE"))"
    return 0
  fi
  mkdir -p "$STATE_DIR"
  rm -f "$PID_FILE"
  : >"$LOG_FILE"
  # Explicit loopback binds prevent exposing the forwarded services on LAN/WAN.
  setsid ssh -N -T \
    -p "$REMOTE_PORT" \
    -o BatchMode=yes \
    -o ControlMaster=no \
    -o ControlPath=none \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    -o TCPKeepAlive=yes \
    -o StrictHostKeyChecking=yes \
    -L "127.0.0.1:${LOCAL_MASTER_PORT}:127.0.0.1:${REMOTE_MASTER_PORT}" \
    -L "127.0.0.1:${LOCAL_CREATOR_PORT}:127.0.0.1:${REMOTE_CREATOR_PORT}" \
    -L "127.0.0.1:${LOCAL_GRPC_PORT}:127.0.0.1:${REMOTE_GRPC_PORT}" \
    "${REMOTE_USER}@${REMOTE_HOST}" >>"$LOG_FILE" 2>&1 &
  echo "$!" >"$PID_FILE"
  for _ in 1 2 3 4 5; do
    is_running && { log "started (pid=$(<"$PID_FILE"))"; return 0; }
    sleep 1
  done
  tail -20 "$LOG_FILE" >&2 || true
  rm -f "$PID_FILE"
  die "SSH tunnel failed to start"
}

stop() {
  if ! is_running; then
    rm -f "$PID_FILE"
    log "not running"
    return 0
  fi
  local pid
  pid="$(<"$PID_FILE")"
  kill "$pid" 2>/dev/null || true
  for _ in 1 2 3 4 5; do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  kill -KILL "$pid" 2>/dev/null || true
  rm -f "$PID_FILE"
  log "stopped"
}

status() {
  check_config
  if is_running; then
    log "running (pid=$(<"$PID_FILE"))"
    printf '  master  http://127.0.0.1:%s\n  creator http://127.0.0.1:%s\n  grpc    127.0.0.1:%s\n' \
      "$LOCAL_MASTER_PORT" "$LOCAL_CREATOR_PORT" "$LOCAL_GRPC_PORT"
    return 0
  fi
  log "stopped"
  return 1
}

case "${1:-status}" in
  start) start ;;
  stop) stop ;;
  restart) stop; start ;;
  status) status ;;
  *) die "usage: $0 {start|stop|restart|status}" ;;
esac
