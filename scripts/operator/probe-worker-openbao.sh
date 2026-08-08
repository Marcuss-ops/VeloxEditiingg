#!/usr/bin/env bash
# Probe a worker's loopback OpenBao endpoint through SSH without authentication.
# Arguments: worker SSH host, worker SSH user, SSH port, OpenBao port, remote CA path.

set -Eeuo pipefail

usage() {
  printf 'usage: %s <ssh_host> <ssh_user> <ssh_port> <openbao_port> <remote_ca_file>\n' "$0" >&2
  exit 2
}
die() { printf 'worker-openbao-probe: %s\n' "$*" >&2; exit 1; }
valid_component() { [[ "$1" =~ ^[A-Za-z0-9._-]+$ ]]; }
valid_port() { [[ "$1" =~ ^[0-9]+$ ]] && (( 1 <= 10#$1 && 10#$1 <= 65535 )); }
valid_path() { [[ "$1" =~ ^/[A-Za-z0-9._/-]+$ ]]; }

(( $# == 5 )) || usage
SSH_HOST="$1"
SSH_USER="$2"
SSH_PORT="$3"
OPENBAO_PORT="$4"
REMOTE_CA_FILE="$5"
valid_component "$SSH_HOST" || die "invalid SSH host"
valid_component "$SSH_USER" || die "invalid SSH user"
valid_port "$SSH_PORT" || die "invalid SSH port"
valid_port "$OPENBAO_PORT" || die "invalid OpenBao port"
valid_path "$REMOTE_CA_FILE" || die "invalid remote CA path"
command -v ssh >/dev/null 2>&1 || die "ssh is required"

for _ in 1 2 3 4 5; do
  if ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes \
    -p "$SSH_PORT" "$SSH_USER@$SSH_HOST" \
    "sudo -n curl --fail --silent --show-error --connect-timeout 5 --max-time 10 --cacert '$REMOTE_CA_FILE' https://127.0.0.1:${OPENBAO_PORT}/v1/sys/health -o /dev/null"; then
    printf 'worker-openbao-probe: PASS TLS health worker=%s\n' "$SSH_HOST"
    exit 0
  fi
  sleep 1
done

die "worker OpenBao TLS health probe failed"
