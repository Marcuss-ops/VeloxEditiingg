#!/usr/bin/env bash
# Verify that all required loopback forwards are accepting TCP connections.
# Arguments: master, creator, grpc, openbao local ports.

set -Eeuo pipefail

(( $# == 4 )) || {
  printf 'usage: %s <master_port> <creator_port> <grpc_port> <openbao_port>\n' "$0" >&2
  exit 2
}

for port in "$@"; do
  [[ "$port" =~ ^[0-9]+$ ]] && (( 1 <= 10#$port && 10#$port <= 65535 )) || {
    printf 'tunnel-check: invalid port\n' >&2
    exit 2
  }
done

for _ in 1 2 3 4 5; do
  ready=1
  for port in "$@"; do
    if ! timeout 1 bash -c "</dev/tcp/127.0.0.1/$port" 2>/dev/null; then
      ready=0
      break
    fi
  done
  if (( ready == 1 )); then
    printf 'tunnel-check: PASS all local forwards ready\n'
    exit 0
  fi
  sleep 1
done

printf 'tunnel-check: FAIL one or more local forwards unavailable\n' >&2
exit 1
