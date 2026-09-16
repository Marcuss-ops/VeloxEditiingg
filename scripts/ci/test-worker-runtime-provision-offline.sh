#!/usr/bin/env bash
# Offline contract checks for the canonical worker runtime provisioner.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SCRIPT="$ROOT/scripts/operator/provision-worker-runtime.sh"

[[ -x "$SCRIPT" ]] || { echo "worker-runtime-provision-offline: missing executable" >&2; exit 1; }
bash -n "$SCRIPT"
bash -n "$ROOT/deploy/runtime/prepare-host.sh"
grep -Fq 'velox-worker-set-config' "$SCRIPT"
grep -Fq 'prepare-host.sh' "$SCRIPT"
# shellcheck disable=SC2016
grep -Fq 'install -o root -g root -m 0755 "$SET_CONFIG_SRC" /opt/velox-worker/velox-worker-set-config' "$ROOT/deploy/runtime/prepare-host.sh"
grep -Fq 'PREPARE_HOST_DST="/opt/velox-worker/prepare-host.sh"' "$ROOT/deploy/runtime/prepare-host.sh"
printf 'worker-runtime-provision-offline: OK\n'
