#!/usr/bin/env bash
set -euo pipefail

# Compatibility wrapper for operators who still have the historical script in
# muscle memory. It deliberately does not build, copy bundles, invoke
# Ansible, or mutate worker hosts directly. CI publishes one signed GHCR
# image; FleetController is the only production deployment owner.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${VELOX_WORKER_IMAGE:-}"
TARGETS="${TARGETS:-}"

fail() {
  printf '[bump-deploy] ERROR: %s\n' "$*" >&2
  exit 1
}

[[ -n "$IMAGE" ]] || fail 'VELOX_WORKER_IMAGE is required; provide the signed GHCR @sha256:<64hex> release'
[[ "$IMAGE" =~ ^ghcr\.io/[a-z0-9._-]+/[a-z0-9._/-]+@sha256:[a-f0-9]{64}$ ]] \
  || fail 'VELOX_WORKER_IMAGE must be a pinned lowercase GHCR @sha256:64 reference'
[[ -n "$TARGETS" ]] || fail 'TARGETS is required (comma-separated immutable worker_id values)'

fleetctl="${FLEETCTL_BIN:-${ROOT_DIR}/scripts/fleetctl}"
[[ -x "$fleetctl" ]] || fail "fleetctl is not executable: $fleetctl"

IFS=',' read -r -a worker_ids <<< "$TARGETS"
for worker_id in "${worker_ids[@]}"; do
  worker_id="${worker_id//[[:space:]]/}"
  [[ -n "$worker_id" ]] || continue
  printf '[bump-deploy] handing worker=%s image=%s to FleetController\n' "$worker_id" "$IMAGE"
  "$fleetctl" update "$worker_id" "$IMAGE" "release=${RELEASE_REASON:-immutable-worker-release}"
done

printf '[bump-deploy] release delegated to FleetController; no Ansible or remote Docker build was run\n'
