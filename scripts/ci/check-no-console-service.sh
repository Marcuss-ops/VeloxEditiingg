#!/usr/bin/env bash
# scripts/ci/check-no-console-service.sh
#
# Step 3/8 of the canonical-purity action plan. Asserts that the
# `velox-worker-console` service is not installed anywhere in the
# repository outside contexts that explicitly recognize it as a
# forbidden unit (cleanup / normalize / this check itself).
#
# Allowed references:
#   * DataServer/data/ansible/playbooks/cleanup_worker.yml
#       purges residual Docker images via `docker rmi`.
#   * DataServer/data/ansible/playbooks/normalize_worker_systemd.yml
#       enumerates surviving velox-worker units and intentionally
#       does NOT whitelist console; the gate that follows fails on
#       any leftover console unit. Comment present.
#   * RemoteCodex/scripts/cleanup-worker.sh
#       `docker rmi velox-worker-console:latest` purge.
#   * scripts/ci/check-no-console-service.sh
#       this script, via the GREP_EXCLUDES allowlist.
#   * README.md
#       documents the invariant and is not an executable/service reference.
#
# Any other reference is a regression of Step 3/8 and fails the build.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

log()  { printf '\u2192 %s\n' "$*"; }
fail() { printf '\u2717 %s\n' "$*" >&2; exit 1; }

VIOLATIONS=$(grep -RnE 'velox-worker-console' . \
  --exclude-dir='.git' \
  --exclude='cleanup_worker.yml' \
  --exclude='normalize_worker_systemd.yml' \
  --exclude='cleanup-worker.sh' \
  --exclude='check-no-console-service.sh' \
  --exclude='README.md' \
  || true)

if [[ -n "$VIOLATIONS" ]]; then
  printf 'Canonical-purity fail: velox-worker-console referenced outside the allowlist.\n' >&2
  printf '\nMatches:\n%s\n\n' "$VIOLATIONS" >&2
  fail "console.service is forbidden — Step 3/8 contract violated"
fi

log "no forbidden velox-worker-console references"
