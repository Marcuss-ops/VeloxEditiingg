#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/ci/check-share-cert.sh — RW-PROD-001 A7 CI guard
# ─────────────────────────────────────────────────────────────────────────────
# Verifies the check-share-cert.sh tool itself works correctly by running
# its built-in self-test suite. This is a tool-integrity guard, not a fleet
# scan: the actual fleet scan runs on deployment hosts, not in CI.
#
# Exit codes:
#   0   check-share-cert.sh self-test passed (tool is intact)
#   1   self-test failed (tool regression — cert-sharing detection is broken)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

log() { printf '→ %s\n' "$*"; }
fail() { printf '✗ %s\n' "$*" >&2; exit 1; }

log "check-share-cert (RW-PROD-001 A7 tool-integrity self-test)"

"$REPO_ROOT/scripts/check-share-cert.sh" self-test || fail "check-share-cert.sh self-test FAILED"

log "check-share-cert OK (tool self-test passed)"
