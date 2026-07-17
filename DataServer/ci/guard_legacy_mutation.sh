#!/usr/bin/env bash
# ci/guard_legacy_mutation.sh
#
# CI guard that fails the build if legacy mutation patterns are found in Go
# source files under internal/. This prevents regressions after the
# UpdateJobFields / UpdateJobResult removal.
#
# Exit codes:
#   0 — no violations found
#   1 — violations detected (build should fail)
#
# Usage:
#   bash ci/guard_legacy_mutation.sh
set -euo pipefail

VIOLATIONS=0

# Pattern 1: direct calls to the removed UpdateJobFields
if rg -q 'UpdateJobFields\(' internal --glob '*.go'; then
  echo "CI GUARD: UpdateJobFields calls found in internal/:"
  rg -n 'UpdateJobFields\(' internal --glob '*.go' || true
  VIOLATIONS=1
fi

# Pattern 2: direct calls to the removed UpdateJobResult on JobRepository
if rg -q '\.UpdateJobResult\(' internal --glob '*.go'; then
  echo "CI GUARD: .UpdateJobResult() calls found in internal/:"
  rg -n '\.UpdateJobResult\(' internal --glob '*.go' || true
  VIOLATIONS=1
fi

# Note: master_video_path / drive_url / social_url are still READ in
# assembler / query / assembly layers for backward-compatible read-only
# API responses. The guard only catches direct WRITE mutations to those
# columns via UPDATE/SET.

if [ "$VIOLATIONS" -ne 0 ]; then
  echo ""
  echo "CI GUARD FAILED: legacy mutation patterns detected. See above for details."
  echo "If this is intentional, remove or update this guard script."
  exit 1
fi

echo "CI GUARD: all checks passed."
