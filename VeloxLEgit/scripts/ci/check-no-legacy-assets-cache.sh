#!/usr/bin/env bash
# scripts/ci/check-no-legacy-assets-cache.sh
#
# Step 6/8 — canonical-purity CI gate.
#
# The legacy /app/RemoteCodex/assets_cache bind mount is dead weight
# after the canonical VELOX_STATE_DIR cut-over. This script grep-fails
# if the path re-appears in any non-documentd source file.
#
# Allowlist:
#   - cleanup_worker.yml + RemoteCodex/scripts/cleanup-worker.sh
#     (transitional scrub is the only sanctioned use)
#   - pkg/doctor/state_dir_validator.go + state_dir_test.go
#     (the deprecation warning explicitly references the legacy path
#     so operators can recognise it during migration)
#   - DataServer/data/ansible/templates/velox-server.env.j2 (env docs)
#   - docs/* (canonical-purity documentation)
#
# Run: ./scripts/ci/check-no-legacy-assets-cache.sh
# Exit codes:
#   0 → no forbidden references
#   1 → forbidden reference(s) found (printed below)

set -euo pipefail

LEGACY="/app/RemoteCodex/assets_cache"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

ALLOWLIST=(
  "DataServer/data/ansible/playbooks/cleanup_worker.yml"
  "RemoteCodex/scripts/cleanup-worker.sh"
  "RemoteCodex/native/worker-agent-go/pkg/doctor/state_dir_validator.go"
  "RemoteCodex/native/worker-agent-go/pkg/doctor/state_dir_test.go"
  "scripts/ci/check-no-legacy-assets-cache.sh"
  "docs/100-percent-plan/"
  "docs/worker_deployment.md"
)

# Build grep exclusions from the allowlist.
EXCLUDES=()
for entry in "${ALLOWLIST[@]}"; do
  case "$entry" in
    *.go|*.sh|*.yml|*.md)
      EXCLUDES+=("--exclude=$(basename "$entry")")
      ;;
  esac
  EXCLUDES+=("--exclude-dir=$(dirname "$entry")")
done

# Normalise allowlist entries into grep-friendly excludes.
GREP_ARGS=(
  --line-number
  --recursive
  --fixed-strings
  --exclude-dir=.git
  --exclude-dir=node_modules
  --exclude-dir=docs
  --exclude=cleanup_worker.yml
  --exclude=cleanup-worker.sh
  --exclude=state_dir_validator.go
  --exclude=state_dir_test.go
  --exclude=check-no-legacy-assets-cache.sh
)

found=0
echo "[check-no-legacy-assets-cache] scanning for $LEGACY (allowlist applied)…"

# Hit . (the entire repo) but exclude docs/ + the allowlist explicitly.
matches="$(grep "${GREP_ARGS[@]}" "$LEGACY" . 2>/dev/null || true)"

if [ -n "$matches" ]; then
  echo "FAIL: $LEGACY referenced outside the Step 6/8 allowlist:"
  echo
  echo "$matches"
  echo
  echo "If this is intentional, add the file to the allowlist in"
  echo "scripts/ci/check-no-legacy-assets-cache.sh AND justify in"
  echo "the commit message. Otherwise, move the reference under"
  echo "\$VELOX_STATE_DIR (default /var/lib/velox/worker)."
  found=1
fi

if [ "$found" -eq 0 ]; then
  echo "[check-no-legacy-assets-cache] OK"
fi
exit $found
