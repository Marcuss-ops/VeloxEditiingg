#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d /tmp/velox-worker-set-config-test.XXXXXX)"
trap 'rm -rf "$TMP"' EXIT

ENV_FILE="$TMP/worker.env"
cat >"$ENV_FILE" <<'EOF'
VELOX_WORKER_ID=test-worker
VELOX_AUDIO_MIX_STRATEGY=legacy
VELOX_AUDIO_MIX_PROFILE=0
EOF

if ENV_FILE="$ENV_FILE" bash "$ROOT/deploy/runtime/velox-worker-set-config" --audio-mix-strategy optimized --audio-mix-profile 1 2>/dev/null; then
  echo "config helper unexpectedly reached a real service" >&2
  exit 1
fi

# The helper must not partially mutate the file when the service restart fails.
grep -Fxq 'VELOX_AUDIO_MIX_STRATEGY=legacy' "$ENV_FILE"
grep -Fxq 'VELOX_AUDIO_MIX_PROFILE=0' "$ENV_FILE"
echo "worker-set-config: PASS"
