#!/usr/bin/env bash
# scripts/ci/check-compatibility-alias-registry.sh
#
# Enforce one owner for temporary audio/voiceover aliases. The registry lives
# in shared/compatibility; consumers must use its reader instead of defining
# local alias lists that can drift during the migration.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

registry="shared/compatibility/registry.go"
[[ -f "$registry" ]] || { echo "compatibility registry missing: $registry" >&2; exit 1; }
grep -q 'type CompatibilityAlias struct' "$registry" || { echo "CompatibilityAlias type missing" >&2; exit 1; }
grep -q 'CanonicalKey: VoiceoverPathsKey' "$registry" || { echo "voiceover registry entry missing" >&2; exit 1; }
grep -q 'RemovalDate:' "$registry" || { echo "alias removal date metadata missing" >&2; exit 1; }
grep -q 'Owner:' "$registry" || { echo "alias owner metadata missing" >&2; exit 1; }
grep -q 'Consumers:' "$registry" || { echo "alias consumer metadata missing" >&2; exit 1; }
grep -q 'MinimumVersion:' "$registry" || { echo "alias minimum version metadata missing" >&2; exit 1; }

required_consumers=(
  "shared/contract/payload_v2.go"
  "DataServer/internal/jobs/enqueue/normalize_media.go"
  "DataServer/internal/remoteengine/dto_assets.go"
  "DataServer/internal/handlers/server/pipeline/worker_payload_projection.go"
  "DataServer/cmd/server/bootstrap_telemetry.go"
  "RemoteCodex/native/worker-agent-go/pkg/api/renderplan/validation.go"
)
for file in "${required_consumers[@]}"; do
  if ! grep -q 'velox-shared/compatibility' "$file"; then
    echo "consumer is not wired to shared compatibility registry: $file" >&2
    exit 1
  fi
done

# The executable consumer surface must call the shared reader. This avoids
# trying to blacklist historical alias names that may remain in comments.
required_calls=(
  "shared/contract/payload_v2.go:compatibility.ReadStringList"
  "DataServer/internal/handlers/server/pipeline/compatibility_alias_parity_test.go:compatibility.VoiceoverPathsKey"
  "DataServer/internal/jobs/enqueue/normalize_media.go:compatibility.ReadStringList"
  "DataServer/internal/remoteengine/dto_assets.go:compatibility.ReadStringList"
  "RemoteCodex/native/worker-agent-go/pkg/api/renderplan/validation.go:compatibility.ReadStringList"
)
for requirement in "${required_calls[@]}"; do
  file="${requirement%%:*}"
  call="${requirement#*:}"
  if ! grep -q "$call" "$file"; then
    echo "shared compatibility reader missing from $file: $call" >&2
    exit 1
  fi
done

(cd shared && go test ./compatibility -count=1)
echo "check-compatibility-alias-registry: OK"
