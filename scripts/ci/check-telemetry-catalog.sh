#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$REPO_ROOT/shared"
go test ./telemetry
go run ./telemetry/cmd/cataloggen \
  -input telemetry/catalog.json \
  -output ../RemoteCodex/native/video-engine-cpp/include/velox/telemetry/catalog_generated.hpp \
  -check

printf 'telemetry catalog parity: OK\n'
