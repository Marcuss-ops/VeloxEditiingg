#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$REPO_ROOT/shared"
go test ./telemetry
go run ./telemetry/cmd/cataloggen \
  -input telemetry/schema/catalog.json \
  -output ../RemoteCodex/native/video-engine-cpp/include/velox/telemetry/catalog_generated.hpp \
  -go-output telemetry/generated/catalog_gen.go \
  -check

# Compile a tiny consumer of the generated C++ binding. This keeps the
# unknown-key rejection contract active even when the heavier native CMake
# test suite is skipped.
cxx="${CXX:-c++}"
if ! command -v "$cxx" >/dev/null 2>&1; then
  printf 'telemetry catalog parity: C++ compiler %q is required\n' "$cxx" >&2
  exit 1
fi
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
cat >"$tmpdir/catalog_check.cpp" <<'CPP'
#include "velox/telemetry/catalog_generated.hpp"

int main() {
    using velox::telemetry::catalog::FindEvent;
    using velox::telemetry::catalog::IsAccountedTopLevelPhase;
    using velox::telemetry::catalog::IsCatalogEvent;
    if (!IsCatalogEvent("engine.encode", "setup")) return 1;
    if (IsCatalogEvent("engine.encode", "invented")) return 2;
    const auto* event = FindEvent("engine.encode", "setup");
    if (event == nullptr || event->owner != "encoder") return 3;
    // Phase taxonomy accounted_ratio guard: only exclusive top-level
    // phases are accounted — a stale kPhases regeneration must fail the
    // gate instead of silently shipping a broken accounted_ratio.
    if (!IsAccountedTopLevelPhase("render")) return 4;
    if (IsAccountedTopLevelPhase("decode")) return 5;
    return 0;
}
CPP
"$cxx" -std=c++20 -Wall -Wextra -Werror \
  -I"$REPO_ROOT/RemoteCodex/native/video-engine-cpp/include" \
  "$tmpdir/catalog_check.cpp" -o "$tmpdir/catalog_check"
"$tmpdir/catalog_check"

printf 'telemetry catalog parity and unknown-key checks: OK\n'
