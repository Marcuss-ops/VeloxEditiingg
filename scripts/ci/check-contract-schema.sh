#!/usr/bin/env bash
# scripts/ci/check-contract-schema.sh
#
# CI guard for the canonical wire schemas under shared/contract/schema/
# and their generated Go/C++ field-name bindings. Closes the schema-vs-
# binding drift loop: any future change to a schema (or to the typed Go
# contracts pinned by contractgen) that leaves the checked-in bindings
# stale fails this script with a non-zero exit code.
#
# Two layers, mirroring scripts/ci/check-telemetry-catalog.sh:
#
#   1. contractgen -check — re-derives the Go + C++ bindings from the
#      schemas and fails if they differ from the checked-in files. It
#      ALSO enforces the architectural parity pins:
#        - job_payload_v2.schema.json top-level keys ==
#          contract.CanonicalTopLevelKeys
#        - render_manifest_v1.schema.json properties.schema.const ==
#          rendermanifest.Schema
#   2. C++ compile smoke — compiles a tiny consumer of the generated
#      header so the C++ binding's contract stays active even when the
#      heavier native CMake suite is skipped.
#
# Run: ./scripts/ci/check-contract-schema.sh
# Exit codes: 0 clean, 1 stale bindings / parity drift / compile failure,
#             2 tooling missing (go / C++ compiler).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

command -v go >/dev/null 2>&1 || {
    printf 'check-contract-schema: go not on PATH (required for contractgen)\n' >&2
    exit 2
}

cd "$REPO_ROOT/shared"

# ── 1. Freshness + parity (no writes: -check fails on any drift) ────────────
go run ./contract/cmd/contractgen \
    -schema-dir contract/schema \
    -go-output contract/payloadfield/payloadfield_gen.go \
    -cpp-output ../RemoteCodex/native/video-engine-cpp/include/velox/contract/payload_fields_generated.hpp \
    -check

# ── 2. Compile a tiny consumer of the generated C++ binding ─────────────────
cxx="${CXX:-c++}"
if ! command -v "$cxx" >/dev/null 2>&1; then
    printf 'check-contract-schema: C++ compiler %q is required\n' "$cxx" >&2
    exit 2
fi
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
cat >"$tmpdir/contract_fields_check.cpp" <<'CPP'
#include "velox/contract/payload_fields_generated.hpp"

int main() {
    using velox::contract::fields::JOB_ID;
    using velox::contract::fields::RENDER_MANIFEST;
    using velox::contract::fields::DELIVERY_PLAN;
    using velox::contract::fields::RENDER_MANIFEST_TRACKS_EVENTS_TIMELINE_START_MS;
    using velox::contract::fields::DELIVERY_PLAN_RETRY_BUDGET;
    if (JOB_ID != "job_id") return 1;
    if (RENDER_MANIFEST != "render_manifest") return 2;
    if (DELIVERY_PLAN != "delivery_plan") return 3;
    if (RENDER_MANIFEST_TRACKS_EVENTS_TIMELINE_START_MS != "timeline_start_ms") return 4;
    if (DELIVERY_PLAN_RETRY_BUDGET != "retry_budget") return 5;
    return 0;
}
CPP
"$cxx" -std=c++20 -Wall -Wextra -Werror \
    -I"$REPO_ROOT/RemoteCodex/native/video-engine-cpp/include" \
    "$tmpdir/contract_fields_check.cpp" -o "$tmpdir/contract_fields_check"
"$tmpdir/contract_fields_check"

printf 'contract schema parity + generated binding freshness: OK\n'
