#!/usr/bin/env bash
# Offline regression tests for check-loc-thresholds.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHECK="$ROOT/scripts/ci/check-loc-thresholds.sh"
WORK="$(mktemp -d /tmp/velox-loc-thresholds.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

fail() {
  printf 'loc-thresholds-test: FAIL: %s\n' "$*" >&2
  exit 1
}
pass() { printf 'loc-thresholds-test: %s\n' "$*"; }

mkdir -p "$WORK/repo/src" \
  "$WORK/repo/generated" \
  "$WORK/repo/vendor" \
  "$WORK/repo/build/CMakeFiles" \
  "$WORK/repo/.github/workflows"

python3 - "$WORK/repo" <<'PY'
from pathlib import Path
import sys

root = Path(sys.argv[1])

def write_lines(path: Path, count: int, prefix: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{prefix}{i}\n" for i in range(count)))

# Exact boundary is allowed; the gate is strictly greater-than 900.
write_lines(root / "src/exact.cpp", 900, "line_")
write_lines(root / "src/exact.hpp", 900, "line_")

# Generated, vendored, CMake build, and CI workflow files must never gate.
write_lines(root / "generated/large.cpp", 1400, "generated_")
write_lines(root / "src/large_generated.hpp", 1400, "generated_")
write_lines(root / "vendor/large.cpp", 1400, "vendor_")
write_lines(root / "build/CMakeFiles/large.hpp", 1400, "build_")
write_lines(root / ".github/workflows/large.cpp", 1400, "workflow_")

# Structural exceptions remain warning-only / known carry-over, not failures.
write_lines(root / "CHANGELOG.md", 1400, "release_")
write_lines(root / "DataServer/api/openapi.yaml", 1400, "schema_")
PY

# Boundary and exclusion checks: no C/C++ violation should be reported.
if ! LOC_GATE_ROOT="$WORK/repo" bash "$CHECK" >"$WORK/pass.out" 2>&1; then
  cat "$WORK/pass.out" >&2
  fail 'exact-boundary or excluded C/C++ files caused a failure'
fi
grep -q 'LOC gate:' "$WORK/pass.out"
grep -q 'file=CHANGELOG.md::STRUCTURAL long-file (no gate)' "$WORK/pass.out" \
  || fail 'CHANGELOG.md structural warning missing'
grep -q 'file=DataServer/api/openapi.yaml::STRUCTURAL long-file (no gate)' "$WORK/pass.out" \
  || fail 'OpenAPI structural warning missing'
if grep -q 'prod-cpp LOC' "$WORK/pass.out"; then
  cat "$WORK/pass.out" >&2
  fail 'generated/build/vendor/workflow C/C++ file reached the gate'
fi

# Add both extension families above the hard boundary and require failure.
python3 - "$WORK/repo/src/over.cpp" "$WORK/repo/src/over.hpp" <<'PY'
from pathlib import Path
import sys
for raw in sys.argv[1:]:
    path = Path(raw)
    path.write_text("".join(f"over_{i}\n" for i in range(901)))
PY
if LOC_GATE_ROOT="$WORK/repo" bash "$CHECK" >"$WORK/fail.out" 2>&1; then
  cat "$WORK/fail.out" >&2
  fail 'over-limit .cpp/.hpp files unexpectedly passed'
fi
grep -q '::error file=./src/over.cpp::prod-cpp LOC 901 exceeds refactor-required threshold 900' "$WORK/fail.out" \
  || fail '.cpp violation annotation missing or changed'
grep -q '::error file=./src/over.hpp::prod-cpp LOC 901 exceeds refactor-required threshold 900' "$WORK/fail.out" \
  || fail '.hpp violation annotation missing or changed'

auto_pass='C/C++ threshold, generated/build/vendor exclusions, and structural exceptions passed'
pass "$auto_pass"
