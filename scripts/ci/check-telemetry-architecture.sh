#!/usr/bin/env bash
# scripts/ci/check-telemetry-architecture.sh
#
# Enforces the telemetry dependency direction without attempting a risky
# whole-repository metrics rewrite. The current compatibility surfaces are
# explicit allowlists; new catalog, formula, timer, or renderer-sink surfaces
# fail closed until they are routed through the canonical owner.
#
# Invariants:
#   1. shared/telemetry/catalog.json is the only shared event catalog source.
#   2. Derived KPI formula vocabulary is owned by the existing canonical
#      performance/metrics projection files, not arbitrary producers.
#   3. PhaseTimer has one definition and no production caller outside its
#      canonical compatibility owner; new parallel phase timers are rejected.
#   4. Render producers cannot write Prometheus or PerformanceReceipt sinks
#      directly. They emit facts to the recorder/transport boundary.
#
# Exit codes: 0 clean, 1 invariant violation.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

fail() {
  printf 'TELEMETRY ARCHITECTURE ERROR: %s\n' "$*" >&2
  exit 1
}

python3 - "$REPO_ROOT" <<'PY'
from pathlib import Path
import re
import sys

root = Path(sys.argv[1])
violations = []


def rel(path: Path) -> str:
    return path.relative_to(root).as_posix()


def production_files(suffixes):
    for base in (root / "shared", root / "DataServer", root / "RemoteCodex"):
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if not path.is_file() or path.suffix not in suffixes:
                continue
            name = path.name
            if name.endswith("_test.go") or name.endswith("_test.cpp") or name.endswith("_test.hpp"):
                continue
            if "/vendor/" in rel(path) or "/node_modules/" in rel(path):
                continue
            yield path


def matching(paths, pattern):
    expression = re.compile(pattern)
    for path in paths:
        text = path.read_text(errors="ignore")
        for number, line in enumerate(text.splitlines(), 1):
            if expression.search(line):
                yield path, number, line.strip()


# 1. One language-neutral event catalog source.
telemetry_root = root / "shared" / "telemetry"
source_catalogs = sorted(
    p for p in telemetry_root.iterdir()
    if p.is_file() and p.suffix.lower() in {".json", ".yaml", ".yml", ".toml"}
)
expected_catalog = telemetry_root / "catalog.json"
if source_catalogs != [expected_catalog]:
    violations.append(
        "shared/telemetry must contain exactly catalog.json as its event catalog source; "
        f"found {[rel(p) for p in source_catalogs]}"
    )

# These names were the old shape of parallel taxonomy registries. The loaded
# canonicalEventDescriptors variable is intentionally allowed; a second list
# of literal keys/origins/scopes/phases is not.
legacy_registry_names = re.compile(
    r"\b(?:canonicalEventKeys|canonicalOriginScope|canonicalPhaseSpecs|canonicalTelemetryEvents)\b"
)
for path, number, line in matching(
    (p for p in production_files({".go"}) if "shared/telemetry" in rel(p)),
    legacy_registry_names.pattern,
):
    violations.append(f"parallel telemetry registry name at {rel(path)}:{number}: {line}")

# A C++ descriptor array is valid only in the generated projection. This
# catches hand-maintained C++ event catalogs without banning normal lookups.
for path, number, line in matching(
    (p for p in production_files({".cpp", ".hpp", ".h"})
     if "video-engine-cpp" in rel(p)
     and not rel(p).endswith("catalog_generated.hpp")),
    r"(?:std::array\s*<\s*EventDescriptor|\bEventDescriptor\s*\{)",
):
    violations.append(f"hand-maintained C++ telemetry descriptor at {rel(path)}:{number}: {line}")

# 2. Formula ownership ratchet. These are the currently distributed
# compatibility projections documented in telemetry-authority-map.md. A new
# formula-bearing production file must be deliberately reviewed and added to
# the canonical projection set; arbitrary producers cannot silently create a
# second definition.
formula_pattern = r"\b(?:accounted_ratio|read_amplification|write_amplification|cpu_wall_ratio|unaccounted_ms|render_factor|recoverable_ms|CacheHitRatio|CacheByteHitRatio|DuplicateDownloadRatio|TempStorageAmplification|EncodeAmplification|RenderSpeedRatio|RenderFactor|EncodeMsPerOutputMinute|CpuMsPerOutputMinute|DownloadThroughputBytesPerSec|SpeedupVsSerial|ParallelEfficiency)\b"
formula_owner_files = {
    "RemoteCodex/native/worker-agent-go/pkg/performance/assembler.go",
    "RemoteCodex/native/worker-agent-go/pkg/performance/performance_receipt_v1.go",
    "DataServer/internal/taskattempts/report.go",
    "DataServer/internal/taskattempts/report_ratios.go",
    "DataServer/internal/store/sqlite_task_attempt_repository.go",
    "DataServer/internal/store/sqlite_task_atomic_persistence_parallelism.go",
    "DataServer/internal/metrics/catalog_parallelism.go",
    "DataServer/internal/metrics/collector_attempts.go",
    "DataServer/internal/metrics/collector.go",
    "DataServer/internal/metrics/collector_engine.go",
    "DataServer/internal/metrics/render_performance.go",
    "DataServer/internal/metrics/render_performance_rollup.go",
    "DataServer/internal/metrics/render_performance_queries.go",
    "DataServer/internal/metrics/supervisor.go",
    "DataServer/internal/metrics/supervisor_sqlite_metrics.go",
    "DataServer/internal/store/sqlite_performance_repository.go",
}
formula_paths = (
    p for p in production_files({".go"})
    if any(part in rel(p) for part in ("/pkg/performance/", "/internal/metrics/", "/internal/taskattempts/", "/internal/store/"))
)
for path, number, line in matching(formula_paths, formula_pattern):
    if rel(path) not in formula_owner_files:
        violations.append(
            f"derived KPI formula outside canonical projection allowlist at {rel(path)}:{number}: {line}"
        )

# 3. One canonical PhaseTimer owner. EventRecorder and the native PhaseRecorder
# are the event journals; PhaseTimer is a legacy compatibility accumulator and
# must not spread to new production call sites.
phase_timer_owner = "RemoteCodex/native/worker-agent-go/internal/telemetry/canonical_phases.go"
phase_timer_definitions = list(matching(
    production_files({".go"}),
    r"\btype\s+PhaseTimer\s+struct\b",
))
if len(phase_timer_definitions) != 1 or rel(phase_timer_definitions[0][0]) != phase_timer_owner:
    violations.append(
        "PhaseTimer must have exactly one production definition in "
        f"{phase_timer_owner}; found {[rel(p) for p, _, _ in phase_timer_definitions]}"
    )
for path, number, line in matching(
    production_files({".go"}),
    r"\b(?:NewPhaseTimer|NewPhaseTimerWithClock)\s*\(",
):
    if rel(path) != phase_timer_owner:
        violations.append(f"parallel production PhaseTimer constructor at {rel(path)}:{number}: {line}")

# Transport mappings may read PhaseMS/DetailedPhases, but only the canonical
# parser/assembler boundary may write or assemble them. A new direct PhaseMS
# writer in a producer is a concurrent timer source.
phase_write_pattern = r"\b(?:PhaseMS\s*\[[^\]]+\]\s*=|PhaseMS\s*=|DetailedPhases\s*=)"
phase_write_owners = {
    "RemoteCodex/native/worker-agent-go/pkg/video/services/native/binary_resolver.go",
    "RemoteCodex/native/worker-agent-go/pkg/video/pipeline/runner.go",
    "RemoteCodex/native/worker-agent-go/pkg/performance/assembler.go",
    "RemoteCodex/native/worker-agent-go/internal/taskrunner/runner_report.go",
    "RemoteCodex/native/worker-agent-go/internal/taskrunner/runner.go",
}
for path, number, line in matching(
    production_files({".go"}), phase_write_pattern
):
    if rel(path) not in phase_write_owners:
        violations.append(f"phase timing write outside canonical owner at {rel(path)}:{number}: {line}")

# 4. Render producers must not know Prometheus or receipt sinks. Operational
# lifecycle metrics remain a documented compatibility surface elsewhere, but
# renderer code can only emit facts/transport data and let projections consume
# them later.
renderer_roots = (
    root / "RemoteCodex" / "native" / "worker-agent-go" / "pkg" / "video",
    root / "RemoteCodex" / "native" / "worker-agent-go" / "internal" / "taskrunner" / "executors",
    root / "RemoteCodex" / "native" / "video-engine-cpp" / "src",
)
renderer_files = (
    p for base in renderer_roots if base.exists() for p in base.rglob("*")
    if p.is_file() and p.suffix in {".go", ".cpp", ".hpp", ".h"}
    and not p.name.endswith("_test.go") and not p.name.endswith("_test.cpp")
)
for path, number, line in matching(
    renderer_files,
    r"(?:GetPrometheusMetrics\s*\(|\bPrometheusMetrics\b|PerformanceReceiptV1\s*\{|NewPerformanceReceiptV1\s*\(|\bDerivedMetrics\s*\{|promauto\.|prometheus\.MustRegister)",
):
    violations.append(f"direct telemetry sink reference from renderer at {rel(path)}:{number}: {line}")

# The receipt constructor is a single projection boundary. A new production
# literal or constructor call outside the performance package is a bypass.
for path, number, line in matching(
    production_files({".go"}),
    r"(?:PerformanceReceiptV1\s*\{|NewPerformanceReceiptV1\s*\(|DerivedMetrics\s*\{)",
):
    path_name = rel(path)
    if path_name not in {
        "RemoteCodex/native/worker-agent-go/pkg/performance/assembler.go",
        "RemoteCodex/native/worker-agent-go/pkg/performance/performance_receipt_v1.go",
    }:
        violations.append(f"receipt construction outside assembler boundary at {path_name}:{number}: {line}")

if violations:
    print("\n".join(f"  - {item}" for item in violations), file=sys.stderr)
    sys.exit(1)

print("telemetry architecture invariants: OK")
PY

# Keep the shell wrapper itself free of accidental whitespace regressions.
git diff --check -- scripts/ci/check-telemetry-architecture.sh
