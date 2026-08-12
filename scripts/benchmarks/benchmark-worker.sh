#!/usr/bin/env bash
#
# benchmark-worker.sh — the DEDICATED SELF-HOSTED benchmark worker
# entrypoint (plan §17 / gate_tiers.go).
#
# This is the ONLY place the TIER-2 performance gate runs: p50/p95 wall
# clock, throughput, CPU/wall ratio and read/write amplification are too
# noisy on shared GitHub runners (neighbor load, different CPU/storage,
# thermal state, VM scheduling), so they are gated HERE — on a stable,
# dedicated host — and NEVER in normal CI. Normal CI (test.yml → make
# verify) runs the TIER-1 deterministic gate through the Go test suite
# instead.
#
# Pipeline:
#   1. build the performance cmds (velox-benchmark, velox-fixture-gen,
#      velox-fixture-gate, velox-benchmark-compare)
#   2. generate the canonical fixture track (velox-fixture-gen) and
#      verify its manifest against the pinned spec digest
#   3. run the benchmark (velox-benchmark) — stub renderer by default
#      with a loud warning; production RenderRunner is wired via
#      VELOX_BENCH_REAL=1 once it exists
#   4. evaluate the TIER-2 performance gate (velox-fixture-gate
#      -tier performance)
#   5. compare against the stored baseline (velox-benchmark-compare
#      -fail-on-regression) when one exists; persist run + baseline
#
# HONESTY NOTE: the production RenderRunner does not exist yet, so
# VELOX_BENCH_REAL=1 currently FAILS (velox-benchmark exits "no
# RenderRunner configured") — that is fail-closed by design. The default
# stub path proves the worker pipeline (build → track → run → tier-2
# gate → baseline compare) end to end; real numbers come once the
# production RenderRunner lands.
#
# Usage:
#   scripts/benchmarks/benchmark-worker.sh
#
# Environment:
#   VELOX_BENCH_FIXTURE     fixture ID (default: COPY_ONLY_CANONICAL_5M_V1)
#   VELOX_BENCH_RUNS        renders per benchmark (default: 5)
#   VELOX_BENCH_CONCURRENCY max concurrent renders (default: 1)
#   VELOX_BENCH_CACHE       warm | cold (default: warm)
#   VELOX_BENCH_REAL        1 = use the real RenderRunner (must exist);
#                           default 0 = stub renderer, NOT a real benchmark
#   VELOX_BENCH_EVIDENCE    evidence dir (default: temp dir)
#   VELOX_BENCH_BASELINE    dir holding the previous baseline-run JSON
#                           (optional; compare is skipped without it)
#   VELOX_BENCH_KEEP        1 = keep an auto-created evidence dir
#   VELOX_BENCH_SKIP_COMPARE 1 = run + gate only, no baseline compare
#
# Exit codes:
#   0 all gates passed (and no baseline regression)
#   1 a render failed, a gate failed, or a regression was detected
#   2 missing dependency / invalid configuration

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
AGENT_DIR="${REPO_ROOT}/RemoteCodex/native/worker-agent-go"

FIXTURE="${VELOX_BENCH_FIXTURE:-COPY_ONLY_CANONICAL_5M_V1}"
RUNS="${VELOX_BENCH_RUNS:-5}"
CONCURRENCY="${VELOX_BENCH_CONCURRENCY:-1}"
CACHE="${VELOX_BENCH_CACHE:-warm}"
REAL="${VELOX_BENCH_REAL:-0}"
BASELINE_DIR="${VELOX_BENCH_BASELINE:-}"
SKIP_COMPARE="${VELOX_BENCH_SKIP_COMPARE:-0}"
EVIDENCE_DIR="${VELOX_BENCH_EVIDENCE:-}"
AUTO_EVIDENCE=0

fail_usage() {
  printf '[benchmark-worker][ERROR] %s\n' "$*" >&2
  exit 2
}
fail_run() {
  printf '[benchmark-worker][FAIL] %s\n' "$*" >&2
  exit 1
}

for tool in go ffmpeg ffprobe; do
  command -v "$tool" >/dev/null 2>&1 || fail_usage "missing dependency: ${tool}"
done
[[ "$RUNS" =~ ^[1-9][0-9]*$ ]] || fail_usage "VELOX_BENCH_RUNS must be a positive integer"
[[ "$CONCURRENCY" =~ ^[1-9][0-9]*$ ]] || fail_usage "VELOX_BENCH_CONCURRENCY must be a positive integer"
[[ "$CACHE" == warm || "$CACHE" == cold ]] || fail_usage "VELOX_BENCH_CACHE must be warm or cold"
[[ "$REAL" == 0 || "$REAL" == 1 ]] || fail_usage "VELOX_BENCH_REAL must be 0 or 1"

if [[ -z "$EVIDENCE_DIR" ]]; then
  EVIDENCE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/velox-benchmark-worker.XXXXXX")"
  AUTO_EVIDENCE=1
else
  mkdir -p "$EVIDENCE_DIR"
fi
cleanup() {
  if [[ "$AUTO_EVIDENCE" == 1 && "${VELOX_BENCH_KEEP:-0}" != 1 ]]; then
    rm -rf -- "$EVIDENCE_DIR"
  fi
}
trap cleanup EXIT

# ── 1. Build the performance cmds ────────────────────────────────────
printf '[benchmark-worker] building performance cmds (fixture=%s runs=%s concurrency=%s cache=%s)\n' \
  "$FIXTURE" "$RUNS" "$CONCURRENCY" "$CACHE"
BIN_DIR="$EVIDENCE_DIR/bin"
mkdir -p "$BIN_DIR"
(
  cd "$AGENT_DIR"
  for cmd in velox-benchmark velox-fixture-gen velox-fixture-gate velox-benchmark-compare; do
    go build -o "$BIN_DIR/$cmd" "./cmd/$cmd" || fail_usage "go build $cmd failed"
  done
)

# ── 2. Build the track + verify the manifest ─────────────────────────
TRACK_DIR="$EVIDENCE_DIR/track"
if [[ "$REAL" == 1 ]]; then
  "$BIN_DIR/velox-fixture-gen" -out-dir "$TRACK_DIR" || fail_run "fixture generation failed"
  "$BIN_DIR/velox-fixture-gen" -verify-manifest "$TRACK_DIR/manifest.json" >/dev/null || \
    fail_run "track manifest does not match the pinned spec digest"
  printf '[benchmark-worker] track generated and manifest verified (%s)\n' "$FIXTURE"
else
  printf '[benchmark-worker][WARNING] stub renderer: this is NOT a real benchmark. '
  printf 'Wire the production RenderRunner and set VELOX_BENCH_REAL=1 for real numbers.\n'
fi

# ── 3. Run the benchmark ─────────────────────────────────────────────
RUN_JSON="$EVIDENCE_DIR/run.json"
GIT_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
BENCH_ARGS=(-fixture "$FIXTURE" -runs "$RUNS" -concurrency "$CONCURRENCY" -cache "$CACHE" -out "$RUN_JSON")
if [[ "$REAL" == 0 ]]; then
  BENCH_ARGS+=(-stub)
fi
if ! "$BIN_DIR/velox-benchmark" -git-commit "$GIT_COMMIT" "${BENCH_ARGS[@]}"; then
  fail_run "velox-benchmark failed"
fi
printf '[benchmark-worker] benchmark run written to %s\n' "$RUN_JSON"

# ── 4. TIER-2 performance gate (dedicated worker ONLY) ───────────────
if ! "$BIN_DIR/velox-fixture-gate" -tier performance -fixture "$FIXTURE" -run "$RUN_JSON"; then
  fail_run "performance gate (tier 2) failed on $FIXTURE"
fi

# ── 5. Baseline compare + persist ────────────────────────────────────
if [[ "$SKIP_COMPARE" == 1 || -z "$BASELINE_DIR" ]]; then
  printf '[benchmark-worker][PASS] performance gate passed (baseline compare skipped)\n'
  exit 0
fi
BASELINE_JSON="$BASELINE_DIR/run.json"
if [[ ! -f "$BASELINE_JSON" ]]; then
  mkdir -p "$BASELINE_DIR"
  cp "$RUN_JSON" "$BASELINE_JSON"
  printf '[benchmark-worker][PASS] performance gate passed; no baseline yet — stored %s as the new baseline\n' "$RUN_JSON"
  exit 0
fi
if ! "$BIN_DIR/velox-benchmark-compare" -base "$BASELINE_JSON" -candidate "$RUN_JSON" -fail-on-regression; then
  fail_run "baseline regression detected on $FIXTURE"
fi
cp "$RUN_JSON" "$BASELINE_JSON"
printf '[benchmark-worker][PASS] performance gate + baseline compare passed; baseline updated\n'
