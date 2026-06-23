#!/usr/bin/env bash
# scripts/ci/check-compute-outcome-labels.sh
#
# CI guard: spec \u00a714 compute-outcome single-family shape enforcement.
#
# The 4 retired split-out family names from the pre-spec \u00a714 compute
# outcome metrics are FORBIDDEN in any dashboard query, alert rule,
# or prometheus rule body:
#
#   velox_compute_seconds_total_failed       \u2192 velox_compute_seconds_total{outcome="failed"}
#   velox_compute_seconds_total_cancelled   \u2192 velox_compute_seconds_total{outcome="cancelled"}
#   velox_compute_seconds_total_stale        \u2192 velox_compute_seconds_total{outcome="stale"}
#   velox_compute_seconds_total_useful       \u2192 velox_compute_seconds_total{outcome="useful"}
#
# All canonical references MUST use the single-family shape:
#
#   velox_compute_seconds_total{outcome=useful|failed|cancelled|stale|speculative_lost}
#
# or the sibling failure-reason family:
#
#   velox_compute_failure_reasons_total{reason=...}
#
# ── Spec-version pin ────────────────────────────────────────────────────
# The 4 banned substrings hardcode the `seconds` family (the canonical
# spec \u00a714 unit). If a future spec introduces
# `velox_compute_hours_total{outcome=...}` or
# `velox_compute_minutes_total{outcome=...}`, the equivalent retired
# shape for that unit MUST be added to BANNED_FAMILIES below by
# deliberate human decision \u2014 automatic detection across unit
# spellings is intentionally out of scope (the spec governs).
#
# ── Scope ────────────────────────────────────────────────────────────────
# 1. Target directories: dashboards/, prometheus/, alerts/. Their
#    absence is a build failure (regression that accidentally removed
#    the observability surface must be caught loudly).
# 2. File extensions under scan: .json (Grafana dashboards),
#    .yml / .yaml (Prometheus rule files). README documentation
#    (.md) is EXEMPT \u2014 README files legitimately reference the
#    banned names as part of documenting the ban. Without this
#    carve-out the guard would auto-flag its own self-documentation.
# 3. Scan mode: full-tree (no diff-scope). The 3 target dirs are NEW,
#    so full-tree and PR-time diff-scoping are equivalent. Full-tree
#    also catches any future regressions on a stale dir.
#
# ── Exit codes ───────────────────────────────────────────────────────────
# 0 OK
# 1 violation detected OR missing required directory (single failure
#   mode matches scripts/ci/check-architecture.sh convention).
#
# Rationale on exit codes: the project's existing CI guards
# (check-architecture.sh, check-task-runtime-invariants.sh,
# check-no-legacy.sh) all collapse to a single exit-1 failure
# shape. This guard follows the convention so the failure modes
# are uniform across the lint suite and the GitHub Actions
# surface area is identical (the step exits non-zero and the run
# goes red).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

# Banned substrings. Each must EXACTLY match the literal family name
# so the new shape's `outcome="useful"` substring is NOT a false
# positive: the new shape contains `_total{` (NOT `_total_useful`).
# See metrics_test.go's negative-assertion list (lines 282-289) for
# the canonical precedent on the substring boundaries.
BANNED_FAMILIES=(
  'velox_compute_seconds_total_failed'
  'velox_compute_seconds_total_cancelled'
  'velox_compute_seconds_total_stale'
  'velox_compute_seconds_total_useful'
)

# Target directories. Their absence is itself a build failure.
TARGETS=(dashboards prometheus alerts)

# File extensions under scan \u2014 see header note 2.
SCAN_EXTS=(json yml yaml)

# File-style extension flags for `grep --include`. We build the
# flags once and reuse per family so the per-iteration cost is O(1).
include_flags=()
for ext in "${SCAN_EXTS[@]}"; do
  include_flags+=(--include="*.${ext}")
done

# 1. Target directories must exist.
missing=0
for d in "${TARGETS[@]}"; do
  if [[ ! -d "$d" ]]; then
    printf 'GUARD ERROR: required directory %s/ is missing \u2014 spec \u00a714 observability surface not bootstrapped.\n' "$d" >&2
    missing=$((missing + 1))
  fi
done
if [[ "$missing" -gt 0 ]]; then
  printf '%d missing target director%s \u2014 cannot run guard.\n' \
    "$missing" "$([ "$missing" -eq 1 ] && echo y || echo ies)" >&2
  exit 1
fi

# 2. Substring scan per banned family. We use `grep -RInF` with
# `--include` flags so markdown README documentation that
# legitimately DESCRIBES the ban is exempt from the scan (without
# this carve-out the guard self-traps).
violations=0
for family in "${BANNED_FAMILIES[@]}"; do
  hits="$(grep -RInF "${include_flags[@]}" "${family}" "${TARGETS[@]}" 2>/dev/null || true)"
  if [[ -n "$hits" ]]; then
    printf 'GUARD ERROR: banned split-family name "%s" found \u2014 use velox_compute_seconds_total{outcome=...} instead:\n' "$family" >&2
    printf '%s\n' "$hits" >&2
    printf '\n' >&2
    violations=$((violations + 1))
  fi
done

# 3. Companion check: when the new shape is in use (single-family
# `velox_compute_seconds_total{outcome=...}`), surface a stderr WARN
# if it is NOT appearing in the scanned files. We scope to the
# extension list (same as the violation scan) so the README docs
# that describe the shape but don't run queries do NOT false-
# trigger the WARN.
if ! grep -RInF "${include_flags[@]}" 'velox_compute_seconds_total{outcome=' "${TARGETS[@]}" 2>/dev/null | grep -q .; then
  printf 'GUARD WARN: no velox_compute_seconds_total{outcome=...} PromQL reference found under %s (scanned extensions: %s) \u2014 dashboards/alerts may not yet consume the new shape.\n' \
    "${TARGETS[*]}" "${SCAN_EXTS[*]}" >&2
fi

if [[ "$violations" -gt 0 ]]; then
  printf '%d compute-outcome-label violation%s \u2014 see above (exit 1).\n' \
    "$violations" "$([ "$violations" -eq 1 ] && echo '' || echo s)" >&2
  exit 1
fi

echo "check-compute-outcome-labels: OK"
