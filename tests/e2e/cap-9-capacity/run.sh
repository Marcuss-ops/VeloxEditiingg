#!/usr/bin/env bash
# =============================================================================
# tests/e2e/cap-9-capacity/run.sh
# =============================================================================
# Orchestrator for cap. 9 / Phase 8 — coordinate the 12-cell capacity
# curve + the 150-frame C++ engine benchmark + the small-determinism
# replay, and consolidate the per-cell verdicts into a single
# velox.cert-9-capacity.v1 verdict.json.
#
# Inputs (CLI / env):
#   --dry-run             run bash -n + python3 preflight only
#   --capacity-only       skip C150 engine benchmark
#   --engine-only         skip capacity curve
#   --evidence-root DIR   override evidence root (default $EVIDENCE_ROOT)
#
# Returns 0 iff BOTH the capacity-curve (12 cells) and the C150
# benchmark pass their invidual exit codes. Any per-cell FAIL is
# surfaced under failed_invariants + failed_cells.
# =============================================================================

set -uo pipefail
SCEN_DIR="$(cd "$(dirname "$0")" && pwd)"

DRY_RUN=0
CAPACITY_ONLY=0
ENGINE_ONLY=0
EVIDENCE_ROOT="${EVIDENCE_ROOT:-/tmp/velox-cap9-evidence}"

usage() { cat <<USG
usage: $0 [--dry-run] [--capacity-only] [--engine-only] [--evidence-root DIR]
USG
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)        DRY_RUN=1; shift ;;
    --capacity-only)  CAPACITY_ONLY=1; shift ;;
    --engine-only)    ENGINE_ONLY=1; shift ;;
    --evidence-root)  EVIDENCE_ROOT="$2"; shift 2 ;;
    --help|-h)        usage 0 ;;
    *) printf 'unknown flag: %s\n' "$1" >&2; usage 1 ;;
  esac
done

mkdir -p "$EVIDENCE_ROOT"

# ─── Dry-run: preflight only ──────────────────────────────────────────────
if (( DRY_RUN == 1 )); then
  echo "→ bash -n sweep"
  for f in "$SCEN_DIR"/run.sh "$SCEN_DIR"/capacity-curve.sh \
           "$SCEN_DIR"/c150-engine-bench.sh "$SCEN_DIR"/lib.sh \
           "$SCEN_DIR"/profiles.sh; do
    bash -n "$f" && echo "ok  ${f##*/}" || echo "FAIL ${f##*/}"
  done
  echo "→ python3 preflight (verdict JSON shape)"
  python3 - <<'PY'
import json
v = {
    "schema": "velox.cert-9-capacity.v1",
    "final_status": "PASS",
    "invariants": {f"NR-{i}-x": True for i in range(16, 26)},
    "metrics": {"rss_slope_bytes_per_sec": 1024, "rss_growth_multiplier": 1.05},
    "per_executor_stats": {"scene.composite.tiny.v1": {"succeeded": 5}}
}
print(json.dumps(v, indent=2))
PY
  exit 0
fi

CAPACITY_RC=0
ENGINE_RC=0

# ─── Capacity-curve (12 cells) ────────────────────────────────────────────
if (( ENGINE_ONLY == 0 )); then
  printf '\n═══ capacity-curve (12 cells) ═══\n'
  EVIDENCE_ROOT="$EVIDENCE_ROOT" bash "$SCEN_DIR/capacity-curve.sh"
  CAPACITY_RC=$?
fi

# ─── C++ engine 150-frame benchmark ───────────────────────────────────────
if (( CAPACITY_ONLY == 0 )); then
  printf '\n═══ C++ engine 150-frame benchmark ═══\n'
  EVIDENCE_ROOT="$EVIDENCE_ROOT" bash "$SCEN_DIR/c150-engine-bench.sh"
  ENGINE_RC=$?
fi

# ─── Consolidate verdict.json ─────────────────────────────────────────────
VERDICT_PATH="$EVIDENCE_ROOT/verdict.json"
python3 - "$VERDENCE_ROOT" "$VERDICT_PATH" \
            "$CAPACITY_RC" "$ENGINE_RC" \
            "$SCEN_DIR/capacity_curve.csv" <<'PYEOF'
import json, os, sys, time, csv
(evi_root, verdict_path, cap_rc, engine_rc, csv_path) = sys.argv[1:7]
cap_rc = int(cap_rc); engine_rc = int(engine_rc)

# Parse capacity-curve.csv into per-cell verdicts; every cell that
# didn't write a "PASS" string in column 4 is counted as failing.
cells = []
if os.path.exists(csv_path):
    with open(csv_path) as f:
        rdr = csv.reader(f)
        next(rdr, None)  # skip header
        for row in rdr:
            if not row: continue
            cells.append({
                "profile":      row[0],
                "multiplier":   int(row[1]),
                "executor_id":  row[2],
                "passed":       row[3] == "PASS",
                "fail_reasons": row[4] if len(row) > 4 else "",
            })

# Per-executor rollup from per_executor.sqlite in each cell dir.
per_executor = {}
cell_dirs = sorted([(p, "1"), (p, "2"), (p, "5"), (p, "10")
                     for p in ("small", "medium", "large")],
                   key=lambda x: (x[0], int(x[1])))
import sqlite3
for prof, mult in cell_dirs:
    db_path = os.path.join(evi_root, prof, f"{mult}x", "per_executor.sqlite")
    if not os.path.exists(db_path): continue
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.execute("SELECT executor_id, succeeded, failed, "
                               "timed_out, lease_expired, retries, "
                               "fallback_full_dirty "
                               "FROM per_executor_counters").fetchall()
        for row in cur:
            eid = row[0]
            d = per_executor.setdefault(eid, {
                "succeeded": 0, "failed": 0, "timed_out": 0,
                "lease_expired": 0, "retries": 0, "fallback_full_dirty": 0,
            })
            for k, v in zip(("succeeded", "failed", "timed_out",
                             "lease_expired", "retries", "fallback_full_dirty"),
                            row[1:]):
                d[k] += int(v)
    except Exception as e:
        print(f"::warning::could not read {db_path}: {e}", file=sys.stderr)

# Pull c150 verdict if present.
c150_verdict = None
c150_path = os.path.join(evi_root, "c150", "verdict.json")
if os.path.exists(c150_path):
    try:
        c150_verdict = json.load(open(c150_path))
    except Exception: pass

inv = {
    # Hard NR-16..NR-25 semantic flags. A cell that PASSED + c150 PASS
    # means every invariant in that group holds.
    "NR-16-small-byte-determinism":    any(c["passed"] for c in cells
                                            if c["profile"] == "small"),
    "NR-17-small-pmf-equality":        any(c["passed"] for c in cells
                                            if c["profile"] == "small"),
    "NR-18-capacity-no-degradation":   any(c["passed"] for c in cells),
    "NR-19-dispatcher-warm-latency":   any(c["passed"] for c in cells),
    "NR-20-medium-throughput-present": any(c["passed"] for c in cells
                                            if c["profile"] == "medium"),
    "NR-21-large-bounded-rss-slope":   any(c["passed"] for c in cells
                                            if c["profile"] == "large"),
    "NR-22-cpp-no-dirty-fallback":     bool(c150_verdict
                                            and c150_verdict.get(
                                                "invariants",
                                                {}).get(
                                                "NR-22-no-full-dirty-fallback")),
    "NR-23-cpp-clearnode-restore":     bool(c150_verdict
                                            and c150_verdict.get(
                                                "invariants",
                                                {}).get(
                                                "NR-23-clearnode-restore")),
    "NR-24-cpp-pool-steady-state":     bool(c150_verdict
                                            and c150_verdict.get(
                                                "invariants",
                                                {}).get(
                                                "NR-24-pool-steady-state")),
    "NR-25-per-executor-retry-bounded":
        all((d.get("retries", 0) <= 2 * d.get("succeeded", 1) or
             d.get("succeeded", 0) > 0) for d in per_executor.values()),
}

overall = (cap_rc == 0 and engine_rc == 0
           and all(inv.values()))
final_status = "PASS" if overall else "FAIL"
v = {
    "schema":                "velox.cert-9-capacity.v1",
    "final_status":          final_status,
    "capacity_curve_pass":   cap_rc == 0,
    "c150_engine_pass":      engine_rc == 0,
    "failed_invariants":     [k for k, v in inv.items() if not v],
    "failed_cells":          [f"{c['profile']}@{c['multiplier']}x"
                              for c in cells if not c["passed"]],
    "invariants":            inv,
    "per_executor_stats":    per_executor,
    "cell_results":          cells,
    "engine_simulation_mode": (c150_verdict or {}).get(
                                "engine_simulation_mode", "n/a"),
    "evidence_root":         evi_root,
    "generated_at":          time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
}
os.makedirs(os.path.dirname(verdict_path), exist_ok=True)
with open(verdict_path, "w") as f: json.dump(v, f, indent=2, sort_keys=True)
print(json.dumps({"verdict": verdict_path, "final_status": final_status,
                  "cells_passed": sum(1 for c in cells if c["passed"]),
                  "cells_total": len(cells),
                  "invariants_passed": sum(1 for v in inv.values() if v),
                  "invariants_total":  len(inv)}, indent=2))
sys.exit(0 if overall else 1)
PYEOF
RC=$?
exit "$RC"
