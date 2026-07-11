#!/usr/bin/env bash
# =============================================================================
# tests/e2e/cap-10-soak/run.sh
# =============================================================================
# Orchestrator for cap. 10 / Phase 9 — the 24h–72h soak simulator.
#
# CLI flag surface mirrors cap-9 run.sh so cap-10 has the same operator UX:
#
#   --dry-run             bash -n + python3 preflight only
#   --24h | --48h | --72h soak-duration (default 24h)
#   --evidence-root DIR   override EVIDENCE_ROOT
#
# Returns 0 iff the simulator's 10 invariants all pass, OR the dry-run
# preflight completes successfully.
# =============================================================================

set -uo pipefail
SCEN_DIR="$(cd "$(dirname "$0")" && pwd)"

DRY_RUN=0
DURATION_HOURS=24
EVIDENCE_ROOT="${EVIDENCE_ROOT:-/tmp/velox-cap10-evidence}"

usage() { cat <<USG
usage: $0 [--dry-run] [--24h | --48h | --72h] [--evidence-root DIR]
USG
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)        DRY_RUN=1; shift ;;
    --24h)            DURATION_HOURS=24; shift ;;
    --48h)            DURATION_HOURS=48; shift ;;
    --72h)            DURATION_HOURS=72; shift ;;
    --evidence-root)  EVIDENCE_ROOT="$2"; shift 2 ;;
    --help|-h)        usage 0 ;;
    *) printf 'unknown flag: %s\n' "$1" >&2; usage 1 ;;
  esac
done

mkdir -p "$EVIDENCE_ROOT"

if (( DRY_RUN == 1 )); then
  echo "→ bash -n sweep (cap-10 simulator + lib)"
  for f in "$SCEN_DIR"/run.sh "$SCEN_DIR"/simulator.sh "$SCEN_DIR"/lib.sh; do
    bash -n "$f" && echo "ok  ${f##*/}" || echo "FAIL ${f##*/}"
  done
  echo "→ python3 preflight (verdict JSON shape)"
  python3 - "$DURATION_HOURS" <<'PY'
import json, sys
hours = int(sys.argv[1])
v = {
    "schema": "velox.cert-10-soak.v1",
    "worker_id": "(operator-supplied)",
    "cert_date": "(operator-supplied)",
    "soak_duration_hours": hours,
    "final_status": "PASS",
    "failed_invariants": [],
    "invariants": {f"NR-{i}-x": True for i in range(26, 36)},
    "evidence": {
        "chaos_events_injected": 0,
        "chaos_events_recovered": 0,
        "max_active_jobs_seen":   15,
        "total_jobs_completed":   0,
        "rss_bytes_start":        32000000,
        "rss_bytes_peak":         32000000,
        "staging_cache_peak_bytes": 0,
    },
    "thresholds": {
        "auth_reject_threshold": 20,
        "rss_slope_max_bytes_per_tick": 53000,
        "staging_tolerance_bytes": 1500000000,
        "watchdog_grace_ticks":  2,
        "reaper_grace_ticks":    2,
        "lease_ttl_ticks":       6,
    },
    "generated_at": "1970-01-01T00:00:00Z",
}
print(json.dumps({"hours": hours, "verdict_shape_keys": list(v.keys())}, indent=2))
PY
  exit 0
fi

printf '\n═══ cap-10 soak simulator (%dh compressed) ═══\n' "$DURATION_HOURS"
DURATION_HOURS="$DURATION_HOURS" EVIDENCE_ROOT="$EVIDENCE_ROOT" \
  bash "$SCEN_DIR/simulator.sh"
SIM_RC=$?

# Aggregate verdict.json schema wrapper (velox.cert-10-soak.v1).
VERDICT_PATH="$EVIDENCE_ROOT/verdict.json"
python3 - "$EVIDENCE_ROOT" "$DURATION_HOURS" "$SIM_RC" "$VERDICT_PATH" <<'PYEOF'
import json, os, sys, time
(evi_root, hours, sim_rc, verdict_path) = sys.argv[1:5]
sim_rc = int(sim_rc)
raw_path = os.path.join(evi_root, "_verdict_raw.json")
raw = {}
if os.path.exists(raw_path):
    raw = json.load(open(raw_path))
inv = raw.get("invariants", {})
evidence = raw.get("evidence", {})
final_status = "PASS" if (sim_rc == 0 and all(inv.values())) else "FAIL"
v = {
    "schema": "velox.cert-10-soak.v1",
    "worker_id": "host-cap10-simulator",
    "cert_date": time.strftime("%Y-%m-%d"),
    "soak_duration_hours": int(hours),
    "final_status": final_status,
    "failed_invariants": [k for k, val in inv.items() if not val],
    "invariants": inv,
    "evidence": evidence,
    "thresholds": raw.get("thresholds", {}),
    "evidence_root": evi_root,
    "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
}
with open(verdict_path, "w") as f:
    json.dump(v, f, indent=2, sort_keys=True)
print(json.dumps({
    "verdict": verdict_path,
    "final_status": final_status,
    "invariants_passed": sum(1 for val in inv.values() if val),
    "invariants_total":  len(inv),
}, indent=2))
sys.exit(0 if final_status == "PASS" else 1)
PYEOF
RC=$?
exit "$RC"
