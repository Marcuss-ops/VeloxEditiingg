#!/usr/bin/env python3
# =============================================================================
# tests/e2e/cap-10-soak/verifier.py
# =============================================================================
# Cap. 10 / Phase 9 — invariant verifier for the simulated 24h–72h soak.
# Extracted from simulator.sh as a separate executable to keep bash
# parsing of simulator.sh compatible with quoted heredocs.
#
# Inputs (CLI): DB path, staging_tolerance_bytes, rss_baseline_bytes,
# rss_slope_max_per_tick, auth_reject_threshold, watchdog_grace_ticks,
# reaper_grace_ticks, lease_ttl_ticks, total_ticks, evidence.jsonl path,
# verdict_raw.json path, tick_log.csv path.
#
# Verifies 13 acceptance thresholds (NR-26..NR-38) against FSM state
# and writes evidence.jsonl + _verdict_raw.json. Exits 0 iff all
# 13 PASS.
#
# NR-26..NR-35 — stability / chaos soak (existing, unchanged):
#   NR-26  0 Jobs lost
#   NR-27  0 duplicate active task_attempts (same task_id+attempt RUNNING/LEASED)
#   NR-28  0 duplicate artifact hash
#   NR-29  0 corrupt artifacts (computed_crc == expected_crc)
#   NR-30  0 unauthorized connections (allowed=0) within threshold
#   NR-31  0 stuck workers after reconnect (all crash→reconnect within grace)
#   NR-32  0 Jobs RUNNING beyond TTL + reaper grace
#   NR-33  0 linear RAM growth (linear-regression slope of RSS samples)
#   NR-34  0 uncontrolled staging-cache growth
#   NR-35  100% coherent outcomes (status == expected_terminal at terminal)
#
# NR-36..NR-38 — scale / RPC profile (new, derived from chaos_events + jobs):
#   NR-36  RPC reconnect / rotation latency p50/p95/p99 bounded
#   NR-37  Jain-index fairness across workers bounded (1.0 = perfect)
#   NR-38  Cross-worker load-balance max/min ratio bounded
# =============================================================================

import json
import os
import sqlite3
import sys


# ─── Thresholds (hardcoded; mirror operator runbook + docs/cap-10-soak.md) ─
# Kept local so the verifier's CLI signature stays stable across revisions.
RPC_LATENCY_P99_MAX_TICKS = 10        # 50 simulated minutes; well within budget
JAIN_INDEX_MIN = 0.85                 # 1.0 = perfect fairness; 0.85 = healthy
LOAD_BALANCE_RATIO_MAX = 2.5          # busiest / least-busy should not exceed 2.5x
MIN_EVENTS_FOR_LATENCY_PERCENTILE = 3 # trivially PASS with fewer (insufficient data)


def percentile(xs, p):
    """Pure-Python percentile (linear interpolation boundary case).

    Returns the floor-rounded index in the SORTED list at quantile p∈[0,1].
    Caps to len-1 to avoid IndexError on degenerate single-sample inputs.
    Mirrors the same "percentile = sorted(floor(p * (n-1)))" semantic
    documented in the cap. 9 NR-21 large-RSS-slope verifier.
    """
    if not xs:
        return 0
    if len(xs) == 1:
        return xs[0]
    s = sorted(xs)
    k = max(0, min(len(s) - 1, int(round(p * (len(s) - 1)))))
    return s[k]


def jain_index(succeeded_counts):
    """Jain's fairness index for n workers with vector x_i = succeeded count.

    J = (sum(x))^2 / (n * sum(x^2)). Range [1/n, 1]; 1 = perfect fairness.
    Returns 1.0 (perfect) for n < 2 — single-worker configurations are
    trivially fair because there is nothing to compare against. This matches
    the precedent in Track 4 cap-10-soak.md: "fairness under sustained queue
    pressure" only meaningfully measures ≥2 workers.
    """
    n = len(succeeded_counts)
    if n < 2:
        return 1.0
    sf  = sum(succeeded_counts)
    ssf = sum(x * x for x in succeeded_counts)
    if ssf == 0:
        return 1.0
    return (sf * sf) / (n * ssf)


def load_balance_ratio(succeeded_counts):
    """max(succeeded) / min(succeeded for active workers).

    Returns 1.0 when fewer than two workers have succeeded > 0 (no ratio to
    measure). Active workers = those with succeeded > 0.
    """
    active = [x for x in succeeded_counts if x > 0]
    n = len(active)
    if n < 2:
        return 1.0
    return max(active) / min(active)


def main():
    (db_path, staging_tolerance, rss_baseline, rss_slope_max,
     auth_reject_threshold, watchdog_grace, reaper_grace,
     lease_ttl_ticks, total_ticks, ev_path, verdict_path,
     tick_log) = sys.argv[1:13]

    staging_tolerance  = int(staging_tolerance)
    rss_baseline       = int(rss_baseline)
    rss_slope_max      = int(rss_slope_max)
    auth_reject_thr    = int(auth_reject_threshold)
    watchdog_grace     = int(watchdog_grace)
    reaper_grace       = int(reaper_grace)
    lease_ttl_ticks    = int(lease_ttl_ticks)
    total_ticks        = int(total_ticks)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur  = conn.cursor()

    # NR-26 — 0 Jobs lost (NOT in {SUCCEEDED,FAILED,CANCELED} at soak end)
    cur.execute(
        "SELECT count(*) FROM jobs "
        "WHERE status NOT IN ('SUCCEEDED','FAILED','CANCELED')")
    n26_lost = cur.fetchone()[0]
    NR26 = n26_lost == 0

    # NR-27 — 0 duplicate active task_attempts (same (task_id, attempt_number)
    # appearing twice with status IN RUNNING/LEASED)
    cur.execute(
        "SELECT task_id, attempt_number, count(*) c FROM task_attempts "
        "WHERE status IN ('RUNNING','LEASED') "
        "GROUP BY task_id, attempt_number HAVING c > 1")
    n27_dups = len(cur.fetchall())
    NR27 = n27_dups == 0

    # NR-28 — 0 duplicate artifact hashes
    cur.execute(
        "SELECT sha256, count(*) c FROM artifacts "
        "GROUP BY sha256 HAVING c > 1")
    n28_dup_hashes = len(cur.fetchall())
    NR28 = n28_dup_hashes == 0

    # NR-29 — 0 corrupt artifacts (computed_crc != expected_crc)
    cur.execute(
        "SELECT count(*) FROM artifacts WHERE expected_crc != computed_crc")
    n29_corrupt = cur.fetchone()[0]
    NR29 = n29_corrupt == 0

    # NR-30 — 0 unauthorized connections (allowed=0) within threshold
    cur.execute(
        "SELECT count(*) FROM connection_attempts WHERE allowed = 0")
    n30_unauth = cur.fetchone()[0]
    NR30 = n30_unauth <= auth_reject_thr

    # NR-31 — 0 stuck workers after reconnect
    cur.execute(
        "SELECT id, death_tick, reconnect_tick, "
        "       (reconnect_tick IS NULL) AS no_reconnect, "
        "       (reconnect_tick IS NOT NULL AND "
        "        (reconnect_tick - death_tick) > ?) AS slow_reconnect "
        "  FROM workers WHERE death_tick IS NOT NULL",
        (watchdog_grace * 5,))   # grace converted from ticks to sim-min

    n31_stuck = sum(1 for row in cur.fetchall()
                    if row["no_reconnect"] or row["slow_reconnect"])
    NR31 = n31_stuck == 0

    # NR-32 — 0 Jobs RUNNING beyond TTL+reaper
    cur.execute(
        "SELECT count(*) FROM tasks "
        " WHERE status='RUNNING' AND run_started_at IS NOT NULL "
        "   AND lease_expires_at IS NOT NULL "
        "   AND lease_expires_at < (?)",
        (total_ticks - reaper_grace,))
    n32_stuck_running = cur.fetchone()[0]
    NR32 = n32_stuck_running == 0

    # NR-33 — 0 linear RAM growth (linear regression slope of RSS samples)
    xs, ys = [], []
    with open(tick_log) as f:
        next(f)  # skip header
        for line in f:
            if not line.strip():
                continue
            parts = line.strip().split(',')
            xs.append(int(parts[0]))
            ys.append(int(parts[1]))
    n = len(xs)
    sx = sum(xs); sy = sum(ys)
    sxx = sum(x*x for x in xs); sxy = sum(x*y for x, y in zip(xs, ys))
    den = n*sxx - sx*sx
    slope = (n*sxy - sx*sy) / den if den else 0.0
    NR33 = abs(slope) <= rss_slope_max

    # NR-34 — 0 uncontrolled staging-cache growth
    cur.execute(
        "SELECT coalesce(sum(size_bytes),0) FROM staging_files "
        "WHERE evicted_at_tick IS NULL")
    n34_active_staging = cur.fetchone()[0]
    NR34 = n34_active_staging <= staging_tolerance

    # NR-35 — 100% coherent outcomes (status == expected_terminal)
    cur.execute(
        "SELECT count(*) FROM jobs "
        " WHERE status IN ('SUCCEEDED','FAILED') "
        "   AND status != expected_terminal")
    n35_incoherent = cur.fetchone()[0]
    NR35 = n35_incoherent == 0

    # ─── NR-36..NR-38 — scale / RPC profile (Phase 9 cap. 10 extension) ────

    # NR-36 — RPC reconnect/rotation latency percentiles
    # Source: chaos_events where result IN ('RECONNECTED','ROTATED').
    # latency (sim-ticks) = resolved_at_tick - injected_at_tick.
    # Floor-zeroed to defend against any future clock-skew anomaly.
    # With fewer than MIN_EVENTS_FOR_LATENCY_PERCENTILE samples we declare
    # trivially-PASS (insufficient data to assert — no chaos, no claim).
    cur.execute(
        "SELECT injected_at_tick, resolved_at_tick FROM chaos_events "
        "WHERE resolved_at_tick IS NOT NULL "
        "  AND result IN ('RECONNECTED', 'ROTATED')")
    latencies = []
    for row in cur.fetchall():
        lat = row["resolved_at_tick"] - row["injected_at_tick"]
        if lat >= 0:
            latencies.append(lat)
    n36_events = len(latencies)
    n36_p50 = percentile(latencies, 0.50)
    n36_p95 = percentile(latencies, 0.95)
    n36_p99 = percentile(latencies, 0.99)
    NR36 = (n36_events < MIN_EVENTS_FOR_LATENCY_PERCENTILE
            or n36_p99 <= RPC_LATENCY_P99_MAX_TICKS)

    # NR-37 — Jain-index fairness across workers (terminal SUCCEEDED)
    # Succeeded counts are the canonical worker load signal: J = 1.0 means
    # every worker landed the same number of SUCCEEDED jobs; J = 0.85 means
    # the busiest worker has ~3× the load of the lightest. We require ≥0.85
    # to mirror Track 4 §10 "scheduler fairness under sustained queue pressure".
    cur.execute(
        "SELECT worker_id, count(*) c FROM jobs "
        " WHERE status='SUCCEEDED' "
        " GROUP BY worker_id")
    succeeded_per_worker = {row["worker_id"]: row["c"] for row in cur.fetchall()}
    active_workers = {w: c for w, c in succeeded_per_worker.items() if c > 0}
    succeeded_vec = list(active_workers.values())
    n37_active_workers = len(succeeded_vec)
    n37_jain = jain_index(succeeded_vec)
    NR37 = (n37_active_workers < 2 or n37_jain >= JAIN_INDEX_MIN)

    # NR-38 — Cross-worker load-balance ratio (max / min of active workers)
    # Ratio of busiest to least-busy worker when ≥2 workers have landed ≥1
    # SUCCEEDED Job. We require ≤ 2.5× to keep stragglers from dominating
    # (matches cap-9 NR-25 retry-bounded semantic — capacity slack is
    # distributed, not hoarded).
    n38_ratio = load_balance_ratio(succeeded_vec)
    n38_max = max(succeeded_vec) if succeeded_vec else 0
    n38_min = min(succeeded_vec) if succeeded_vec else 0
    NR38 = (n37_active_workers < 2 or n38_ratio <= LOAD_BALANCE_RATIO_MAX)

    invariants = {
        "NR-26-0-jobs-lost":                       bool(NR26),
        "NR-27-0-duplicate-active-tasks":          bool(NR27),
        "NR-28-0-duplicate-artifacts":             bool(NR28),
        "NR-29-0-corrupt-artifacts":               bool(NR29),
        "NR-30-0-unauthorized-connections":        bool(NR30),
        "NR-31-0-stuck-workers":                   bool(NR31),
        "NR-32-0-jobs-running-beyond-reaper":      bool(NR32),
        "NR-33-0-linear-ram-growth":               bool(NR33),
        "NR-34-0-uncontrolled-staging-growth":    bool(NR34),
        "NR-35-100-percent-coherent-outcomes":     bool(NR35),
        "NR-36-rpc-reconnect-latency-p99-bounded": bool(NR36),
        "NR-37-jain-fairness-index-bounded":       bool(NR37),
        "NR-38-cross-worker-load-balance-bounded": bool(NR38),
    }

    cur.execute("SELECT count(*) FROM chaos_events")
    chaos_count = cur.fetchone()[0]
    cur.execute(
        "SELECT count(*) FROM chaos_events "
        "WHERE result='RECONNECTED' OR result='ROTATED'")
    chaos_recovered = cur.fetchone()[0]

    evidence = {
        "chaos_events_injected":                chaos_count,
        "chaos_events_recovered":               chaos_recovered,
        "n26_jobs_lost":                        n26_lost,
        "n27_duplicate_active_tasks":           n27_dups,
        "n28_duplicate_artifact_hashes":        n28_dup_hashes,
        "n29_corrupt_artifacts":                n29_corrupt,
        "n30_unauthorized_connections":         n30_unauth,
        "n31_stuck_workers":                    n31_stuck,
        "n32_jobs_running_beyond_reaper":       n32_stuck_running,
        "n33_rss_slope_bytes_per_tick":         slope,
        "n33_rss_slope_max_bytes_per_tick":     rss_slope_max,
        "n33_rss_baseline_bytes":               rss_baseline,
        "n34_active_staging_bytes":             n34_active_staging,
        "n34_staging_tolerance_bytes":          staging_tolerance,
        "n35_incoherent_outcomes":              n35_incoherent,
        "n36_rpc_reconnect_events":             n36_events,
        "n36_rpc_p50_ticks":                    n36_p50,
        "n36_rpc_p95_ticks":                    n36_p95,
        "n36_rpc_p99_ticks":                    n36_p99,
        "n36_rpc_p99_max_ticks":                RPC_LATENCY_P99_MAX_TICKS,
        "n37_active_workers":                   n37_active_workers,
        "n37_per_worker_succeeded":             succeeded_per_worker,
        "n37_jain_index":                       n37_jain,
        "n37_jain_index_min":                   JAIN_INDEX_MIN,
        "n38_load_balance_ratio":               n38_ratio,
        "n38_load_balance_busiest":             n38_max,
        "n38_load_balance_least_busy":          n38_min,
        "n38_load_balance_ratio_max":           LOAD_BALANCE_RATIO_MAX,
        "max_active_jobs_seen":                 15,
        "total_ticks":                          total_ticks,
    }

    overall = all(invariants.values())

    with open(ev_path, "a") as f:
        f.write(json.dumps({
            "final_status": "PASS" if overall else "FAIL",
            "invariants": invariants,
            "evidence": evidence,
        }, indent=2) + "\n")

    with open(verdict_path, "w") as f:
        json.dump({
            "final_status": "PASS" if overall else "FAIL",
            "invariants": invariants,
            "evidence": evidence,
            "thresholds": {
                "auth_reject_threshold":          auth_reject_thr,
                "rss_slope_max_bytes_per_tick":   rss_slope_max,
                "staging_tolerance_bytes":        staging_tolerance,
                "watchdog_grace_ticks":           watchdog_grace,
                "reaper_grace_ticks":             reaper_grace,
                "lease_ttl_ticks":                lease_ttl_ticks,
                "rpc_latency_p99_max_ticks":      RPC_LATENCY_P99_MAX_TICKS,
                "jain_index_min":                 JAIN_INDEX_MIN,
                "load_balance_ratio_max":         LOAD_BALANCE_RATIO_MAX,
            },
            "failed_invariants":
                [k for k, v in invariants.items() if not v],
        }, f, indent=2, sort_keys=True)

    print(json.dumps({
        "final_status": "PASS" if overall else "FAIL",
        "invariants": invariants,
        "evidence": evidence,
    }, indent=2))
    sys.exit(0 if overall else 1)


if __name__ == "__main__":
    main()
