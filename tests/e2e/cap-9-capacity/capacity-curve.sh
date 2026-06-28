#!/usr/bin/env bash
# =============================================================================
# tests/e2e/cap-9-capacity/capacity-curve.sh
# =============================================================================
# Main capacity-curve simulator for cap. 9 / Phase 8. Drives the 12-cell
# matrix (3 profiles × 4 capacity multipliers) and emits per-cell evidence
# + per-cell verdicts that run.sh aggregates into verdict.json.
#
# Per (profile, multiplier) cell, it:
#   1. Builds a SQLite DB sized to N = MULTIPLIER * 8 baseline jobs.
#   2. Runs the FSM in a fast advancing-loop, sampling RSS at intervals
#      proportional to the profile's expected duration.
#   3. For SMALL: assembles 5 reruns of the deterministic synthetic frame
#      and asserts byte-identical sha256 + lex-sorted PMF equality (NR-16, NR-17).
#   4. For LARGE:  asserts bounded RSS growth (TOT_GROWTH < 1.5×MIN) +
#      linear-regression slope (NR-21).
#   5. Bumps per-executor counters (succeeded / failed / retries) so
#      NR-25 retry bounds can be checked at orchestration time.
#
# Returns final rollup exit code 0 iff every cell passed.
# =============================================================================

set -uo pipefail
SCEN_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./lib.sh
source "$SCEN_DIR/lib.sh"
# shellcheck source=./profiles.sh
source "$SCEN_DIR/profiles.sh"

EVIDENCE_ROOT="${EVIDENCE_ROOT:-/tmp/velox-cap9-evidence}"
mkdir -p "$EVIDENCE_ROOT"

# ─── Cap-9 invocation knobs ───────────────────────────────────────────────
PROFILES=("small" "medium" "large")
MULTIPLIERS=(1 2 5 10)
BASELINE_JOBS=8                # jobs at multiplier=1; scales linearly
N_RERUNS_SMALL=5               # needed for byte-determinism (NR-16)
RSS_SAMPLE_BUDGET=20           # samples for Large profile linear-regression
CSV_OUT="$EVIDENCE_ROOT/capacity_curve.csv"
printf 'profile,multiplier,executor_id,cell_pass,fail_reasons\n' >"$CSV_OUT"

# ─── FSM advancer ─────────────────────────────────────────────────────────
run_cell() {
  local profile="$1" multiplier="$2"
  rm_begin_cell "$profile" "$multiplier"
  profile_for "$profile"
  local cells_in_flight_budget=$multiplier
  local db="$CELL_DIR/cell.sqlite"

  init_cap9_db "$db"

  # Seed N jobs, each with N_CUTS tasks (for Medium/Large composite work)
  local jobs_n=$(( BASELINE_JOBS * multiplier ))
  local tasks_per_job=1
  [[ "$profile" != "small" ]] && tasks_per_job=$N_CUTS

  local NOW; NOW=$(date +%s)
  sqlite3 "$db" <<SQL >/dev/null
$(for j in $(seq 1 "$jobs_n"); do
    job_id="job-${profile}-${multiplier}x-${j}"
    echo "INSERT INTO jobs(id,status,video_name,spec_hash,executor_id,worker_class)
          VALUES ('$job_id','PENDING','vidcap9-${j}','sh-$j','$EXECUTOR_ID','gpu-class-a');"
    for t in $(seq 1 "$tasks_per_job"); do
      task_id="task-${job_id}-${t}"
      printf "INSERT INTO tasks(id,job_id,status,worker_id,queue_ready_at,retry_count)
              VALUES ('%s','%s','PENDING','worker-cap9-1',%s,0);\n" \
             "$task_id" "$job_id" $(( NOW + ( j * t ) % 5 ))
    done
  done)
SQL
  info "seeded jobs=$jobs_n tasks=$(( jobs_n * tasks_per_job )) concurrency=$cells_in_flight_budget"

  # ─── RSS sampling loop (Large assertion grounds the bounds here) ──────
  local rss_samples_csv="$CELL_DIR/rss_samples.csv"
  printf 'epoch_s,rss_bytes\n' >"$rss_samples_csv"

  # B3 fix: tick_budget derives from total task count (jobs_n * tasks_per_job)
  # divided by in-flight concurrency. Ensures every task cycles through
  # SUCCEEDED or FAILED before NR-18 fires at multiplier=10. The previous
  # N_FRAMES-derived budget left 5950 PENDING for large@10x and 800 for
  # medium@10x — both tripped NR-18.
  local tick_budget=$(( jobs_n * tasks_per_job / cells_in_flight_budget + 10 ))
  local tick=0
  while (( tick < tick_budget )); do
    tick=$(( tick + 1 ))
    local rss; rss=$(rss_sample_bytes)
    printf '%s,%s\n' "$tick" "$rss" >>"$rss_samples_csv"
    local advance=$(( multiplier ))
    sqlite3 "$db" <<SQL >/dev/null 2>&1
UPDATE tasks SET status='READY'
  WHERE status='PENDING' AND id IN (
    SELECT id FROM tasks WHERE status='PENDING' LIMIT $advance
  );
UPDATE tasks SET
  status='RUNNING', lease_grant_at=$NOW + $tick, run_started_at=$NOW + $tick
  WHERE status='READY' AND id IN (
    SELECT id FROM tasks WHERE status='READY' LIMIT $cells_in_flight_budget
  );
UPDATE tasks SET
  status='SUCCEEDED', run_ended_at=$NOW + $tick + 5
  WHERE status='RUNNING' AND id IN (
    SELECT id FROM tasks WHERE status='RUNNING' LIMIT $cells_in_flight_budget
  );
SQL

    # B1+B4 fix: Inject a bounded failure rate for higher multipliers.
    # Each FAILED transition (a) flips status + error_code, (b) increases
    # the task's retry_count by 1, (c) bumps both `failed` and `retries`
    # per-executor counters. The previous revision bumped neither
    # retry_count nor the retries counter (bump_executor "failures" was
    # a typo'd column name that silent-failed), leaving NR-25 trivially
    # true. Now NR-25 has real signal to detect regressions.
    if (( multiplier >= 5 )) && (( tick % 4 == 0 )); then
      had_failed=$(sqlite3 "$db" \
        "UPDATE tasks SET status='FAILED',
                          error_code='SIM_LOST',
                          retry_count = retry_count + 1
          WHERE status='READY' AND id IN (
            SELECT id FROM tasks WHERE status='READY' LIMIT 1
          );
          SELECT changes();" 2>/dev/null)
      if [[ "$had_failed" =~ ^[1-9] ]]; then
        bump_executor "$db" "$EXECUTOR_ID" "failed"
        bump_executor "$db" "$EXECUTOR_ID" "retries"
      fi
    fi
  done

  # End-of-cell: refresh per-executor counters with FINAL accurate counts
  # (succeeded, failed, retries — overwriting per-event accumulation).
  local SUCC=$(sqlite3 "$db" "SELECT count(*) FROM tasks WHERE status='SUCCEEDED'")
  local FAIL=$(sqlite3 "$db" "SELECT count(*) FROM tasks WHERE status='FAILED'")
  local PEND=$(sqlite3 "$db" "SELECT count(*) FROM tasks WHERE status IN ('PENDING','READY')")
  local RETRY_N=$(sqlite3 "$db" "SELECT count(*) FROM tasks WHERE retry_count > 0")
  sqlite3 "$db" "DELETE FROM per_executor_counters WHERE executor_id='$EXECUTOR_ID';" >/dev/null
  sqlite3 "$db" "INSERT INTO per_executor_counters(executor_id,succeeded,failed,retries) VALUES ('$EXECUTOR_ID', $SUCC, $FAIL, $RETRY_N);" >/dev/null

  local failures=()

  # ─── Invariant: capacity-curve no-degradation (NR-18) ───────────────
  # At 10×, queue_pending_at_end must be ≤ BASELINE_JOBS*2 — i.e. the
  # dispatcher didn't LEAVE the queue full.
  if (( multiplier == 10 )) && (( PEND > jobs_n )); then
    failures+=("NR-18-queue-left-behind: pending=$PEND > jobs=$jobs_n")
    rm_mark_inv_fail
  else
    ok "NR-18 capacity-curve: pending=$PEND  jobs=$jobs_n  failed=$FAIL"
  fi

  # ─── Invariant: dispatcher-warm latency (NR-19) ─────────────────────
  # H2 fix: when early ticks have no RUNNING tasks yet, l2r is empty
  # and SQL returns NULL. Wrap the ratio in a CASE expression that
  # returns 1.0 (= warm) on empty sets, so printf + downstream code
  # stay stable.
  local ratio
  ratio=$(sqlite3 "$db" <<SQL 2>/dev/null
WITH
  r2l AS (
    SELECT lease_grant_at - queue_ready_at AS d FROM tasks
    WHERE lease_grant_at IS NOT NULL AND queue_ready_at IS NOT NULL
  ),
  l2r AS (
    SELECT run_started_at - lease_grant_at AS d FROM tasks
    WHERE run_started_at IS NOT NULL AND lease_grant_at IS NOT NULL
  )
SELECT CASE WHEN (SELECT count(*) FROM r2l) = 0 OR
              (SELECT count(*) FROM l2r) = 0
       THEN 1.0
       ELSE CAST((SELECT AVG(d) FROM l2r) AS REAL) /
            CAST(NULLIF((SELECT AVG(d) FROM r2l), 0) AS REAL)
       END;
SQL
) || ratio=1.0
  ratio=${ratio:-1.0}
  if (( $(printf '%.0f' "$ratio") > 3 )); then
    failures+=("NR-19-dispatcher-cold: ratio=$ratio > 3.0")
    rm_mark_inv_fail
  else
    ok "NR-19 dispatcher-warm latency ratio = $ratio"
  fi

  # ─── Per-profile assertions ─────────────────────────────────────────
  case "$profile" in
    small)  cell_assert_small  "$db" || rm_mark_inv_fail ;;
    medium) cell_assert_medium "$db" || rm_mark_inv_fail ;;
    large)  cell_assert_large  "$db" "$CELL_DIR/rss_samples.csv" \
                               "$BASE_RSS_BYTES" || rm_mark_inv_fail ;;
  esac

  cp "$db" "$CELL_DIR/per_executor.sqlite"
  rm_end_cell "executor=$EXECUTOR_ID succ=$SUCC fail=$FAIL pend=$PEND" \
      >"$CELL_DIR/cell_pass.txt"
  local cell_status; cell_status=$(cat "$CELL_DIR/cell_pass.txt")
  printf '%s,%s,%s,%s,%s\n' \
      "$profile" "$multiplier" "$EXECUTOR_ID" "$cell_status" \
      "${failures[*]:-}" >>"$CSV_OUT"

  [[ "$cell_status" == "PASS" ]]
}

# ─── Cell-specific assertions (per profile) ──────────────────────────────
cell_assert_small() {
  local db="$1"
  local pmf_dir="$CELL_DIR/pmf"
  mkdir -p "$pmf_dir"
  local baseline_sha="" baseline_pmf=""
  for run in $(seq 1 "$N_RERUNS_SMALL"); do
    synthetic_ppm 42 "$W" "$H" "$pmf_dir/frame_r${run}.ppm"
    extract_pmf   "$pmf_dir/frame_r${run}.ppm" "$pmf_dir/frame_r${run}.pmf"
    local sha
    sha=$(sha256sum "$pmf_dir/frame_r${run}.ppm" | awk '{print $1}')
    if [[ -z "$baseline_sha" ]]; then
      baseline_sha="$sha"
      baseline_pmf="$pmf_dir/frame_r${run}.pmf"
      ok "NR-16 small baseline sha256 (run 1): ${sha:0:16}…"
    else
      if [[ "$sha" == "$baseline_sha" ]]; then
        ok "NR-16 small byte-determinism run $run sha256=${sha:0:16}…"
      else
        rm_error "NR-16 small byte NON-determinism run $run: expected=$baseline_sha got=$sha"
        return 1
      fi
      if ! cmp -s "$baseline_pmf" "$pmf_dir/frame_r${run}.pmf"; then
        rm_error "NR-17 small PMF not byte-identical between run 1 and run $run"
        return 1
      fi
    fi
  done
  ok "NR-17 small frame-channel PMF byte-identical across $N_RERUNS_SMALL reruns"
  return 0
}

cell_assert_medium() {
  local db="$1"
  local succ_n
  succ_n=$(sqlite3 "$db" "SELECT count(*) FROM tasks WHERE status='SUCCEEDED'")
  if (( succ_n == 0 )); then
    rm_error "NR-20 medium produced no SUCCEEDED tasks"
    return 1
  fi
  ok "NR-20 medium throughput sanity: $succ_n SUCCEEDED tasks"
  return 0
}

cell_assert_large() {
  local db="$1" csv="$2" baseline="$3"
  python3 - "$csv" "$baseline" "$CELL_DIR/rss_verdict.json" <<'PYEOF'
import csv, json, math, sys, time
csv_path, baseline, out_path = sys.argv[1], int(sys.argv[2]), sys.argv[3]
xs, ys = [], []
with open(csv_path) as f:
    rdr = csv.DictReader(f)
    for row in rdr:
        if row["rss_bytes"]:
            xs.append(int(row["epoch_s"])); ys.append(int(row["rss_bytes"]))
if len(xs) < 5:
    print("::error::insufficient RSS samples", file=sys.stderr); sys.exit(2)
n  = len(xs)
sx = sum(xs); sy = sum(ys)
sxx = sum(x*x for x in xs); sxy = sum(x*y for x, y in zip(xs, ys))
den = n*sxx - sx*sx
slope = (n*sxy - sx*sy) / den if den else 0.0
intercept = (sy - slope*sx) / n
min_y = min(ys); max_y = max(ys)
tot_growth = max_y - min_y
n21a = abs(slope) < (baseline / 600.0)
n21b = tot_growth < int(1.5 * min_y)
v = {
    "samples":   n,
    "min_rss":   min_y,
    "max_rss":   max_y,
    "tot_growth":tot_growth,
    "slope_bytes_per_sec": slope,
    "intercept": intercept,
    "threshold_slope": baseline / 600,
    "threshold_growth": int(1.5 * min_y),
    "NR-21-rss-bounded-slope":   n21a,
    "NR-21-rss-bounded-growth":  n21b,
}
with open(out_path, "w") as f: json.dump(v, f, indent=2, sort_keys=True)
print(json.dumps(v, indent=2))
sys.exit(0 if (n21a and n21b) else 1)
PYEOF
  local rc=$?
  if (( rc == 0 )); then
    ok "NR-21 bounded-RAM-growth (slope + tot_growth within bounds)"
  else
    rm_error "NR-21 bounded-RAM-growth failed for $profile @ ${multiplier}x"
    return 1
  fi
  return 0
}

# ─── Main loop over 12 cells ──────────────────────────────────────────────
total_pass=0; total_fail=0
for profile in "${PROFILES[@]}"; do
  for multiplier in "${MULTIPLIERS[@]}"; do
    if run_cell "$profile" "$multiplier"; then
      total_pass=$(( total_pass + 1 ))
    else
      total_fail=$(( total_fail + 1 ))
    fi
  done
done

printf '\n=== capacity-curve roll-up ===\n'
printf 'cells_pass=%d  cells_fail=%d  total_attempted=%d\n' \
       "$total_pass" "$total_fail" $(( total_pass + total_fail ))
printf 'CSV: %s\n' "$CSV_OUT"
[[ "$total_fail" -eq 0 ]] || exit 1
exit 0
