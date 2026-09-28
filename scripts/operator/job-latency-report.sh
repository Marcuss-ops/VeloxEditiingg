#!/usr/bin/env bash
# scripts/operator/job-latency-report.sh — one-shot, per-job latency
# decomposition for the master database.
#
# WHY THIS EXISTS: on the 51 the operator rewrote the same three
# ad-hoc SQL queries by hand in a single day (phase timings, queue
# depth/age, prefetch events blocking dispatch) to answer "why is this
# job late?". Every datum the answer needs is already collected — the
# script is only the missing convenience. It is READ-ONLY: it opens
# the SQLite database in mode=ro and never writes.
#
# WHAT IT PRINTS, per job:
#   1. e2e decomposition  submit → terminal (queue wait vs execution),
#      the Master-clock view — the same numbers behind
#      velox_job_e2e_duration_seconds{phase};
#   2. scheduling detail  task_attempt_metrics: queue_ms, lease_wait_ms,
#      time_to_first_worker_ms, pending_tasks_at_start,
#      active_workers_at_start (migration 074);
#   3. durable phase rows task_phase_timings (top phases by duration);
#   4. the attempt waterfall — milestone buckets from
#      task_attempt_reports.raw_report_json, partitioned exactly like
#      observability/waterfall_builder.go (12 buckets + honest
#      "unclassified" gaps + coverage_pct);
#   5. prefetch journal   job_events lifecycle + the failure events that
#      park a job in PENDING (the Milton case).
#
# USAGE:
#   scripts/operator/job-latency-report.sh                    # 10 most recent jobs
#   scripts/operator/job-latency-report.sh -n 25              # more jobs
#   scripts/operator/job-latency-report.sh JOB_ID [JOB_ID...] # specific jobs
#   scripts/operator/job-latency-report.sh --db /path/to/velox.db -n 5
#
# DB resolution order: --db flag, $VELOX_DB_PATH, the production
# default, then the repo-local dev databases.
#
# Requires: bash, python3 (stdlib sqlite3 — no third-party modules).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

usage() {
  cat >&2 <<'EOF'
Usage: job-latency-report.sh [--db PATH] [--limit N] [JOB_ID ...]

  -n, --limit N   how many most-recent jobs to report when no JOB_ID is given
                  (default 10)
      --db PATH   SQLite database to read (default: $VELOX_DB_PATH, then
                  /var/lib/velox/data/velox.db, then the repo-local dev DBs)
  -h, --help      show this help

Read-only. Prints the submit→terminal decomposition, scheduling columns,
durable phases, the attempt waterfall and the prefetch journal for each job.
EOF
}

LIMIT=10
DB="${VELOX_DB_PATH:-}"
JOB_IDS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n|--limit)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      LIMIT="$2"; shift 2 ;;
    --db)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      DB="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    -*) printf 'unknown option: %s\n' "$1" >&2; usage; exit 2 ;;
    *) JOB_IDS+=("$1"); shift ;;
  esac
done

if [[ -z "$DB" ]]; then
  for candidate in \
    /var/lib/velox/data/velox.db \
    /var/lib/velox-server/velox.db \
    "$REPO_ROOT/.velox/data/velox.db" \
    "$REPO_ROOT/DataServer/.velox/data/velox.db"; do
    if [[ -f "$candidate" ]]; then DB="$candidate"; break; fi
  done
fi

if [[ -z "$DB" || ! -f "$DB" ]]; then
  printf 'job-latency-report: database not found (looked for $VELOX_DB_PATH, /var/lib/velox/data/velox.db, repo-local dev DBs)\n' >&2
  printf 'pass one explicitly: %s --db /path/to/velox.db\n' "$0" >&2
  exit 1
fi

command -v python3 >/dev/null 2>&1 || {
  printf 'job-latency-report: python3 is required (stdlib sqlite3)\n' >&2; exit 1; }

exec python3 - "$DB" "$LIMIT" "${JOB_IDS[@]+"${JOB_IDS[@]}"}" <<'PY'
"""Per-job latency decomposition. Read-only by construction (mode=ro)."""

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone

DB_PATH, LIMIT = sys.argv[1], int(sys.argv[2])
JOB_IDS = sys.argv[3:]

# Same partition as DataServer/internal/observability/waterfall_builder.go.
BUCKET_DEFS = [
    ("dispatch_to_execution", "attempt.accepted", "execution.started"),
    ("pre_asset_setup", "execution.started", "assets.requested"),
    ("asset_preparation", "assets.requested", "assets.all_ready"),
    ("pre_plan_wait", "assets.all_ready", "plan.started"),
    ("plan_compile", "plan.started", "plan.completed"),
    ("pre_render_wait", "plan.completed", "render.started"),
    ("render", "render.started", "render.completed"),
    ("finalize", "render.completed", "output.durable"),
    ("publish_queue_wait", "output.durable", "publish.started"),
    ("publish", "publish.started", "publish.completed"),
    ("result_finalize", "publish.completed", "result.sent"),
    ("result_ingest", "result.sent", "attempt.completed"),
]

PREFETCH_FAILURE_EVENTS = (
    "prefetch.prejob_prepare_failed",
    "prefetch.prefetch_failed",
    "prefetch.prefetch_error",
)


def fmt_ms(value):
    if value is None:
        return "—"
    value = int(value)
    sign = "-" if value < 0 else ""
    value = abs(value)
    if value < 1000:
        return f"{sign}{value}ms"
    seconds, ms = divmod(value, 1000)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")
    if ms and not hours:
        parts.append(f"{ms:03d}ms")
    return sign + " ".join(parts)


def parse_time(raw):
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def ms_between(start, end):
    start, end = parse_time(start), parse_time(end)
    if start is None or end is None:
        return None
    delta = int((end - start).total_seconds() * 1000)
    return delta if delta >= 0 else None


def scalar(conn, sql, params=()):
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else None


def milestones_from(raw_report):
    if not raw_report:
        return {}
    try:
        payload = json.loads(raw_report)
    except (TypeError, ValueError):
        return {}
    out = {}
    for sample in payload.get("milestones") or []:
        name = sample.get("name")
        if not name:
            continue
        elapsed = sample.get("elapsed_ms", sample.get("elapsedMs"))
        try:
            elapsed = int(elapsed)
        except (TypeError, ValueError):
            continue
        out[name] = elapsed
    return out


def print_waterfall(milestones):
    """Partition the milestone timeline exactly like BuildAttemptWaterfall."""
    buckets, missing, accounted = [], [], 0
    for name, start_key, end_key in BUCKET_DEFS:
        if start_key not in milestones:
            missing.append(start_key)
        if end_key not in milestones:
            missing.append(end_key)
        if start_key not in milestones or end_key not in milestones:
            continue
        start, end = milestones[start_key], milestones[end_key]
        if end < start:
            continue
        buckets.append((name, start, end, end - start))
        accounted += end - start
    wall = max(milestones.values()) if milestones else 0
    print(f"    waterfall (worker milestones, wall ≈ {fmt_ms(wall)})")
    if not buckets:
        print("      no milestone timeline in the durable report")
        return
    for name, _start, _end, duration in buckets:
        print(f"      {name:<24} {fmt_ms(duration):>14}")
    # Honest gaps: anything between bucket boundaries is UNKNOWN, never
    # attributed to a neighbour (same rule as the master read model).
    cursor, unclassified = 0, 0
    for _name, start, end, _duration in sorted(buckets, key=lambda b: b[1]):
        if start > cursor:
            gap = start - cursor
            print(f"      {'unclassified':<24} {fmt_ms(gap):>14}   (gap before {_name})")
            unclassified += gap
        cursor = max(cursor, end)
    if wall > cursor:
        gap = wall - cursor
        print(f"      {'unclassified':<24} {fmt_ms(gap):>14}   (tail)")
        unclassified += gap
    coverage = (accounted / wall * 100) if wall else 0.0
    print(f"      accounted {fmt_ms(accounted)}  unaccounted {fmt_ms(unclassified)}  "
          f"coverage {coverage:.1f}%")
    if missing:
        seen, ordered = set(), []
        for name in missing:
            if name not in seen:
                seen.add(name)
                ordered.append(name)
        print(f"      MISSING MILESTONE: {', '.join(ordered)} (segment left unattributed)")


def report_job(conn, job_id):
    job = conn.execute(
        "SELECT status, video_name, created_at, started_at, completed_at "
        "FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
    if job is None:
        print(f"JOB {job_id}  (not found)")
        print()
        return
    status, video, created, started, completed = job
    submit_to_start = ms_between(created, started)
    execute_ms = ms_between(started, completed)
    total_ms = ms_between(created, completed)

    print(f"JOB {job_id}  status={status or '?'}  {video or ''}".rstrip())
    print(f"  created={created or '—'}  started={started or '—'}  completed={completed or '—'}")
    print(f"  e2e total ........ {fmt_ms(total_ms):>14}   (submit → terminal)")
    print(f"    submit→start ... {fmt_ms(submit_to_start):>14}   (queue wait, master clock)")
    print(f"    start→terminal . {fmt_ms(execute_ms):>14}   (execution + delivery)")

    attempt = conn.execute(
        "SELECT id, status, worker_id, started_at, completed_at "
        "FROM task_attempts WHERE job_id = ? ORDER BY attempt_number DESC LIMIT 1",
        (job_id,)).fetchone()
    if attempt is None:
        print("  no attempt row yet (job never claimed)")
        print()
        return
    attempt_id, attempt_status, worker_id, attempt_started, attempt_completed = attempt
    print(f"  attempt {attempt_id}  status={attempt_status}  worker={worker_id or '—'}  "
          f"wall={fmt_ms(ms_between(attempt_started, attempt_completed))}")

    metrics = conn.execute(
        "SELECT queue_ms, lease_wait_ms, time_to_first_worker_ms, "
        "pending_tasks_at_start, active_workers_at_start, "
        "wall_clock_seconds, cpu_time_ms "
        "FROM task_attempt_metrics WHERE attempt_id = ?", (attempt_id,)).fetchone()
    if metrics is None:
        print("  task_attempt_metrics row missing for this attempt")
    else:
        (queue_ms, lease_wait_ms, ttfw_ms, pending_at_start, active_workers,
         wall_clock_s, cpu_ms) = metrics
        print("  scheduling (task_attempt_metrics, migration 074)")
        print(f"    queue_ms ................ {fmt_ms(queue_ms)}")
        print(f"    lease_wait_ms ........... {fmt_ms(lease_wait_ms)}")
        print(f"    time_to_first_worker_ms . {fmt_ms(ttfw_ms)}")
        print(f"    pending_tasks_at_start .. {pending_at_start}   "
              f"active_workers_at_start .. {active_workers}")
        if wall_clock_s:
            print(f"    wall_clock_seconds ...... {wall_clock_s}s   cpu_time_ms .. {fmt_ms(cpu_ms)}")

    phases = conn.execute(
        "SELECT phase, duration_ms FROM task_phase_timings "
        "WHERE attempt_id = ? ORDER BY duration_ms DESC LIMIT 6", (attempt_id,)).fetchall()
    if phases:
        print("  durable phases (task_phase_timings, top by duration)")
        for phase, duration in phases:
            print(f"    {phase:<24} {fmt_ms(duration):>14}")

    raw_report = conn.execute(
        "SELECT raw_report_json FROM task_attempt_reports WHERE attempt_id = ?",
        (attempt_id,)).fetchone()
    if raw_report is None:
        print("  no durable report for this attempt (pre-milestone worker?)")
    else:
        print_waterfall(milestones_from(raw_report[0]))

    events = conn.execute(
        "SELECT event, COUNT(*) FROM job_events WHERE job_id = ? "
        "AND event LIKE 'prefetch.%' GROUP BY event ORDER BY 2 DESC",
        (job_id,)).fetchall()
    print("  prefetch journal (job_events)")
    if not events:
        print("    no prefetch events recorded")
    else:
        for event, count in events:
            print(f"    {event:<38} x{count}")
    failures = conn.execute(
        "SELECT event, timestamp, raw_json FROM job_events "
        "WHERE job_id = ? AND event IN (?, ?, ?) ORDER BY timestamp DESC LIMIT 3",
        (job_id, *PREFETCH_FAILURE_EVENTS)).fetchall()
    for event, timestamp, raw in failures:
        reason = ""
        try:
            reason = json.loads(raw).get("error_reason", "")
        except (TypeError, ValueError):
            pass
        print(f"    FAILURE {timestamp} {event}: {reason or '(no error_reason)'}")
    if failures:
        print("    ↑ a failed prefetch parks the job in PENDING "
              "(dispatch_status=prefetch_failed): no lease is taken until assets are ready")
    print()


def main():
    uri = f"file:{os.path.abspath(DB_PATH)}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        if JOB_IDS:
            ids = list(JOB_IDS)
        else:
            ids = [row[0] for row in conn.execute(
                "SELECT job_id FROM jobs "
                "ORDER BY COALESCE(NULLIF(completed_at,''), NULLIF(updated_at,''), created_at) DESC "
                "LIMIT ?", (LIMIT,)).fetchall()]
        if not ids:
            print("no jobs found")
            return
        print(f"# job latency report — {os.path.abspath(DB_PATH)} ({len(ids)} job(s))\n")
        for job_id in ids:
            report_job(conn, job_id)
    finally:
        conn.close()


main()
PY
