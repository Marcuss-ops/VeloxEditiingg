#!/usr/bin/env bash
# telemetry_done.sh — fail-closed production telemetry certification.
# Run after the pinned cold/warm and unpinned job matrix; this gate does not
# submit work and certifies only persisted worker → Master evidence.

set -euo pipefail

DB_PATH="${1:-/opt/velox/current/.velox/data/velox.db}"
MIN_ATTEMPTS="${2:-4}"

python3 - "$DB_PATH" "$MIN_ATTEMPTS" <<'PY'
import sqlite3
import sys

db_path = sys.argv[1]
min_attempts = int(sys.argv[2])
expected_workers = {
    "velox-worker-13197",
    "velox-worker-523925eb",
    "host_57_129_132_133",
    "host_57_131_20_173",
}
checks = []

def check(name, ok, detail):
    checks.append((name, bool(ok), detail))

try:
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    rows = db.execute("""
        SELECT a.id AS attempt_id, a.worker_id, a.status,
               m.cpu_time_ms, m.cpu_percent_peak, m.peak_rss_bytes,
               m.disk_read_bytes, m.disk_write_bytes,
               m.network_rx_bytes, m.network_tx_bytes,
               m.frames_encoded, m.ffmpeg_speed_ratio,
               m.media_duration_seconds, m.wall_clock_seconds,
               m.logical_cpu_count, m.effective_cpu_count,
               m.output_file_size, m.output_sha256,
               c.cpu_price_per_second, c.storage_price_per_gb,
               c.network_price_per_gb, c.cpu_time_seconds_total,
               c.storage_gb_written, c.network_gb_egressed,
               c.output_minutes_total,
               (SELECT COUNT(*) FROM task_execution_events e
                  WHERE e.attempt_id = a.id) AS phase_events,
               (SELECT COUNT(*) FROM task_output_artifacts o
                  WHERE o.attempt_id = a.id) AS output_declarations
          FROM task_attempts a
          JOIN task_attempt_metrics m ON m.attempt_id = a.id
          LEFT JOIN task_attempt_cost_basis c ON c.attempt_id = a.id
         WHERE a.status = 'SUCCEEDED'
         ORDER BY COALESCE(a.completed_at, a.updated_at, a.created_at) DESC
         LIMIT ?
    """, (min_attempts,)).fetchall()
except Exception as exc:
    print(f"TELEMETRY_CERTIFICATION ERROR: {exc}", file=sys.stderr)
    sys.exit(2)

check("contract/schema", len(rows) >= min_attempts,
      f"successful attempts with typed rows={len(rows)} required>={min_attempts}")
workers = {row["worker_id"] for row in rows}
check("worker coverage", expected_workers.issubset(workers),
      f"workers={sorted(workers)} expected={sorted(expected_workers)}")

for row in rows:
    aid = row["attempt_id"]
    prefix = f"attempt={aid} worker={row['worker_id']}"
    fields = [
        ("cpu accounting", row["cpu_time_ms"] > 0, f"cpu_time_ms={row['cpu_time_ms']}"),
        ("cpu peak", row["cpu_percent_peak"] > 0, f"cpu_percent_peak={row['cpu_percent_peak']}"),
        ("memory peak", row["peak_rss_bytes"] > 0, f"peak_rss_bytes={row['peak_rss_bytes']}"),
        ("wall clock", row["wall_clock_seconds"] > 0, f"wall_clock_seconds={row['wall_clock_seconds']}"),
        ("frames", row["frames_encoded"] > 0, f"frames_encoded={row['frames_encoded']}"),
        ("ffmpeg speed", row["ffmpeg_speed_ratio"] > 0, f"ffmpeg_speed_ratio={row['ffmpeg_speed_ratio']}"),
        ("media duration", row["media_duration_seconds"] > 0, f"media_duration_seconds={row['media_duration_seconds']}"),
        ("effective CPU", row["effective_cpu_count"] > 0, f"effective_cpu_count={row['effective_cpu_count']}"),
        ("phase telemetry", row["phase_events"] > 0, f"phase_events={row['phase_events']}"),
        ("output declaration", row["output_declarations"] > 0, f"output_declarations={row['output_declarations']}"),
    ]
    for name, ok, detail in fields:
        check(name, ok, f"{prefix} {detail}")
    for name in ("disk_read_bytes", "disk_write_bytes", "network_rx_bytes", "network_tx_bytes"):
        check(f"non-negative {name}", row[name] >= 0, f"{prefix} {name}={row[name]}")
    limit = row["wall_clock_seconds"] * row["effective_cpu_count"] * 1000 * 1.20
    check("CPU sanity", row["cpu_time_ms"] <= limit,
          f"{prefix} cpu_ms={row['cpu_time_ms']} limit_ms={limit:.1f}")
    cost = row["cpu_time_seconds_total"] * row["cpu_price_per_second"]
    cost += row["storage_gb_written"] * row["storage_price_per_gb"]
    cost += row["network_gb_egressed"] * row["network_price_per_gb"]
    check("cost attribution", row["output_minutes_total"] > 0 and cost > 0,
          f"{prefix} cost={cost:.8f} output_minutes={row['output_minutes_total']}")

print("VELOX TELEMETRY CERTIFICATION")
print("=============================")
for name, ok, detail in checks:
    print(f"{name:<28} {'PASS' if ok else 'FAIL'}  {detail}")
passed = sum(ok for _, ok, _ in checks)
total = len(checks)
print(f"\nTOTAL: {passed}/{total} PASS")
if passed != total:
    print("TELEMETRY = NOT CERTIFIED")
    sys.exit(1)
print("TELEMETRY = CERTIFIED")
PY
