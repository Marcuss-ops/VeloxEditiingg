#!/usr/bin/env python3
"""Collect the existing persisted attempt/phase/cache metrics for a run.

This intentionally reads the master SQLite read models instead of adding a
benchmark-only metric path. It is useful after a real submission has returned
job_id/attempt_id from the worker-cert or API runner.
"""

import argparse
import json
import sqlite3
from datetime import datetime, timezone


def row_dict(cursor, row):
    return {description[0]: value for description, value in zip(cursor.description, row)}


def query_one(conn, sql, args):
    cursor = conn.execute(sql, args)
    row = cursor.fetchone()
    return row_dict(cursor, row) if row else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, help="Master SQLite database")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    conn.row_factory = None
    attempt_rows = conn.execute(
        "SELECT id, task_id, status, attempt_number, created_at, started_at, completed_at "
        "FROM task_attempts WHERE job_id = ? ORDER BY attempt_number ASC",
        (args.job_id,),
    ).fetchall()

    attempts = []
    for attempt_id, task_id, status, attempt_number, created_at, started_at, completed_at in attempt_rows:
        metrics = query_one(conn, "SELECT * FROM task_attempt_metrics WHERE attempt_id = ?", (attempt_id,))
        cache = query_one(conn, "SELECT * FROM task_attempt_cache_stats WHERE attempt_id = ?", (attempt_id,))
        phase_cursor = conn.execute(
            "SELECT * FROM task_phase_timings WHERE attempt_id = ? ORDER BY wall_start, phase",
            (attempt_id,),
        )
        phases = [row_dict(phase_cursor, row) for row in phase_cursor.fetchall()]

        metrics = metrics or {}
        cache = cache or {}
        media_seconds = float(metrics.get("media_duration_seconds") or metrics.get("total_input_duration_sec") or 0)
        wall_seconds = float(metrics.get("wall_clock_seconds") or 0)
        output_bytes = int(metrics.get("output_file_size") or metrics.get("output_bytes") or 0)
        temp_bytes = int(metrics.get("temp_bytes_written") or 0)
        input_bytes = int(metrics.get("input_bytes") or 0)
        duplicate_bytes = int(metrics.get("duplicate_download_bytes") or 0)
        cache_hits = int(cache.get("cache_hits") or 0)
        cache_misses = int(cache.get("cache_misses") or 0)
        local_cache_bytes = int(metrics.get("bytes_from_local_cache") or 0)
        blob_bytes = int(metrics.get("bytes_from_blobstore") or 0)
        drive_bytes = int(metrics.get("bytes_from_drive") or 0)

        phase_ms = {}
        for phase in phases:
            name = phase.get("phase") or phase.get("action") or "unknown"
            phase_ms[name] = phase_ms.get(name, 0) + int(phase.get("duration_ms") or 0)

        attempts.append({
            "attempt_id": attempt_id,
            "task_id": task_id,
            "status": status,
            "attempt_number": attempt_number,
            "created_at": created_at,
            "started_at": started_at,
            "completed_at": completed_at,
            "metrics": metrics,
            "cache": cache,
            "phase_duration_ms": phase_ms,
            "derived": {
                "render_factor": wall_seconds / media_seconds if media_seconds > 0 else 0,
                "temp_amplification": temp_bytes / output_bytes if output_bytes > 0 else 0,
                "download_amplification": input_bytes / max(input_bytes - duplicate_bytes, 1),
                "cache_hit_ratio": cache_hits / max(cache_hits + cache_misses, 1),
                "retry_count": max(int(attempt_number or 1) - 1, 0),
            },
            "scorecard": {
                "queue_wait_ms": int(metrics.get("queue_ms") or 0),
                "worker_start_delay_ms": int(metrics.get("time_to_first_worker_ms") or 0),
                "download_ms": int(metrics.get("engine_asset_download_ms") or phase_ms.get("download", 0)),
                "compile_ms": int(metrics.get("pipeline_compile_ms") or phase_ms.get("compile", 0)),
                "render_ms": int(metrics.get("pipeline_render_ms") or phase_ms.get("render", 0)),
                "concat_ms": int(metrics.get("engine_concat_ms") or phase_ms.get("composite", 0)),
                "upload_ms": phase_ms.get("upload", 0),
                "cpu_time_ms": int(metrics.get("cpu_time_ms") or 0),
                "cpu_peak_percent": float(metrics.get("cpu_percent_peak") or 0),
                "rss_peak_bytes": int(metrics.get("rss_peak_bytes") or metrics.get("peak_rss_bytes") or 0),
                "disk_read_bytes": int(metrics.get("disk_read_bytes") or 0),
                "disk_write_bytes": int(metrics.get("disk_write_bytes") or 0),
                "network_rx_bytes": int(metrics.get("network_rx_bytes") or 0),
                "network_tx_bytes": int(metrics.get("network_tx_bytes") or 0),
                "iowait_ms": int(metrics.get("iowait_ms") or 0),
                "cache_hit_count": cache_hits,
                "cache_miss_count": cache_misses,
                "cache_hit_bytes": local_cache_bytes,
                "download_bytes": drive_bytes + blob_bytes,
                "temp_bytes_written": temp_bytes,
                "output_bytes": output_bytes,
            },
        })

    report = {
        "schema": "velox.video-generator-certification-report.v1",
        "job_id": args.job_id,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "attempt_count": len(attempts),
        "attempts": attempts,
        "metric_sources": {
            "phase_timing": "task_phase_timings",
            "resource_and_quality": "task_attempt_metrics",
            "cache": "task_attempt_cache_stats",
            "retry_history": "task_attempts.attempt_number",
            "derived_ratios": "taskattempts.AttemptMetrics/AttemptCacheStats ratio definitions",
        },
        "missing_metric_fields": sorted({
            field
            for attempt in attempts
            for field in ("wall_clock_seconds", "cpu_time_ms", "peak_rss_bytes", "temp_bytes_written", "output_file_size", "output_sha256")
            if not attempt["metrics"].get(field)
        }),
    }
    with open(args.out, "w", encoding="utf-8") as output:
        json.dump(report, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps({"ok": True, "job_id": args.job_id, "attempt_count": len(attempts), "out": args.out}))


if __name__ == "__main__":
    main()
