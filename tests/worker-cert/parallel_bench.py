#!/usr/bin/env python3
"""Certify the maximum safe worker concurrency for a given worker.

This harness deliberately owns no placement, lease, download, or cleanup logic.
It only changes the operator-selected worker cap through an explicit command,
submits the canonical real-asset jobs concurrently, polls their lifecycle, and
samples already-exported Prometheus metrics.

The primary safety gate is **host memory**: peak_ram_ratio =
peak(host_memory_used) / (peak(host_memory_used) +
min(host_memory_available)) must stay below --max-peak-memory-ratio
(default 0.80).  Process-level RSS (velox_worker_process_rss_bytes) is
collected for diagnostics only and does NOT drive the certification
decision — it underestimates real memory consumption because it does not
include the concurrent C++ engine process trees.

A live run requires:
  VELOX_MASTER_URL, VELOX_ADMIN_TOKEN (or TOKEN_FILE),
  PARALLEL_BENCH_WORKER_ID, PARALLEL_BENCH_SET_CAP_CMD,
  PARALLEL_BENCH_METRICS_URL (worker /metrics or a master projection), and
  the canonical assets fixture used by build_real_payload.py.

The cap command is intentionally explicit and operator-owned. It may contain
{cap}, {worker_id}, and {master_url}, for example:
  PARALLEL_BENCH_SET_CAP_CMD='ssh velox-worker "sudo velox-admin-worker set-max-active-jobs {cap}"'

No result is called certified when required metrics are unavailable.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# ── bench package imports ─────────────────────────────────────────────────
from bench.models import DEFAULT_MAX_CAP
from bench.http import read_token, provision_m2m, delete_m2m
from bench.search import dynamic_cap_search
from bench.runner import run_cap_command, run_one_cap, wait_cap
from bench.report import choose_limit
from bench.gates import HARD_STOP_GATES


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker-id", default=os.getenv("PARALLEL_BENCH_WORKER_ID", ""))
    parser.add_argument(
        "--master-url",
        default=os.getenv("VELOX_MASTER_URL", "http://127.0.0.1:8000").rstrip("/"),
    )
    parser.add_argument("--metrics-url", action="append", default=[])
    parser.add_argument("--set-cap-command", default=os.getenv("PARALLEL_BENCH_SET_CAP_CMD", ""))
    parser.add_argument(
        "--builder",
        type=Path,
        default=Path(__file__).with_name("build_real_payload.py"),
    )
    parser.add_argument(
        "--fixtures",
        type=Path,
        default=Path(__file__).with_name("fixtures") / "assets.json",
    )
    parser.add_argument(
        "--destination",
        required=not bool(os.getenv("BENCH_DESTINATION_ID", "").strip()),
        default=os.getenv("BENCH_DESTINATION_ID", "").strip() or None,
        help="explicit delivery destination_id; implicit destinations are forbidden",
    )
    parser.add_argument(
        "--jobs", type=int, default=int(os.getenv("PARALLEL_BENCH_JOBS", "6"))
    )
    parser.add_argument(
        "--poll-timeout-s",
        type=int,
        default=int(os.getenv("BENCH_POLL_TIMEOUT_S", "300")),
    )
    parser.add_argument("--wait-cap-timeout-s", type=int, default=120)
    parser.add_argument("--sample-interval-s", type=float, default=2.0)
    parser.add_argument("--min-throughput-gain-pct", type=float, default=5.0)
    parser.add_argument("--max-p95-ms", type=float, default=None)
    parser.add_argument("--max-error-rate", type=float, default=0.0)
    parser.add_argument("--max-iowait-ratio", type=float, default=0.25)
    parser.add_argument(
        "--max-peak-memory-ratio",
        type=float,
        default=0.85,
        help="reject caps where peak host memory used/total exceeds this ratio (0-1)",
    )
    parser.add_argument(
        "--max-fd-util-ratio",
        type=float,
        default=0.80,
        help="reject caps where FD utilization exceeds this ratio (0-1)",
    )
    parser.add_argument(
        "--min-disk-free-bytes",
        type=float,
        default=10_000_000_000,
        help="reject caps where minimum disk free bytes falls below this threshold",
    )
    parser.add_argument(
        "--max-cap",
        type=int,
        default=DEFAULT_MAX_CAP,
        help="highest concurrency cap to test (every cap 1..N is measured)",
    )
    parser.add_argument(
        "--correctness-command",
        default=os.getenv("PARALLEL_BENCH_CORRECTNESS_CMD", ""),
        help="operator-owned verifier command; placeholders: {job_id}, {worker_id}, {master_url}; exit 0 means correct video",
    )
    parser.add_argument(
        "--response-dir",
        type=Path,
        default=None,
        help="directory for terminal job JSON responses passed to the correctness hook",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--leave-cap",
        action="store_true",
        help="Do not restore MaxActiveJobs=1 after the matrix",
    )
    args = parser.parse_args()
    if not args.metrics_url:
        env_urls = os.getenv("PARALLEL_BENCH_METRICS_URL", "")
        args.metrics_url = [u.strip() for u in env_urls.split(",") if u.strip()]
    args.master_url = args.master_url.rstrip("/")
    return args


def main() -> int:
    args = parse_args()

    if (
        args.jobs < 1
        or args.max_cap < 1
        or not args.worker_id
        or not args.set_cap_command
        or not args.metrics_url
    ):
        print(
            "live certification requires --worker-id, --set-cap-command, and --metrics-url",
            file=sys.stderr,
        )
        return 2
    if not args.builder.is_file() or not args.fixtures.is_file():
        print("canonical payload builder or assets fixture is missing", file=sys.stderr)
        return 2

    if args.dry_run:
        sweep: list[int] = []
        cap = 1
        while cap <= args.max_cap:
            sweep.append(cap)
            cap *= 2
        print(
            json.dumps(
                {
                    "search_strategy": "exponential_sweep_then_binary_search",
                    "max_cap": args.max_cap,
                    "exponential_sweep": sweep,
                    "worker_id": args.worker_id,
                    "jobs": args.jobs,
                    "set_cap_command": args.set_cap_command,
                    "correctness_command": args.correctness_command,
                    "response_dir": str(args.response_dir) if args.response_dir else "",
                    "decision_metric": "correct_videos_per_hour",
                },
                indent=2,
            )
        )
        return 0

    if not args.correctness_command.strip() or args.response_dir is None:
        print(
            "live certification requires --correctness-command and --response-dir",
            file=sys.stderr,
        )
        return 2

    # ── Provision ephemeral M2M credentials ──────────────────────────────
    try:
        admin_token = read_token()
        client_id, m2m_token = provision_m2m(args.master_url, admin_token)
    except Exception as exc:
        print(f"prerequisite failure: {exc}", file=sys.stderr)
        return 2

    def run_cap(cap: int):
        run_cap_command(args.set_cap_command, cap, args.worker_id, args.master_url, False)
        wait_cap(args.master_url, admin_token, args.worker_id, cap, args.wait_cap_timeout_s)
        return run_one_cap(args, cap, admin_token, m2m_token)

    try:
        results, exp_caps, bin_caps = dynamic_cap_search(
            run_cap,
            args.max_cap,
            max_error_rate=args.max_error_rate,
            max_iowait=args.max_iowait_ratio,
            max_peak_memory_ratio=args.max_peak_memory_ratio,
            max_fd_util_ratio=args.max_fd_util_ratio,
            min_disk_free_bytes=args.min_disk_free_bytes,
        )
    except Exception as exc:
        print(f"certification failed: {exc}", file=sys.stderr)
        return 3
    finally:
        if not args.leave_cap:
            try:
                run_cap_command(args.set_cap_command, 1, args.worker_id, args.master_url, False)
            except Exception as exc:
                print(f"WARNING: failed to restore MaxActiveJobs=1: {exc}", file=sys.stderr)
        delete_m2m(args.master_url, admin_token, client_id)

    # ── Evaluate results and produce certification report ────────────────
    choose_limit(
        results,
        args.min_throughput_gain_pct,
        args.max_p95_ms,
        args.max_error_rate,
        args.max_iowait_ratio,
        args.max_peak_memory_ratio,
        args.max_fd_util_ratio,
        args.min_disk_free_bytes,
    )
    eligible = [r.max_active_jobs for r in results if r.efficient]
    efficient_limit = max(eligible) if eligible else None
    recommended_limit = max(1, efficient_limit - 1) if efficient_limit is not None else None

    certified = (
        efficient_limit is not None
        and all(r.status == "PASS" for r in results if r.efficient)
        and results[0].max_active_jobs == 1
        and results[0].efficient is True
    )

    print(
        json.dumps(
            {
                "certified": certified,
                "certified_max_jobs": efficient_limit,
                "recommended_production_jobs": recommended_limit,
                "total_tests": len(results),
            },
            indent=2,
        )
    )
    return 0 if certified else 4


if __name__ == "__main__":
    raise SystemExit(main())
