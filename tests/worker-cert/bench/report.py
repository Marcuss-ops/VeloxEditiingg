"""Certification decision logic and final report generation."""

from __future__ import annotations

from bench.models import CapResult
from bench.bottleneck import classify_bottleneck
from bench.gates import check_hard_stop_gates


def choose_limit(
    results: list[CapResult],
    min_gain_pct: float,
    max_p95_ms: float | None,
    max_error_rate: float,
    max_iowait: float,
    max_peak_memory_ratio: float,
    max_fd_util_ratio: float,
    min_disk_free_bytes: float,
) -> None:
    """Evaluate every cap result and set efficient/decision/limiting_resource.

    Hard-stop gates (RAM, FD, disk) are evaluated FIRST via
    check_hard_stop_gates().  If any gate fails, the cell is immediately
    disqualified — no throughput-gain argument can override it.
    """
    previous_eligible: CapResult | None = None
    baseline_valid = bool(results) and results[0].max_active_jobs == 1

    for index, result in enumerate(results):
        # ── Hard-stop gates (evaluated first, override everything) ──────
        gates = check_hard_stop_gates(
            result, max_peak_memory_ratio, max_fd_util_ratio, min_disk_free_bytes
        )
        result.hard_stop_gates = gates
        failed_gates = [name for name, g in gates.items() if not g["passed"]]

        checks: list[str] = []
        if failed_gates:
            checks.extend(f"hard_stop:{name}" for name in failed_gates)

        # ── Soft checks (can be overridden by throughput gain) ─────────
        if result.status != "PASS":
            checks.append(result.status.lower())
        if result.correct_videos < result.succeeded:
            checks.append("incorrect_video")
        if result.error_rate > max_error_rate:
            checks.append(f"error_rate>{max_error_rate}")
        if max_p95_ms is not None and (
            result.latency_p95_ms is None or result.latency_p95_ms > max_p95_ms
        ):
            checks.append("p95_limit")
        if result.disk_wait_avg_ratio is not None and result.disk_wait_avg_ratio > max_iowait:
            checks.append("iowait_limit")
        gain = None
        if index > 0 and previous_eligible is None:
            checks.append("baseline_unavailable")
        if previous_eligible and previous_eligible.correct_videos_per_hour > 0:
            gain = (
                (result.correct_videos_per_hour / previous_eligible.correct_videos_per_hour - 1)
                * 100
            )
            if gain < min_gain_pct:
                checks.append(f"correct_video_gain<{min_gain_pct}%")
        if index == 0 and not baseline_valid:
            checks.append("missing_cap_1_baseline")

        result.efficient = not checks
        result.decision = (
            "baseline"
            if previous_eligible is None and result.efficient
            else "eligible"
            if result.efficient
            else "; ".join(checks)
        )
        result.limiting_resource = classify_bottleneck(result)
        if result.efficient:
            previous_eligible = result
