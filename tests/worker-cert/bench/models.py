"""Dataclasses, constants, and small pure helpers for capacity certification."""

from __future__ import annotations

import math
import time
from dataclasses import dataclass


DEFAULT_MAX_CAP = 8
REQUIRED_METRICS = (
    "cpu_utilization_ratio",
    "host_memory_used_bytes",
    "host_memory_available_bytes",
    "disk_wait_ratio",
    "disk_free_bytes",
    "scratch_current_bytes",
    "scratch_peak_bytes",
    "cache_hits",
    "cache_misses",
    "downloads",
    "duplicate_downloads",
    "duplicate_download_bytes",
    "errors",
)


def now_ms() -> int:
    return time.monotonic_ns() // 1_000_000


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def cap_matrix(max_cap: int) -> tuple[int, ...]:
    """Return every cap so the certified boundary is actually measured."""
    if max_cap < 1:
        raise ValueError("max cap must be at least 1")
    return tuple(range(1, max_cap + 1))


@dataclass
class JobResult:
    job_id: str
    status: str
    latency_ms: float | None
    error: str = ""
    # Correctness is deliberately separate from lifecycle status. A SUCCEEDED
    # job without an artifact verification result is not a certified video.
    correct: bool | None = None
    correctness_error: str = ""


@dataclass
class CapResult:
    max_active_jobs: int
    status: str
    wall_time_ms: float
    throughput_jobs_per_hour: float
    correct_videos: int
    correct_videos_per_hour: float
    succeeded: int
    failed: int
    error_rate: float
    latency_mean_ms: float | None
    latency_p95_ms: float | None
    cpu_avg_ratio: float | None
    cpu_peak_ratio: float | None
    rss_avg_bytes: float | None  # diagnostic only — host memory is authoritative
    rss_peak_bytes: float | None  # diagnostic only — host memory is authoritative
    host_memory_used_avg_bytes: float | None
    host_memory_used_peak_bytes: float | None
    host_memory_available_bytes: float | None
    host_memory_peak_ratio: float | None
    fd_util_avg_ratio: float | None
    fd_util_peak_ratio: float | None
    disk_wait_avg_ratio: float | None
    disk_free_min_bytes: float | None
    cache_hits: float | None
    cache_misses: float | None
    cache_hit_ratio: float | None
    downloads: float | None
    duplicate_downloads: float | None
    duplicate_download_bytes: float | None
    errors: float | None
    missing_metrics: list[str]
    jobs: list[JobResult]
    scratch_current_bytes: float | None = None
    scratch_peak_bytes: float | None = None
    gpu_util_avg_ratio: float | None = None
    gpu_util_peak_ratio: float | None = None
    hard_stop_gates: dict[str, dict[str, object]] | None = None
    efficient: bool | None = None
    decision: str = ""
    limiting_resource: str = "UNKNOWN"
