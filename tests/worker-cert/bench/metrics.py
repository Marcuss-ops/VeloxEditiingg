"""Prometheus parsing, observation extraction, and gauge aggregation."""

from __future__ import annotations

import statistics
from typing import Any


def parse_prometheus(text: str) -> dict[str, float]:
    """Parse exposition values, retaining metric-family sums by base name.

    Labels are intentionally ignored: the caller supplies one worker endpoint
    or a master projection scoped to the worker.  This also avoids introducing
    job/asset labels into the certification result.
    """
    values: dict[str, float] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or " " not in line:
            continue
        name, raw = line.split(None, 1)
        raw = raw.split()[0]
        try:
            value = float(raw)
        except ValueError:
            continue
        # Keep the exact labelled sample as well as the unlabelled family.
        base = name.split("{", 1)[0]
        if base.endswith("_bucket") or base.endswith("_sum") or base.endswith("_count"):
            continue
        if "{" in name:
            values[name] = values.get(name, 0.0) + value
        else:
            values[base] = values.get(base, 0.0) + value
    return values


def metric(values: dict[str, float], *names: str) -> float | None:
    """Return the first matching metric value from *values*."""
    for name in names:
        if name in values:
            return values[name]
    return None


def normalize_ratio(value: float | None) -> float | None:
    """Normalize a ratio value, accepting both [0,1] and micro-unit forms."""
    if value is None:
        return None
    if 0.0 <= value <= 1.0:
        return value
    if 1_000_000 <= value <= 1_000_000_000:
        return value / 1_000_000
    return None


def extract_observations(values: dict[str, float]) -> dict[str, float | None]:
    """Extract canonical observation dict from raw Prometheus values."""
    return {
        "cpu_utilization_ratio": normalize_ratio(
            metric(values, "velox_worker_cpu_utilization_ratio", "velox_cpu_utilization_ratio")
        ),
        "rss_bytes": metric(
            values,
            "velox_worker_process_rss_bytes",
            "velox_worker_process_rss_peak_bytes",
            "process_resident_memory_bytes",
        ),
        "host_memory_used_bytes": metric(
            values, "velox_worker_memory_used_bytes", "velox_memory_used_bytes"
        ),
        "host_memory_available_bytes": metric(
            values, "velox_worker_memory_available_bytes", "velox_memory_available_bytes"
        ),
        "disk_wait_ratio": normalize_ratio(
            metric(
                values,
                "velox_worker_cpu_iowait_ratio",
                "velox_cpu_iowait_ratio",
                "velox_disk_wait_ratio",
            )
        ),
        "fd_util": normalize_ratio(
            metric(
                values, "velox_worker_fd_utilization_ratio", "velox_fd_utilization_ratio"
            )
        ),
        "disk_free_bytes": metric(
            values, "velox_worker_disk_free_bytes", "velox_disk_free_bytes"
        ),
        "scratch_current_bytes": metric(
            values, "velox_worker_scratch_current_bytes", "velox_scratch_current_bytes"
        ),
        "scratch_peak_bytes": metric(
            values, "velox_worker_scratch_peak_bytes", "velox_scratch_peak_bytes"
        ),
        "cache_hits": metric(
            values,
            'velox_cache_requests_total{result="hit"}',
            "velox_asset_cache_hits_total",
            "velox_asset_cache_hit_total",
        ),
        "cache_misses": metric(
            values,
            'velox_cache_requests_total{result="miss"}',
            "velox_asset_cache_misses_total",
            "velox_asset_cache_miss_total",
        ),
        "downloads": metric(
            values, "velox_cache_downloads_total", "velox_asset_cache_download_total"
        ),
        "duplicate_downloads": metric(
            values,
            "velox_cache_duplicate_downloads_total",
            "velox_duplicate_downloads_total",
            "velox_task_duplicate_downloads_total",
        ),
        "duplicate_download_bytes": metric(
            values,
            "velox_cache_duplicate_download_bytes_total",
            "velox_duplicate_download_bytes_total",
            "velox_task_duplicate_download_bytes_total",
        ),
        "errors": metric(
            values,
            "velox_worker_errors_total",
            "velox_compute_failure_reasons_total",
            "velox_task_errors_total",
        ),
        "gpu_util_avg_ratio": normalize_ratio(
            metric(values, "velox_worker_gpu_util_avg_percent", "velox_gpu_util_avg_percent")
        ),
        "gpu_util_peak_ratio": normalize_ratio(
            metric(values, "velox_worker_gpu_util_peak_percent", "velox_gpu_util_peak_percent")
        ),
    }


def delta(before: float | None, after: float | None) -> float | None:
    """Non-negative difference between two metric snapshots."""
    if before is None or after is None:
        return None
    return max(0.0, after - before)


def peak_memory_ratio(
    used_peak: float | None, available_min: float | None
) -> float | None:
    """Compute peak host memory ratio (used / total) from observed peaks.

    The total is reconstructed as used_peak + available_min at the moment
    of lowest headroom.  When either operand is missing the ratio is
    unknown and the safety gate cannot fire.
    """
    if used_peak is None or available_min is None:
        return None
    total = used_peak + available_min
    if total <= 0:
        return None
    return used_peak / total


def aggregate_gauge(
    samples: list[dict[str, float | None]], key: str, fn: str
) -> float | None:
    """Aggregate a gauge across samples using *fn* ('avg' or 'max')."""
    values = [float(s[key]) for s in samples if s.get(key) is not None]
    if not values:
        return None
    return statistics.mean(values) if fn == "avg" else max(values)
