"""Dynamic cap search: exponential sweep followed by binary search."""

from __future__ import annotations

from typing import Any, Callable

from bench.models import CapResult
from bench.gates import passes_safety_gates


def dynamic_cap_search(
    test_fn: Callable[[int], CapResult],
    max_cap: int,
    max_error_rate: float = 0.0,
    max_iowait: float = 0.25,
    max_peak_memory_ratio: float = 0.85,
    max_fd_util_ratio: float = 0.80,
    min_disk_free_bytes: float = 10_000_000_000,
) -> tuple[list[CapResult], list[int], list[int]]:
    """Find the true certified max via exponential sweep + binary search.

    Phase 1 (exponential): test 1, 2, 4, 8, ... until a safety gate
    fails or max_cap is reached.
    Phase 2 (binary): narrow between last safe and first unsafe cap.

    The safety-gate check is a hard per-result go/no-go (status, errors,
    memory, FD, iowait, disk).  The full choose_limit() evaluation
    (including throughput-gain comparison) runs AFTER the search on the
    complete result set.

    Returns (results, exponential_caps, binary_caps) where results is
    sorted by cap for choose_limit() consumption.
    """
    if max_cap < 1:
        raise ValueError("max cap must be at least 1")

    all_results: dict[int, CapResult] = {}
    exponential_caps: list[int] = []
    binary_caps: list[int] = []

    # ── Phase 1: Exponential sweep ──────────────────────────────────────
    cap = 1
    while cap <= max_cap:
        exponential_caps.append(cap)
        result = test_fn(cap)
        all_results[cap] = result
        if not passes_safety_gates(
            result, max_error_rate, max_iowait,
            max_peak_memory_ratio, max_fd_util_ratio, min_disk_free_bytes,
        ):
            break
        cap *= 2

    # ── Phase 2: Binary search (only if sweep found a failure) ──────────
    tested = sorted(all_results.keys())
    last_ok = None
    first_fail = None
    for c in tested:
        r = all_results[c]
        if passes_safety_gates(
            r, max_error_rate, max_iowait,
            max_peak_memory_ratio, max_fd_util_ratio, min_disk_free_bytes,
        ):
            last_ok = c
        elif first_fail is None:
            first_fail = c

    if last_ok is not None and first_fail is not None and first_fail - last_ok > 1:
        lo, hi = last_ok + 1, first_fail - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            if mid in all_results:
                result = all_results[mid]
            else:
                binary_caps.append(mid)
                result = test_fn(mid)
                all_results[mid] = result
            if passes_safety_gates(
                result, max_error_rate, max_iowait,
                max_peak_memory_ratio, max_fd_util_ratio, min_disk_free_bytes,
            ):
                last_ok = mid
                lo = mid + 1
            else:
                first_fail = mid
                hi = mid - 1

    results = [all_results[c] for c in sorted(all_results.keys())]
    return results, exponential_caps, binary_caps
