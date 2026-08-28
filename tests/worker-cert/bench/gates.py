"""Hard-stop gates and safety gate evaluation for capacity certification.

Three resource gates IMMEDIATELY disqualify a cap cell — no
throughput-gain argument can override them.  A cell that exceeds any
of these thresholds is unsafe for production regardless of how many
correct videos it produced.

The gates are evaluated both by ``passes_safety_gates()`` (used by the
dynamic search to stop probing) and by ``choose_limit()`` (used for the
final certification decision).

Unknown metrics (None) are treated as PASS — the certification refuses
to declare a limit rather than silently ignoring the resource.
"""

from __future__ import annotations

from bench.models import CapResult


HARD_STOP_GATES: dict[str, dict[str, str]] = {
    "peak_ram": {
        "metric": "host_memory_peak_ratio",
        "threshold": "<= 0.85",
        "description": "Peak host memory used / total must stay at or below 85%",
    },
    "fd_util": {
        "metric": "fd_util_peak_ratio",
        "threshold": "< 0.80",
        "description": "FD utilization peak must stay below 80%",
    },
    "disk_free": {
        "metric": "disk_free_min_bytes",
        "threshold": "> 10 GiB",
        "description": "Minimum disk free bytes must stay above the safety margin",
    },
}


def check_hard_stop_gates(
    result: CapResult,
    max_peak_memory_ratio: float,
    max_fd_util_ratio: float,
    min_disk_free_bytes: float,
) -> dict[str, dict[str, object]]:
    """Evaluate the three hard-stop gates against a single CapResult.

    Returns a dict keyed by gate name.  Each value has:
      - passed: bool
      - value: the observed metric value (or None if unavailable)
      - threshold: the configured limit
      - reason: human-readable failure reason (empty when passed)
    """
    gates: dict[str, dict[str, object]] = {}

    # Gate 1: Peak RAM
    ram_val = result.host_memory_peak_ratio
    ram_pass = ram_val is None or ram_val <= max_peak_memory_ratio
    gates["peak_ram"] = {
        "passed": ram_pass,
        "value": ram_val,
        "threshold": max_peak_memory_ratio,
        "reason": "" if ram_pass else f"peak_ram={ram_val:.3f} > {max_peak_memory_ratio}",
    }

    # Gate 2: FD utilization
    fd_val = result.fd_util_peak_ratio
    fd_pass = fd_val is None or fd_val < max_fd_util_ratio
    gates["fd_util"] = {
        "passed": fd_pass,
        "value": fd_val,
        "threshold": max_fd_util_ratio,
        "reason": "" if fd_pass else f"fd_util={fd_val:.3f} >= {max_fd_util_ratio}",
    }

    # Gate 3: Disk free space
    disk_val = result.disk_free_min_bytes
    disk_pass = disk_val is None or disk_val > min_disk_free_bytes
    gates["disk_free"] = {
        "passed": disk_pass,
        "value": disk_val,
        "threshold": min_disk_free_bytes,
        "reason": "" if disk_pass else f"disk_free={int(disk_val)} < {int(min_disk_free_bytes)}",
    }

    return gates


def hard_stops_passed(gates: dict[str, dict[str, object]]) -> bool:
    """Return True only if every hard-stop gate passed."""
    return all(g["passed"] for g in gates.values())


def passes_safety_gates(
    result: CapResult,
    max_error_rate: float,
    max_iowait: float,
    max_peak_memory_ratio: float,
    max_fd_util_ratio: float,
    min_disk_free_bytes: float,
) -> bool:
    """Hard safety-gate check used by the dynamic search to decide
    whether to continue probing higher caps.

    This is intentionally separate from choose_limit() which also
    evaluates throughput gain — the search needs a per-result go/no-go
    that does not depend on the previous eligible result.

    The three hard-stop gates (RAM, FD, disk) are evaluated via
    check_hard_stop_gates(); additional checks (status, errors, iowait)
    are evaluated inline.
    """
    if result.status != "PASS":
        return False
    if result.correct_videos < result.succeeded:
        return False
    if result.error_rate > max_error_rate:
        return False
    if result.disk_wait_avg_ratio is not None and result.disk_wait_avg_ratio > max_iowait:
        return False
    gates = check_hard_stop_gates(
        result, max_peak_memory_ratio, max_fd_util_ratio, min_disk_free_bytes
    )
    return hard_stops_passed(gates)
