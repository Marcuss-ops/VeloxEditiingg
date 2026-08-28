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
import math
import os
import shlex
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any


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


def read_token() -> str:
    value = os.getenv("VELOX_ADMIN_TOKEN", "").strip()
    if not value and os.getenv("TOKEN_FILE"):
        path = Path(os.environ["TOKEN_FILE"])
        if path.is_file():
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.startswith("VELOX_ADMIN_TOKEN="):
                    value = line.split("=", 1)[1].strip().strip("'\"")
                    break
    if not value or any(ch in value for ch in "\r\n"):
        raise RuntimeError("VELOX_ADMIN_TOKEN or TOKEN_FILE is required")
    return value


def http_json(method: str, url: str, token: str, body: Any = None, timeout: float = 30) -> tuple[int, dict[str, Any], dict[str, str]]:
    data = None
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            return response.status, json.loads(raw) if raw else {}, dict(response.headers)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw[:500]}
        return exc.code, payload, dict(exc.headers)


def scrape_text(url: str, token: str) -> str:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
        return ""


def parse_prometheus(text: str) -> dict[str, float]:
    """Parse exposition values, retaining metric-family sums by base name.

    Labels are intentionally ignored: the caller supplies one worker endpoint
    or a master projection scoped to the worker. This also avoids introducing
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
        # Cache hit/miss is distinguished by result=..., while gauges and
        # counters without labels remain addressable by their base name.
        base = name.split("{", 1)[0]
        if base.endswith("_bucket") or base.endswith("_sum") or base.endswith("_count"):
            continue
        if "{" in name:
            values[name] = values.get(name, 0.0) + value
        else:
            values[base] = values.get(base, 0.0) + value
    return values


def metric(values: dict[str, float], *names: str) -> float | None:
    for name in names:
        if name in values:
            return values[name]
    return None


def normalize_ratio(value: float | None) -> float | None:
    if value is None:
        return None
    # The canonical worker gauges are ratios in [0,1]. Values above one are
    # accepted only when they are clearly micro-units; ambiguous values fail
    # closed so a bad scrape cannot influence the cap decision.
    if 0.0 <= value <= 1.0:
        return value
    if 1_000_000 <= value <= 1_000_000_000:
        return value / 1_000_000
    return None


def extract_observations(values: dict[str, float]) -> dict[str, float | None]:
    return {
        "cpu_utilization_ratio": normalize_ratio(metric(values, "velox_worker_cpu_utilization_ratio", "velox_cpu_utilization_ratio")),
        # Diagnostic only — process RSS does NOT include concurrent C++ engines;
        # host_memory_* is the authoritative metric for the safety gate.
        "rss_bytes": metric(values, "velox_worker_process_rss_bytes", "velox_worker_process_rss_peak_bytes", "process_resident_memory_bytes"),
        "host_memory_used_bytes": metric(values, "velox_worker_memory_used_bytes", "velox_memory_used_bytes"),
        "host_memory_available_bytes": metric(values, "velox_worker_memory_available_bytes", "velox_memory_available_bytes"),
        "disk_wait_ratio": normalize_ratio(metric(values, "velox_worker_cpu_iowait_ratio", "velox_cpu_iowait_ratio", "velox_disk_wait_ratio")),
        "fd_util": normalize_ratio(metric(values, "velox_worker_fd_utilization_ratio", "velox_fd_utilization_ratio")),
        "disk_free_bytes": metric(values, "velox_worker_disk_free_bytes", "velox_disk_free_bytes"),
        "scratch_current_bytes": metric(values, "velox_worker_scratch_current_bytes", "velox_scratch_current_bytes"),
        "scratch_peak_bytes": metric(values, "velox_worker_scratch_peak_bytes", "velox_scratch_peak_bytes"),
        "cache_hits": metric(values, "velox_cache_requests_total{result=\"hit\"}", "velox_asset_cache_hits_total", "velox_asset_cache_hit_total"),
        "cache_misses": metric(values, "velox_cache_requests_total{result=\"miss\"}", "velox_asset_cache_misses_total", "velox_asset_cache_miss_total"),
        "downloads": metric(values, "velox_cache_downloads_total", "velox_asset_cache_download_total"),
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
        "errors": metric(values, "velox_worker_errors_total", "velox_compute_failure_reasons_total", "velox_task_errors_total"),
        # GPU utilization — N/A on CPU-only workers; not yet exposed as
        # Prometheus gauges on the heartbeat path (attempt-level only).
        # When gauges land, the classifier will pick them up automatically.
        "gpu_util_avg_ratio": normalize_ratio(metric(values, "velox_worker_gpu_util_avg_percent", "velox_gpu_util_avg_percent")),
        "gpu_util_peak_ratio": normalize_ratio(metric(values, "velox_worker_gpu_util_peak_percent", "velox_gpu_util_peak_percent")),
    }


def delta(before: float | None, after: float | None) -> float | None:
    if before is None or after is None:
        return None
    return max(0.0, after - before)


def _peak_memory_ratio(used_peak: float | None, available_min: float | None) -> float | None:
    """Compute peak host memory ratio (used / total) from observed peaks.

    The total is reconstructed as used_peak + available_min at the moment
    of lowest headroom.  When either operand is missing the ratio is
    unknown and the safety gate cannot fire — the certification will
    refuse to declare a limit rather than silently ignoring memory.
    """
    if used_peak is None or available_min is None:
        return None
    total = used_peak + available_min
    if total <= 0:
        return None
    return used_peak / total


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


def cap_matrix(max_cap: int) -> tuple[int, ...]:
    """Return every cap so the certified boundary is actually measured."""
    if max_cap < 1:
        raise ValueError("max cap must be at least 1")
    return tuple(range(1, max_cap + 1))


# ── Hard-stop gates ──────────────────────────────────────────────────────
#
# Three resource gates that IMMEDIATELY disqualify a cap cell — no
# throughput-gain argument can override them.  A cell that exceeds any
# of these thresholds is unsafe for production regardless of how many
# correct videos it produced.
#
# The gates are evaluated both by _passes_safety_gates() (used by the
# dynamic search to stop probing) and by choose_limit() (used for the
# final certification decision).
#
# Unknown metrics (None) are treated as PASS — the certification refuses
# to declare a limit rather than silently ignoring the resource.  This
# is the correct fail-closed behavior: if we can't measure it, we can't
# certify it, but we also don't fail a cell that otherwise looks safe.

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


def _check_hard_stop_gates(
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


def _hard_stops_passed(gates: dict[str, dict[str, object]]) -> bool:
    """Return True only if every hard-stop gate passed."""
    return all(g["passed"] for g in gates.values())


def _passes_safety_gates(
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
    _check_hard_stop_gates(); additional checks (status, errors, iowait)
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
    gates = _check_hard_stop_gates(result, max_peak_memory_ratio, max_fd_util_ratio, min_disk_free_bytes)
    return _hard_stops_passed(gates)


def dynamic_cap_search(
    test_fn: Any,
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
        if not _passes_safety_gates(result, max_error_rate, max_iowait,
                                    max_peak_memory_ratio, max_fd_util_ratio,
                                    min_disk_free_bytes):
            break
        cap *= 2

    # ── Phase 2: Binary search (only if sweep found a failure) ──────────
    tested = sorted(all_results.keys())
    last_ok = None
    first_fail = None
    for c in tested:
        r = all_results[c]
        if _passes_safety_gates(r, max_error_rate, max_iowait,
                                max_peak_memory_ratio, max_fd_util_ratio,
                                min_disk_free_bytes):
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
            if _passes_safety_gates(result, max_error_rate, max_iowait,
                                    max_peak_memory_ratio, max_fd_util_ratio,
                                    min_disk_free_bytes):
                last_ok = mid
                lo = mid + 1
            else:
                first_fail = mid
                hi = mid - 1

    results = [all_results[c] for c in sorted(all_results.keys())]
    return results, exponential_caps, binary_caps


def classify_bottleneck(result: CapResult) -> str:
    """Classify the dominant observed resource using one canonical policy.

    Precedence is chosen so that the tightest / hardest-to-resolve bound
    wins: FD exhaustion is a hard crash, memory pressure triggers OOM
    kills, GPU saturation stalls NVENC pipelines, I/O wait starves CPU,
    and only last does raw CPU saturation dominate.

    Thresholds (derived from the capacity certification plan):
      FD_BOUND:       fd_util_peak >= 0.80
      MEMORY_BOUND:   host_memory_peak_ratio >= 0.85
      GPU_BOUND:      gpu_util_peak >= 0.90  (N/A when metrics absent)
      IO_BOUND:       iowait_avg >= 0.25
      CPU_BOUND:      cpu_peak >= 0.90
    """
    if result.fd_util_peak_ratio is not None and result.fd_util_peak_ratio >= 0.80:
        return "FD_BOUND"
    if result.host_memory_peak_ratio is not None and result.host_memory_peak_ratio >= 0.85:
        return "MEMORY_BOUND"
    if result.gpu_util_peak_ratio is not None and result.gpu_util_peak_ratio >= 0.90:
        return "GPU_BOUND"
    if result.disk_wait_avg_ratio is not None and result.disk_wait_avg_ratio >= 0.25:
        return "IO_BOUND"
    if result.cpu_peak_ratio is not None and result.cpu_peak_ratio >= 0.90:
        return "CPU_BOUND"
    return "UNKNOWN"


def render_command(template: str, **values: str | int) -> str:
    """Substitute trusted placeholders as shell-quoted arguments.

    The command itself is intentionally operator-owned, but job/artifact
    values come from HTTP responses and must not be interpolated raw.
    """
    rendered = template
    for name, value in values.items():
        rendered = rendered.replace("{" + name + "}", shlex.quote(str(value)))
    return rendered


def command_for(template: str, cap: int, worker_id: str, master_url: str) -> str:
    return render_command(template, cap=cap, worker_id=worker_id, master_url=master_url)


def run_cap_command(template: str, cap: int, worker_id: str, master_url: str, dry_run: bool) -> None:
    required = ("{cap}", "{worker_id}", "{master_url}")
    if any(token not in template for token in required):
        raise ValueError("set-cap command must include {cap}, {worker_id}, and {master_url}")
    command = command_for(template, cap, worker_id, master_url)
    if dry_run:
        print(f"DRY-RUN set cap={cap}: {command}")
        return
    completed = subprocess.run(command, shell=True, text=True, timeout=120)
    if completed.returncode:
        raise RuntimeError(f"cap command failed for {cap}: exit={completed.returncode}")


def build_payload(builder: Path, fixtures: Path, worker_id: str, destination: str, suffix: int) -> dict[str, Any]:
    command = [
        sys.executable,
        str(builder),
        "--fixtures", str(fixtures),
        "--worker-id", worker_id,
        "--placement-pin-worker-id", worker_id,
        "--destination", destination,
        "--idempotency-suffix", str(suffix),
        "--strict",
    ]
    output = subprocess.check_output(command, text=True, stderr=subprocess.PIPE)
    return json.loads(output)


def provision_m2m(master_url: str, admin_token: str) -> tuple[str, str]:
    client_id = f"parallel-bench-{int(time.time())}-{os.getpid()}"
    status, body, _ = http_json("POST", f"{master_url}/api/v1/admin/m2m/keys", admin_token, {
        "client_id": client_id,
        "description": "parallelism certification ephemeral client",
        "scopes": ["jobs.submit"],
        "rate_limit_rps": 20,
        "rate_limit_burst": 40,
        "quota_max_scenes": 100,
        "quota_max_total_secs": 3600,
    })
    if status != 201 or not body.get("plaintext_secret"):
        raise RuntimeError(f"M2M provisioning failed: HTTP {status}: {body}")
    return client_id, str(body["plaintext_secret"])


def delete_m2m(master_url: str, admin_token: str, client_id: str) -> None:
    http_json("DELETE", f"{master_url}/api/v1/admin/m2m/keys/{urllib.parse.quote(client_id)}", admin_token, timeout=10)


def wait_cap(master_url: str, admin_token: str, worker_id: str, cap: int, timeout_s: int) -> None:
    deadline = time.monotonic() + timeout_s
    url = f"{master_url}/api/v1/admin/workers/{urllib.parse.quote(worker_id)}"
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        status, last, _ = http_json("GET", url, admin_token, timeout=10)
        advertised_cap = last.get("max_active_jobs", last.get("task_slots", -1))
        active_jobs = last.get("active_jobs", last.get("active_tasks", 0))
        if status == 200 and int(advertised_cap) == cap and int(active_jobs) == 0:
            return
        time.sleep(1)
    raise RuntimeError(f"worker cap did not converge to {cap}: {last}")


def run_correctness_command(template: str, job_id: str, worker_id: str, master_url: str, artifact_url: str = "", response_json: str = "") -> tuple[bool | None, str]:
    """Run the operator-owned artifact verifier for one terminal job.

    The hook must exit 0 only when the produced video passes the site's
    canonical verification (for example verify_artifact.sh plus a downloaded
    artifact). Non-zero is an incorrect video; an empty hook is an unknown
    result and therefore prevents certification rather than becoming zero.
    """
    if not template.strip():
        return None, "correctness hook not configured"
    if artifact_url:
        parsed_artifact = urllib.parse.urlparse(artifact_url)
        parsed_master = urllib.parse.urlparse(master_url)
        if parsed_artifact.scheme:
            if parsed_artifact.scheme not in {"http", "https"}:
                return False, "artifact URL must use http or https"
            if parsed_artifact.netloc != parsed_master.netloc:
                return False, "artifact URL host is outside the configured master host"
    required = ("{job_id}", "{worker_id}", "{master_url}", "{artifact_url}", "{response_json}")
    if any(token not in template for token in required):
        return None, "correctness hook must include all job/artifact/response placeholders"
    command = render_command(
        template, job_id=job_id, worker_id=worker_id, master_url=master_url,
        artifact_url=artifact_url, response_json=response_json,
    )
    try:
        completed = subprocess.run(command, shell=True, text=True, capture_output=True, timeout=900)
    except subprocess.TimeoutExpired:
        return False, "verifier timeout after 900s"
    except (OSError, ValueError) as exc:
        return False, f"verifier invocation failed: {exc}"
    if completed.returncode == 0:
        return True, ""
    detail = (completed.stderr or completed.stdout or "").strip().replace("\n", " ")
    return False, f"verifier exit={completed.returncode}{(': ' + detail[:300]) if detail else ''}"


def submit_and_poll(master_url: str, bearer: str, payload: dict[str, Any], timeout_s: int, correctness_command: str = "", worker_id: str = "", response_dir: Path | None = None) -> JobResult:
    start = now_ms()
    status, body, _ = http_json("POST", f"{master_url}/api/v1/jobs", bearer, payload, timeout=30)
    if status != 202 or not body.get("job_id"):
        return JobResult("", "SUBMIT_FAILED", None, f"HTTP {status}: {body}")
    job_id = str(body["job_id"])
    deadline = time.monotonic() + timeout_s
    last_status = ""
    last_body: dict[str, Any] = {}
    while time.monotonic() < deadline:
        status, last_body, _ = http_json("GET", f"{master_url}/api/v1/jobs/{urllib.parse.quote(job_id)}", bearer, timeout=15)
        if status != 200:
            return JobResult(job_id, "POLL_FAILED", None, f"HTTP {status}: {last_body}")
        last_status = str(last_body.get("status", ""))
        if last_status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            latency = float(now_ms() - start)
            if last_status != "SUCCEEDED":
                return JobResult(job_id, last_status, latency, str(last_body.get("error", last_status)))
            response_json = ""
            artifact_url = str(last_body.get("artifact_url") or last_body.get("artifact_path") or last_body.get("output_path") or "")
            if response_dir is not None:
                response_dir.mkdir(parents=True, exist_ok=True)
                safe_job_id = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in job_id)
                response_path = response_dir / f"{safe_job_id}.json"
                response_path.write_text(json.dumps(last_body, sort_keys=True), encoding="utf-8")
                response_json = str(response_path)
            correct, correctness_error = run_correctness_command(
                correctness_command, job_id, worker_id, master_url, artifact_url, response_json,
            )
            return JobResult(job_id, last_status, latency, "", correct, correctness_error)
        time.sleep(1)
    return JobResult(job_id, "TIMEOUT", None, f"last_status={last_status}")


class Sampler:
    def __init__(self, urls: list[str], token: str, interval_s: float) -> None:
        self.urls = urls
        self.token = token
        self.interval_s = interval_s
        self.samples: list[dict[str, float | None]] = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def finish(self) -> None:
        self.stop.set()
        self.thread.join(timeout=max(2.0, self.interval_s * 2))

    def _run(self) -> None:
        while not self.stop.is_set():
            merged: dict[str, float] = {}
            for url in self.urls:
                for name, value in parse_prometheus(scrape_text(url, self.token)).items():
                    merged[name] = merged.get(name, 0.0) + value
            if merged:
                self.samples.append(extract_observations(merged))
            self.stop.wait(self.interval_s)


def aggregate_gauge(samples: list[dict[str, float | None]], key: str, fn: str) -> float | None:
    values = [float(s[key]) for s in samples if s.get(key) is not None]
    if not values:
        return None
    return statistics.mean(values) if fn == "avg" else max(values)


def run_one_cap(args: argparse.Namespace, cap: int, admin_token: str, m2m_token: str) -> CapResult:
    run_start = time.monotonic()
    run_before: dict[str, float] = {}
    for url in args.metrics_url:
        for name, value in parse_prometheus(scrape_text(url, admin_token)).items():
            run_before[name] = run_before.get(name, 0.0) + value
    before_obs = extract_observations(run_before)
    sampler = Sampler(args.metrics_url, admin_token, args.sample_interval_s)
    sampler.start()
    jobs: list[JobResult] = []
    try:
        payloads = [build_payload(args.builder, args.fixtures, args.worker_id, args.destination, cap * 10000 + i) for i in range(args.jobs)]
        with ThreadPoolExecutor(max_workers=args.jobs) as pool:
            futures = [pool.submit(
                submit_and_poll, args.master_url, m2m_token, p, args.poll_timeout_s,
                args.correctness_command, args.worker_id, args.response_dir,
            ) for p in payloads]
            for future in as_completed(futures):
                jobs.append(future.result())
    finally:
        sampler.finish()
    run_after: dict[str, float] = {}
    for url in args.metrics_url:
        for name, value in parse_prometheus(scrape_text(url, admin_token)).items():
            run_after[name] = run_after.get(name, 0.0) + value
    after_obs = extract_observations(run_after)
    wall_ms = (time.monotonic() - run_start) * 1000
    succeeded = sum(j.status == "SUCCEEDED" for j in jobs)
    failed = len(jobs) - succeeded
    correct_videos = sum(j.correct is True for j in jobs)
    latencies = [j.latency_ms for j in jobs if j.latency_ms is not None and j.status == "SUCCEEDED"]
    correctness_missing = any(j.status == "SUCCEEDED" and j.correct is None for j in jobs)
    correctness_failed = any(j.status == "SUCCEEDED" and j.correct is False for j in jobs)
    samples = sampler.samples
    missing = [key for key in REQUIRED_METRICS if before_obs.get(key) is None or after_obs.get(key) is None]
    hits = delta(before_obs.get("cache_hits"), after_obs.get("cache_hits"))
    misses = delta(before_obs.get("cache_misses"), after_obs.get("cache_misses"))
    hit_ratio = (hits / (hits + misses)) if hits is not None and misses is not None and hits + misses > 0 else None
    error_counter = delta(before_obs.get("errors"), after_obs.get("errors"))
    errors_count = float(failed) if error_counter is None else max(float(failed), error_counter)
    return CapResult(
        max_active_jobs=cap,
        status=(
            "FAIL" if failed > 0 or correctness_failed else
            "INCOMPLETE" if missing or correctness_missing else
            "PASS"
        ),
        wall_time_ms=wall_ms,
        throughput_jobs_per_hour=(succeeded / (wall_ms / 3_600_000)) if wall_ms > 0 else 0,
        correct_videos=correct_videos,
        correct_videos_per_hour=(correct_videos / (wall_ms / 3_600_000)) if wall_ms > 0 else 0,
        succeeded=succeeded,
        failed=failed,
        error_rate=(failed / len(jobs)) if jobs else 1.0,
        latency_mean_ms=statistics.mean(latencies) if latencies else None,
        latency_p95_ms=percentile(latencies, 0.95),
        cpu_avg_ratio=aggregate_gauge(samples, "cpu_utilization_ratio", "avg"),
        cpu_peak_ratio=aggregate_gauge(samples, "cpu_utilization_ratio", "max"),
        rss_avg_bytes=aggregate_gauge(samples, "rss_bytes", "avg"),
        rss_peak_bytes=aggregate_gauge(samples, "rss_bytes", "max"),
        host_memory_used_avg_bytes=aggregate_gauge(samples, "host_memory_used_bytes", "avg"),
        host_memory_used_peak_bytes=aggregate_gauge(samples, "host_memory_used_bytes", "max"),
        host_memory_available_bytes=aggregate_gauge(samples, "host_memory_available_bytes", "min"),
        host_memory_peak_ratio=_peak_memory_ratio(
            aggregate_gauge(samples, "host_memory_used_bytes", "max"),
            aggregate_gauge(samples, "host_memory_available_bytes", "min"),
        ),
        fd_util_avg_ratio=aggregate_gauge(samples, "fd_util", "avg"),
        fd_util_peak_ratio=aggregate_gauge(samples, "fd_util", "max"),
        disk_wait_avg_ratio=aggregate_gauge(samples, "disk_wait_ratio", "avg"),
        disk_free_min_bytes=aggregate_gauge(samples, "disk_free_bytes", "min"),
        scratch_current_bytes=aggregate_gauge(samples, "scratch_current_bytes", "max"),
        scratch_peak_bytes=aggregate_gauge(samples, "scratch_peak_bytes", "max"),
        gpu_util_avg_ratio=normalize_ratio(aggregate_gauge(samples, "gpu_util_avg_ratio", "avg")),
        gpu_util_peak_ratio=normalize_ratio(aggregate_gauge(samples, "gpu_util_peak_ratio", "max")),
        cache_hits=hits,
        cache_misses=misses,
        cache_hit_ratio=hit_ratio,
        downloads=delta(before_obs.get("downloads"), after_obs.get("downloads")),
        duplicate_downloads=delta(before_obs.get("duplicate_downloads"), after_obs.get("duplicate_downloads")),
        duplicate_download_bytes=delta(before_obs.get("duplicate_download_bytes"), after_obs.get("duplicate_download_bytes")),
        errors=errors_count,
        missing_metrics=missing,
        jobs=jobs,
    )


def choose_limit(results: list[CapResult], min_gain_pct: float, max_p95_ms: float | None, max_error_rate: float, max_iowait: float, max_peak_memory_ratio: float, max_fd_util_ratio: float, min_disk_free_bytes: float) -> None:
    """Evaluate every cap result and set efficient/decision/limiting_resource.

    Hard-stop gates (RAM, FD, disk) are evaluated FIRST via
    _check_hard_stop_gates().  If any gate fails, the cell is immediately
    disqualified — no throughput-gain argument can override it.
    """
    # Cap 1 is the measured baseline. Compare higher caps only with the last
    # eligible result, so an incomplete/intermittently bad cell cannot poison
    # every later comparison. If cap 1 is not valid, no higher cap can become
    # a substitute baseline for this certification.
    previous_eligible: CapResult | None = None
    baseline_valid = bool(results) and results[0].max_active_jobs == 1
    for index, result in enumerate(results):
        # ── Hard-stop gates (evaluated first, override everything) ──────
        gates = _check_hard_stop_gates(result, max_peak_memory_ratio, max_fd_util_ratio, min_disk_free_bytes)
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
        if max_p95_ms is not None and (result.latency_p95_ms is None or result.latency_p95_ms > max_p95_ms):
            checks.append("p95_limit")
        if result.disk_wait_avg_ratio is not None and result.disk_wait_avg_ratio > max_iowait:
            checks.append("iowait_limit")
        gain = None
        if index > 0 and previous_eligible is None:
            checks.append("baseline_unavailable")
        if previous_eligible and previous_eligible.correct_videos_per_hour > 0:
            gain = (result.correct_videos_per_hour / previous_eligible.correct_videos_per_hour - 1) * 100
            if gain < min_gain_pct:
                checks.append(f"correct_video_gain<{min_gain_pct}%")
        if index == 0 and not baseline_valid:
            checks.append("missing_cap_1_baseline")
        result.efficient = not checks
        result.decision = "baseline" if previous_eligible is None and result.efficient else "eligible" if result.efficient else "; ".join(checks)
        result.limiting_resource = classify_bottleneck(result)
        if result.efficient:
            previous_eligible = result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker-id", default=os.getenv("PARALLEL_BENCH_WORKER_ID", ""))
    parser.add_argument("--master-url", default=os.getenv("VELOX_MASTER_URL", "http://127.0.0.1:8000").rstrip("/"))
    parser.add_argument("--metrics-url", action="append", default=[])
    parser.add_argument("--set-cap-command", default=os.getenv("PARALLEL_BENCH_SET_CAP_CMD", ""))
    parser.add_argument("--builder", type=Path, default=Path(__file__).with_name("build_real_payload.py"))
    parser.add_argument("--fixtures", type=Path, default=Path(__file__).with_name("fixtures") / "assets.json")
    parser.add_argument(
        "--destination",
        required=not bool(os.getenv("BENCH_DESTINATION_ID", "").strip()),
        default=os.getenv("BENCH_DESTINATION_ID", "").strip() or None,
        help="explicit delivery destination_id; implicit destinations are forbidden",
    )
    parser.add_argument("--jobs", type=int, default=int(os.getenv("PARALLEL_BENCH_JOBS", "6")))
    parser.add_argument("--poll-timeout-s", type=int, default=int(os.getenv("BENCH_POLL_TIMEOUT_S", "300")))
    parser.add_argument("--wait-cap-timeout-s", type=int, default=120)
    parser.add_argument("--sample-interval-s", type=float, default=2.0)
    parser.add_argument("--min-throughput-gain-pct", type=float, default=5.0)
    parser.add_argument("--max-p95-ms", type=float, default=None)
    parser.add_argument("--max-error-rate", type=float, default=0.0)
    parser.add_argument("--max-iowait-ratio", type=float, default=0.25)
    parser.add_argument("--max-peak-memory-ratio", type=float, default=0.85,
                        help="reject caps where peak host memory used/total exceeds this ratio (0-1)")
    parser.add_argument("--max-fd-util-ratio", type=float, default=0.80,
                        help="reject caps where FD utilization exceeds this ratio (0-1)")
    parser.add_argument("--min-disk-free-bytes", type=float, default=10_000_000_000,
                        help="reject caps where minimum disk free bytes falls below this threshold")
    parser.add_argument("--max-cap", type=int, default=DEFAULT_MAX_CAP,
                        help="highest concurrency cap to test (every cap 1..N is measured)")
    parser.add_argument(
        "--correctness-command", default=os.getenv("PARALLEL_BENCH_CORRECTNESS_CMD", ""),
        help="operator-owned verifier command; placeholders: {job_id}, {worker_id}, {master_url}; exit 0 means correct video",
    )
    parser.add_argument("--response-dir", type=Path, default=None, help="directory for terminal job JSON responses passed to the correctness hook")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--leave-cap", action="store_true", help="Do not restore MaxActiveJobs=1 after the matrix")
    args = parser.parse_args()
    if not args.metrics_url:
        env_urls = os.getenv("PARALLEL_BENCH_METRICS_URL", "")
        args.metrics_url = [u.strip() for u in env_urls.split(",") if u.strip()]
    args.master_url = args.master_url.rstrip("/")
    return args


def main() -> int:
    args = parse_args()
    if args.jobs < 1 or args.max_cap < 1 or not args.worker_id or not args.set_cap_command or not args.metrics_url:
        print("live certification requires --worker-id, --set-cap-command, and --metrics-url", file=sys.stderr)
        return 2
    if not args.builder.is_file() or not args.fixtures.is_file():
        print("canonical payload builder or assets fixture is missing", file=sys.stderr)
        return 2
    if args.dry_run:
        # Show the exponential sweep sequence (what would actually be tested).
        sweep: list[int] = []
        cap = 1
        while cap <= args.max_cap:
            sweep.append(cap)
            cap *= 2
        print(json.dumps({
            "search_strategy": "exponential_sweep_then_binary_search",
            "max_cap": args.max_cap,
            "exponential_sweep": sweep,
            "worker_id": args.worker_id,
            "jobs": args.jobs,
            "set_cap_command": args.set_cap_command,
            "correctness_command": args.correctness_command,
            "response_dir": str(args.response_dir) if args.response_dir else "",
            "decision_metric": "correct_videos_per_hour",
        }, indent=2))
        return 0
    if not args.correctness_command.strip() or args.response_dir is None:
        print("live certification requires --correctness-command and --response-dir", file=sys.stderr)
        return 2

    try:
        admin_token = read_token()
        client_id, m2m_token = provision_m2m(args.master_url, admin_token)
    except Exception as exc:  # fail closed before changing cap
        print(f"prerequisite failure: {exc}", file=sys.stderr)
        return 2

    def run_cap(cap: int) -> CapResult:
        run_cap_command(args.set_cap_command, cap, args.worker_id, args.master_url, False)
        wait_cap(args.master_url, admin_token, args.worker_id, cap, args.wait_cap_timeout_s)
        return run_one_cap(args, cap, admin_token, m2m_token)

    try:
        results, exp_caps, bin_caps = dynamic_cap_search(
            run_cap, args.max_cap,
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

    tested_caps = [r.max_active_jobs for r in results]
    choose_limit(results, args.min_throughput_gain_pct, args.max_p95_ms, args.max_error_rate, args.max_iowait_ratio, args.max_peak_memory_ratio, args.max_fd_util_ratio, args.min_disk_free_bytes)
    eligible = [r.max_active_jobs for r in results if r.efficient]
    efficient_limit = max(eligible) if eligible else None
    recommended_limit = max(1, efficient_limit - 1) if efficient_limit is not None else None
    boundary = next((r for r in results if not r.efficient), None)
    report = {
        "schema": "velox.parallelism-certification.v1",
        "worker_id": args.worker_id,
        "master_url": args.master_url,
        "search_strategy": {
            "method": "exponential_sweep_then_binary_search",
            "max_cap": args.max_cap,
            "exponential_sweep": exp_caps,
            "binary_search": bin_caps,
            "total_tests": len(results),
        },
        "tested_caps": tested_caps,
        "jobs_per_cap": args.jobs,
        "metrics_urls": args.metrics_url,
        "correctness_command_configured": bool(args.correctness_command.strip()),
        "response_dir": str(args.response_dir) if args.response_dir else "",
        "decision_metric": "correct_videos_per_hour",
        "protocol": {"lease_owner": "master", "singleflight_owner": "worker-cache", "cap_selection": "operator command hook"},
        "efficient_limit": efficient_limit,
        "certified_max_jobs": efficient_limit,
        "recommended_production_jobs": recommended_limit,
        "limiting_resource": (next((r.limiting_resource for r in reversed(results) if r.efficient and r.limiting_resource != "UNKNOWN"), "UNKNOWN")),
        "boundary_rejection": ({"jobs": boundary.max_active_jobs, "reason": boundary.decision} if boundary else None),
        "hard_stop_gates": HARD_STOP_GATES,
        "hard_stop_gate_thresholds": {
            "peak_ram": args.max_peak_memory_ratio,
            "fd_util": args.max_fd_util_ratio,
            "disk_free_bytes": args.min_disk_free_bytes,
        },
        "max_peak_memory_ratio": args.max_peak_memory_ratio,
        "max_fd_util_ratio": args.max_fd_util_ratio,
        "min_disk_free_bytes": args.min_disk_free_bytes,
        "certified": (
            efficient_limit is not None
            and all(r.status == "PASS" for r in results if r.efficient)
            and results[0].max_active_jobs == 1
            and results[0].efficient is True
        ),
        "results": [{**asdict(r), "jobs": [asdict(j) for j in r.jobs]} for r in results],
    }
    # The report is persisted by the Master through capacity_benchmark_runs
    # and its normalized child tables. Do not create a competing JSON file.
    print(json.dumps({"certified": report["certified"], "certified_max_jobs": efficient_limit, "recommended_production_jobs": recommended_limit, "total_tests": len(results)}, indent=2))
    return 0 if report["certified"] else 4


if __name__ == "__main__":
    raise SystemExit(main())
