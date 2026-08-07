#!/usr/bin/env python3
"""Certify the existing worker concurrency limiter at MaxActiveJobs 1, 2, 3.

This harness deliberately owns no placement, lease, download, or cleanup logic.
It only changes the operator-selected worker cap through an explicit command,
submits the canonical real-asset jobs concurrently, polls their lifecycle, and
samples already-exported Prometheus metrics.

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


CAPS = (1, 2, 3)
REQUIRED_METRICS = (
    "cpu_utilization_ratio",
    "rss_bytes",
    "disk_wait_ratio",
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
        "rss_bytes": metric(values, "velox_worker_process_rss_bytes", "velox_worker_process_rss_peak_bytes", "process_resident_memory_bytes"),
        "disk_wait_ratio": normalize_ratio(metric(values, "velox_worker_cpu_iowait_ratio", "velox_cpu_iowait_ratio", "velox_disk_wait_ratio")),
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
    }


def delta(before: float | None, after: float | None) -> float | None:
    if before is None or after is None:
        return None
    return max(0.0, after - before)


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
    rss_avg_bytes: float | None
    rss_peak_bytes: float | None
    disk_wait_avg_ratio: float | None
    cache_hits: float | None
    cache_misses: float | None
    cache_hit_ratio: float | None
    downloads: float | None
    duplicate_downloads: float | None
    duplicate_download_bytes: float | None
    errors: float | None
    missing_metrics: list[str]
    jobs: list[JobResult]
    efficient: bool | None = None
    decision: str = ""


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
        disk_wait_avg_ratio=aggregate_gauge(samples, "disk_wait_ratio", "avg"),
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


def choose_limit(results: list[CapResult], min_gain_pct: float, max_p95_ms: float | None, max_error_rate: float, max_iowait: float) -> None:
    # Cap 1 is the measured baseline. Compare higher caps only with the last
    # eligible result, so an incomplete/intermittently bad cell cannot poison
    # every later comparison. If cap 1 is not valid, no higher cap can become
    # a substitute baseline for this certification.
    previous_eligible: CapResult | None = None
    baseline_valid = bool(results) and results[0].max_active_jobs == 1
    for index, result in enumerate(results):
        checks: list[str] = []
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
    parser.add_argument("--max-iowait-ratio", type=float, default=0.35)
    parser.add_argument(
        "--correctness-command", default=os.getenv("PARALLEL_BENCH_CORRECTNESS_CMD", ""),
        help="operator-owned verifier command; placeholders: {job_id}, {worker_id}, {master_url}; exit 0 means correct video",
    )
    parser.add_argument("--response-dir", type=Path, default=None, help="directory for terminal job JSON responses passed to the correctness hook")
    parser.add_argument("--output", type=Path, default=Path("parallelism-certification.json"))
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
    if args.jobs < 1 or not args.worker_id or not args.set_cap_command or not args.metrics_url:
        print("live certification requires --worker-id, --set-cap-command, and --metrics-url", file=sys.stderr)
        return 2
    if not args.builder.is_file() or not args.fixtures.is_file():
        print("canonical payload builder or assets fixture is missing", file=sys.stderr)
        return 2
    if args.dry_run:
        print(json.dumps({
            "caps": CAPS,
            "worker_id": args.worker_id,
            "jobs": args.jobs,
            "set_cap_commands": [command_for(args.set_cap_command, c, args.worker_id, args.master_url) for c in CAPS],
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

    results: list[CapResult] = []
    try:
        for cap in CAPS:
            run_cap_command(args.set_cap_command, cap, args.worker_id, args.master_url, False)
            wait_cap(args.master_url, admin_token, args.worker_id, cap, args.wait_cap_timeout_s)
            results.append(run_one_cap(args, cap, admin_token, m2m_token))
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

    choose_limit(results, args.min_throughput_gain_pct, args.max_p95_ms, args.max_error_rate, args.max_iowait_ratio)
    eligible = [r.max_active_jobs for r in results if r.efficient]
    efficient_limit = max(eligible) if eligible else None
    report = {
        "schema": "velox.parallelism-certification.v1",
        "worker_id": args.worker_id,
        "master_url": args.master_url,
        "caps": list(CAPS),
        "jobs_per_cap": args.jobs,
        "metrics_urls": args.metrics_url,
        "correctness_command_configured": bool(args.correctness_command.strip()),
        "response_dir": str(args.response_dir) if args.response_dir else "",
        "decision_metric": "correct_videos_per_hour",
        "protocol": {"lease_owner": "master", "singleflight_owner": "worker-cache", "cap_selection": "operator command hook"},
        "efficient_limit": efficient_limit,
        "certified": efficient_limit is not None and all(r.status == "PASS" for r in results),
        "results": [{**asdict(r), "jobs": [asdict(j) for j in r.jobs]} for r in results],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"certified": report["certified"], "efficient_limit": efficient_limit, "output": str(args.output)}, indent=2))
    return 0 if report["certified"] else 4


if __name__ == "__main__":
    raise SystemExit(main())
