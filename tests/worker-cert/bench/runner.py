"""Command rendering, payload building, job submission/polling, and cap execution."""

from __future__ import annotations

import json
import shlex
import statistics
import subprocess
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from bench.models import CapResult, JobResult, REQUIRED_METRICS, now_ms, percentile
from bench.http import http_json
from bench.metrics import (
    aggregate_gauge,
    delta,
    extract_observations,
    normalize_ratio,
    parse_prometheus,
    peak_memory_ratio,
)
from bench.sampling import Sampler


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


def run_cap_command(
    template: str, cap: int, worker_id: str, master_url: str, dry_run: bool
) -> None:
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


def build_payload(
    builder: Path,
    fixtures: Path,
    worker_id: str,
    destination: str,
    suffix: int,
) -> dict[str, Any]:
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


def wait_cap(
    master_url: str,
    admin_token: str,
    worker_id: str,
    cap: int,
    timeout_s: int,
) -> None:
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


def run_correctness_command(
    template: str,
    job_id: str,
    worker_id: str,
    master_url: str,
    artifact_url: str = "",
    response_json: str = "",
) -> tuple[bool | None, str]:
    """Run the operator-owned artifact verifier for one terminal job."""
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
        template,
        job_id=job_id,
        worker_id=worker_id,
        master_url=master_url,
        artifact_url=artifact_url,
        response_json=response_json,
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


def submit_and_poll(
    master_url: str,
    bearer: str,
    payload: dict[str, Any],
    timeout_s: int,
    correctness_command: str = "",
    worker_id: str = "",
    response_dir: Path | None = None,
) -> JobResult:
    """Submit a job and poll until terminal, running the correctness hook on success."""
    start = now_ms()
    status, body, _ = http_json("POST", f"{master_url}/api/v1/jobs", bearer, payload, timeout=30)
    if status != 202 or not body.get("job_id"):
        return JobResult("", "SUBMIT_FAILED", None, f"HTTP {status}: {body}")
    job_id = str(body["job_id"])
    deadline = time.monotonic() + timeout_s
    last_status = ""
    last_body: dict[str, Any] = {}
    while time.monotonic() < deadline:
        status, last_body, _ = http_json(
            "GET", f"{master_url}/api/v1/jobs/{urllib.parse.quote(job_id)}", bearer, timeout=15
        )
        if status != 200:
            return JobResult(job_id, "POLL_FAILED", None, f"HTTP {status}: {last_body}")
        last_status = str(last_body.get("status", ""))
        if last_status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            latency = float(now_ms() - start)
            if last_status != "SUCCEEDED":
                return JobResult(job_id, last_status, latency, str(last_body.get("error", last_status)))
            response_json = ""
            artifact_url = str(
                last_body.get("artifact_url")
                or last_body.get("artifact_path")
                or last_body.get("output_path")
                or ""
            )
            if response_dir is not None:
                response_dir.mkdir(parents=True, exist_ok=True)
                safe_job_id = "".join(
                    ch if ch.isalnum() or ch in "-_." else "_" for ch in job_id
                )
                response_path = response_dir / f"{safe_job_id}.json"
                response_path.write_text(json.dumps(last_body, sort_keys=True), encoding="utf-8")
                response_json = str(response_path)
            correct, correctness_error = run_correctness_command(
                correctness_command, job_id, worker_id, master_url, artifact_url, response_json,
            )
            return JobResult(job_id, last_status, latency, "", correct, correctness_error)
        time.sleep(1)
    return JobResult(job_id, "TIMEOUT", None, f"last_status={last_status}")


def run_one_cap(args: Any, cap: int, admin_token: str, m2m_token: str) -> CapResult:
    """Execute a single cap cell: set cap, submit jobs, sample metrics, return CapResult."""
    run_start = time.monotonic()

    # ── Pre-run snapshot ────────────────────────────────────────────────
    run_before: dict[str, float] = {}
    for url in args.metrics_url:
        for name, value in parse_prometheus(scrape_text(url, admin_token)).items():
            run_before[name] = run_before.get(name, 0.0) + value
    before_obs = extract_observations(run_before)

    # ── Sample metrics in background ────────────────────────────────────
    sampler = Sampler(args.metrics_url, admin_token, args.sample_interval_s)
    sampler.start()

    # ── Submit jobs concurrently ────────────────────────────────────────
    jobs: list[JobResult] = []
    try:
        payloads = [
            build_payload(args.builder, args.fixtures, args.worker_id, args.destination, cap * 10000 + i)
            for i in range(args.jobs)
        ]
        with ThreadPoolExecutor(max_workers=args.jobs) as pool:
            futures = [
                pool.submit(
                    submit_and_poll,
                    args.master_url,
                    m2m_token,
                    p,
                    args.poll_timeout_s,
                    args.correctness_command,
                    args.worker_id,
                    args.response_dir,
                )
                for p in payloads
            ]
            for future in as_completed(futures):
                jobs.append(future.result())
    finally:
        sampler.finish()

    # ── Post-run snapshot ───────────────────────────────────────────────
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
    hit_ratio = (
        (hits / (hits + misses))
        if hits is not None and misses is not None and hits + misses > 0
        else None
    )
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
        host_memory_peak_ratio=peak_memory_ratio(
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
        duplicate_downloads=delta(
            before_obs.get("duplicate_downloads"), after_obs.get("duplicate_downloads")
        ),
        duplicate_download_bytes=delta(
            before_obs.get("duplicate_download_bytes"), after_obs.get("duplicate_download_bytes")
        ),
        errors=errors_count,
        missing_metrics=missing,
        jobs=jobs,
    )
