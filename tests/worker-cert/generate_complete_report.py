#!/usr/bin/env python3
"""Build a complete worker-cert report without inventing missing evidence.

The sequential benchmark is an orchestration report, not proof that the final
media artifact passed. This aggregator keeps every run identity and timing
field, merges optional job evidence and renderer sidecars, and applies the
media PASS/FAIL gates fail-closed. Missing evidence is reported as FAIL with
an explicit NOT_RUN evidence state.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

SCHEMA = "tests/worker-cert/complete_report@1"
SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
REQUIRED_TIMESTAMP_KEYS = (
    "submit_started_at",
    "accepted_at",
    "lease_granted_at",
    "task_started_at",
    "assets_ready_at",
    "render_started_at",
    "render_completed_at",
    "artifact_declared_at",
    "artifact_committed_at",
    "job_completed_at",
)
REQUIRED_DURATION_KEYS = (
    "submit",
    "queue_wait",
    "lease_to_start",
    "asset_materialization",
    "render",
    "artifact_commit",
    "total",
)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def as_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def as_int(value: Any) -> int | None:
    number = as_number(value)
    if number is None or not number.is_integer():
        return None
    return int(number)


def parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def status(name: str, passed: bool, detail: str, *, evidence_status: str = "OBSERVED") -> dict[str, str]:
    return {
        "name": name,
        "status": "PASS" if passed else "FAIL",
        "evidence_status": evidence_status,
        "detail": detail,
    }


def review_status(name: str, detail: str) -> dict[str, str]:
    return {"name": name, "status": "REVIEW_REQUIRED", "evidence_status": "REVIEW_REQUIRED", "detail": detail}


def missing_status(name: str, detail: str) -> dict[str, str]:
    return status(name, False, detail, evidence_status="NOT_RUN")


def flatten_asset_records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        records: list[dict[str, Any]] = []
        for key, item in value.items():
            if isinstance(item, dict):
                record = dict(item)
                record.setdefault("asset_id", key)
                records.append(record)
        return records
    return []


def candidate_json_files(root: Path) -> Iterable[Path]:
    if not root.is_dir():
        return ()
    return (path for path in root.rglob("*.json") if path.is_file())


def find_job_evidence(root: Path | None, job_id: str) -> tuple[dict[str, Any] | None, str | None]:
    if root is None:
        return None, None
    for path in candidate_json_files(root):
        try:
            value = read_json(path)
        except (OSError, json.JSONDecodeError):
            continue
        candidates: list[Any] = value if isinstance(value, list) else [value]
        for candidate in candidates:
            if isinstance(candidate, dict) and candidate.get("job_id") == job_id:
                return candidate, str(path)
    return None, None


def sidecar_from(evidence: dict[str, Any] | None, sidecar_root: Path | None, job_id: str, artifact_path: str) -> tuple[dict[str, Any] | None, str | None]:
    if evidence:
        for key in ("sidecar", "progress_sidecar", "sidecar_path", "progress_path"):
            value = evidence.get(key)
            if isinstance(value, dict):
                return value, f"evidence.{key}"
            if isinstance(value, str) and value:
                candidate = Path(value)
                if not candidate.is_absolute() and sidecar_root:
                    candidate = sidecar_root / candidate
                if candidate.is_file():
                    try:
                        return read_json(candidate), str(candidate)
                    except (OSError, json.JSONDecodeError):
                        pass
    candidates: list[Path] = []
    if artifact_path:
        candidates.append(Path(artifact_path + ".progress.json"))
    if sidecar_root:
        candidates.extend(sidecar_root.rglob(f"{job_id}*.progress.json"))
    for candidate in candidates:
        if candidate.is_file():
            try:
                return read_json(candidate), str(candidate)
            except (OSError, json.JSONDecodeError):
                continue
    return None, None


def evidence_check(evidence: dict[str, Any] | None, *keys: str) -> Any:
    current: Any = evidence
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def build_timestamps(run: dict[str, Any], evidence: dict[str, Any] | None) -> dict[str, Any]:
    source = evidence.get("timestamps", {}) if isinstance(evidence, dict) else {}
    timestamps = {key: source.get(key) for key in REQUIRED_TIMESTAMP_KEYS}
    # Preserve the benchmark's original fields without pretending their exact
    # phase semantics are known.
    timestamps["source_started_at"] = run.get("started_at")
    timestamps["source_completed_at"] = run.get("completed_at")
    timestamps["source_written_at"] = evidence.get("written_at") if evidence else None
    return timestamps


def validate_timestamps(timestamps: dict[str, Any]) -> dict[str, Any]:
    missing = [key for key in REQUIRED_TIMESTAMP_KEYS if not timestamps.get(key)]
    if missing:
        return missing_status("timestamps", f"missing canonical lifecycle timestamps: {', '.join(missing)}")
    parsed = [parse_time(timestamps[key]) for key in REQUIRED_TIMESTAMP_KEYS]
    if any(value is None for value in parsed):
        return status("timestamps", False, "one or more canonical timestamps are not ISO-8601 values")
    assert all(value is not None for value in parsed)
    if any(left > right for left, right in zip(parsed, parsed[1:])):
        return status("timestamps", False, "canonical lifecycle timestamps are not chronological")
    return status("timestamps", True, "canonical lifecycle timestamps are present and chronological")


def evidence_assets(evidence: dict[str, Any] | None) -> Any:
    if not isinstance(evidence, dict):
        return None
    return evidence.get("assets") or evidence.get("asset_operations") or evidence_check(evidence, "metrics", "asset_operations")


def evidence_cache(evidence: dict[str, Any] | None) -> Any:
    if not isinstance(evidence, dict):
        return None
    return evidence.get("cache") or evidence.get("cache_stats") or evidence_check(evidence, "metrics", "cache_stats")


def evidence_section(evidence: dict[str, Any] | None, name: str) -> Any:
    if not isinstance(evidence, dict):
        return None
    return evidence.get(name) or evidence_check(evidence, "metrics", name)


def validator_checks(evidence: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Index checks emitted by verify_media_artifacts.sh across all artifacts."""
    if not isinstance(evidence, dict):
        return {}
    artifacts = evidence.get("artifacts")
    if not isinstance(artifacts, list):
        return {}
    indexed: dict[str, dict[str, Any]] = {}
    for artifact_index, artifact in enumerate(artifacts):
        checks = artifact.get("checks") if isinstance(artifact, dict) else None
        if not isinstance(checks, list):
            continue
        for item in checks:
            if isinstance(item, dict) and item.get("name"):
                indexed[f"{artifact_index}:{item['name']}"] = item
    return indexed


def validator_group_status(checks: dict[str, dict[str, Any]], names: tuple[str, ...], group: str) -> dict[str, Any] | None:
    """Require every requested check for every validator artifact."""
    if not checks:
        return None
    grouped: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for key, item in checks.items():
        if ":" in key:
            artifact_index, check_name = key.split(":", 1)
        else:
            artifact_index, check_name = "manual", key
        if check_name in names:
            grouped[artifact_index][check_name] = item
    if not grouped:
        return None
    failed: list[str] = []
    review: list[str] = []
    incomplete: list[str] = []
    for artifact_index, artifact_checks in grouped.items():
        missing = [name for name in names if name not in artifact_checks]
        if missing:
            incomplete.append(f"artifact {artifact_index}: {', '.join(missing)}")
            continue
        failed.extend(f"artifact {artifact_index}:{name}" for name in names if artifact_checks[name].get("status") == "FAIL")
        review.extend(f"artifact {artifact_index}:{name}" for name in names if artifact_checks[name].get("status") == "REVIEW_REQUIRED")
    if incomplete:
        return status(group, False, f"validator checks incomplete: {'; '.join(incomplete)}")
    if failed:
        return status(group, False, f"validator checks failed: {', '.join(failed)}")
    if review:
        return review_status(group, f"validator checks require review: {', '.join(review)}")
    return status(group, True, "validator checks passed for every artifact")


def build_durations(run: dict[str, Any], timestamps: dict[str, Any], evidence: dict[str, Any] | None) -> dict[str, Any]:
    source = evidence.get("durations_ms", {}) if isinstance(evidence, dict) else {}
    durations: dict[str, Any] = {
        key: source.get(key, source.get(f"{key}_ms"))
        for key in REQUIRED_DURATION_KEYS
    }
    durations["submit"] = durations["submit"] if durations["submit"] is not None else run.get("submit_latency_ms")
    durations["render"] = durations["render"] if durations["render"] is not None else run.get("render_time_ms")
    started = parse_time(timestamps.get("source_started_at"))
    completed = parse_time(timestamps.get("source_completed_at"))
    if durations["total"] is None and started and completed:
        durations["total"] = int((completed - started).total_seconds() * 1000)
    return durations


def validate_durations(durations: dict[str, Any]) -> dict[str, Any]:
    invalid = [key for key in REQUIRED_DURATION_KEYS if as_number(durations.get(key)) is None or as_number(durations.get(key)) < 0]
    if invalid:
        return status("durations", False, f"missing or negative durations: {', '.join(invalid)}")
    return status("durations", True, "all required durations are numeric and non-negative")


def build_run(run: dict[str, Any], evidence_root: Path | None, sidecar_root: Path | None, expected_duration_ms: int) -> dict[str, Any]:
    job_id = str(run.get("job_id") or "")
    target_worker = str(run.get("target_worker") or "")
    response_worker = str(run.get("resp_worker_id") or "")
    evidence, evidence_path = find_job_evidence(evidence_root, job_id)
    artifact_url = str((evidence or {}).get("artifact_url") or run.get("artifact_url") or "")
    artifact_path = str((evidence or {}).get("artifact_path") or run.get("artifact_path") or "")
    sidecar, sidecar_path = sidecar_from(evidence, sidecar_root, job_id, artifact_path)
    timestamps = build_timestamps(run, evidence)
    durations = build_durations(run, timestamps, evidence)

    identifiers = {
        "job_id": job_id or None,
        "task_id": run.get("task_id") or (evidence or {}).get("task_id"),
        "attempt_id": run.get("attempt_id") or (evidence or {}).get("attempt_id"),
        "lease_id": run.get("lease_id") or (evidence or {}).get("lease_id"),
        "worker_id": response_worker or target_worker or None,
        "placement_pin_worker_id": target_worker or None,
    }
    identity_ok = all(identifiers[key] for key in ("job_id", "task_id", "attempt_id", "lease_id", "worker_id", "placement_pin_worker_id"))
    placement_ok = bool(target_worker and response_worker and target_worker == response_worker and run.get("pin_ok") in (True, "true"))
    raw_assets = evidence_assets(evidence)
    assets = flatten_asset_records(raw_assets)
    asset_failures: list[str] = []
    for asset in assets:
        asset_id = asset.get("asset_id") or "<missing-id>"
        digest = str(asset.get("sha256") or asset.get("expected_sha256") or "")
        digest_source = "sha256" if asset.get("sha256") else "expected_sha256" if asset.get("expected_sha256") else ""
        if not digest and SHA256_RE.fullmatch(str(asset.get("asset_id") or "")):
            digest = str(asset["asset_id"])
            digest_source = "content-addressed asset_id"
        if not SHA256_RE.fullmatch(digest):
            asset_failures.append(f"{asset_id}: missing valid sha256/expected_sha256")
        if asset.get("sha256_verified") is not True or asset.get("integrity_valid") is not True:
            asset_failures.append(f"{asset_id}: SHA-256/size integrity not verified")
        asset["integrity_digest"] = digest or None
        asset["integrity_digest_source"] = digest_source or None
        if asset.get("cache_status") not in ("hit", "miss"):
            asset_failures.append(f"{asset_id}: missing cache_status")
    asset_status = status("asset_integrity", bool(assets) and not asset_failures, "; ".join(asset_failures) if asset_failures else "all asset records verified") if assets else missing_status("asset_integrity", "no per-asset integrity records were supplied")
    cache_evidence = evidence_cache(evidence)
    cache_has_counters = isinstance(cache_evidence, dict) and any(key in cache_evidence for key in ("cache_hits", "cache_misses", "cache_corruptions"))
    cache_status = status("cache", bool(assets) and not asset_failures and cache_has_counters, "cache counters plus per-asset hit/miss and integrity were recorded") if assets else (status("cache", True, "cache counters were recorded") if cache_has_counters else missing_status("cache", "no cache hit/miss records were supplied"))

    checks = validator_checks(evidence)
    render_evidence = evidence_section(evidence, "render")
    render_size = as_number((render_evidence or {}).get("artifact_size_bytes")) if isinstance(render_evidence, dict) else None
    if render_size is None:
        render_size = as_number((evidence or {}).get("artifact_size_bytes"))
    if render_size is None:
        render_size = as_number(run.get("artifact_size_bytes"))
    render_duration = as_number((render_evidence or {}).get("output_duration_ms")) if isinstance(render_evidence, dict) else None
    if render_duration is None:
        render_duration = as_number((evidence or {}).get("output_duration_ms"))
    render_ok = bool(artifact_url) and bool(render_size and render_size > 0) and bool(render_duration and abs(render_duration - expected_duration_ms) <= 500)
    render_detail = f"artifact_url={'present' if artifact_url else 'missing'}, size={render_size or 0}, duration_ms={render_duration or 'missing'}, expected={expected_duration_ms}"
    render_status = validator_group_status(checks, ("file_size", "ffprobe", "stream_layout", "codecs", "duration", "frame_extraction", "ebur128_clipping"), "render") or status("render", render_ok, render_detail)

    audio = evidence_section(evidence, "audio")
    audio_status = (status("audio", True, "voiceover, background music and final audio stream recorded") if isinstance(audio, dict) and as_number(audio.get("voiceover_tracks")) is not None and as_number(audio.get("voiceover_tracks")) >= 1 and as_number(audio.get("background_music_tracks")) is not None and as_number(audio.get("background_music_tracks")) >= 1 and as_number(audio.get("final_audio_streams")) is not None and as_number(audio.get("final_audio_streams")) >= 1 else missing_status("audio", "no audio evidence was supplied"))
    validator_audio = validator_group_status(checks, ("loudness_volume", "voiceover_presence", "background_music_presence", "voiceover_sync"), "audio")
    if validator_audio:
        audio_status = validator_audio

    subtitles = evidence_section(evidence, "subtitles")
    subtitles_status = (status("subtitles", True, "ASS burn-in, timing and style checks passed") if isinstance(subtitles, dict) and subtitles.get("format") == "ass" and subtitles.get("burned_in") is True and subtitles.get("timing_pass") is True and subtitles.get("style_pass") is True else missing_status("subtitles", "no subtitle evidence was supplied"))
    validator_subtitles = validator_group_status(checks, ("ass_styles", "ass_layout_style", "ass_timing", "ass_overrides", "ass_burn_in_styles"), "subtitles")
    if validator_subtitles:
        subtitles_status = validator_subtitles

    sidecar_shape_ok = isinstance(sidecar, dict) and isinstance(sidecar.get("phase_ms"), dict) and isinstance(sidecar.get("segments"), list)
    sidecar_identity = sidecar.get("identity", {}) if isinstance(sidecar, dict) else {}
    identity_keys = ("job_id", "task_id", "attempt_id", "lease_id")
    sidecar_matches = bool(
        isinstance(sidecar, dict)
        and all(
            (not identifiers[key])
            or sidecar.get(key) == identifiers[key]
            or sidecar_identity.get(key) == identifiers[key]
            for key in identity_keys
        )
        and any(sidecar.get(key) or sidecar_identity.get(key) for key in identity_keys)
    )
    evidence_identity = evidence.get("identity", {}) if isinstance(evidence, dict) else {}
    if not sidecar_matches and isinstance(evidence, dict) and isinstance(evidence.get("sidecar"), dict):
        sidecar_matches = all(
            (not identifiers[key])
            or evidence.get(key) == identifiers[key]
            or evidence_identity.get(key) == identifiers[key]
            for key in identity_keys
        ) and any(evidence.get(key) or evidence_identity.get(key) for key in identity_keys)
    if not sidecar_matches and sidecar_path and all(
        not identifiers[key] or identifiers[key] in sidecar_path for key in identity_keys
    ) and all(not identifiers[key] for key in ("task_id", "attempt_id", "lease_id")):
        sidecar_matches = bool(job_id and job_id in sidecar_path)
    sidecar_ok = sidecar_shape_ok and sidecar_matches
    sidecar_captured = sidecar_shape_ok
    sidecar_associated = sidecar_ok
    sidecar_master_registered = isinstance(evidence, dict) and evidence.get("sidecar_master_registered") is True
    if sidecar_associated and sidecar_master_registered:
        sidecar_status = status("sidecar", True, f"sidecar captured, associated and registered at Master from {sidecar_path}")
    elif sidecar_shape_ok:
        reason = "Master registration evidence is missing" if sidecar_associated else "job/task/attempt/lease association is missing"
        sidecar_status = status("sidecar", False, reason)
    else:
        sidecar_status = missing_status("sidecar", "renderer .progress.json sidecar was not captured")

    duration_consistency = status("duration_consistency", True, "total duration agrees with lifecycle timestamps")
    started = parse_time(timestamps.get("source_started_at"))
    completed = parse_time(timestamps.get("source_completed_at"))
    total_duration = as_number(durations.get("total"))
    if started and completed and total_duration is not None:
        observed_ms = (completed - started).total_seconds() * 1000
        if abs(observed_ms - total_duration) > 500:
            duration_consistency = status("duration_consistency", False, f"total={total_duration}ms differs from source interval={observed_ms:.0f}ms")
    criteria = [
        status("job_succeeded", run.get("status") == "SUCCEEDED", f"job status={run.get('status')!r}"),
        status("worker_pin", placement_ok, f"target={target_worker!r}, response={response_worker!r}, pin_ok={run.get('pin_ok')!r}"),
        status("identity", identity_ok, "job/task/attempt/lease/worker/pin identifiers are present" if identity_ok else "one or more job/task/attempt/lease identity fields are missing"),
        validate_timestamps(timestamps),
        validate_durations(durations),
        duration_consistency,
        asset_status,
        cache_status,
        render_status,
        audio_status,
        subtitles_status,
        sidecar_status,
    ]
    passed = all(item["status"] == "PASS" for item in criteria)
    failures = [item["name"] for item in criteria if item["status"] != "PASS"]
    worker_id = response_worker or target_worker or "unknown-worker"
    return {
        "run_index": run.get("run_idx"),
        "worker_id": worker_id,
        "placement_pin_worker_id": target_worker or None,
        "identifiers": identifiers,
        "status": "PASS" if passed else "FAIL",
        "timestamps": timestamps,
        "durations_ms": durations,
        "assets": assets,
        "render": {
            "artifact_url": artifact_url or None,
            "artifact_path": artifact_path or None,
            "artifact_size_bytes": render_size or 0,
            "output_duration_ms": render_duration,
            "expected_duration_ms": expected_duration_ms,
        },
        "audio": audio if isinstance(audio, dict) else None,
        "subtitles": subtitles if isinstance(subtitles, dict) else None,
        "cache": {"records": assets, "status": cache_status["status"]},
        "sidecar": {"status": sidecar_status["status"], "captured": sidecar_captured, "associated": sidecar_associated, "master_registered": sidecar_master_registered, "path": sidecar_path, "data": sidecar if sidecar_shape_ok else None},
        "criteria": criteria,
        "missing_or_failed_criteria": failures,
        "source": {"benchmark_run": run, "evidence_path": evidence_path},
    }


def make_worker_summary(worker_id: str, runs: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(item["status"] for item in runs)
    failures = Counter(name for run in runs for name in run["missing_or_failed_criteria"])
    return {
        "worker_id": worker_id,
        "runs_total": len(runs),
        "runs_passed": counts.get("PASS", 0),
        "runs_failed": counts.get("FAIL", 0),
        "criteria_failures": dict(sorted(failures.items())),
        "conclusion": "PASS" if runs and counts.get("FAIL", 0) == 0 else "FAIL",
        "conclusion_detail": "all runs satisfied every mandatory criterion" if runs and counts.get("FAIL", 0) == 0 else "at least one mandatory criterion failed or evidence was not captured",
    }


def build_report(benchmark_path: Path, output_path: Path, evidence_root: Path | None, sidecar_root: Path | None, expected_duration_ms: int) -> dict[str, Any]:
    benchmark = read_json(benchmark_path)
    if not isinstance(benchmark, dict) or not isinstance(benchmark.get("runs"), list):
        raise ValueError("benchmark must be an object containing a runs array")
    runs = [build_run(item, evidence_root, sidecar_root, expected_duration_ms) for item in benchmark["runs"] if isinstance(item, dict)]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for run in runs:
        grouped[run["worker_id"]].append(run)
    workers = [make_worker_summary(worker_id, worker_runs) for worker_id, worker_runs in sorted(grouped.items())]
    overall = "PASS" if runs and all(run["status"] == "PASS" for run in runs) else "FAIL"
    return {
        "schema": SCHEMA,
        "generated_at": now_iso(),
        "overall_status": overall,
        "evidence_quality": "COMPLETE" if overall == "PASS" else "INCOMPLETE_OR_FAILED",
        "criteria_policy": {
            "missing_evidence": "FAIL",
            "job_succeeded_is_not_media_pass": True,
            "review_required_is_not_pass": True,
            "expected_duration_ms": expected_duration_ms,
            "required_sections": ["identifiers", "timestamps", "durations_ms", "assets", "cache", "render", "audio", "subtitles", "sidecar"],
        },
        "source": {
            "benchmark_path": str(benchmark_path),
            "evidence_root": str(evidence_root) if evidence_root else None,
            "sidecar_root": str(sidecar_root) if sidecar_root else None,
            "benchmark_schema": benchmark.get("schema"),
            "benchmark_written_at": benchmark.get("written_at"),
        },
        "summary": {
            "workers": len(workers),
            "runs": len(runs),
            "runs_passed": sum(run["status"] == "PASS" for run in runs),
            "runs_failed": sum(run["status"] == "FAIL" for run in runs),
        },
        "workers": workers,
        "runs": runs,
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--evidence-root", type=Path)
    parser.add_argument("--sidecar-root", type=Path)
    parser.add_argument("--expected-duration-ms", type=int, default=12000)
    args = parser.parse_args(argv)
    if args.expected_duration_ms <= 0:
        parser.error("--expected-duration-ms must be positive")
    try:
        report = build_report(args.benchmark, args.output, args.evidence_root, args.sidecar_root, args.expected_duration_ms)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=args.output.parent, prefix=f".{args.output.name}.", suffix=".partial", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(json.dumps(report, indent=2, sort_keys=False) + "\n")
    temporary.replace(args.output)
    print(f"complete report: {report['overall_status']} workers={report['summary']['workers']} runs={report['summary']['runs']} output={args.output}")
    return 0 if report["overall_status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
