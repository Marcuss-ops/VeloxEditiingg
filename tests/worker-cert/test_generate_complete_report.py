#!/usr/bin/env python3
"""Tests for generate_complete_report.py using only temporary fixtures."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import generate_complete_report as report


class CompleteReportTest(unittest.TestCase):
    def test_missing_media_evidence_fails_closed_and_preserves_identity(self) -> None:
        benchmark = {
            "schema": "tests/worker-cert/sequential_bench@1",
            "written_at": "2026-07-31T12:00:00Z",
            "runs": [
                {
                    "job_id": "job-1",
                    "status": "SUCCEEDED",
                    "run_idx": 1,
                    "started_at": "2026-07-31T12:00:01Z",
                    "completed_at": "2026-07-31T12:00:03Z",
                    "render_time_ms": 2000,
                    "artifact_size_bytes": 0,
                    "artifact_url": "",
                    "target_worker": "worker-a",
                    "resp_worker_id": "worker-a",
                    "task_id": "task-1",
                    "attempt_id": "attempt-1",
                    "lease_id": "lease-1",
                    "pin_ok": "true",
                }
            ],
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            benchmark_path = root / "benchmark.json"
            output_path = root / "complete_report.json"
            benchmark_path.write_text(json.dumps(benchmark), encoding="utf-8")
            generated = report.build_report(benchmark_path, output_path, None, None, 12000)
            run = generated["runs"][0]
            self.assertEqual(run["status"], "FAIL")
            self.assertEqual(run["identifiers"]["job_id"], "job-1")
            self.assertEqual(run["identifiers"]["task_id"], "task-1")
            self.assertEqual(run["identifiers"]["attempt_id"], "attempt-1")
            self.assertEqual(run["identifiers"]["lease_id"], "lease-1")
            self.assertIn("asset_integrity", run["missing_or_failed_criteria"])
            self.assertIn("cache", run["missing_or_failed_criteria"])
            self.assertIn("sidecar", run["missing_or_failed_criteria"])
            self.assertEqual(generated["workers"][0]["conclusion"], "FAIL")

    def test_complete_evidence_can_pass_and_sidecar_must_be_associated(self) -> None:
        digest = "a" * 64
        benchmark = {
            "runs": [{
                "job_id": "job-2", "status": "SUCCEEDED", "run_idx": 1,
                "started_at": "2026-07-31T12:00:01Z", "completed_at": "2026-07-31T12:00:03Z",
                "render_time_ms": 2000, "artifact_size_bytes": 100,
                "artifact_url": "https://example.invalid/artifact.mp4",
                "target_worker": "worker-b", "resp_worker_id": "worker-b",
                "task_id": "task-2", "attempt_id": "attempt-2", "lease_id": "lease-2", "pin_ok": "true",
            }]
        }
        timestamps = {key: "2026-07-31T12:00:01Z" for key in report.REQUIRED_TIMESTAMP_KEYS}
        timestamps["job_completed_at"] = "2026-07-31T12:00:03Z"
        evidence = {
            "job_id": "job-2",
            "timestamps": timestamps,
            "durations_ms": {**{key: 1 for key in report.REQUIRED_DURATION_KEYS}, "total": 2000},
            "assets": [{"asset_id": "asset-1", "sha256": digest, "sha256_verified": True, "integrity_valid": True, "cache_status": "miss"}],
            "cache": {"cache_hits": 0, "cache_misses": 1},
            "render": {"artifact_size_bytes": 100, "output_duration_ms": 12000},
            "audio": {"voiceover_tracks": 1, "background_music_tracks": 1, "final_audio_streams": 1},
            "subtitles": {"format": "ass", "burned_in": True, "timing_pass": True, "style_pass": True},
            "sidecar": {"job_id": "job-2", "task_id": "task-2", "attempt_id": "attempt-2", "lease_id": "lease-2", "phase_ms": {}, "segments": []},
            "sidecar_master_registered": True,
            "artifacts": [{"checks": [
                {"name": "file_size", "status": "PASS"},
                {"name": "ffprobe", "status": "PASS"},
                {"name": "stream_layout", "status": "PASS"},
                {"name": "codecs", "status": "PASS"},
                {"name": "duration", "status": "PASS"},
                {"name": "frame_extraction", "status": "PASS"},
                {"name": "ebur128_clipping", "status": "PASS"},
                {"name": "loudness_volume", "status": "PASS"},
                {"name": "voiceover_presence", "status": "PASS"},
                {"name": "background_music_presence", "status": "PASS"},
                {"name": "voiceover_sync", "status": "PASS"},
                {"name": "ass_styles", "status": "PASS"},
                {"name": "ass_layout_style", "status": "PASS"},
                {"name": "ass_timing", "status": "PASS"},
                {"name": "ass_overrides", "status": "PASS"},
                {"name": "ass_burn_in_styles", "status": "PASS"}
            ]}],
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            benchmark_path = root / "benchmark.json"
            evidence_root = root / "evidence"
            evidence_root.mkdir()
            output_path = root / "complete_report.json"
            benchmark_path.write_text(json.dumps(benchmark), encoding="utf-8")
            (evidence_root / "run.json").write_text(json.dumps(evidence), encoding="utf-8")
            generated = report.build_report(benchmark_path, output_path, evidence_root, None, 12000)
            self.assertEqual(generated["overall_status"], "PASS")
            self.assertEqual(generated["runs"][0]["status"], "PASS")
            self.assertEqual(generated["runs"][0]["sidecar"]["status"], "PASS")
            self.assertTrue(generated["runs"][0]["sidecar"]["captured"])
            self.assertTrue(generated["runs"][0]["sidecar"]["associated"])
            self.assertTrue(generated["runs"][0]["sidecar"]["master_registered"])

    def test_validator_review_required_cannot_become_pass(self) -> None:
        checks = {
            "loudness_volume": {"name": "loudness_volume", "status": "REVIEW_REQUIRED"},
            "voiceover_presence": {"name": "voiceover_presence", "status": "REVIEW_REQUIRED"},
        }
        result = report.validator_group_status(checks, ("loudness_volume", "voiceover_presence"), "audio")
        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "REVIEW_REQUIRED")
        self.assertNotEqual(result["status"], "PASS")

    def test_mismatched_sidecar_and_non_chronological_timestamps_fail(self) -> None:
        benchmark = {
            "runs": [{
                "job_id": "job-3", "status": "SUCCEEDED", "run_idx": 1,
                "started_at": "2026-07-31T12:00:01Z", "completed_at": "2026-07-31T12:00:03Z",
                "render_time_ms": 2000, "target_worker": "worker-c", "resp_worker_id": "worker-c",
                "task_id": "task-3", "attempt_id": "attempt-3", "lease_id": "lease-3", "pin_ok": "true",
            }]
        }
        timestamps = {key: "2026-07-31T12:00:01Z" for key in report.REQUIRED_TIMESTAMP_KEYS}
        timestamps["render_started_at"] = "2026-07-31T12:00:05Z"
        timestamps["render_completed_at"] = "2026-07-31T12:00:04Z"
        evidence = {
            "job_id": "job-3", "timestamps": timestamps,
            "durations_ms": {key: 1 for key in report.REQUIRED_DURATION_KEYS},
            "sidecar": {"job_id": "job-3", "task_id": "wrong-task", "attempt_id": "wrong-attempt", "lease_id": "wrong-lease", "phase_ms": {}, "segments": []},
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            benchmark_path = root / "benchmark.json"
            evidence_root = root / "evidence"
            evidence_root.mkdir()
            benchmark_path.write_text(json.dumps(benchmark), encoding="utf-8")
            (evidence_root / "run.json").write_text(json.dumps(evidence), encoding="utf-8")
            generated = report.build_report(benchmark_path, root / "out.json", evidence_root, None, 12000)
            failed = generated["runs"][0]["missing_or_failed_criteria"]
            self.assertIn("timestamps", failed)
            self.assertIn("sidecar", failed)


if __name__ == "__main__":
    unittest.main()
