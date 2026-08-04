#!/usr/bin/env python3
"""Offline tests for parallel_bench.py.

These tests never contact a master or worker. They verify the evidence and
selection logic that is safe to run in CI; the live matrix remains operator
owned because it changes MaxActiveJobs and submits real jobs.
"""

from __future__ import annotations

import importlib.util
import pathlib
import unittest


SCRIPT = pathlib.Path(__file__).with_name("parallel_bench.py")
import sys

spec = importlib.util.spec_from_file_location("parallel_bench", SCRIPT)
assert spec and spec.loader
bench = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = bench
spec.loader.exec_module(bench)


class ParallelBenchTest(unittest.TestCase):
    def test_parse_prometheus_preserves_low_cardinality_labels(self) -> None:
        values = bench.parse_prometheus(
            """
# HELP velox_cache_requests_total requests
velox_cache_requests_total{result="hit"} 7
velox_cache_requests_total{result="miss"} 3
velox_cache_downloads_total 2
velox_worker_cpu_iowait_ratio 0.25
"""
        )
        self.assertEqual(values['velox_cache_requests_total{result="hit"}'], 7)
        self.assertEqual(values['velox_cache_requests_total{result="miss"}'], 3)
        self.assertEqual(values["velox_cache_downloads_total"], 2)
        observations = bench.extract_observations(values)
        self.assertEqual(observations["cache_hits"], 7)
        self.assertEqual(observations["cache_misses"], 3)
        self.assertEqual(observations["disk_wait_ratio"], 0.25)

    def test_percentile_is_interpolated_p95(self) -> None:
        self.assertAlmostEqual(bench.percentile([100, 200, 300, 400], 0.95), 385)

    def test_decision_uses_correct_videos_per_hour(self) -> None:
        def result(cap: int, correct_per_hour: float) -> bench.CapResult:
            return bench.CapResult(
                max_active_jobs=cap,
                status="PASS",
                wall_time_ms=1000,
                throughput_jobs_per_hour=correct_per_hour,
                correct_videos=6,
                correct_videos_per_hour=correct_per_hour,
                succeeded=6,
                failed=0,
                error_rate=0,
                latency_mean_ms=100,
                latency_p95_ms=120,
                cpu_avg_ratio=0.5,
                cpu_peak_ratio=0.7,
                rss_avg_bytes=100,
                rss_peak_bytes=120,
                disk_wait_avg_ratio=0.1,
                cache_hits=5,
                cache_misses=1,
                cache_hit_ratio=5 / 6,
                downloads=1,
                duplicate_downloads=0,
                duplicate_download_bytes=0,
                errors=0,
                missing_metrics=[],
                jobs=[],
            )

        results = [result(1, 10), result(2, 10.4), result(3, 13)]
        bench.choose_limit(results, min_gain_pct=5, max_p95_ms=None, max_error_rate=0, max_iowait=0.35)
        self.assertTrue(results[0].efficient)  # cap 1 is the baseline
        self.assertIn("correct_video_gain<5%", results[1].decision)
        self.assertTrue(results[2].efficient)
        self.assertEqual(results[0].decision, "baseline")

    def test_incomplete_without_correctness_hook_cannot_be_certified(self) -> None:
        job = bench.JobResult("job-1", "SUCCEEDED", 100, correct=None, correctness_error="hook missing")
        self.assertIsNone(job.correct)

    def test_ratio_rejects_ambiguous_units(self) -> None:
        self.assertIsNone(bench.normalize_ratio(2.0))
        self.assertAlmostEqual(bench.normalize_ratio(1_000_000), 1.0)

    def test_correctness_hook_rejects_untrusted_artifact_urls(self) -> None:
        command = "echo {job_id} {worker_id} {master_url} {artifact_url} {response_json}"
        correct, detail = bench.run_correctness_command(
            command, "job-1", "worker-1", "https://master.example", "file:///etc/passwd", "/tmp/job.json",
        )
        self.assertFalse(correct)
        self.assertIn("http or https", detail)

    def test_correctness_hook_allows_relative_artifact_urls(self) -> None:
        correct, detail = bench.run_correctness_command(
            "true {job_id} {worker_id} {master_url} {artifact_url} {response_json}",
            "job-1", "worker-1", "https://master.example", "/api/v1/artifacts/job-1", "/tmp/job.json",
        )
        self.assertTrue(correct, detail)


if __name__ == "__main__":
    unittest.main()
