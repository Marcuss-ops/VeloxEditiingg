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
                host_memory_used_avg_bytes=1_000_000_000,
                host_memory_used_peak_bytes=1_200_000_000,
                host_memory_available_bytes=6_800_000_000,
                host_memory_peak_ratio=0.15,
                fd_util_avg_ratio=0.2,
                fd_util_peak_ratio=0.3,
                disk_wait_avg_ratio=0.1,
                disk_free_min_bytes=50_000_000_000,
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
        bench.choose_limit(results, min_gain_pct=5, max_p95_ms=None, max_error_rate=0, max_iowait=0.35, max_peak_memory_ratio=0.85, max_fd_util_ratio=0.80, min_disk_free_bytes=10_000_000_000)
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

    def test_peak_memory_ratio_rejects_high_usage(self) -> None:
        """A cap with peak host memory above the threshold must be rejected."""
        def result(cap: int, peak_ratio: float | None) -> bench.CapResult:
            return bench.CapResult(
                max_active_jobs=cap,
                status="PASS",
                wall_time_ms=1000,
                throughput_jobs_per_hour=20,
                correct_videos=6,
                correct_videos_per_hour=20,
                succeeded=6,
                failed=0,
                error_rate=0,
                latency_mean_ms=100,
                latency_p95_ms=120,
                cpu_avg_ratio=0.5,
                cpu_peak_ratio=0.7,
                rss_avg_bytes=100,
                rss_peak_bytes=120,
                host_memory_used_avg_bytes=1_000_000_000,
                host_memory_used_peak_bytes=1_000_000_000,
                host_memory_available_bytes=2_000_000_000,
                host_memory_peak_ratio=peak_ratio,
                fd_util_avg_ratio=0.2,
                fd_util_peak_ratio=0.3,
                disk_wait_avg_ratio=0.1,
                disk_free_min_bytes=50_000_000_000,
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

        # cap 1: 75% memory — eligible;  cap 2: 90% memory — rejected
        results = [result(1, 0.75), result(2, 0.90)]
        bench.choose_limit(results, min_gain_pct=5, max_p95_ms=None, max_error_rate=0, max_iowait=0.35, max_peak_memory_ratio=0.85, max_fd_util_ratio=0.80, min_disk_free_bytes=10_000_000_000)
        self.assertTrue(results[0].efficient)
        self.assertFalse(results[1].efficient)
        self.assertIn("peak_memory>0.85", results[1].decision)

    def test_peak_memory_ratio_unknown_does_not_block(self) -> None:
        """When memory ratio is None (metrics unavailable), the gate must not reject."""
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
                host_memory_used_avg_bytes=None,
                host_memory_used_peak_bytes=None,
                host_memory_available_bytes=None,
                host_memory_peak_ratio=None,
                fd_util_avg_ratio=None,
                fd_util_peak_ratio=None,
                disk_wait_avg_ratio=0.1,
                disk_free_min_bytes=None,
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

        results = [result(1, 10), result(2, 12)]
        bench.choose_limit(results, min_gain_pct=5, max_p95_ms=None, max_error_rate=0, max_iowait=0.35, max_peak_memory_ratio=0.85, max_fd_util_ratio=0.80, min_disk_free_bytes=10_000_000_000)
        self.assertTrue(results[0].efficient)
        self.assertTrue(results[1].efficient)  # not blocked by unknown memory

    def test_peak_memory_ratio_passes_below_threshold(self) -> None:
        """A cap well below the memory threshold must pass."""
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
                host_memory_used_avg_bytes=2_000_000_000,
                host_memory_used_peak_bytes=2_000_000_000,
                host_memory_available_bytes=14_000_000_000,
                host_memory_peak_ratio=0.125,
                fd_util_avg_ratio=0.2,
                fd_util_peak_ratio=0.3,
                disk_wait_avg_ratio=0.1,
                disk_free_min_bytes=50_000_000_000,
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

        results = [result(1, 10), result(2, 12), result(3, 14)]
        bench.choose_limit(results, min_gain_pct=5, max_p95_ms=None, max_error_rate=0, max_iowait=0.35, max_peak_memory_ratio=0.85, max_fd_util_ratio=0.80, min_disk_free_bytes=10_000_000_000)
        for r in results:
            self.assertTrue(r.efficient, f"cap {r.max_active_jobs} should be efficient: {r.decision}")

    def test_compute_peak_memory_ratio(self) -> None:
        """_peak_memory_ratio returns used/(used+available) or None."""
        self.assertAlmostEqual(bench._peak_memory_ratio(3_000_000_000, 5_000_000_000), 0.375)
        self.assertIsNone(bench._peak_memory_ratio(None, 5_000_000_000))
        self.assertIsNone(bench._peak_memory_ratio(3_000_000_000, None))
        self.assertIsNone(bench._peak_memory_ratio(0, 0))

    def _make_cap_result(self, cap: int, correct_per_hour: float, **overrides: object) -> bench.CapResult:
        """Helper to build a CapResult with safe defaults for all new fields."""
        defaults = dict(
            max_active_jobs=cap, status="PASS", wall_time_ms=1000,
            throughput_jobs_per_hour=correct_per_hour, correct_videos=6,
            correct_videos_per_hour=correct_per_hour, succeeded=6, failed=0,
            error_rate=0, latency_mean_ms=100, latency_p95_ms=120,
            cpu_avg_ratio=0.5, cpu_peak_ratio=0.7, rss_avg_bytes=100,
            rss_peak_bytes=120,
            host_memory_used_avg_bytes=2_000_000_000,
            host_memory_used_peak_bytes=2_000_000_000,
            host_memory_available_bytes=14_000_000_000,
            host_memory_peak_ratio=0.125,
            fd_util_avg_ratio=0.2, fd_util_peak_ratio=0.3,
            disk_wait_avg_ratio=0.1, disk_free_min_bytes=50_000_000_000,
            cache_hits=5, cache_misses=1, cache_hit_ratio=5 / 6,
            downloads=1, duplicate_downloads=0, duplicate_download_bytes=0,
            errors=0, missing_metrics=[], jobs=[],
        )
        defaults.update(overrides)
        return bench.CapResult(**defaults)  # type: ignore[arg-type]

    def test_fd_util_rejects_high_usage(self) -> None:
        """A cap with FD utilization above the threshold must be rejected."""
        results = [
            self._make_cap_result(1, 10, fd_util_peak_ratio=0.60),
            self._make_cap_result(2, 12, fd_util_peak_ratio=0.85),
        ]
        bench.choose_limit(results, min_gain_pct=5, max_p95_ms=None, max_error_rate=0,
                           max_iowait=0.35, max_peak_memory_ratio=0.85,
                           max_fd_util_ratio=0.80, min_disk_free_bytes=10_000_000_000)
        self.assertTrue(results[0].efficient)
        self.assertFalse(results[1].efficient)
        self.assertIn("fd_util>0.8", results[1].decision)

    def test_fd_util_unknown_does_not_block(self) -> None:
        """When FD util is None, the gate must not reject."""
        results = [
            self._make_cap_result(1, 10, fd_util_peak_ratio=None),
            self._make_cap_result(2, 12, fd_util_peak_ratio=None),
        ]
        bench.choose_limit(results, min_gain_pct=5, max_p95_ms=None, max_error_rate=0,
                           max_iowait=0.35, max_peak_memory_ratio=0.85,
                           max_fd_util_ratio=0.80, min_disk_free_bytes=10_000_000_000)
        self.assertTrue(results[0].efficient)
        self.assertTrue(results[1].efficient)

    def test_disk_free_rejects_below_threshold(self) -> None:
        """A cap with disk free below threshold must be rejected."""
        results = [
            self._make_cap_result(1, 10, disk_free_min_bytes=50_000_000_000),
            self._make_cap_result(2, 12, disk_free_min_bytes=5_000_000_000),
        ]
        bench.choose_limit(results, min_gain_pct=5, max_p95_ms=None, max_error_rate=0,
                           max_iowait=0.35, max_peak_memory_ratio=0.85,
                           max_fd_util_ratio=0.80, min_disk_free_bytes=10_000_000_000)
        self.assertTrue(results[0].efficient)
        self.assertFalse(results[1].efficient)
        self.assertIn("disk_free<", results[1].decision)

    def test_disk_free_unknown_does_not_block(self) -> None:
        """When disk_free is None, the gate must not reject."""
        results = [
            self._make_cap_result(1, 10, disk_free_min_bytes=None),
            self._make_cap_result(2, 12, disk_free_min_bytes=None),
        ]
        bench.choose_limit(results, min_gain_pct=5, max_p95_ms=None, max_error_rate=0,
                           max_iowait=0.35, max_peak_memory_ratio=0.85,
                           max_fd_util_ratio=0.80, min_disk_free_bytes=10_000_000_000)
        self.assertTrue(results[0].efficient)
        self.assertTrue(results[1].efficient)

    def test_all_gates_pass_when_below_thresholds(self) -> None:
        """All safety gates pass when all metrics are below thresholds."""
        results = [
            self._make_cap_result(1, 10),
            self._make_cap_result(2, 12),
            self._make_cap_result(3, 14),
        ]
        bench.choose_limit(results, min_gain_pct=5, max_p95_ms=None, max_error_rate=0,
                           max_iowait=0.35, max_peak_memory_ratio=0.85,
                           max_fd_util_ratio=0.80, min_disk_free_bytes=10_000_000_000)
        for r in results:
            self.assertTrue(r.efficient, f"cap {r.max_active_jobs} should be efficient: {r.decision}")

    def test_multiple_gates_can_reject_simultaneously(self) -> None:
        """Multiple safety gates can fire on the same cap."""
        results = [
            self._make_cap_result(1, 10),
            self._make_cap_result(2, 12,
                                  host_memory_peak_ratio=0.90,
                                  fd_util_peak_ratio=0.95,
                                  disk_free_min_bytes=3_000_000_000),
        ]
        bench.choose_limit(results, min_gain_pct=5, max_p95_ms=None, max_error_rate=0,
                           max_iowait=0.35, max_peak_memory_ratio=0.85,
                           max_fd_util_ratio=0.80, min_disk_free_bytes=10_000_000_000)
        self.assertTrue(results[0].efficient)
        self.assertFalse(results[1].efficient)
        self.assertIn("peak_memory", results[1].decision)
        self.assertIn("fd_util", results[1].decision)
        self.assertIn("disk_free", results[1].decision)

    def test_cap_matrix_measures_every_cap_through_configured_limit(self) -> None:
        self.assertEqual(bench.cap_matrix(6), (1, 2, 3, 4, 5, 6))
        with self.assertRaises(ValueError):
            bench.cap_matrix(0)

    def test_bottleneck_classifier_uses_canonical_precedence(self) -> None:
        base = self._make_cap_result(4, 20)
        self.assertEqual(bench.classify_bottleneck(base), "UNKNOWN")
        self.assertEqual(bench.classify_bottleneck(
            self._make_cap_result(4, 20, cpu_peak_ratio=0.95)), "CPU_BOUND")
        self.assertEqual(bench.classify_bottleneck(
            self._make_cap_result(4, 20, host_memory_peak_ratio=0.90)), "MEMORY_BOUND")
        self.assertEqual(bench.classify_bottleneck(
            self._make_cap_result(4, 20, disk_wait_avg_ratio=0.40)), "IO_BOUND")
        self.assertEqual(bench.classify_bottleneck(
            self._make_cap_result(4, 20, fd_util_peak_ratio=0.90,
                                  host_memory_peak_ratio=0.90)), "FD_BOUND")


if __name__ == "__main__":
    unittest.main()
