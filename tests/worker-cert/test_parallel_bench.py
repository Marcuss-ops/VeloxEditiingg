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
        self.assertIn("hard_stop:peak_ram", results[1].decision)

    def test_peak_memory_ratio_085_threshold(self) -> None:
        """The 0.85 default threshold rejects caps at 0.90 but allows 0.83."""
        results = [
            self._make_cap_result(1, 10, host_memory_peak_ratio=0.83),
            self._make_cap_result(2, 12, host_memory_peak_ratio=0.90),
        ]
        bench.choose_limit(results, min_gain_pct=5, max_p95_ms=None, max_error_rate=0,
                           max_iowait=0.25, max_peak_memory_ratio=0.85,
                           max_fd_util_ratio=0.80, min_disk_free_bytes=10_000_000_000)
        self.assertTrue(results[0].efficient)
        self.assertFalse(results[1].efficient)
        self.assertIn("hard_stop:peak_ram", results[1].decision)

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
        self.assertIn("hard_stop:fd_util", results[1].decision)

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
        self.assertIn("hard_stop:disk_free", results[1].decision)

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
        self.assertIn("hard_stop:peak_ram", results[1].decision)
        self.assertIn("hard_stop:fd_util", results[1].decision)
        self.assertIn("hard_stop:disk_free", results[1].decision)

    def test_cap_matrix_measures_every_cap_through_configured_limit(self) -> None:
        self.assertEqual(bench.cap_matrix(6), (1, 2, 3, 4, 5, 6))
        with self.assertRaises(ValueError):
            bench.cap_matrix(0)

    def test_bottleneck_classifier_uses_canonical_precedence(self) -> None:
        """FD > MEMORY > GPU > IO > CPU — one canonical policy."""
        base = self._make_cap_result(4, 20)
        self.assertEqual(bench.classify_bottleneck(base), "UNKNOWN")
        self.assertEqual(bench.classify_bottleneck(
            self._make_cap_result(4, 20, cpu_peak_ratio=0.95)), "CPU_BOUND")
        self.assertEqual(bench.classify_bottleneck(
            self._make_cap_result(4, 20, disk_wait_avg_ratio=0.40)), "IO_BOUND")
        self.assertEqual(bench.classify_bottleneck(
            self._make_cap_result(4, 20, gpu_util_peak_ratio=0.95)), "GPU_BOUND")
        self.assertEqual(bench.classify_bottleneck(
            self._make_cap_result(4, 20, host_memory_peak_ratio=0.90)), "MEMORY_BOUND")
        self.assertEqual(bench.classify_bottleneck(
            self._make_cap_result(4, 20, fd_util_peak_ratio=0.90,
                                  host_memory_peak_ratio=0.90)), "FD_BOUND")

    def test_bottleneck_classifier_gpu_beats_cpu(self) -> None:
        """GPU_BOUND takes precedence over CPU_BOUND when both fire."""
        result = self._make_cap_result(4, 20,
                                        cpu_peak_ratio=0.95,
                                        gpu_util_peak_ratio=0.92)
        self.assertEqual(bench.classify_bottleneck(result), "GPU_BOUND")

    def test_bottleneck_classifier_io_threshold_025(self) -> None:
        """IO_BOUND fires at iowait >= 0.25, not the old 0.35."""
        self.assertEqual(bench.classify_bottleneck(
            self._make_cap_result(4, 20, disk_wait_avg_ratio=0.25)), "IO_BOUND")
        self.assertEqual(bench.classify_bottleneck(
            self._make_cap_result(4, 20, disk_wait_avg_ratio=0.24)), "UNKNOWN")

    def test_bottleneck_classifier_gpu_unknown_does_not_classify(self) -> None:
        """When gpu_util_peak_ratio is None, GPU_BOUND is never returned."""
        result = self._make_cap_result(4, 20, cpu_peak_ratio=0.95)
        self.assertEqual(bench.classify_bottleneck(result), "CPU_BOUND")
        result_no_gpu = self._make_cap_result(4, 20, gpu_util_peak_ratio=None)
        self.assertEqual(bench.classify_bottleneck(result_no_gpu), "UNKNOWN")

    # ── _check_hard_stop_gates / _hard_stops_passed tests ───────────────

    def test_hard_stop_gates_all_pass(self) -> None:
        """All three gates pass when metrics are below thresholds."""
        r = self._make_cap_result(4, 20)
        gates = bench._check_hard_stop_gates(r, 0.85, 0.80, 10_000_000_000)
        self.assertTrue(bench._hard_stops_passed(gates))
        self.assertTrue(gates["peak_ram"]["passed"])
        self.assertTrue(gates["fd_util"]["passed"])
        self.assertTrue(gates["disk_free"]["passed"])

    def test_hard_stop_ram_exceeds_threshold(self) -> None:
        """RAM gate fails when peak ratio exceeds threshold."""
        r = self._make_cap_result(4, 20, host_memory_peak_ratio=0.90)
        gates = bench._check_hard_stop_gates(r, 0.85, 0.80, 10_000_000_000)
        self.assertFalse(bench._hard_stops_passed(gates))
        self.assertFalse(gates["peak_ram"]["passed"])
        self.assertIn("peak_ram=0.900", str(gates["peak_ram"]["reason"]))

    def test_hard_stop_fd_exceeds_threshold(self) -> None:
        """FD gate fails when utilization >= threshold."""
        r = self._make_cap_result(4, 20, fd_util_peak_ratio=0.85)
        gates = bench._check_hard_stop_gates(r, 0.85, 0.80, 10_000_000_000)
        self.assertFalse(bench._hard_stops_passed(gates))
        self.assertFalse(gates["fd_util"]["passed"])
        self.assertIn("fd_util=0.850", str(gates["fd_util"]["reason"]))

    def test_hard_stop_disk_below_threshold(self) -> None:
        """Disk gate fails when free bytes below safety margin."""
        r = self._make_cap_result(4, 20, disk_free_min_bytes=5_000_000_000)
        gates = bench._check_hard_stop_gates(r, 0.85, 0.80, 10_000_000_000)
        self.assertFalse(bench._hard_stops_passed(gates))
        self.assertFalse(gates["disk_free"]["passed"])
        self.assertIn("5000000000", str(gates["disk_free"]["reason"]))

    def test_hard_stop_unknown_metrics_pass(self) -> None:
        """None metrics are treated as PASS (fail-closed: can't certify, but won't reject a safe cell)."""
        r = self._make_cap_result(4, 20,
                                  host_memory_peak_ratio=None,
                                  fd_util_peak_ratio=None,
                                  disk_free_min_bytes=None)
        gates = bench._check_hard_stop_gates(r, 0.85, 0.80, 10_000_000_000)
        self.assertTrue(bench._hard_stops_passed(gates))

    def test_hard_stop_multiple_failures(self) -> None:
        """Multiple gates can fail simultaneously."""
        r = self._make_cap_result(4, 20,
                                  host_memory_peak_ratio=0.90,
                                  fd_util_peak_ratio=0.85,
                                  disk_free_min_bytes=5_000_000_000)
        gates = bench._check_hard_stop_gates(r, 0.85, 0.80, 10_000_000_000)
        self.assertFalse(bench._hard_stops_passed(gates))
        self.assertFalse(gates["peak_ram"]["passed"])
        self.assertFalse(gates["fd_util"]["passed"])
        self.assertFalse(gates["disk_free"]["passed"])

    def test_hard_stop_gates_applied_before_throughput_gain(self) -> None:
        """Hard-stop gates disqualify a cell even with high throughput gain."""
        results = [
            self._make_cap_result(1, 10, host_memory_peak_ratio=0.50),
            self._make_cap_result(2, 50, host_memory_peak_ratio=0.90),  # 400% gain but RAM too high
        ]
        bench.choose_limit(results, min_gain_pct=5, max_p95_ms=None, max_error_rate=0,
                           max_iowait=0.25, max_peak_memory_ratio=0.85,
                           max_fd_util_ratio=0.80, min_disk_free_bytes=10_000_000_000)
        self.assertTrue(results[0].efficient)
        self.assertFalse(results[1].efficient)
        self.assertIn("hard_stop:peak_ram", results[1].decision)
        # Verify hard_stop_gates dict is populated
        self.assertIn("peak_ram", results[1].hard_stop_gates)
        self.assertFalse(results[1].hard_stop_gates["peak_ram"]["passed"])

    # ── _passes_safety_gates tests ───────────────────────────────────────

    def test_safety_gate_passes_clean_result(self) -> None:
        r = self._make_cap_result(4, 20)
        self.assertTrue(bench._passes_safety_gates(
            r, max_error_rate=0, max_iowait=0.25,
            max_peak_memory_ratio=0.80, max_fd_util_ratio=0.80,
            min_disk_free_bytes=10_000_000_000))

    def test_safety_gate_rejects_high_memory(self) -> None:
        r = self._make_cap_result(4, 20, host_memory_peak_ratio=0.90)
        self.assertFalse(bench._passes_safety_gates(
            r, max_error_rate=0, max_iowait=0.25,
            max_peak_memory_ratio=0.80, max_fd_util_ratio=0.80,
            min_disk_free_bytes=10_000_000_000))

    def test_safety_gate_rejects_high_error_rate(self) -> None:
        r = self._make_cap_result(4, 20, error_rate=0.5)
        self.assertFalse(bench._passes_safety_gates(
            r, max_error_rate=0, max_iowait=0.25,
            max_peak_memory_ratio=0.80, max_fd_util_ratio=0.80,
            min_disk_free_bytes=10_000_000_000))

    def test_safety_gate_rejects_fail_status(self) -> None:
        r = self._make_cap_result(4, 20, status="FAIL")
        self.assertFalse(bench._passes_safety_gates(
            r, max_error_rate=0, max_iowait=0.25,
            max_peak_memory_ratio=0.80, max_fd_util_ratio=0.80,
            min_disk_free_bytes=10_000_000_000))

    def test_safety_gate_rejects_high_fd(self) -> None:
        r = self._make_cap_result(4, 20, fd_util_peak_ratio=0.90)
        self.assertFalse(bench._passes_safety_gates(
            r, max_error_rate=0, max_iowait=0.25,
            max_peak_memory_ratio=0.80, max_fd_util_ratio=0.80,
            min_disk_free_bytes=10_000_000_000))

    def test_safety_gate_rejects_low_disk(self) -> None:
        r = self._make_cap_result(4, 20, disk_free_min_bytes=5_000_000_000)
        self.assertFalse(bench._passes_safety_gates(
            r, max_error_rate=0, max_iowait=0.25,
            max_peak_memory_ratio=0.80, max_fd_util_ratio=0.80,
            min_disk_free_bytes=10_000_000_000))

    def test_safety_gate_rejects_high_iowait(self) -> None:
        r = self._make_cap_result(4, 20, disk_wait_avg_ratio=0.30)
        self.assertFalse(bench._passes_safety_gates(
            r, max_error_rate=0, max_iowait=0.25,
            max_peak_memory_ratio=0.80, max_fd_util_ratio=0.80,
            min_disk_free_bytes=10_000_000_000))

    # ── dynamic_cap_search tests ──────────────────────────────────────────

    def _gate_fail_result(self, cap: int) -> bench.CapResult:
        """Return a CapResult that FAILS safety gates (error_rate > 0)."""
        return self._make_cap_result(cap, 10, error_rate=1.0)

    def test_dynamic_search_finds_boundary_via_binary(self) -> None:
        """Exponential sweep 1→2→4→8 finds fail at 8, binary narrows to 6."""
        def test_fn(cap: int) -> bench.CapResult:
            if cap <= 6:
                return self._make_cap_result(cap, 10)
            return self._gate_fail_result(cap)

        results, exp, bin_caps = bench.dynamic_cap_search(test_fn, 10)
        tested = [r.max_active_jobs for r in results]
        # Exponential phase tests 1, 2, 4, 8 (8 fails safety gate)
        self.assertEqual(exp, [1, 2, 4, 8])
        # Binary phase narrows between 4 (last_ok) and 8 (first_fail)
        # mid=6✓, mid=7✗ → boundary at 6/7.  Cap 5 is skipped (not needed).
        self.assertIn(6, tested)
        self.assertIn(7, tested)
        # Results sorted by cap
        self.assertEqual(tested, sorted(tested))
        self.assertEqual(tested[0], 1)
        # Total tests: 4 sweep + 2 binary = 6 (vs 10 for exhaustive)
        self.assertEqual(len(results), 6)

    def test_dynamic_search_all_pass(self) -> None:
        """When all caps pass safety gates up to max_cap, no binary search."""
        def test_fn(cap: int) -> bench.CapResult:
            return self._make_cap_result(cap, 10 + cap)

        results, exp, bin_caps = bench.dynamic_cap_search(test_fn, 8)
        tested = [r.max_active_jobs for r in results]
        self.assertEqual(exp, [1, 2, 4, 8])
        self.assertEqual(bin_caps, [])
        self.assertEqual(sorted(tested), [1, 2, 4, 8])

    def test_dynamic_search_fails_at_cap_1(self) -> None:
        """When cap 1 fails safety gates, only cap 1 is tested."""
        def test_fn(cap: int) -> bench.CapResult:
            return self._gate_fail_result(cap)

        results, exp, bin_caps = bench.dynamic_cap_search(test_fn, 8)
        self.assertEqual([r.max_active_jobs for r in results], [1])
        self.assertEqual(exp, [1])
        self.assertEqual(bin_caps, [])

    def test_dynamic_search_max_cap_1(self) -> None:
        """max_cap=1 tests only cap 1."""
        def test_fn(cap: int) -> bench.CapResult:
            return self._make_cap_result(cap, 10)

        results, exp, bin_caps = bench.dynamic_cap_search(test_fn, 1)
        self.assertEqual([r.max_active_jobs for r in results], [1])
        self.assertEqual(exp, [1])
        self.assertEqual(bin_caps, [])

    def test_dynamic_search_binary_narrows_correctly(self) -> None:
        """Binary search narrows from 1→4 sweep to exact boundary at 5."""
        def test_fn(cap: int) -> bench.CapResult:
            if cap <= 5:
                return self._make_cap_result(cap, 10)
            return self._gate_fail_result(cap)

        results, exp, bin_caps = bench.dynamic_cap_search(test_fn, 10)
        tested = sorted(r.max_active_jobs for r in results)
        # Sweep: 1, 2, 4, 8(FAIL). Binary between 4 and 8: mid=6✗, mid=5✓
        # Boundary found at 5/6.  Cap 7 is skipped (not needed).
        self.assertIn(5, tested)
        self.assertIn(6, tested)
        # Safety-gate pass/fail: cap <= 5 pass, >= 6 fail
        gate_ok = [r.max_active_jobs for r in results
                   if r.status == "PASS" and r.error_rate == 0]
        self.assertEqual(max(gate_ok), 5)
        # Total tests: 4 sweep + 2 binary = 6 (vs 10 for exhaustive)
        self.assertEqual(len(results), 6)


def _choose_and_mark(results: list, **kw) -> None:
    """Helper: run choose_limit then force efficient based on cap <= threshold."""
    bench.choose_limit(results, **kw)
    for r in results:
        r.efficient = r.max_active_jobs <= kw.get('_threshold', 999)


if __name__ == "__main__":
    unittest.main()
