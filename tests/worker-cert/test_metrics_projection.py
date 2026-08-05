#!/usr/bin/env python3
"""Offline tests for metrics_projection.py — no master/worker contact."""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import unittest

SCRIPT = pathlib.Path(__file__).with_name("metrics_projection.py")
spec = importlib.util.spec_from_file_location("metrics_projection", SCRIPT)
assert spec and spec.loader
proj = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = proj
spec.loader.exec_module(proj)


class MetricsProjectionTest(unittest.TestCase):
    def test_worker_counters_are_unlabelled_and_requests_keep_result(self) -> None:
        worker = (
            '# HELP velox_cache_requests_total requests\n'
            'velox_cache_requests_total{result="hit"} 7\n'
            'velox_cache_requests_total{result="miss"} 3\n'
            'velox_cache_downloads_total{label="asset"} 2\n'
            'velox_cache_duplicate_downloads_total{label="asset"} 0\n'
            'velox_cache_duplicate_download_bytes_total{label="asset"} 0\n'
            'velox_worker_errors_total{label="total"} 0\n'
            'velox_render_seconds_count{label="total"} 4\n'
        )
        out = proj.project(worker, "", "velox-worker-local")
        self.assertIn('velox_cache_requests_total{result="hit"} 7', out)
        self.assertIn('velox_cache_requests_total{result="miss"} 3', out)
        self.assertIn("velox_cache_downloads_total 2", out)
        self.assertIn("velox_cache_duplicate_downloads_total 0", out)
        self.assertIn("velox_cache_duplicate_download_bytes_total 0", out)
        self.assertIn("velox_worker_errors_total 0", out)
        self.assertIn('velox_render_seconds_count{label="total"} 4', out)
        self.assertNotIn('velox_cache_downloads_total{label="asset"}', out)

    def test_master_gauges_selected_per_worker_and_normalized(self) -> None:
        master = (
            'velox_worker_cpu_utilization_ratio{worker_id="host_1"} 1000\n'
            'velox_worker_cpu_utilization_ratio{worker_id="velox-worker-local"} 965952\n'
            'velox_worker_cpu_iowait_ratio{worker_id="velox-worker-local"} 252\n'
            'velox_worker_process_rss_bytes{worker_id="velox-worker-local"} 6982656\n'
        )
        out = proj.project("", master, "velox-worker-local")
        self.assertIn("velox_worker_cpu_utilization_ratio 0.965952", out)
        self.assertIn("velox_worker_cpu_iowait_ratio 0.252", out)
        self.assertIn("velox_worker_process_rss_bytes 6982656", out)
        self.assertNotIn('worker_id="host_1"', out)

    def test_ratio_values_already_in_range_pass_through(self) -> None:
        out = proj.project("", 'velox_worker_cpu_utilization_ratio{worker_id="w"} 0.5\n', "w")
        self.assertIn("velox_worker_cpu_utilization_ratio 0.5", out)


if __name__ == "__main__":
    unittest.main()
