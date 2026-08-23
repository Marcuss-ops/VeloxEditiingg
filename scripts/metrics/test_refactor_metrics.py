#!/usr/bin/env python3
import tempfile
import unittest
from pathlib import Path

import refactor_metrics


class RefactorMetricsTest(unittest.TestCase):
    def test_line_stats_are_physical_and_partitioned(self):
        stats = refactor_metrics.line_stats("package x\n\n// note\nfunc main() {}\n")
        self.assertEqual(stats, {"loc": 4, "blank_loc": 1, "comment_loc": 1, "code_loc": 2})

    def test_complexity_ignores_comments_and_strings(self):
        source = '''
        // if for switch &&
        value := "if && ||"
        if ready && enabled { return value }
        '''
        complexity, decisions = refactor_metrics.complexity(source, "go")
        self.assertEqual(decisions, 2)
        self.assertEqual(complexity, 3)

    def test_duplicate_windows_are_normalized_and_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a.go").write_text(
                "\n".join(["package a", "func shared() {", "  value := 1", "  if value > 0 {", "    value++", "  }", "}"]) + "\n",
                encoding="utf-8",
            )
            (root / "b.go").write_text(
                "\n".join(["package b", "func shared() {", "  value := 99", "  if value > 0 {", "    value++", "  }", "}"]) + "\n",
                encoding="utf-8",
            )
            _, first = refactor_metrics.analyze_tree(root)
            _, second = refactor_metrics.analyze_tree(root)
        self.assertGreater(first["totals"]["duplicate_blocks"], 0)
        self.assertEqual(first, second)

    def test_responsibility_categories_are_stable(self):
        categories = refactor_metrics.responsibilities(
            "DataServer/internal/store/artifact_upload.go",
            "func ValidateArtifact() { telemetry.RecordMetric() }",
            "go",
        )
        self.assertEqual(categories, ["artifact", "persistence", "telemetry", "validation"])


if __name__ == "__main__":
    unittest.main()
