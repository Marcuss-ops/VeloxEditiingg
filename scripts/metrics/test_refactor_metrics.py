import importlib.util
import sys
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("refactor_metrics.py")
SPEC = importlib.util.spec_from_file_location("refactor_metrics", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
metrics = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = metrics
SPEC.loader.exec_module(metrics)


class RefactorMetricsTests(unittest.TestCase):
    def test_generated_files_are_excluded(self):
        self.assertTrue(metrics.is_generated(Path("api/message.pb.go")))
        self.assertTrue(metrics.is_generated(Path("pkg/vendor/example/file.go")))
        self.assertFalse(metrics.is_generated(Path("internal/handler.go")))

    def test_comment_and_string_stripping_preserves_code_lines(self):
        source = 'func f() {\n  // if hidden\n  value := "for hidden"\n  if value != "" { run() }\n}\n'
        cleaned = metrics.strip_comments_and_strings(source)
        self.assertEqual(cleaned.count("\n"), source.count("\n"))
        self.assertNotIn("hidden", cleaned)
        self.assertIn("if", cleaned)

    def test_line_stats_and_complexity_are_deterministic(self):
        source = "func f(x int) {\n if x > 0 {\n  run()\n }\n}\n"
        self.assertEqual(metrics.line_stats(source)["loc"], 5)
        first = metrics.complexity(source, "go")
        self.assertEqual(first, metrics.complexity(source, "go"))
        self.assertGreaterEqual(first[0], 1)


if __name__ == "__main__":
    unittest.main()
