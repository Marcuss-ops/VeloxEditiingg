package taskrunner

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestNoDirectTaskExecutionReportMetricsAccess keeps the deprecated map behind
// the named compatibility methods on TaskExecutionReport. It intentionally
// scans production files in the taskrunner and worker packages, where report
// consumers live, and ignores tests plus comments. New compatibility code must
// use LegacyMetrics/LegacyMetric/SetLegacyMetric instead of reintroducing a
// direct report.Metrics lookup.
func TestNoDirectTaskExecutionReportMetricsAccess(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	moduleRoot := filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
	for _, relativeDir := range []string{"internal/taskrunner", "internal/worker"} {
		dir := filepath.Join(moduleRoot, relativeDir)
		err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Metrics" {
					return true
				}
				identifier, ok := selector.X.(*ast.Ident)
				if ok && identifier.Name == "report" {
					t.Errorf("%s: direct report.Metrics access; use the explicit legacy compatibility boundary", path)
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", relativeDir, err)
		}
	}
}
