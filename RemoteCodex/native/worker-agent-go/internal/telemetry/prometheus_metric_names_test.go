package telemetry

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// packageDir returns the directory of this package's source, so the test can
// scan the production metric definitions regardless of the invocation cwd.
func packageDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(file)
}

// metricNameDef matches a Prometheus metric name definition, e.g.
//
//	Name: "velox_asset_cache_hit_total",
var metricNameDef = regexp.MustCompile(`Name:\s*"velox_[a-z0-9_]+"`)

// TestPrometheusMetricNamesAreUnique pins the "no duplicated metric key
// definitions" invariant on the Prometheus projection: every velox_* metric
// name is defined exactly once across the production metric definition files.
//
// Prometheus itself registers metrics globally, so a duplicated name would
// collide at scrape/registration time or silently overwrite — the catalog and
// the Prometheus facade must agree on one definition per key. The scan covers
// the production prometheus*.go files only (never _test.go) so a test helper
// can never accidentally satisfy the gate.
//
// Assumption: all metric name definitions live in files named prometheus*.go
// (the repo convention). A definition added to a differently named file would
// silently escape this scan — keep metric definitions in the prometheus*
// files, exactly as the registry split documents.
func TestPrometheusMetricNamesAreUnique(t *testing.T) {
	defs, err := filepath.Glob(filepath.Join(packageDir(t), "prometheus*.go"))
	if err != nil {
		t.Fatalf("glob prometheus files: %v", err)
	}
	seen := map[string]string{} // metric name -> defining file
	for _, path := range defs {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, match := range metricNameDef.FindAllString(string(raw), -1) {
			name := match[len(`Name: "`) : len(match)-1]
			if prev, dup := seen[name]; dup {
				t.Errorf("duplicate Prometheus metric name %q defined in %s and %s", name, prev, path)
				continue
			}
			seen[name] = path
		}
	}

	if len(seen) < 40 {
		t.Fatalf("only %d metric name definitions found; the scan is not covering the registry", len(seen))
	}
}
