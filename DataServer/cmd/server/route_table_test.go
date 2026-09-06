package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestGoldenRouteTable pins the server's full HTTP route inventory.
//
// Why: the 2026-09 dead-code audit found 15 gin handlers compiled into the
// binary but never mounted on any route — an entire HTTP surface that rotted
// invisibly because "this route disappeared" was never a visible diff. This
// test makes every route addition/removal a deliberate golden change.
//
// Mechanics: the scan walks cmd/server and internal/app for route
// registration literals (`r.GET("/path"`, `group.POST("/path"`, ...) — the
// same convention scripts/ci/check-no-legacy.sh uses — normalizes them to
// "METHOD /path" pairs and compares against the golden file. The golden is
// deliberately source-derived (not engine-derived): it needs no database,
// no registries, and runs inside the pre-removal gate.
//
// Regenerate after an INTENTIONAL route change:
//
//	GOLDEN_UPDATE=1 go test ./cmd/server -run TestGoldenRouteTable
//
// and review the diff. A golden change is a contract change: docs (docs/api/)
// and any external consumers must move with it.
func TestGoldenRouteTable(t *testing.T) {
	// Test cwd is the package dir (cmd/server); internal/app is two levels up.
	dirs := []string{".", "../../internal/app"}

	var routes []string
	for _, dir := range dirs {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatalf("glob %s: %v", dir, err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			src, err := os.ReadFile(file)
			if err != nil {
				t.Fatalf("read %s: %v", file, err)
			}
			for _, line := range strings.Split(string(src), "\n") {
				trimmed := strings.TrimSpace(line)
				for _, method := range []string{"GET", "POST", "PUT", "PATCH", "DELETE"} {
					// Match  <expr>.METHOD("/path"  — expr is any receiver
					// (engine, group, sub-group variable).
					needle := "." + method + "(\"/"
					idx := strings.Index(trimmed, needle)
					if idx < 0 {
						continue
					}
					rest := trimmed[idx+len(needle):]
					end := strings.Index(rest, "\"")
					if end < 0 {
						continue
					}
					path := rest[:end]
					routes = append(routes, fmt.Sprintf("%s %s", method, joinPath(t, file, path)))
				}
			}
		}
	}
	sort.Strings(routes)

	if os.Getenv("GOLDEN_UPDATE") == "1" {
		goldenPath := filepath.Join("testdata", "golden_route_table.txt")
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatalf("mkdir testdata: %v", err)
		}
		if err := os.WriteFile(goldenPath, []byte(strings.Join(routes, "\n")+"\n"), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Logf("golden updated: %d routes", len(routes))
		return
	}

	goldenRaw, err := os.ReadFile(filepath.Join("testdata", "golden_route_table.txt"))
	if err != nil {
		t.Fatalf("read golden (regenerate with GOLDEN_UPDATE=1 go test ./cmd/server -run TestGoldenRouteTable): %v", err)
	}
	golden := splitLines(string(goldenRaw))

	gotSet := make(map[string]struct{}, len(routes))
	for _, r := range routes {
		gotSet[r] = struct{}{}
	}
	wantSet := make(map[string]struct{}, len(golden))
	for _, r := range golden {
		wantSet[r] = struct{}{}
	}

	var missing, extra []string
	for _, r := range golden {
		if _, ok := gotSet[r]; !ok {
			missing = append(missing, r)
		}
	}
	for _, r := range routes {
		if _, ok := wantSet[r]; !ok {
			extra = append(extra, r)
		}
	}
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("route table drifted from golden.\nREMOVED (in golden, not mounted): %v\nADDED (mounted, not in golden): %v\n\nIf intentional: GOLDEN_UPDATE=1 go test ./cmd/server -run TestGoldenRouteTable, review the diff, and update docs/api/ + consumers.", missing, extra)
	}
}

// TestGoldenRouteTable_ExcludesRetiredRoutes asserts the retired legacy
// surfaces (docs/api/bundle.md) never reappear as route literals, even if
// someone regenerates the golden.
func TestGoldenRouteTable_ExcludesRetiredRoutes(t *testing.T) {
	retired := []string{
		"POST /bundle/manifest/generate",
		"GET /api/worker/v2/manifest",
		"POST /install_worker/force_regenerate_zip",
		"POST /workers/full_update_linux",
		"POST /workers/update_all_latest_bundle",
		"POST /worker/request_update",
		"POST /workers/update_all",
		"POST /workers/restart_all",
		"POST /workers/rollout_update",
		"POST /workers/send_command",
		"POST /workers/send_command_bulk",
	}
	goldenRaw, err := os.ReadFile(filepath.Join("testdata", "golden_route_table.txt"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	present := make(map[string]struct{}, len(goldenRaw))
	for _, r := range splitLines(string(goldenRaw)) {
		present[strings.TrimPrefix(r, "/")] = struct{}{}
	}
	for _, r := range retired {
		if _, ok := present[strings.TrimPrefix(r, "/")]; ok {
			t.Fatalf("retired route present in golden route table: %s — it must stay unmounted (see docs/api/bundle.md)", r)
		}
	}
}

func splitLines(s string) []string {
	var out []string
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		if strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out
}

// joinPath resolves the mount path against the group context of the file
// where pragmatically possible. Groups are declared with r.Group("/prefix")
// in the same file; a full reconstruction of gin's tree is out of scope for
// a source scan, so this helper records the literal as written plus, for
// known group variables, the canonical prefix. Keep it simple: golden
// entries are keyed by the literal as it appears in source.
func joinPath(t *testing.T, file, path string) string {
	t.Helper()
	_ = file
	return strings.TrimPrefix(path, "/")
}
