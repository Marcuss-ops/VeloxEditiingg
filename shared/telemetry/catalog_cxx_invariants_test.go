package telemetry

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// repoRoot walks up from the test source file until it finds go.work — the
// marker of the repository root. The generated C++ header lives under
// RemoteCodex/, outside the shared module, so the test cannot rely on a
// module-relative path.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.work not found above " + file)
		}
		dir = parent
	}
}

// cxxEventRow matches one generated C++ EventDescriptor row and captures the
// key, component and action columns, e.g.
//
//	{ "engine.input.open", "engine.input", "open", "engine", ... },
var cxxEventRow = regexp.MustCompile(`\{\s*"([a-z0-9_.]+)",\s*"([a-z0-9_.]+)",\s*"([a-z0-9_.]+)"`)

// TestAllCXXGeneratedEventKeysExistInCatalog pins the language-neutral
// catalog invariant end to end: the C++ generated binding
// (catalog_generated.hpp, produced by shared/telemetry/cmd/cataloggen from
// catalog.json) must contain exactly the same event set as the Go catalog.
//
// The C++ side has no second taxonomy: every key it can look up through
// catalog::IsCatalogEvent/FindEvent must resolve in the shared catalog, the
// key must be the component.action composite, and the C++ event count must
// match Catalog.Count() exactly. A hand-edited header, a stale regeneration
// or a key added to only one side fails this gate.
func TestAllCXXGeneratedEventKeysExistInCatalog(t *testing.T) {
	header := filepath.Join(repoRoot(t), "RemoteCodex", "native", "video-engine-cpp",
		"include", "velox", "telemetry", "catalog_generated.hpp")
	raw, err := os.ReadFile(header)
	if err != nil {
		t.Fatalf("read generated C++ header: %v", err)
	}

	// The kEvents array spans from the declaration line to the closing "}};".
	text := string(raw)
	start := strings.Index(text, "kEvents = {{")
	if start < 0 {
		t.Fatal("generated C++ header has no kEvents array")
	}
	body := text[start:]
	end := strings.Index(body, "}};")
	if end < 0 {
		t.Fatal("generated C++ kEvents array is unterminated")
	}
	body = body[:end]

	rows := cxxEventRow.FindAllStringSubmatch(body, -1)
	if len(rows) == 0 {
		t.Fatal("no EventDescriptor rows parsed from generated C++ header")
	}

	seen := make(map[string]string, len(rows)) // key -> component.action evidence
	for _, row := range rows {
		key, component, action := row[1], row[2], row[3]
		if want := component + "." + action; key != want {
			t.Errorf("C++ key %q is not its component.action composite %q", key, want)
		}
		if prev, dup := seen[key]; dup {
			t.Errorf("duplicate C++ event key %q (already from %s)", key, prev)
		}
		seen[key] = component + "/" + action

		spec, ok := Catalog.Lookup(component, action)
		if !ok {
			t.Errorf("C++ event %s not present in the shared Go catalog", key)
			continue
		}
		if spec.Component != component || spec.Action != action {
			t.Errorf("catalog entry for %s resolves to %s.%s", key, spec.Component, spec.Action)
		}
	}

	// Reverse direction (explicit, not just implied by cardinality): every
	// Go catalog event must also be present in the C++ binding. Together with
	// the per-row lookup above this pins that no event exists on only one
	// side, even if the count check below were ever weakened.
	for _, d := range canonicalEventDescriptors {
		key := d.Key()
		if _, ok := seen[key]; !ok {
			t.Errorf("Go catalog event %s missing from the generated C++ binding", key)
		}
	}

	// Count parity: the C++ generated binding is the same event set as the
	// Go catalog — no event may exist on only one side.
	if len(rows) != Catalog.Count() {
		t.Errorf("C++ event count=%d, Go catalog count=%d (regeneration drift or parallel taxonomy)",
			len(rows), Catalog.Count())
	}
}
