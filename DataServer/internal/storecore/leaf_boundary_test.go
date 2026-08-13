package storecore

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// leafPackages are the repository leaf packages split out of the
// internal/store god-package. Each owns SQLite persistence for one domain and
// MUST depend on storecore (or other leaves), never on internal/store —
// otherwise the god-package becomes a dependency magnet again.
//
// When a new leaf is extracted, add its directory name here so the boundary
// is enforced from day one.
var leafPackages = []string{
	"completionstore",
	"renderfingerprintstore",
	"smokerunstore",
}

// TestLeafRepositoriesDoNotImportStore enforces the dependency direction
// leaf → storecore (never leaf → internal/store). It scans every .go file of
// each leaf package for an import of velox-server/internal/store.
func TestLeafRepositoriesDoNotImportStore(t *testing.T) {
	root := findStorecoreRoot(t)
	var violations []string
	for _, leaf := range leafPackages {
		dir := filepath.Join(root, "internal", leaf)
		walkErr := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || filepath.Ext(path) != ".go" {
				return nil
			}
			b, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			if strings.Contains(string(b), `"velox-server/internal/store"`) {
				rel, _ := filepath.Rel(root, path)
				violations = append(violations, rel)
			}
			return nil
		})
		if walkErr != nil {
			t.Fatalf("walk %s: %v", leaf, walkErr)
		}
	}
	if len(violations) > 0 {
		t.Fatalf("leaf repository packages must not import velox-server/internal/store. "+
			"Move the dependency to storecore (or another leaf) and re-export from store if a compatibility facade is needed:\n  %v", violations)
	}
}

func findStorecoreRoot(t *testing.T) string {
	t.Helper()
	cur, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("abs cwd: %v", err)
	}
	const maxLevelsUp = 8
	for i := 0; i <= maxLevelsUp; i++ {
		if _, statErr := os.Stat(filepath.Join(cur, "internal")); statErr == nil {
			return cur
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			break
		}
		cur = parent
	}
	t.Skip("leaf_boundary_test cannot find an ancestor directory containing `internal/`")
	return ""
}
