package store

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// newDeploymentTestStore stands up a fresh on-disk SQLite store with the
// deployment_records table ready and the workers referenced by its FK seeded.
func newDeploymentTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	return newDeploymentTestStoreAt(t, filepath.Join(t.TempDir(), "deployment-test.db"))
}

// newDeploymentTestStoreAt creates the canonical deployment test store on an
// explicit path. Recovery tests reuse the path after closing the first handle.
func newDeploymentTestStoreAt(t *testing.T, path string) *SQLiteStore {
	t.Helper()
	s, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("NewSQLiteStore(%s): %v", path, err)
	}
	if err := s.CreateDeploymentRecordsTableIfNotExists(); err != nil {
		t.Fatalf("CreateDeploymentRecordsTableIfNotExists: %v", err)
	}
	seeds := []struct{ id, name string }{
		{"wicket", "wicket-vps"},
		{"velox-worker-523925eb", "velox-worker-523925eb-vps"},
	}
	for _, sd := range seeds {
		if _, err := s.db.Exec(
			`INSERT INTO workers (worker_id, worker_name, node_role, raw_json, migrated_at) VALUES (?, ?, 'worker', '{}', datetime('now'))`,
			sd.id, sd.name,
		); err != nil {
			t.Fatalf("seed workers %s: %v", sd.id, err)
		}
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// reopenDeploymentTestStore reopens an existing deployment DB after the
// original handle was closed. Table bootstrap is idempotent and worker rows
// are intentionally not reseeded because they persist in the file.
func reopenDeploymentTestStore(t *testing.T, path string) *SQLiteStore {
	t.Helper()
	s, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("reopen NewSQLiteStore(%s): %v", path, err)
	}
	if err := s.CreateDeploymentRecordsTableIfNotExists(); err != nil {
		t.Fatalf("reopen CreateDeploymentRecordsTableIfNotExists: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func deploymentTestDigest(c rune) string {
	return "sha256:" + strings.Repeat(string(c), 64)
}

func deploymentTimePtr(t time.Time) *time.Time { return &t }
