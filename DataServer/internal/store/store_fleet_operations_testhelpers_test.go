package store

import (
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func newFleetTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fleet-test.db")
	s, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	if err := s.CreateFleetOperationsTableIfNotExists(); err != nil {
		t.Fatalf("CreateFleetOperationsTableIfNotExists: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close()
	})
	return s
}

// TestFleetStore_InsertAndGet verifies the canonical GET
// round-trip: insert a QUEUED row, fetch by ID, all fields
// preserved (timestamps parsed back, payload marshaled intact).
