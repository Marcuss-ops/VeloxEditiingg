package workers

import (
	"testing"
	"velox-server/internal/store"
)

func newTestRegistry(t *testing.T) *Registry {
	t.Helper()
	// Use a file-based SQLite store in the temp dir for persistence tests
	s, err := store.NewSQLiteStore(t.TempDir() + "/test_workers.db")
	if err != nil {
		t.Fatalf("failed to create test SQLite store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return New(s)
}

func int64FromMap(t *testing.T, m map[string]interface{}, key string) int64 {
	t.Helper()
	if m == nil {
		t.Fatalf("metrics map is nil looking for %s", key)
	}
	switch v := m[key].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case float64:
		return int64(v)
	default:
		t.Fatalf("unexpected type for %s: %T", key, m[key])
		return 0
	}
}
