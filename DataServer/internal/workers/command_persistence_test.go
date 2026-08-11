package workers

import (
	"testing"

	"velox-server/internal/store"
)

func TestPushCommandReturnsEmptyIDWhenPersistenceFails(t *testing.T) {
	db, err := store.NewSQLiteStore(t.TempDir() + "/command-persistence.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	manager := NewCommandManager(db)
	if err := db.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}
	if commandID := manager.PushCommand("command-persistence-worker", "restart_worker", nil); commandID != "" {
		t.Fatalf("PushCommand returned an ID after persistence failure: %q", commandID)
	}
}
