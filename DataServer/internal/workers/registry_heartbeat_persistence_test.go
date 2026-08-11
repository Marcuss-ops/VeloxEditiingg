package workers

import (
	"context"
	"testing"

	"velox-server/internal/store"
)

func TestHeartbeatPersistenceFailureDoesNotAdvanceInMemoryReadModel(t *testing.T) {
	db, err := store.NewSQLiteStore(t.TempDir() + "/heartbeat-persistence.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	registry := New(db)
	if err := registry.HeartbeatWithSession(context.Background(), "", "heartbeat-worker", "worker-one", "job-before-failure", nil); err != nil {
		t.Fatalf("initial heartbeat: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}
	if err := registry.HeartbeatWithSession(context.Background(), "", "heartbeat-worker", "worker-one", "job-after-failure", nil); err == nil {
		t.Fatal("heartbeat succeeded after its durable store was closed")
	}

	got, ok := registry.inMem["heartbeat-worker"]
	if !ok {
		t.Fatal("failed heartbeat removed the existing in-memory worker")
	}
	if got.CurrentJob != "job-before-failure" {
		t.Fatalf("in-memory worker advanced after failed persistence: current_job=%q", got.CurrentJob)
	}
}
