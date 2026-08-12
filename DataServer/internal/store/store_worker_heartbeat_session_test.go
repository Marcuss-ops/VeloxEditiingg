package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestPersistWorkerHeartbeatRejectsUnknownOrMismatchedSession(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/worker-heartbeat-session.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	heartbeat := func(workerID string) []byte {
		raw, err := json.Marshal(map[string]any{
			"worker_id": workerID,
			"status":    "idle",
		})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}

	err = s.PersistWorkerHeartbeat(context.Background(), heartbeat("worker-missing-session"), "missing-session")
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("unknown session error=%v, want ErrTransitionConflict", err)
	}

	if _, err := s.DB().Exec(`INSERT INTO workers(worker_id, worker_name, node_role, raw_json, migrated_at)
		VALUES (?, ?, 'worker', '{}', ?)`, "worker-a", "worker-a", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertSession(&PersistedSession{
		SessionID: "worker-a-session",
		WorkerID:  "worker-a",
		TokenHash: "worker-a-token",
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	err = s.PersistWorkerHeartbeat(context.Background(), heartbeat("worker-b"), "worker-a-session")
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("mismatched worker/session error=%v, want ErrTransitionConflict", err)
	}
}
