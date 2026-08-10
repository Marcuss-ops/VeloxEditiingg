package store

import (
	"context"
	"encoding/json"
	"testing"
)

func TestPersistWorkerHeartbeat_PersistsCanonicalAttemptEvents(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/worker-canonical-events.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.DB().Exec(`INSERT INTO workers(worker_id,worker_name,node_role,raw_json,migrated_at) VALUES(?,?,?,?,?)`,
		"worker-events-1", "worker-events-1", "worker", "{}", "2026-08-10T12:00:00Z"); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{
		"worker_id": "worker-events-1", "status": "busy", "current_job": "job-events-1",
		"metrics": map[string]any{"active_jobs": []any{map[string]any{
			"job_id": "job-events-1", "task_id": "task-events-1", "attempt_id": "attempt-events-1",
			"attempt": 1, "lease_id": "lease-events-1", "job_type": "render",
			"status": "RUNNING", "started_at": "2026-08-10T12:00:00Z",
			"canonical_attempt_events": []any{map[string]any{
				"event_id": "attempt-event-attempt-events-1-worker-0", "event_name": "ATTEMPT_STARTED",
				"event_index": 0, "phase": "render", "status": "ok",
			}},
		}}},
	})
	if err := s.PersistWorkerHeartbeat(context.Background(), raw, ""); err != nil {
		t.Fatal(err)
	}

	runtime, err := s.GetWorkerTaskRuntimeByJob(context.Background(), "job-events-1")
	if err != nil {
		t.Fatal(err)
	}
	if runtime == nil || len(runtime.CanonicalAttemptEvents) != 1 {
		t.Fatalf("canonical events=%+v, want one event", runtime)
	}
	if got := runtime.CanonicalAttemptEvents[0]["event_name"]; got != "ATTEMPT_STARTED" {
		t.Fatalf("event name=%v, want ATTEMPT_STARTED", got)
	}
}
