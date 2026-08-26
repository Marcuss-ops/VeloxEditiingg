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
	if _, err := s.DB().Exec(`INSERT INTO task_attempts
		(id, task_id, job_id, attempt_number, worker_id, lease_id, status, report_version, created_at, updated_at)
		VALUES (?, ?, ?, 1, ?, ?, 'RUNNING', 0, ?, ?)`,
		"attempt-events-1", "task-events-1", "job-events-1", "worker-events-1", "lease-events-1",
		"2026-08-10T12:00:00Z", "2026-08-10T12:00:00Z"); err != nil {
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
	if len(runtime.AttemptMilestones) != 0 {
		t.Fatalf("milestones=%+v, want none without attempt_milestones in heartbeat", runtime.AttemptMilestones)
	}
}

// TestPersistWorkerHeartbeat_FoldsAttemptMilestonesIntoLiveProjection locks
// STEP A on the master side: the worker's attempt_milestones heartbeat field
// must ride the same canonical_events_json column as canonical_attempt_events
// (no schema change) and be split back out by the reader as typed samples.
func TestPersistWorkerHeartbeat_FoldsAttemptMilestonesIntoLiveProjection(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/worker-milestones.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.DB().Exec(`INSERT INTO workers(worker_id,worker_name,node_role,raw_json,migrated_at) VALUES(?,?,?,?,?)`,
		"worker-milestones-1", "worker-milestones-1", "worker", "{}", "2026-08-26T12:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DB().Exec(`INSERT INTO task_attempts
		(id, task_id, job_id, attempt_number, worker_id, lease_id, status, report_version, created_at, updated_at)
		VALUES (?, ?, ?, 1, ?, ?, 'RUNNING', 0, ?, ?)`,
		"attempt-milestones-1", "task-milestones-1", "job-milestones-1", "worker-milestones-1", "lease-milestones-1",
		"2026-08-26T12:00:00Z", "2026-08-26T12:00:00Z"); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{
		"worker_id": "worker-milestones-1", "status": "busy", "current_job": "job-milestones-1",
		"metrics": map[string]any{"active_jobs": []any{map[string]any{
			"job_id": "job-milestones-1", "task_id": "task-milestones-1", "attempt_id": "attempt-milestones-1",
			"attempt": 1, "lease_id": "lease-milestones-1", "job_type": "render",
			"status": "RUNNING", "started_at": "2026-08-26T12:00:00Z",
			"canonical_attempt_events": []any{map[string]any{
				"event_id": "attempt-event-attempt-milestones-1-worker-0", "event_name": "ATTEMPT_STARTED",
				"event_index": 0, "phase": "render", "status": "ok",
			}},
			"attempt_milestones": []any{
				map[string]any{"name": "execution.started", "sequence": 1, "elapsed_ms": 0, "occurred_at": "2026-08-26T12:00:00Z"},
				map[string]any{"name": "assets.requested", "sequence": 2, "elapsed_ms": 211, "occurred_at": "2026-08-26T12:00:00Z"},
				map[string]any{"name": "assets.all_ready", "sequence": 3, "elapsed_ms": 298421, "occurred_at": "2026-08-26T12:04:58Z"},
			},
		}}},
	})
	if err := s.PersistWorkerHeartbeat(context.Background(), raw, ""); err != nil {
		t.Fatal(err)
	}

	runtime, err := s.GetWorkerTaskRuntimeByJob(context.Background(), "job-milestones-1")
	if err != nil {
		t.Fatal(err)
	}
	if runtime == nil {
		t.Fatal("runtime projection is nil")
	}
	if len(runtime.CanonicalAttemptEvents) != 1 {
		t.Fatalf("canonical events=%+v, want exactly one event", runtime.CanonicalAttemptEvents)
	}
	if len(runtime.AttemptMilestones) != 3 {
		t.Fatalf("milestones=%+v, want 3 samples", runtime.AttemptMilestones)
	}
	want := []struct {
		name      string
		sequence  uint64
		elapsedMS int64
	}{
		{"execution.started", 1, 0},
		{"assets.requested", 2, 211},
		{"assets.all_ready", 3, 298421},
	}
	for i, w := range want {
		got := runtime.AttemptMilestones[i]
		if string(got.Name) != w.name || got.Sequence != w.sequence || got.ElapsedMS != w.elapsedMS {
			t.Fatalf("milestone[%d]=%+v, want name=%s sequence=%d elapsed_ms=%d", i, got, w.name, w.sequence, w.elapsedMS)
		}
	}
}
