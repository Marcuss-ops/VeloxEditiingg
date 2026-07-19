package store

import (
	"context"
	"encoding/json"
	"testing"
)

func TestPersistWorkerHeartbeatReconcilesRuntimeAtomically(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/worker-runtime.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	raw, _ := json.Marshal(map[string]any{
		"worker_id": "worker-runtime-1", "worker_name": "pc-b", "status": "busy",
		"current_job": "job-1", "schedulable": true, "node_role": "worker",
		"metrics": map[string]any{"active_jobs": []any{map[string]any{
			"job_id": "job-1", "task_id": "task-1", "attempt_id": "attempt-1",
			"attempt": 1, "lease_id": "lease-1", "job_type": "scene.composite.v1",
			"progress_percent": 45, "progress_scene": 3, "progress_total": 10,
			"progress_stage": "building_scene",
		}}},
	})
	if err := s.PersistWorkerHeartbeat(context.Background(), raw, ""); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM worker_task_runtime WHERE task_id='task-1' AND progress_percent=45`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("runtime projection count=%d, want 1", count)
	}

	raw2, _ := json.Marshal(map[string]any{
		"worker_id": "worker-runtime-1", "worker_name": "pc-b", "status": "idle",
		"schedulable": true, "metrics": map[string]any{"active_jobs": []any{}},
	})
	if err := s.PersistWorkerHeartbeat(context.Background(), raw2, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM worker_task_runtime WHERE worker_id='worker-runtime-1'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("runtime should tolerate first missing heartbeat, rows=%d", count)
	}
	if err := s.PersistWorkerHeartbeat(context.Background(), raw2, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM worker_task_runtime WHERE worker_id='worker-runtime-1'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("stale runtime rows=%d, want 0", count)
	}
}

func TestWorkerRuntimeMigrationConstraints(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/worker-runtime-constraints.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, table := range []string{"workers", "worker_sessions", "worker_task_runtime", "worker_metric_samples", "worker_events", "worker_commands"} {
		var count int
		if err := s.DB().QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("required table %s is missing", table)
		}
	}
	if _, err := s.DB().Exec(`INSERT INTO workers(worker_id,worker_name,node_role,raw_json,migrated_at) VALUES('constraint-worker','w','worker','{}',datetime('now'))`); err != nil {
		t.Fatal(err)
	}
	assertDBError := func(name, query string, args ...any) {
		t.Helper()
		if _, err := s.DB().Exec(query, args...); err == nil {
			t.Fatalf("%s unexpectedly succeeded", name)
		}
	}
	assertDBError("master node role", `INSERT INTO workers(worker_id,worker_name,node_role,raw_json,migrated_at) VALUES('bad-role','w','master','{}',datetime('now'))`)
	assertDBError("unknown session worker", `INSERT INTO worker_sessions(session_id,worker_id,token_hash,created_at,expires_at,last_seen,status) VALUES('bad-session','missing','x',datetime('now'),datetime('now','+1 hour'),datetime('now'),'ACTIVE')`)
	assertDBError("invalid runtime status", `INSERT INTO worker_task_runtime(task_id,job_id,attempt_id,attempt_number,worker_id,session_id,lease_id,executor_id,runtime_status,started_at,updated_at) VALUES('bad-task','j','a',1,'constraint-worker','s','l','e','BROKEN',datetime('now'),datetime('now'))`)
	assertDBError("invalid progress", `INSERT INTO worker_task_runtime(task_id,job_id,attempt_id,attempt_number,worker_id,session_id,lease_id,executor_id,runtime_status,progress_percent,started_at,updated_at) VALUES('bad-progress','j','a',1,'constraint-worker','s','l','e','RUNNING',140,datetime('now'),datetime('now'))`)
	if _, err := s.DB().Exec(`INSERT INTO worker_sessions(session_id,worker_id,token_hash,created_at,expires_at,last_seen,status,connected_at,last_seen_at) VALUES('session-1','constraint-worker','x',datetime('now'),datetime('now','+1 hour'),datetime('now'),'ACTIVE',datetime('now'),datetime('now'))`); err != nil {
		t.Fatal(err)
	}
	assertDBError("second active session", `INSERT INTO worker_sessions(session_id,worker_id,token_hash,created_at,expires_at,last_seen,status) VALUES('session-2','constraint-worker','x',datetime('now'),datetime('now','+1 hour'),datetime('now'),'ACTIVE')`)
	if _, err := s.DB().Exec(`INSERT INTO worker_sessions(session_id,worker_id,token_hash,created_at,expires_at,last_seen,status,session_type) VALUES('asset-session','constraint-worker','x',datetime('now'),datetime('now','+1 hour'),datetime('now'),'ACTIVE','asset')`); err != nil {
		t.Fatalf("asset session should coexist with control session: %v", err)
	}
}
