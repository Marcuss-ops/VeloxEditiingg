package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
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

// TestInsertSession_CollisionRejectsDifferentTokenHash
// (RW-PROD-005 §3 anti-collision invariant).
//
// Behaviour asserted:
//   * The FIRST InsertSession on a fresh worker_id succeeds (no prior ACTIVE).
//   * The SECOND InsertSession on the SAME worker_id with a DIFFERENT
//     token_hash is rejected with ErrWorkerIDCollision — the master is
//     protecting the registry against two physical machines sharing an
//     identity. This is the handler-level pre-emptive probe path that
//     short-circuits before any INSERT is attempted.
//   * The SECOND InsertSession on the SAME worker_id with the SAME
//     token_hash succeeds (legitimate reconnect: same machine, fresh
//     session). The pre-emptive UPDATE demotes the prior ACTIVE and the
//     INSERT goes through.
//   * The collision-reject path returns errors.Is(err, ErrWorkerIDCollision)
//     so callers can detect via the typed sentinel without string-matching.
//   * The DB-level trigger (worker_sessions_one_active / migrations 094 +
//     095) continues to backstop the race window as a defensive layer.
func TestInsertSession_CollisionRejectsDifferentTokenHash(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/worker-runtime-collision.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Anti-collision tests require the parent worker row in
	// workers BEFORE InsertSession (FK / trigger invariant from
	// migration 094). Without this seed the test fails with
	// `worker session references unknown worker`.
	if _, err := s.DB().Exec(
		`INSERT INTO workers(worker_id,worker_name,node_role,raw_json,migrated_at) VALUES(?,?,?,?,?)`,
		"col-worker-1", "col-test", "worker", "{}", "datetime('now')",
	); err != nil {
		t.Fatalf("seed workers row failed: %v", err)
	}

	const (
		workerID  = "col-worker-1"
		tokenA    = "token-hash-AAA"
		tokenB    = "token-hash-BBB" // distinct credential surface
		expFuture = "datetime('now','+1 hour')"
	)

	// 1) First session: no prior ACTIVE. Must succeed.
	first := &PersistedSession{
		SessionID:   "session-A",
		WorkerID:    workerID,
		SessionType: "control",
		IPAddress:   "test-ip",
		CreatedAt:   time.Now().UTC(),
		ExpiresAt:   time.Now().UTC().Add(24 * time.Hour),
		TokenHash:   tokenA,
	}
	if err := s.InsertSession(first); err != nil {
		t.Fatalf("InsertSession(first) unexpectedly failed: %v", err)
	}

	// Sanity: row is ACTIVE in DB.
	var activeCount int
	if err := s.DB().QueryRow(
		`SELECT COUNT(*) FROM worker_sessions WHERE worker_id=? AND status='ACTIVE'`, workerID,
	).Scan(&activeCount); err != nil {
		t.Fatal(err)
	}
	if activeCount != 1 {
		t.Fatalf("after first InsertSession active count=%d, want 1", activeCount)
	}

	// 2) Second session, DIFFERENT token_hash → ErrWorkerIDCollision.
	second := &PersistedSession{
		SessionID:   "session-B",
		WorkerID:    workerID,
		SessionType: "control",
		IPAddress:   "test-ip",
		CreatedAt:   time.Now().UTC(),
		ExpiresAt:   time.Now().UTC().Add(24 * time.Hour),
		TokenHash:   tokenB,
	}
	secondErr := s.InsertSession(second)
	if secondErr == nil {
		t.Fatal("InsertSession(different token) must reject collision, got nil")
	}
	if !errors.Is(secondErr, ErrWorkerIDCollision) {
		t.Fatalf("collision error must wrap ErrWorkerIDCollision, got: %v", secondErr)
	}

	// Sanity: DB still has exactly 1 ACTIVE row, the original session-A.
	if err := s.DB().QueryRow(
		`SELECT COUNT(*) FROM worker_sessions WHERE worker_id=? AND status='ACTIVE'`, workerID,
	).Scan(&activeCount); err != nil {
		t.Fatal(err)
	}
	if activeCount != 1 {
		t.Fatalf("collision must NOT have inserted session-B; active count=%d, want 1", activeCount)
	}

	// 3) Third session, SAME token_hash → legitimate reconnect succeeds.
	third := &PersistedSession{
		SessionID:   "session-C",
		WorkerID:    workerID,
		SessionType: "control",
		TokenHash:   tokenA, // same as first → reconnect
	}
	if err := s.InsertSession(third); err != nil {
		t.Fatalf("InsertSession(same token=reconnect) unexpectedly failed: %v", err)
	}
	// After reconnect: session-A has been demoted, session-C is ACTIVE.
	if err := s.DB().QueryRow(
		`SELECT COUNT(*) FROM worker_sessions WHERE worker_id=? AND status='ACTIVE' AND session_id='session-C'`, workerID,
	).Scan(&activeCount); err != nil {
		t.Fatal(err)
	}
	if activeCount != 1 {
		t.Fatalf("after reconnect session-C active count=%d, want 1", activeCount)
	}
}

// TestCheckActiveSessionCollision_ReturnsMatchingHash
// (RW-PROD-005 §3 anti-collision invariant).
//
// Behaviour asserted:
//   * CheckActiveSessionCollision returns "" + nil for a worker_id with
//     no ACTIVE session (no false-positive collisions on fresh boot).
//   * CheckActiveSessionCollision returns the matching token_hash when an
//     ACTIVE session exists for the worker_id.
//   * The function is session-type-scoped (control vs asset) — an ACTIVE
//     asset session does NOT shadow a candidate control session.
func TestCheckActiveSessionCollision_ReturnsMatchingHash(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/worker-runtime-collision-probe.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.DB().Exec(
		`INSERT INTO workers(worker_id,worker_name,node_role,raw_json,migrated_at) VALUES(?,?,?,?,?)`,
		"probe-worker-1", "probe-test", "worker", "{}", "datetime('now')",
	); err != nil {
		t.Fatalf("seed workers row failed: %v", err)
	}

	const (
		workerID = "probe-worker-1"
		tokenX   = "probe-token-XXX"
	)

	// Empty store: must return "" (no collision candidate).
	got, err := s.CheckActiveSessionCollision(workerID, "control")
	if err != nil {
		t.Fatalf("CheckActiveSessionCollision on empty store failed: %v", err)
	}
	if got != "" {
		t.Fatalf("empty store should return token_hash='', got %q", got)
	}

	// Seed one ACTIVE control session.
	if err := s.InsertSession(&PersistedSession{
		SessionID:   "session-probe-1",
		WorkerID:    workerID,
		SessionType: "control",
		IPAddress:   "test-ip",
		CreatedAt:   time.Now().UTC(),
		ExpiresAt:   time.Now().UTC().Add(24 * time.Hour),
		TokenHash:   tokenX,
	}); err != nil {
		t.Fatalf("seed InsertSession failed: %v", err)
	}

	// After seeding: must return the matching token_hash.
	got, err = s.CheckActiveSessionCollision(workerID, "control")
	if err != nil {
		t.Fatalf("CheckActiveSessionCollision after seed failed: %v", err)
	}
	if got != tokenX {
		t.Fatalf("post-seed token_hash=%q, want %q", got, tokenX)
	}

	// Session-type scoping: probing for "asset" must return "" even
	// though a control session is ACTIVE (they're disjoint namespaces).
	got, err = s.CheckActiveSessionCollision(workerID, "asset")
	if err != nil {
		t.Fatalf("CheckActiveSessionCollision(asset) failed: %v", err)
	}
	if got != "" {
		t.Fatalf("probing asset-scoped collision on a control-ACTIVE worker must return '', got %q", got)
	}
}
