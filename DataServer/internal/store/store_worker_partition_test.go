package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// TestPartitionDetectionEmitsTaskRuntimeDisappeared is the contract
// test for the heartbeat-driven network-partition feature (commit
// landing migration 097). It exercises the full PersistWorkerHeartbeat
// pipeline end-to-end:
//
//  1. Initial heartbeat populates a worker + 3 active
//     worker_task_runtime rows; status=CONNECTED.
//  2. A second heartbeat arrives with last_heartbeat_at 6 minutes
//     in the past (well past the partition threshold of 300s) and an
//     empty active_jobs list (so the partition-time fan-out has rows
//     to update — the existing missing_heartbeats>=2 reconcile path
//     is NOT used because the rows are still under the threshold at
//     missing_heartbeats=1).
//
// Expected outcomes:
//
//   - workers.connection_state transitions to PARTITIONED_SUSPECTED.
//   - All 3 worker_task_runtime rows flip runtime_status to
//     PARTITIONED_SUSPECTED (the migration 097 trigger guard
//     explicitly accepts this token).
//   - 3 TASK_RUNTIME_DISAPPEARED rows are inserted into worker_events
//     with reason_code='partition_timeout' (one per runtime row; the
//     distinct reason_code separates this from the legacy
//     reason_code='heartbeat_missing' path driven by
//     missing_heartbeats>=2 reconciliation).
//   - 1 WORKER_PARTITION_DETECTED row is inserted (the worker-level
//     transition on the workers row).
//   - 0 TASK_RUNTIME_DISAPPEARED rows carry reason_code='heartbeat_missing'
//     because missing_heartbeats stays at 1 (under the 2 threshold).
func TestPartitionDetectionEmitsTaskRuntimeDisappeared(t *testing.T) {
	s, err := NewSQLiteStore(t.TempDir() + "/partition-detected.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	workerID := "partition-test-1"
	now := time.Now().UTC()

	// Step 1 — initial CONNECTED heartbeat that establishes the worker
	// row and 3 active worker_task_runtime rows.
	initialRaw, _ := json.Marshal(map[string]any{
		"worker_id":   workerID,
		"worker_name": "node-b",
		"status":      "busy",
		"current_job": "job-A",
		"schedulable": true,
		"node_role":   "worker",
		"metrics": map[string]any{
			"active_jobs": []any{
				map[string]any{
					"job_id": "job-1", "task_id": "task-1",
					"attempt_id": "attempt-1", "attempt": 1,
					"lease_id": "lease-1",
					"job_type": "scene.composite.v1",
				},
				map[string]any{
					"job_id": "job-2", "task_id": "task-2",
					"attempt_id": "attempt-2", "attempt": 1,
					"lease_id": "lease-2",
					"job_type": "scene.composite.v1",
				},
				map[string]any{
					"job_id": "job-3", "task_id": "task-3",
					"attempt_id": "attempt-3", "attempt": 1,
					"lease_id": "lease-3",
					"job_type": "scene.composite.v1",
				},
			},
		},
	})
	if err := s.PersistWorkerHeartbeat(context.Background(), initialRaw, ""); err != nil {
		t.Fatalf("initial heartbeat: %v", err)
	}

	// Sanity: connection_state is CONNECTED after the initial heartbeat.
	var connState string
	if err := s.DB().QueryRow(`SELECT connection_state FROM workers WHERE worker_id=?`, workerID).Scan(&connState); err != nil {
		t.Fatal(err)
	}
	if connState != "CONNECTED" {
		t.Fatalf("post-initial connection_state=%q, want CONNECTED", connState)
	}

	// Sanity: 3 runtime rows inserted.
	var initialRuntimeCount int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM worker_task_runtime WHERE worker_id=?`, workerID).Scan(&initialRuntimeCount); err != nil {
		t.Fatal(err)
	}
	if initialRuntimeCount != 3 {
		t.Fatalf("initial runtime count=%d, want 3", initialRuntimeCount)
	}

	// Step 2 — drive a heartbeat with last_heartbeat_at 6 minutes in
	// the past (past DefaultPartitionThreshold = 300s) and an empty
	// active_jobs list. The partition-time bulk-emit must fire:
	// detectAndPersistPartitionTransition flips connection_state to
	// PARTITIONED_SUSPECTED, then bulkEmitTaskRuntimeDisappearedOnPartition
	// emits + status-flips all 3 rows. The existing reconcile-worker
	// missing_heartbeats>=2 path does NOT fire (3 rows present, each
	// bumped missing_heartbeats from 0 to 1, still under 2).
	staleHB := now.Add(-6 * time.Minute).Format(time.RFC3339Nano)
	staleRaw, _ := json.Marshal(map[string]any{
		"worker_id":      workerID,
		"last_heartbeat": staleHB,
		"status":         "RUNNING",
		"schedulable":    true,
		"metrics":        map[string]any{"active_jobs": []any{}},
	})
	if err := s.PersistWorkerHeartbeat(context.Background(), staleRaw, "session-stale"); err != nil {
		t.Fatalf("stale heartbeat: %v", err)
	}

	// Assert 1 — workers.connection_state transitioned to PARTITIONED_SUSPECTED.
	if err := s.DB().QueryRow(`SELECT connection_state FROM workers WHERE worker_id=?`, workerID).Scan(&connState); err != nil {
		t.Fatal(err)
	}
	if connState != "PARTITIONED_SUSPECTED" {
		t.Fatalf("post-stale connection_state=%q, want PARTITIONED_SUSPECTED", connState)
	}

	// Assert 2 — all 3 worker_task_runtime rows carry runtime_status='PARTITIONED_SUSPECTED'.
	var partitionedSuspectedCount int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM worker_task_runtime WHERE worker_id=? AND runtime_status=?`,
		workerID, "PARTITIONED_SUSPECTED").Scan(&partitionedSuspectedCount); err != nil {
		t.Fatal(err)
	}
	if partitionedSuspectedCount != 3 {
		t.Fatalf("partitioned_suspected runtime count=%d, want 3 (all rows should have flipped)", partitionedSuspectedCount)
	}

	// Assert 3 — 3 TASK_RUNTIME_DISAPPEARED rows with reason_code='partition_timeout'.
	var partitionEmittedCount int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM worker_events
		WHERE worker_id=? AND event_type='TASK_RUNTIME_DISAPPEARED' AND reason_code='partition_timeout'`,
		workerID).Scan(&partitionEmittedCount); err != nil {
		t.Fatal(err)
	}
	if partitionEmittedCount != 3 {
		t.Fatalf("TASK_RUNTIME_DISAPPEARED partition_timeout rows=%d, want 3 (one per runtime row)", partitionEmittedCount)
	}

	// Assert 4 — 1 WORKER_PARTITION_DETECTED row (the worker-level event).
	var workerPartitionDetected int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM worker_events
		WHERE worker_id=? AND event_type='WORKER_PARTITION_DETECTED'`,
		workerID).Scan(&workerPartitionDetected); err != nil {
		t.Fatal(err)
	}
	if workerPartitionDetected != 1 {
		t.Fatalf("WORKER_PARTITION_DETECTED rows=%d, want 1 (one per worker-state transition)", workerPartitionDetected)
	}

	// Assert 5 — 0 TASK_RUNTIME_DISAPPEARED rows with reason_code='heartbeat_missing'
	// because missing_heartbeats stayed at 1 (< 2) on each row.
	var heartbeatMissingCount int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM worker_events
		WHERE worker_id=? AND event_type='TASK_RUNTIME_DISAPPEARED' AND reason_code='heartbeat_missing'`,
		workerID).Scan(&heartbeatMissingCount); err != nil {
		t.Fatal(err)
	}
	if heartbeatMissingCount != 0 {
		t.Fatalf("TASK_RUNTIME_DISAPPEARED heartbeat_missing rows=%d, want 0 (partition path should dominate, reconcile path stays at missing_heartbeats=1)", heartbeatMissingCount)
	}

	// Assert 6 — idem-potency: a second stale heartbeat at the same
	// age does NOT re-emit events (the runtime_status filter excludes
	// already-partitioned rows from both the SELECT and the UPDATE in
	// bulkEmitTaskRuntimeDisappearedOnPartition).
	if err := s.PersistWorkerHeartbeat(context.Background(), staleRaw, "session-stale"); err != nil {
		t.Fatalf("second stale heartbeat: %v", err)
	}
	var partitionEmittedAfterRepeat int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM worker_events
		WHERE worker_id=? AND event_type='TASK_RUNTIME_DISAPPEARED' AND reason_code='partition_timeout'`,
		workerID).Scan(&partitionEmittedAfterRepeat); err != nil {
		t.Fatal(err)
	}
	if partitionEmittedAfterRepeat != 3 {
		t.Fatalf("TASK_RUNTIME_DISAPPEARED partition_timeout rows after repeat=%d, want 3 (emit must be idempotent)", partitionEmittedAfterRepeat)
	}

	// Assert 7 — recovery: a fresh heartbeat flips connection_state
	// back to CONNECTED + emits WORKER_PARTITION_RESOLVED.
	freshHB := now.Format(time.RFC3339Nano)
	freshRaw, _ := json.Marshal(map[string]any{
		"worker_id":      workerID,
		"last_heartbeat": freshHB,
		"status":         "idle",
		"schedulable":    true,
		"metrics":        map[string]any{"active_jobs": []any{}},
	})
	if err := s.PersistWorkerHeartbeat(context.Background(), freshRaw, "session-fresh"); err != nil {
		t.Fatalf("fresh heartbeat: %v", err)
	}
	if err := s.DB().QueryRow(`SELECT connection_state FROM workers WHERE worker_id=?`, workerID).Scan(&connState); err != nil {
		t.Fatal(err)
	}
	if connState != "CONNECTED" {
		t.Fatalf("post-recovery connection_state=%q, want CONNECTED", connState)
	}
	var resolvedCount int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM worker_events
		WHERE worker_id=? AND event_type='WORKER_PARTITION_RESOLVED'`,
		workerID).Scan(&resolvedCount); err != nil {
		t.Fatal(err)
	}
	if resolvedCount != 1 {
		t.Fatalf("WORKER_PARTITION_RESOLVED rows=%d, want 1 (recovery must emit exactly one RESOLVED event)", resolvedCount)
	}
}
