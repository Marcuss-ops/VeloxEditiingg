package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// store_worker_events.go owns the worker_events INSERT helpers.
// The heartbeat path emits three flavours of events today:
//
//	WORKER_STATE_CHANGED        — status machine transitions on
//	                               the workers row.
//	TASK_RUNTIME_DISAPPEARED     — reconciler-detected stale rows
//	                               in worker_task_runtime.
//	WORKER_STALE_DETECTED        — first-time heartbeat after the
//	                               last_heartbeat_at age crossed
//	                               WorkersConfig.StaleThresholdSeconds.
//	WORKER_PARTITION_DETECTED    — heartbeat age crossed
//	                               WorkersConfig.PartitionThresholdSeconds,
//	                               OR the reconciler detected a
//	                               worker that stopped sending
//	                               heartbeats entirely.
//	WORKER_PARTITION_RESOLVED    — heartbeat resumed after a
//	                               PARTITIONED state.
//
// All helpers receive *sql.Tx from the caller and never open their
// own. Details are JSON-serialized so the audit ledger is
// self-describing (no separate schema-migration needed to add a
// field).

func appendWorkerStateChangedEvent(ctx context.Context, tx *sql.Tx, workerID, sessionID, oldStatus, newStatus, oldJob, newJob, now string) error {
	details, _ := json.Marshal(map[string]any{
		"old_status": oldStatus,
		"new_status": newStatus,
		"old_job":    oldJob,
		"new_job":    newJob,
	})
	if _, err := tx.ExecContext(ctx, `INSERT INTO worker_events
		(event_id,worker_id,session_id,event_type,severity,details_json,created_at)
		VALUES (?,?,?,?,?,?,?)`, uuid.NewString(), workerID, sessionID,
		"WORKER_STATE_CHANGED", "INFO", string(details), now); err != nil {
		return fmt.Errorf("append worker event: %w", err)
	}
	return nil
}

func appendTaskRuntimeDisappearedEvent(ctx context.Context, tx *sql.Tx, workerID, jobID, taskID, attemptID, now string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO worker_events
		(event_id,worker_id,job_id,task_id,attempt_id,event_type,severity,reason_code,created_at)
		VALUES (?,?,?,?,?,?,?,?,?)`, uuid.NewString(), workerID, jobID, taskID, attemptID,
		"TASK_RUNTIME_DISAPPEARED", "WARN", "heartbeat_missing", now); err != nil {
		return err
	}
	return nil
}

// appendWorkerStaleDetectedEvent records the canonical event for a
// heartbeat-age crossing WorkersConfig.StaleThresholdSeconds. The
// transition emits at WARN severity (the worker is still believed
// alive — just slow).
//
// lastHB is the worker's current last_heartbeat_at (may be empty
// for a never-heartbeated worker); staleSeconds is the threshold
// the heartbeat crossed. details_json captures both for the audit
// trail so a future regression in the threshold derivation can be
// re-derived from the event alone.
func appendWorkerStaleDetectedEvent(ctx context.Context, tx *sql.Tx, workerID, lastHB string, staleSeconds int, now string) error {
	details, _ := json.Marshal(map[string]any{
		"last_heartbeat_at":   lastHB,
		"stale_threshold_sec": staleSeconds,
	})
	if _, err := tx.ExecContext(ctx, `INSERT INTO worker_events
		(event_id,worker_id,event_type,severity,reason_code,details_json,created_at)
		VALUES (?,?,?,?,?,?,?)`, uuid.NewString(), workerID,
		"WORKER_STALE_DETECTED", "WARN", connectionStateChangeReasonStaleDelayed,
		string(details), now); err != nil {
		return fmt.Errorf("append worker stale event: %w", err)
	}
	return nil
}

// appendWorkerPartitionDetectedEvent records the canonical event
// for a heartbeat-age crossing WorkersConfig.PartitionThresholdSeconds.
// Emits at ERROR severity — the worker is presumed unreachable from
// the master's side until a fresh heartbeat arrives.
//
// Called from two paths:
//   - detectAndPersistPartitionTransition (heartbeat-time, when
//     the worker came back from a long outage and the persisted
//     last_heartbeat_at is now past the threshold).
//   - reconcileOnePartition (reconciliation, when no heartbeat has
//     arrived at all within the threshold).
//
// The reason_code is identical across both paths so dashboards can
// group events regardless of which detector fired.
func appendWorkerPartitionDetectedEvent(ctx context.Context, tx *sql.Tx, workerID, lastHB string, partitionSeconds int, now string) error {
	details, _ := json.Marshal(map[string]any{
		"last_heartbeat_at":       lastHB,
		"partition_threshold_sec": partitionSeconds,
	})
	if _, err := tx.ExecContext(ctx, `INSERT INTO worker_events
		(event_id,worker_id,event_type,severity,reason_code,details_json,created_at)
		VALUES (?,?,?,?,?,?,?)`, uuid.NewString(), workerID,
		"WORKER_PARTITION_DETECTED", "ERROR", connectionStateChangeReasonPartitionTimeout,
		string(details), now); err != nil {
		return fmt.Errorf("append worker partition event: %w", err)
	}
	return nil
}

// appendWorkerPartitionResolvedEvent records the canonical event
// for a worker transitioning OUT of PARTITIONED back to CONNECTED.
// Emits at INFO severity — the worker is reachable again, the
// master does NOT need to escalate.
//
// Called only from detectAndPersistPartitionTransition (heartbeat-
// time): the reconciler does not emit RESOLVED events because
// resolution requires a fresh heartbeat, which by definition flows
// through PersistWorkerHeartbeat.
func appendWorkerPartitionResolvedEvent(ctx context.Context, tx *sql.Tx, workerID, lastHB, now string) error {
	details, _ := json.Marshal(map[string]any{
		"last_heartbeat_at": lastHB,
	})
	if _, err := tx.ExecContext(ctx, `INSERT INTO worker_events
		(event_id,worker_id,event_type,severity,reason_code,details_json,created_at)
		VALUES (?,?,?,?,?,?,?)`, uuid.NewString(), workerID,
		"WORKER_PARTITION_RESOLVED", "INFO", connectionStateChangeReasonPartitionResolved,
		string(details), now); err != nil {
		return fmt.Errorf("append worker partition resolved event: %w", err)
	}
	return nil
}

// connectionStateChangeReasonStaleDelayed /
// connectionStateChangeReasonPartitionTimeout /
// connectionStateChangeReasonPartitionResolved are package-level
// constants declared in store_worker_runtime_recovery.go (where
// the state machine lives). They are referenced directly from this
// file because both files share the same package — no re-export
// shim needed.
