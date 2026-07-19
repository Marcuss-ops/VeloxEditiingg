package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// store_worker_events.go owns the worker_events INSERT helpers.
// The heartbeat path emits three flavours of events today:
//
//	WORKER_STATE_CHANGED         — status machine transitions on
//	                                the workers row.
//	TASK_RUNTIME_DISAPPEARED     — reconciler-detected stale rows
//	                                in worker_task_runtime
//	                                (reason_code=heartbeat_missing),
//	                                OR the partition-time bulk-emit
//	                                fan-out
//	                                (reason_code=partition_timeout).
//	WORKER_STALE_DETECTED        — first-time heartbeat after the
//	                                last_heartbeat_at age crossed
//	                                WorkersConfig.StaleThresholdSeconds.
//	WORKER_PARTITION_DETECTED    — heartbeat age crossed
//	                                WorkersConfig.PartitionThresholdSeconds
//	                                (emitted on transition into
//	                                PARTITIONED_SUSPECTED), OR the
//	                                reconciler detected a worker that
//	                                stopped sending heartbeats entirely
//	                                (emitted on transition into
//	                                PARTITIONED).
//	WORKER_PARTITION_RESOLVED    — heartbeat resumed after a
//	                                PARTITIONED_SUSPECTED or
//	                                PARTITIONED state.
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

// appendTaskRuntimeDisappearedEvent records the canonical event for
// a single runtime-task row disappearing. reasonCode is supplied by
// the caller and surfaces on the row's reason_code column so
// dashboards can filter by event_type=TASK_RUNTIME_DISAPPEARED +
// reason_code to drill into the cause without parsing details_json.
//
// Canonical values:
//
//   - "heartbeat_missing" (connectionStateChangeReasonHeartbeatMissing):
//     driven by reconcileWorkerRuntime when missing_heartbeats>=2.
//   - "partition_timeout" (connectionStateChangeReasonPartitionTimeoutTask):
//     driven by the network-partition detector
//     (bulkEmitTaskRuntimeDisappearedOnPartition) when the worker's
//     connection_state crosses the partition threshold.
//
// An empty reasonCode is replaced with the historical default
// "heartbeat_missing" so legacy callers that omit it keep their
// existing audit-trail string.
func appendTaskRuntimeDisappearedEvent(ctx context.Context, tx *sql.Tx, workerID, jobID, taskID, attemptID, reasonCode, now string) error {
	if reasonCode == "" {
		reasonCode = connectionStateChangeReasonHeartbeatMissing
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO worker_events
		(event_id,worker_id,job_id,task_id,attempt_id,event_type,severity,reason_code,created_at)
		VALUES (?,?,?,?,?,?,?,?,?)`, uuid.NewString(), workerID, jobID, taskID, attemptID,
		"TASK_RUNTIME_DISAPPEARED", "WARN", reasonCode, now); err != nil {
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

// WorkerEventRow is the read-side row shape returned by
// ListWorkerEvents. Mirrors the worker_events schema (migration
// 094). Fields with no NOT NULL constraint at the schema level
// (worker_id, session_id, job_id, task_id, attempt_id, reason_code)
// are typed as sql.NullString so the SQL NULL survives the scan.
//
// `DetailsJSON` is the raw details_json column; the handler layer
// parses it into a structured map (or surfaces it as raw JSON)
// after sanitization. We deliberately do NOT parse it inside the
// store layer because the canonical redaction surface (sanitiseHostname,
// redactIPv4, redactIPv6, redactSecretHex) lives in the handler
// package and the store package must remain pure data access.
type WorkerEventRow struct {
	EventID     string
	WorkerID    sql.NullString
	SessionID   sql.NullString
	JobID       sql.NullString
	TaskID      sql.NullString
	AttemptID   sql.NullString
	EventType   string
	Severity    string
	ReasonCode  sql.NullString
	DetailsJSON string
	CreatedAt   string
}

// ListWorkerEvents returns up to `limit` events for `workerID`,
// newest first (ORDER BY created_at DESC).
//
// Parameters:
//
//	workerID  — exact match on worker_id. Empty string returns an
//	            empty slice (defense against an empty :worker_id
//	            param).
//	eventType — optional exact match on event_type. Pass "" to
//	            disable the type filter (returns all event types).
//	            Known canonical types (used by dashboards for
//	            filter chips):
//	              WORKER_STATE_CHANGED
//	              TASK_RUNTIME_DISAPPEARED
//	              WORKER_STALE_DETECTED
//	              WORKER_PARTITION_DETECTED
//	              WORKER_PARTITION_RESOLVED
//	            Unknown types are still queryable so a future
//	            event-type addition does not require a handler
//	            change — the applier just matches the literal.
//	since     — optional RFC3339 lower bound on created_at. Pass ""
//	            to disable.
//	limit     — caller-supplied upper bound, clamped to [1, 1000].
//
// The function NEVER mutates state. Safe for concurrent use.
func ListWorkerEvents(ctx context.Context, db *sql.DB, workerID, eventType, since string, limit int) ([]WorkerEventRow, error) {
	if workerID == "" {
		return []WorkerEventRow{}, nil
	}
	if limit < 1 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	args := []interface{}{workerID}
	q := `SELECT event_id, worker_id, session_id, job_id, task_id, attempt_id,
		       event_type, severity, reason_code, details_json, created_at
		  FROM worker_events
		 WHERE worker_id = ?`
	if strings.TrimSpace(eventType) != "" {
		q += ` AND event_type = ?`
		args = append(args, eventType)
	}
	if strings.TrimSpace(since) != "" {
		q += ` AND created_at >= ?`
		args = append(args, since)
	}
	q += ` ORDER BY created_at DESC, event_id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list worker events: %w", err)
	}
	defer rows.Close()
	out := make([]WorkerEventRow, 0, limit)
	for rows.Next() {
		var r WorkerEventRow
		if err := rows.Scan(
			&r.EventID, &r.WorkerID, &r.SessionID, &r.JobID, &r.TaskID, &r.AttemptID,
			&r.EventType, &r.Severity, &r.ReasonCode, &r.DetailsJSON, &r.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("list worker events: scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list worker events: rows: %w", err)
	}
	return out, nil
}
