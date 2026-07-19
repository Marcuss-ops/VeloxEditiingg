package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// store_worker_runtime_recovery.go owns the STALE / PARTITIONED_SUSPECTED /
// PARTITIONED state-machine that mirrors workers.ConnectionStatus at the
// persistent layer. PersistWorkerHeartbeat is the SINGLE writer of
// connection_state (single-writer tx contract): every heartbeat
// recomputes the state from last_heartbeat_at and emits the
// canonical worker_events rows when the state transitions.
//
// State machine:
//
//	CONNECTED       --age>=StaleThreshold-->     STALE
//	STALE           --age>=PartitionThreshold--> PARTITIONED_SUSPECTED
//	CONNECTED       --age>=PartitionThreshold--> PARTITIONED_SUSPECTED
//	                                              (worker resurfaces
//	                                               after a long outage)
//	STALE                  --age<StaleThreshold-->     CONNECTED (recovery)
//	PARTITIONED_SUSPECTED  --age<StaleThreshold-->     CONNECTED (recovery)
//
// Note: connection_state=PARTITIONED is reachable only via the
// reconciler (ReconcileWorkerPartitions) — the heartbeat-time
// detector writes PARTITIONED_SUSPECTED instead. The two states
// are intentionally distinct:
//
//   - PARTITIONED_SUSPECTED: heartbeat-driven suspect after a
//     resurface — the worker MAY still be alive but acknowledged
//     late. Transitions back to CONNECTED on the next fresh
//     heartbeat.
//   - PARTITIONED: reconciler-confirmed unreachable — the heartbeat
//     stream has stopped entirely. Cannot transition to
//     PARTITIONED_SUSPECTED without a fresh heartbeat firing
//     PersistWorkerHeartbeat, at which point the heartbeat-time
//     path takes over.
//
// Events emitted on transitions:
//
//	any                -> STALE                       WORKER_STALE_DETECTED (WARN)
//	any                -> PARTITIONED_SUSPECTED       WORKER_PARTITION_DETECTED (ERROR)
//	PARTITIONED_SUSPECTED -> CONNECTED                 WORKER_PARTITION_RESOLVED (INFO)
//	PARTITIONED       -> CONNECTED                     WORKER_PARTITION_RESOLVED (INFO)
//
// Reconciliation: ReconcileWorkerPartitions is the periodic pass
// that detects workers whose heartbeat stream has stopped entirely
// (no PersistWorkerHeartbeat call). It is the master's recovery
// surface for partitions that don't surface a heartbeat, and the
// only writer of the bare PARTITIONED state from the persistent mirror.

const (
	// connectionStateConnected is the canonical fresh-heartbeat state.
	connectionStateConnected = "CONNECTED"
	// connectionStateStale is the canonical within-grace-but-aging state.
	connectionStateStale = "STALE"
	// connectionStatePartitionedSuspected is the canonical heartbeat-
	// driven suspect state — emitted by detectAndPersistPartitionTransition
	// when last_heartbeat_at crosses WorkersConfig.PartitionThresholdSeconds
	// during a heartbeat-time transition. Distinct from
	// connectionStatePartitioned (which the reconciler writes when the
	// heartbeat stream has stopped entirely).
	connectionStatePartitionedSuspected = "PARTITIONED_SUSPECTED"
	// connectionStatePartitioned is the canonical reconciler-confirmed
	// unreachable state. Only ReconcileWorkerPartitions /
	// reconcileOnePartition writes this value (single-writer
	// separation).
	connectionStatePartitioned = "PARTITIONED"
)

// computeConnectionState derives the canonical state from a
// heartbeat timestamp + threshold pair. Pure function — no I/O, no
// DB — so tests and dashboards can call it directly.
//
//   - lastHB empty / unparseable      → PARTITIONED (we have no
//     signal that the worker is alive; the worst-case state is the
//     safe default for monitoring surfaces).
//   - age >= partitionSeconds         → PARTITIONED
//   - age >= staleSeconds             → STALE
//   - age <  staleSeconds             → CONNECTED
//
// computeConnectionState derives the canonical state from a
// heartbeat timestamp + threshold pair. Pure function — no I/O, no
// DB — so tests and dashboards can call it directly.
// Heartbeat-driven branches emit PARTITIONED_SUSPECTED, NOT
// PARTITIONED; the bare PARTITIONED state is reserved for the
// reconciler code path so dashboards can distinguish "worker came
// back late" from "worker stream stopped entirely".
//
//   - lastHB empty / unparseable      → PARTITIONED_SUSPECTED (the
//     heartbeat time detector cannot prove the worker is dead, only
//     that it has stopped responding within the threshold window)
//   - age >= partitionSeconds         → PARTITIONED_SUSPECTED
//   - age >= staleSeconds             → STALE
//   - age <  staleSeconds             → CONNECTED
func computeConnectionState(lastHB string, now time.Time, staleSeconds, partitionSeconds int) string {
	if lastHB == "" {
		return connectionStatePartitionedSuspected
	}
	t, err := time.Parse(time.RFC3339Nano, lastHB)
	if err != nil {
		return connectionStatePartitionedSuspected
	}
	age := now.Sub(t.UTC())
	switch {
	case age >= time.Duration(partitionSeconds)*time.Second:
		return connectionStatePartitionedSuspected
	case age >= time.Duration(staleSeconds)*time.Second:
		return connectionStateStale
	default:
		return connectionStateConnected
	}
}

// detectAndPersistPartitionTransition reads the worker's prior
// connection_state, computes the new state from the heartbeat
// timestamp, emits a worker_events row if the state transitioned,
// and writes the new state back to the workers row.
//
// All work happens inside the supplied *sql.Tx so the state
// transition and the events ledger stay atomic with the heartbeat
// snapshot upsert that owns this transaction (single-writer
// contract).
//
// Returns the NEW connection_state so the caller can log it.
// Returns ("", nil) if the worker row does not yet exist (e.g.,
// first heartbeat on a freshly-registered worker — the upsert
// earlier in PersistWorkerHeartbeat created the row).
func detectAndPersistPartitionTransition(
	ctx context.Context,
	tx *sql.Tx,
	workerID string,
	lastHB string,
	now time.Time,
	staleSeconds, partitionSeconds int,
) (string, error) {
	if workerID == "" {
		return "", nil
	}
	newState := computeConnectionState(lastHB, now, staleSeconds, partitionSeconds)

	var oldState string
	row := tx.QueryRowContext(ctx, `SELECT connection_state FROM workers WHERE worker_id=?`, workerID)
	if err := row.Scan(&oldState); err != nil {
		if err == sql.ErrNoRows {
			// First heartbeat on a worker row that has not been
			// upserted yet (rare — EnsureWorkerRecord normally
			// creates it before the first heartbeat). The default
			// column value is CONNECTED, so the transition is from
			// "" (unset) to newState.
			oldState = ""
		} else {
			return "", fmt.Errorf("partition detection: read state: %w", err)
		}
	}

	// Same-state short-circuit avoids emitting redundant events and
	// avoids bumping last_state_change_at on every heartbeat.
	if oldState == newState {
		return newState, nil
	}

	// Emit the canonical event for the transition. The heartbeat-
	// time detector specifically emits PARTITIONED_SUSPECTED (not
	// PARTITIONED); the bare PARTITIONED reach is reserved for the
	// reconciler code path. Recovery from either terminal state
	// iterates back to CONNECTED + WORKER_PARTITION_RESOLVED.
	nowRFC3339 := now.UTC().Format(time.RFC3339Nano)
	switch {
	case oldState != connectionStateStale && newState == connectionStateStale:
		if err := appendWorkerStaleDetectedEvent(ctx, tx, workerID, lastHB, staleSeconds, nowRFC3339); err != nil {
			return "", err
		}
	case oldState != connectionStatePartitionedSuspected && newState == connectionStatePartitionedSuspected:
		if err := appendWorkerPartitionDetectedEvent(ctx, tx, workerID, lastHB, partitionSeconds, nowRFC3339); err != nil {
			return "", err
		}
	case (oldState == connectionStatePartitionedSuspected || oldState == connectionStatePartitioned) && newState == connectionStateConnected:
		if err := appendWorkerPartitionResolvedEvent(ctx, tx, workerID, lastHB, nowRFC3339); err != nil {
			return "", err
		}
	}

	// Persist the new state + bump last_state_change_at. UPDATE
	// rather than UPSERT because the heartbeat path always calls
	// upsertWorkerExec first, which creates the row if needed.
	if _, err := tx.ExecContext(ctx,
		`UPDATE workers SET connection_state=?, last_state_change_at=? WHERE worker_id=?`,
		newState, nowRFC3339, workerID,
	); err != nil {
		return "", fmt.Errorf("partition detection: write state: %w", err)
	}
	return newState, nil
}

// ReconcileWorkerPartitions scans the workers table for rows whose
// last_heartbeat_at is older than PartitionThresholdSeconds and
// whose connection_state is not already PARTITIONED. For each
// match, it transitions the row to PARTITIONED, emits
// WORKER_PARTITION_DETECTED, and bumps last_state_change_at — all
// in one transaction per worker.
//
// This is the recovery surface for the case where the worker
// heartbeat stream has stopped entirely (no PersistWorkerHeartbeat
// call ever fires for the partitioned worker). The master is
// expected to invoke this periodically (e.g., from the master
// cron loop or a dedicated background goroutine).
//
// Returns the count of workers transitioned into PARTITIONED during
// the pass. Errors are surfaced per-worker; a partial failure does
// not abort the rest of the pass (each worker is its own tx).
func (s *SQLiteStore) ReconcileWorkerPartitions(ctx context.Context, staleSeconds, partitionSeconds int) (int, error) {
	if staleSeconds <= 0 || partitionSeconds <= 0 {
		return 0, nil
	}
	if staleSeconds > partitionSeconds {
		// Validate the threshold pair before touching the DB so a
		// misconfiguration surfaces immediately rather than as a
		// silent "always-partitioned" pass.
		return 0, fmt.Errorf("reconcile partitions: staleSeconds (%d) > partitionSeconds (%d)", staleSeconds, partitionSeconds)
	}
	now := time.Now().UTC()
	cutoff := now.Add(-time.Duration(partitionSeconds) * time.Second).Format(time.RFC3339Nano)
	nowRFC3339 := now.Format(time.RFC3339Nano)

	rows, err := s.db.QueryContext(ctx,
		`SELECT worker_id, COALESCE(last_heartbeat_at,'')
		   FROM workers
		  WHERE connection_state != ?
		    AND (last_heartbeat_at IS NULL OR last_heartbeat_at = '' OR last_heartbeat_at < ?)`,
		connectionStatePartitioned, cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("reconcile partitions: scan: %w", err)
	}
	defer rows.Close()

	type candidate struct {
		workerID string
		lastHB   string
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.workerID, &c.lastHB); err != nil {
			rows.Close()
			return 0, fmt.Errorf("reconcile partitions: scan row: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("reconcile partitions: rows: %w", err)
	}

	transitioned := 0
	var firstErr error
	for _, c := range candidates {
		if err := s.reconcileOnePartition(ctx, c.workerID, c.lastHB, staleSeconds, partitionSeconds, nowRFC3339); err != nil {
			// Per-worker tx failure does not abort the rest of
			// the pass; we keep going so a single broken row
			// does not delay the recovery of the rest of the
			// fleet. The first error is reported at the end so
			// the caller can surface a partial-failure count.
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		transitioned++
	}
	return transitioned, firstErr
}

// reconcileOnePartition is the per-worker tx wrapper used by
// ReconcileWorkerPartitions. Kept private — callers go through the
// public ReconcileWorkerPartitions which fans out across the
// candidates slice.
func (s *SQLiteStore) reconcileOnePartition(
	ctx context.Context,
	workerID, lastHB string,
	staleSeconds, partitionSeconds int,
	nowRFC3339 string,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx for %s: %w", workerID, err)
	}
	defer tx.Rollback()

	if err := appendWorkerPartitionDetectedEvent(ctx, tx, workerID, lastHB, partitionSeconds, nowRFC3339); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE workers
		    SET connection_state=?, last_state_change_at=?, status=?
		  WHERE worker_id=?`,
		connectionStatePartitioned, nowRFC3339, connectionStatePartitioned, workerID,
	); err != nil {
		return fmt.Errorf("update %s: %w", workerID, err)
	}
	return tx.Commit()
}

// eventJSONDetails is a tiny helper that marshals a map to its
// canonical JSON string form for storage in worker_events.
// details_json. Returns "" if marshalling fails (the event row is
// still inserted with an empty details column so a single bad
// payload never blocks the audit ledger).
func eventJSONDetails(v map[string]any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// Canonical reason_code values surfaced on the worker_events rows
// emitted by detectAndPersistPartitionTransition /
// appendWorkerPartitionDetectedEvent. Exported (package-level) so
// tests in this package and dashboards in other packages can pin
// the audit-trail string without a re-export shim.
const (
	connectionStateChangeReasonStaleDelayed      = "heartbeat_delayed"
	connectionStateChangeReasonPartitionTimeout  = "heartbeat_timeout"
	connectionStateChangeReasonPartitionResolved = "heartbeat_resumed"
)

// Canonical reason_code values surfaced on the TASK_RUNTIME_DISAPPEARED
// rows emitted by reconcileWorkerRuntime (heartbeat-miss path) and
// bulkEmitTaskRuntimeDisappearedOnPartition (partition-time path).
// Distinct from the WORKER-level reason codes above: those carry a
// per-worker signal; these carry a per-task signal so dashboards can
// filter by event_type=TASK_RUNTIME_DISAPPEARED + reason_code to drill
// into the cause without parsing the details_json.
//
//   - "heartbeat_missing": the row's missing_heartbeats counter crossed 2.
//   - "partition_timeout": the worker's connection_state crossed
//     PARTITIONED_SUSPECTED via the heartbeat-time detector; the
//     bulk-emit fan-out is the per-task mirror of that signal.
const (
	connectionStateChangeReasonHeartbeatMissing     = "heartbeat_missing"
	connectionStateChangeReasonPartitionTimeoutTask = "partition_timeout"
)
