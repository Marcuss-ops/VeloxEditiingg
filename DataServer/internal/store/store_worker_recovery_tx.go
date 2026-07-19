// store_worker_recovery_tx.go is the recovery-path *sql.Tx primitives
// file.
//
// Per-package single-writer tx contract (see store_worker_runtime.go
// for the canonical statement, and store_worker_runtime_recovery.go
// for the recovery-path state-machine):
//
//   - PersistWorkerHeartbeat (store_worker_heartbeat.go) is the
//     heartbeat-path opener of *sql.Tx (s.db.BeginTx). It is the
//     ONLY opener on the heartbeat path.
//   - THIS FILE is the second (and only other) per-package opener of
//     *sql.Tx on the recovery path. reconcileOnePartition opens
//     s.db.BeginTx directly because the recovery loop cannot
//     piggyback on a heartbeat: candidates whose heartbeat stream
//     has stopped entirely never produce a PersistWorkerHeartbeat
//     call, so the per-candidate tx must be opened here.
//
// Together these two sites are the per-package single-writer
// exception. No other function in the package is permitted to open
// a *sql.Tx. See store_worker_runtime.go for the rationale and the
// per-package invariant that the two-site exception preserves.
//
// What lives in this file:
//
//   - reconcileOnePartition: the per-worker tx wrapper invoked by
//     ReconcileWorkerPartitions (the public recovery entry-point in
//     store_worker_runtime_recovery.go). Opens its own *sql.Tx
//     because there is no heartbeat path available. Writes the
//     PARTITIONED state transition + last_state_change_at + status
//
//   - WORKER_PARTITION_DETECTED event row atomically inside one
//     transaction, so a per-candidate failure cannot corrupt the
//     audit trail of another candidate.
//
//   - persistPartitionedStateTx: private *sql.Tx helper that
//     executes the workers row UPDATE for the PARTITIONED
//     transition. Kept private so it can be unit-tested in
//     isolation with a stub Tx, without faking the
//     s.db.BeginTx + appendWorkerPartitionDetectedEvent machinery
//     in reconcileOnePartition.
//
// What does NOT live here:
//
//   - detectAndPersistPartitionTransition (STAYS in
//     store_worker_runtime_recovery.go): it is a heartbeat-path
//     helper that RECEIVES *sql.Tx from PersistWorkerHeartbeat, not
//     an opener. Moving it here would blur the boundary between
//     the two writers of workers.connection_state.
//   - appendWorkerPartitionDetectedEvent (LIVES in
//     store_worker_events.go): the per-event audit-trail helpers
//     are shared between the heartbeat path and the recovery
//     path; they stay co-located with the other event primitives,
//     not on the recovery-specific path.
//
// Per-file invariant: only reconcileOnePartition on this file calls
// s.db.BeginTx. persistPartitionedStateTx receives *sql.Tx as a
// parameter — it NEVER opens its own transaction.
//
// Cross-references:
//
//   - store_worker_runtime.go (shell)         — per-package
//     single-writer
//     contract;
//     cross-references
//     from both
//     BEGIN-TX openings.
//   - store_worker_heartbeat.go               — heartbeat-path
//     BEGIN-TX site
//     (#1 of 2).
//   - store_worker_runtime_recovery.go        — state-machine
//   - heartbeat-path
//     detector
//     (detectAndPersistPartitionTransition)
//   - public recovery
//     entry-point
//     (ReconcileWorkerPartitions).
//   - store_worker_events.go                  — audit-trail
//     payload helpers
//     (appendWorkerPartitionDetectedEvent,
//     etc.), shared by
//     both paths.
package store

import (
	"context"
	"database/sql"
	"fmt"
)

// reconcileOnePartition is the per-worker tx wrapper used by
// ReconcileWorkerPartitions. Kept private — callers go through the
// public ReconcileWorkerPartitions which fans out across the
// candidates slice.
//
// Tx semantics:
//
//   - s.db.BeginTx opens a fresh transaction per candidate worker
//     (recovery path: there is no PersistWorkerHeartbeat available
//     to piggyback on, even in principle — the whole purpose of
//     this loop is to make the persistent mirror catch up when no
//     heartbeat activity is happening).
//   - defer tx.Rollback() ensures a tx.Rollback fires on any return
//     path BEFORE tx.Commit() — so on the success path, the deferred
//     rollback is a no-op SQL-level once Commit() has finalized the
//     transaction.
//   - On any non-error path, tx.Commit() finalizes the per-worker
//     write. On error, the deferred rollback reverts the per-worker
//     UPDATE + the WORKER_PARTITION_DETECTED audit row so a partial
//     failure does not corrupt the audit trail.
//
// Per-candidate failure is reported back to ReconcileWorkerPartitions,
// which keeps the loop alive for the remaining candidates. A single
// broken worker row cannot delay the recovery of the rest of the
// fleet.
//
// Cross-references:
//
//   - Called by: ReconcileWorkerPartitions
//     (store_worker_runtime_recovery.go).
//   - Emits:     WORKER_PARTITION_DETECTED event via
//     appendWorkerPartitionDetectedEvent
//     (store_worker_events.go).
//   - Writes:    workers.connection_state / last_state_change_at /
//     status via persistPartitionedStateTx (this file).
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
	if err := persistPartitionedStateTx(ctx, tx, workerID, nowRFC3339); err != nil {
		return err
	}
	return tx.Commit()
}

// persistPartitionedStateTx writes the per-worker PARTITIONED state,
// last_state_change_at, and status columns inside the supplied
// *sql.Tx. Private so it can be unit-tested in isolation — a stub
// Tx can drive the UPDATE without faking the full s.db.BeginTx
// machinery in reconcileOnePartition.
//
// Single-writer invariant: this helper is the ONLY writer of the
// bare PARTITIONED state for the workers.connection_state column
// on the recovery path. The heartbeat-time detector
// (detectAndPersistPartitionTransition, in
// store_worker_runtime_recovery.go) writes PARTITIONED_SUSPECTED,
// never PARTITIONED. The two writers target disjoint state values
// so the per-package single-writer invariant (only one writer of
// workers.connection_state at a time) holds even though the
// underlying column is shared.
//
// Returns a wrapped error on tx.ExecContext failure so the caller
// (reconcileOnePartition) can surface the failing worker ID through
// the deferred tx.Rollback.
func persistPartitionedStateTx(
	ctx context.Context,
	tx *sql.Tx,
	workerID, nowRFC3339 string,
) error {
	if _, err := tx.ExecContext(ctx,
		`UPDATE workers
		    SET connection_state=?, last_state_change_at=?, status=?
		  WHERE worker_id=?`,
		connectionStatePartitioned, nowRFC3339, connectionStatePartitioned, workerID,
	); err != nil {
		return fmt.Errorf("update %s: %w", workerID, err)
	}
	return nil
}
