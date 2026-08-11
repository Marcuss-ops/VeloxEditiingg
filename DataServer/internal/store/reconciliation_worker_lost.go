package store

// reconciliation_worker_lost.go — canonical WORKER_LOST reconciler
// (Phase A3).
//
// A worker whose heartbeat stream has stopped entirely can never fire
// the heartbeat-path detector (detectAndPersistPartitionTransition);
// the recovery-side entry point ReconcileWorkerPartitions exists for
// exactly that case and is documented as "intended to be invoked on a
// wall-clock cadence from the MASTER scheduler". This reconciler is
// that invocation: it implements reconcile.Reconciler over the
// existing canonical store method so the master supervisor runs it on
// the same reconciliation tick as the other registry entries.
//
// Semantics (inherited from ReconcileWorkerPartitions):
//
//   - a worker whose last_heartbeat_at is older than partitionSeconds
//     (default 300s, VELOX_WORKER_PARTITION_THRESHOLD_SECONDS) and
//     whose connection_state is not already PARTITIONED is
//     transitioned to PARTITIONED inside its own per-worker
//     transaction, with the WORKER_PARTITION_DETECTED audit row and
//     last_state_change_at bump;
//   - staleSeconds > partitionSeconds is a configuration error that is
//     surfaced immediately (no silent always-partitioned pass);
//   - already-PARTITIONED workers are never re-touched (idempotent).
//
// Terminal jobs are not in scope for this pass (it operates on the
// workers table); the job-level terminal-safety rule is enforced by
// the AWAITING_ARTIFACT / DELIVERY_PENDING reconcilers' CAS guards.

import (
	"context"
	"fmt"
	"time"
)

// defaultWorkerStaleSeconds / defaultWorkerPartitionSeconds mirror the
// canonical read-side thresholds in workers/registry_query.go and the
// config defaults (VELOX_WORKER_STALE_THRESHOLD_SECONDS=150,
// VELOX_WORKER_PARTITION_THRESHOLD_SECONDS=300).
const (
	defaultWorkerStaleSeconds     = 150
	defaultWorkerPartitionSeconds = 300
)

// WorkerLostReconciler implements reconcile.Reconciler for the worker
// partition sweep.
type WorkerLostReconciler struct {
	store            *SQLiteStore
	staleSeconds     int
	partitionSeconds int
}

// NewWorkerLostReconciler wires the reconciler. The threshold pair is
// required: passing <=0 for either falls back to the canonical defaults
// (matching ReconcileWorkerPartitions' validation contract). A nil
// store is a MISCONFIGURED capability and is rejected here — a registry
// entry that silently succeeds without a store would be a hidden noop
// (AGENTS.md §6).
func NewWorkerLostReconciler(s *SQLiteStore, staleSeconds, partitionSeconds int) (*WorkerLostReconciler, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("worker lost reconciler: store is not initialized")
	}
	if staleSeconds <= 0 {
		staleSeconds = defaultWorkerStaleSeconds
	}
	if partitionSeconds <= 0 {
		partitionSeconds = defaultWorkerPartitionSeconds
	}
	return &WorkerLostReconciler{store: s, staleSeconds: staleSeconds, partitionSeconds: partitionSeconds}, nil
}

// SetThresholds overrides the STALE/PARTITIONED pair. A non-positive
// value keeps the current value.
func (r *WorkerLostReconciler) SetThresholds(staleSeconds, partitionSeconds int) {
	if staleSeconds > 0 {
		r.staleSeconds = staleSeconds
	}
	if partitionSeconds > 0 {
		r.partitionSeconds = partitionSeconds
	}
}

// Reconcile implements reconcile.Reconciler by delegating to the
// canonical recovery entry point. `now` is not forwarded on purpose:
// ReconcileWorkerPartitions reads its own clock; the framework's `now`
// stays authoritative for the job-level reconcilers whose conditions
// are driven by persisted timestamps.
func (r *WorkerLostReconciler) Reconcile(ctx context.Context, _ time.Time) error {
	if r == nil || r.store == nil || r.store.db == nil {
		return fmt.Errorf("worker lost reconciler: store is not initialized")
	}
	_, err := r.store.ReconcileWorkerPartitions(ctx, r.staleSeconds, r.partitionSeconds)
	return err
}
