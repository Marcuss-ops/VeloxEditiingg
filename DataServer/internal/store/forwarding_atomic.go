// Package store provides the SQLite persistence layer for forwarding state.
package store

import (
	"context"
	"time"

	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
)

// Atomic forwarding operations spanning creator_forwardings and job/task creation.

// ── Atomic Enqueue + Forward ───────────────────────────────────────────

// AtomicForwardAndEnqueue combines the Job+Task+TaskSpec creation AND the
// forwarding status update into a single SQLite transaction. The SQL now
// lives in the forwardingstore leaf (forwardingstore_atomic.go); the
// cross-domain Job+Task creator is injected into the leaf at construction
// (see NewSQLiteStoreFromHandle). This method keeps the historical store
// name as a thin facade for existing callers.
func (s *SQLiteStore) AtomicForwardAndEnqueue(
	ctx context.Context,
	forwardingID string,
	job *jobs.Job,
	taskSpec *taskgraph.TaskSpec,
	priority int,
	runnerID string,
	leaseID string,
) error {
	return s.forwarding.AtomicForwardAndEnqueue(ctx, forwardingID, job, taskSpec, priority, runnerID, leaseID)
}

// MarkCreatorForwardingReadySync transitions a PENDING/POLLING forwarding to
// READY_TO_FORWARD WITHOUT a (locked_by, lease_id) CAS (the synchronous
// handler path). SQL lives in the forwardingstore leaf.
func (s *SQLiteStore) MarkCreatorForwardingReadySync(ctx context.Context, forwardingID, payloadJSON, payloadSHA256 string) error {
	return s.forwarding.MarkCreatorForwardingReadySync(ctx, forwardingID, payloadJSON, payloadSHA256)
}

// MarkCreatorForwardingEnqueueRetry moves a forwarding that failed to enqueue
// (FORWARDING or READY_TO_FORWARD) to RETRY_WAIT with a backoff delay.
func (s *SQLiteStore) MarkCreatorForwardingEnqueueRetry(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg string, nextAttemptAt time.Time) error {
	return s.forwarding.MarkCreatorForwardingEnqueueRetry(ctx, forwardingID, runnerID, leaseID, errorCode, errorMsg, nextAttemptAt)
}
