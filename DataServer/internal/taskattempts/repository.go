package taskattempts

import (
	"context"
	"errors"
)

// ErrIdentityMismatch is the canonical sentinel returned when a TaskResult's
// wire identity tuple (task_id, job_id, attempt_id, attempt_number,
// worker_id, lease_id) does not match the authoritative stored values.
//
// PR-2 fix/canonical-attempt-identity. Callers must drop the offending
// message without transitioning state — the request may be a legitimate
// retry from a stale worker OR an impersonation attempt, and we cannot
// distinguish without operator review.
var ErrIdentityMismatch = errors.New("taskattempts: identity mismatch")

// Reader is the read-only attempt query surface.
type Reader interface {
	// Get returns a single attempt by ID, or (nil, nil) on missing.
	Get(ctx context.Context, id string) (*TaskAttempt, error)

	// GetByTaskIDAndWorkerAndLease returns the active attempt for the
	// (task_id, worker_id, lease_id) tuple. Used by the master's
	// handleTaskResult identity validation as the wire-fallback path when
	// a worker has not yet adopted the canonical attempt_id (PR-2 rollout).
	// Returns (nil, nil) when no attempt exists.
	GetByTaskIDAndWorkerAndLease(ctx context.Context, taskID, workerID, leaseID string) (*TaskAttempt, error)

	// ListByTaskID returns all attempts for a task, ordered by attempt number.
	ListByTaskID(ctx context.Context, taskID string) ([]TaskAttempt, error)

	// GetActiveAttempt returns the current non-terminal attempt for a task, or (nil, nil).
	GetActiveAttempt(ctx context.Context, taskID string) (*TaskAttempt, error)
}

// Writer is the canonical write-only attempt mutation surface.
type Writer interface {
	// Create inserts a new attempt in PENDING state.
	Create(ctx context.Context, attempt *TaskAttempt) error

	// SetStatus performs a CAS status change from → to.
	SetStatus(ctx context.Context, id string, from, to AttemptStatus, revision int) error

	// CompleteFinal marks an attempt as terminal (SUCCEEDED or FAILED) with
	// the worker-identity CAS tuple. Idempotent on already-terminal attempts.
	CompleteFinal(ctx context.Context, id, workerID, leaseID string, status AttemptStatus, errorCode, errorMessage string, revision int) error

	// Delete hard-deletes an attempt. Returns no error if already gone.
	Delete(ctx context.Context, id string) error

	// PersistMetrics inserts or replaces the typed AttemptMetrics row for an
	// attempt (Scorecard v1 / migration 054). Idempotent on INSERT OR REPLACE
	// keyed by attempt_id. Called from the master ingestion path after the
	// atomic close-write so the typed execution metrics and the terminal
	// status transition commit under the per-task mutex.
	PersistMetrics(ctx context.Context, metrics AttemptMetrics) error

	// PersistCacheStats hoists the (typed, attempted-derivable) cache snapshot
	// for an attempt into task_attempt_cache_stats. Idempotent INSERT OR
	// REPLACE. Fields the worker hasn't yet surfaced (Hits, Misses,
	// Evictions, Corruptions, Entries) are stored as 0 — a deliberate,
	// documented approximation; BytesUsed is derived from exec metrics.
	PersistCacheStats(ctx context.Context, stats AttemptCacheStats) error

	// PersistCostBasis inserts or replaces the cost envelope so
	// cost_per_output_minute is a 1-lookup read downstream.
	PersistCostBasis(ctx context.Context, basis AttemptCostBasis) error

	// PersistPhaseTimingsDetailed inserts or replaces detailed
	// per-phase timing rows (component, action, phase_order, bytes,
	// frames, metadata_json). Idempotent on (attempt_id, component,
	// action). Replaces the simpler PersistPhaseTimings contract
	// when the worker surfaces the richer Scorecard v2 shape.
	PersistPhaseTimingsDetailed(ctx context.Context, attemptID string, timings []PhaseTimingDetailed) error

	// PersistSegmentTimings inserts or replaces per-segment timing
	// records from the C++ engine sidecar segments[] array.
	// Idempotent on (attempt_id, segment_index). Callers should
	// delete-and-reinsert the full slate for the attempt each time
	// the worker reports, so the table stays in sync with the
	// authoritative sidecar.
	PersistSegmentTimings(ctx context.Context, attemptID string, segments []SegmentTiming) error
}

// MetricsReader is the read-side of the typed metrics envelope so the
// Prometheus exporter and the scorecard reporter can roll up attempt
// rows on a periodic supervisor loop (see F8 follow-up). Implied by
// Repository today; declared explicitly so callers and stubs can wire
// just the metrics endpoints when the write surface is not needed.
type MetricsReader interface {
	GetMetrics(ctx context.Context, attemptID string) (*AttemptMetrics, error)
	GetCacheStats(ctx context.Context, attemptID string) (*AttemptCacheStats, error)
	GetCostBasis(ctx context.Context, attemptID string) (*AttemptCostBasis, error)
}

// Repository combines Reader and Writer into a single attempt persistence contract.
type Repository interface {
	Reader
	Writer
}
