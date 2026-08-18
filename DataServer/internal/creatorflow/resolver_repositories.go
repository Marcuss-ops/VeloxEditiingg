package creatorflow

import (
	"context"

	"velox-server/internal/forwardingcontract"
	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
)

// ForwardingRepository owns the SQL lifecycle of a creator_forwardings
// row from Resolve's perspective. Every method is invoked once per
// Resolve call (or zero times on the idempotent fast-path), and the
// trust contract is identical to the corresponding SQLiteStore
// methods: CAS semantics, ErrTransitionConflict on conflict.
type ForwardingRepository interface {
	// GetCreatorForwardingBySource locates the canonical row for a
	// (provider, source_job_id, target_executor_id) triple. Returns
	// (nil, nil) when no row exists yet, mirroring store's idiom.
	GetCreatorForwardingBySource(ctx context.Context, provider, sourceJobID, targetExecutorID string) (*forwardingcontract.CreatorForwarding, error)

	// InsertCreatorForwarding creates the initial PENDING row for the
	// handler sync path. The UNIQUE constraint on
	// (source_provider, source_job_id, target_executor_id) makes
	// concurrent calls converge. Returns the result of the insert (or
	// the existing row on idempotent duplicate) so callers can read the
	// persisted forwarding_id.
	InsertCreatorForwarding(ctx context.Context, cf *forwardingcontract.CreatorForwarding) (*forwardingcontract.InsertCreatorForwardingResult, error)

	// UpsertCreatorForwardingPayload stamps payload + source_status
	// onto an existing row (runner path). Preserves status.
	UpsertCreatorForwardingPayload(ctx context.Context, forwardingID, payloadJSON, payloadSHA256 string) error

	// MarkCreatorForwardingReadySync promotes PENDING/POLLING →
	// READY_TO_FORWARD without a lease CAS (handler sync path).
	MarkCreatorForwardingReadySync(ctx context.Context, forwardingID, payloadJSON, payloadSHA256 string) error

	// EnsureForwarded is the repair-path idempotency primitive. Idempotent
	// across FORWARDED / FORWARDING / READY_TO_FORWARD states: writes
	// (FORWARDING|FORWARDED ← FORWARDED, target_job_id=jobID) on any
	// non-terminal state. Returns nil on FORWARDED (already there).
	// Returns ErrTransitionConflict on terminal states (FAILED, BLOCKED).
	EnsureForwarded(ctx context.Context, forwardingID, jobID string) error

	// AtomicForwardAndEnqueue packs (READY_TO_FORWARD → FORWARDING →
	// INSERT job/task/task_spec → FORWARDING → FORWARDED) in one tx. When
	// runnerID/leaseID are present, both CAS boundaries are lease-fenced;
	// the synchronous handler passes both empty.
	AtomicForwardAndEnqueue(ctx context.Context, forwardingID string, job *jobs.Job, spec *taskgraph.TaskSpec, priority int, runnerID, leaseID string) error
}

// JobLookup is the idempotency pre-check surface. The canonical
// implementation is jobs.Writer (writers implement Get); tests
// pass a fake that returns nil on cache miss.
type JobLookup interface {
	Get(ctx context.Context, id string) (*jobs.Job, error)
}
