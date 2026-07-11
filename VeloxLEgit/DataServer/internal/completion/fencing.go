// Package completion / fencing.go
//
// sql-allowlist: completion FenceTuple central gate — read-only check on
// attempt_commits tightly coupled to DeclareOutputs/RecordUploadProgress;
// future refactor candidate for moving the (task_id, attempt_id) lookup
// into the UoW AttemptCommits repo.
//
// Artifact Commit Protocol (Fase 2.2 of docs/completion-protocol.md):
// the FenceTuple is the canonical doubling identity for every CAS step
// in the commit pipeline. Tasks / attempts can be retried across
// leases and workers (a stale worker on a reaped lease must NOT be
// able to mark a task SUCCEEDED), so every transition through
// Coordinator MUST gate on this tuple and reject any input that does
// not match the canonical (worker_id, lease_id, task_revision) in
// attempt_commits.
//
// The tuple is also the wire-side JOIN key used by ReconcileAttempt:
// repairing a commit row in DECLARED|UPLOADING|RECEIVED requires
// first verifying that the supplied fence MATCHES the row, before the
// supervisor transitions it to a terminal status.
//
// Phase 2.2 centralises the gate behind FenceTuple.Read and
// FenceTuple.ReadOrMissing. Every transition method (DeclareOutputs,
// RecordUploadProgress, CompleteUpload, CommitAttempt,
// ReconcileAttempt) calls one of these at entry; the method body then
// reuses the returned state for any subsequent CAS predicate. The
// pure-data helpers (SQLWhere/SQLArgs) are retained for inline CAS
// clauses and now include task_revision in the WHERE so that the
// inline read is also revision-strict by construction.
package completion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// FenceTuple is the canonical doubling identity used at every CAS
// step through the Coordinator.
//
// Field inventory:
//
//   - TaskID    : the canonical task id (UUID, set at Job creation
//     and stable for the Task's whole lifetime).
//   - AttemptID : the canonical attempt id (UUID, minted at
//     ClaimNextWithAttemptAtomic and stamped on the task
//     row at the same instant).
//   - WorkerID  : the worker advertising the report. Stable only for
//     the lifetime of the worker's claim.
//   - LeaseID   : the per-claim UUID that the master minted at Offer
//     time. A new Attempt bumps LeaseID; the same
//     LeaseID contractually bounds the InvariantReport.
//   - Revision  : the master-side task_revision stamped on
//     attempt_commits at DeclareOutputs time. Re-read
//     and re-compared by the central gate function on
//     every subsequent transition.
type FenceTuple struct {
	TaskID    string
	AttemptID string
	WorkerID  string
	LeaseID   string
	Revision  int
}

// Validate returns nil iff the tuple is well-formed (all identity
// strings non-empty, revision non-negative). One Validate per input
// surface (declare / progress / complete) is sufficient — the helpers
// below assume the caller has already validated.
func (f FenceTuple) Validate() error {
	if f.TaskID == "" {
		return fmt.Errorf("completion.FenceTuple: TaskID empty: %+v", f)
	}
	if f.AttemptID == "" {
		return fmt.Errorf("completion.FenceTuple: AttemptID empty: %+v", f)
	}
	if f.WorkerID == "" {
		return fmt.Errorf("completion.FenceTuple: WorkerID empty: %+v", f)
	}
	if f.LeaseID == "" {
		return fmt.Errorf("completion.FenceTuple: LeaseID empty: %+v", f)
	}
	if f.Revision < 0 {
		return fmt.Errorf("completion.FenceTuple: Revision < 0: %+v", f)
	}
	return nil
}

// SQLWhere returns the canonical "AND/OR-able" predicate with `?`
// placeholders in the same order as SQLArgs(). Callers MUST use
// both helpers together to keep the parameter order in sync.
//
// Predicates:
//
//	task_id = ? AND attempt_id = ? AND worker_id = ? AND lease_id = ? AND task_revision = ?
//
// task_revision is included so that an inline CAS whose WHERE clause
// uses SQLWhere is automatically revision-strict. This is a
// belt-and-braces complement to the central FenceTuple.Read gate: if
// a future maintainer forgets to call Read at the top of a new
// transition, the inline CAS predicate still rejects a stale
// revision replay.
func (f FenceTuple) SQLWhere() string {
	return "task_id = ? AND attempt_id = ? AND worker_id = ? AND lease_id = ? AND task_revision = ?"
}

// SQLArgs returns the placeholder values for SQLWhere(), ordered
// (task_id, attempt_id, worker_id, lease_id, task_revision).
//
// Callers MUST keep this in lock-step with SQLWhere() — both helpers
// come as a pair, and any change to one's order MUST be reflected in
// the other's signature.
func (f FenceTuple) SQLArgs() []any {
	return []any{f.TaskID, f.AttemptID, f.WorkerID, f.LeaseID, f.Revision}
}

// Equal reports whether two FenceTuples are byte-for-byte identical.
// Used by tests and reconciler sanity checks; not a database-fidelity
// primitive (use Read for CAS predicates).
func (f FenceTuple) Equal(g FenceTuple) bool {
	return f == g
}

// AttemptCommitState is the canonical attempt_commits row snapshot
// returned by the Read gate. Coordinator methods receive the same
// shape they will use for downstream CAS predicates (commit_id
// identifies the row; status + task_revision round-trip for tests
// and for any CAS that needs to gate on terminal status).
type AttemptCommitState struct {
	CommitID     string
	Status       string
	TaskRevision int
}

// Read is the central fence gate for every transition method
// EXCEPT DeclareOutputs. It looks up the attempt_commits row
// matching (task_id, attempt_id) and validates the
// (worker_id, lease_id, task_revision) tuple against the canonical
// row.
//
// Returns:
//   - (state, nil) on a successful match — state carries the
//     canonical commit_id, status, and stored task_revision. The
//     caller MUST use state.CommitID for any subsequent CAS
//     predicate; the fence is now fully validated.
//   - (nil, ErrAttemptCommitNotFound) when no attempt_commits row
//     exists for (task_id, attempt_id).
//   - (nil, ErrTransitionConflict) when the row exists but its
//     (worker_id, lease_id, task_revision) does not match the
//     supplied fence — a stale-worker / reaped-lease /
//     bumped-revision replay. The error wraps the sentinel so
//     callers use errors.Is(err, ErrTransitionConflict).
//
// Every Coordinator transition method that takes a FenceTuple MUST
// invoke Read at entry. DeclareOutputs uses ReadOrMissing instead
// (the no-row path is the legitimate "first declare" entry).
func (f FenceTuple) Read(ctx context.Context, tx *sql.Tx) (*AttemptCommitState, error) {
	if verr := f.Validate(); verr != nil {
		return nil, fmt.Errorf("%w: %v", ErrFenceMismatch, verr)
	}
	var (
		commitID     string
		status       string
		storedWorker string
		storedLease  string
		storedRev    int
	)
	err := tx.QueryRowContext(ctx, `
		SELECT commit_id, status, worker_id, lease_id, task_revision
		  FROM attempt_commits
		 WHERE task_id = ? AND attempt_id = ?`,
		f.TaskID, f.AttemptID,
	).Scan(&commitID, &status, &storedWorker, &storedLease, &storedRev)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: no attempt_commits row for (task_id=%s attempt_id=%s)",
				ErrAttemptCommitNotFound, f.TaskID, f.AttemptID)
		}
		return nil, fmt.Errorf("completion.FenceTuple.Read: %w", err)
	}
	if storedWorker != f.WorkerID || storedLease != f.LeaseID || storedRev != f.Revision {
		return nil, fmt.Errorf("%w: fence mismatch (stored worker_id=%q lease_id=%q revision=%d status=%q; supplied worker_id=%q lease_id=%q revision=%d)",
			ErrTransitionConflict, storedWorker, storedLease, storedRev, status, f.WorkerID, f.LeaseID, f.Revision)
	}
	return &AttemptCommitState{CommitID: commitID, Status: status, TaskRevision: storedRev}, nil
}

// ReadOrMissing is the DeclareOutputs variant of Read. It returns
// (nil, nil) when no attempt_commits row exists yet, allowing
// DeclareOutputs to perform the canonical INSERT-OR-IGNORE on
// (task_id, attempt_id). When a row exists, the
// (worker_id, lease_id, task_revision) tuple is validated exactly
// as in Read and ErrTransitionConflict is surfaced on mismatch.
//
// All OTHER transition methods MUST use Read (not ReadOrMissing):
// a missing row is a hard error for RecordUploadProgress,
// CompleteUpload, CommitAttempt, ReconcileAttempt.
func (f FenceTuple) ReadOrMissing(ctx context.Context, tx *sql.Tx) (*AttemptCommitState, error) {
	if verr := f.Validate(); verr != nil {
		return nil, fmt.Errorf("%w: %v", ErrFenceMismatch, verr)
	}
	var (
		commitID     string
		status       string
		storedWorker string
		storedLease  string
		storedRev    int
	)
	err := tx.QueryRowContext(ctx, `
		SELECT commit_id, status, worker_id, lease_id, task_revision
		  FROM attempt_commits
		 WHERE task_id = ? AND attempt_id = ?`,
		f.TaskID, f.AttemptID,
	).Scan(&commitID, &status, &storedWorker, &storedLease, &storedRev)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("completion.FenceTuple.ReadOrMissing: %w", err)
	}
	if storedWorker != f.WorkerID || storedLease != f.LeaseID || storedRev != f.Revision {
		return nil, fmt.Errorf("%w: fence mismatch (stored worker_id=%q lease_id=%q revision=%d status=%q; supplied worker_id=%q lease_id=%q revision=%d)",
			ErrTransitionConflict, storedWorker, storedLease, storedRev, status, f.WorkerID, f.LeaseID, f.Revision)
	}
	return &AttemptCommitState{CommitID: commitID, Status: status, TaskRevision: storedRev}, nil
}
