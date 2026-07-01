// Package completion / fencing.go
//
// Artifact Commit Protocol (Fase 2.2 of docs/completion-protocol.md):
// the FenceTuple is the canonical doubling identity for every CAS step
// in the commit pipeline. Tasks / attempts can be retried across
// leases and workers (a stale worker on a reaped lease must NOT be
// able to mark a task SUCCEEDED), so every transition through
// Coordinator MUST gate on this tuple and reject any input that does
// not match the canonical (worker_id, lease_id, revision) in
// attempt_commits.
//
// The tuple is also the wire-side JOIN key used by ReconcileAttempt:
// repairing a commit row in DECLARED|UPLOADING|RECEIVED requires
// first verifying that the supplied fence MATCHES the row, before the
// supervisor transitions it to a terminal status.
//
// Pure-data file: no SQL, no I/O. The SQLWhere/SQLArgs helpers
// produce strings/values that callers feed into TX-bound Exec / Query
// calls (see coordinator.go).
package completion

import "fmt"

// FenceTuple is the canonical doubling identity used at every CAS
// step through the Coordinator.
//
// Field inventory:
//
//   - TaskID    : the canonical task id (UUID, set at Job creation
//                 and stable for the Task's whole lifetime).
//   - AttemptID : the canonical attempt id (UUID, minted at
//                 ClaimNextWithAttemptAtomic and stamped on the task
//                 row at the same instant — see
//                 internal/store/sqlite_task_repository.go).
//   - WorkerID  : the worker advertising the report. Stable only for
//                 the lifetime of the worker's claim.
//   - LeaseID   : the per-claim UUID that the master minted at Offer
//                 time. A new Attempt bumps LeaseID; the same LeaseID
//                 contractually bounds InvariantReport.
//
// NOTE: revision is in the tuple even though the current 061-065
// schema does NOT store revision on attempt_commits. Phase 2 will
// CHECK task_revisions on the join (read tasks.revision at Declare
// time, store the value on the attempt_commits row separately or
// re-derive from tasks on every CAS). For this phase's SQL the
// revision is gated but not persisted — a future migration 066 will
// add attempt_commits.task_revision stamped at Declare time.
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
//
// Empty-string detection explicitly does NOT allow whitespace-only
// strings; callers MUST TrimSpace before constructing the tuple.
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
//   task_id = ? AND attempt_id = ? AND worker_id = ? AND lease_id = ?
//
// Revision is a separate gate because the FenceTuple.Validate
// documentation notes the schema does not yet persist revision on
// attempt_commits (Fase 2 will add the column and re-derive it here).
// Callers that need revision-gated CAS today should re-read tasks
// .revision and append "... AND ? <= (SELECT revision FROM tasks
// WHERE task_id = ?)" or similar.
func (f FenceTuple) SQLWhere() string {
	return "task_id = ? AND attempt_id = ? AND worker_id = ? AND lease_id = ?"
}

// SQLArgs returns the placeholder values for SQLWhere(), ordered
// (task_id, attempt_id, worker_id, lease_id).
//
// Callers MUST keep this in lock-step with SQLWhere() — both helpers
// come as a pair, and any change to one's order MUST be reflected in
// the other's signature.
func (f FenceTuple) SQLArgs() []any {
	return []any{f.TaskID, f.AttemptID, f.WorkerID, f.LeaseID}
}

// Equal reports whether two FenceTuples are byte-for-byte identical.
// Used by tests and reconciler sanity checks; not a database-fidelity
// primitive (use SQLWhere/SQLArgs for CAS predicates).
func (f FenceTuple) Equal(g FenceTuple) bool {
	return f == g
}
