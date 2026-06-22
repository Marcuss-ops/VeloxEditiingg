// Package jobs defines the canonical job domain model.
//
// jobs.Status is the single source of truth for job state constants.
// Both store.JobStatus and jobs.Status are the canonical types.
// here, so the entire codebase shares one set of statuses at compile time.
//
// State machine:
//
//	PENDING → LEASED → RUNNING → AWAITING_ARTIFACT → SUCCEEDED
//	              ↓           ↓                 ↓
//	           CANCELLED  RETRY_WAIT       CANCELLED / artifact-timeout → FAILED
//	                            ↓
//	                         PENDING (retry)
//	                                ↓
//	                            FAILED
//
// PR-02: AWAITING_ARTIFACT was added between RUNNING and SUCCEEDED so
// that handleTaskResult's maybeTransitionJob can mark "all tasks
// succeeded" without writing the terminal SUCCEEDED itself. The actual
// SUCCEEDED flip is reserved for the verified-finalization path
// (`internal/artifacts/sqlite_finalization_repository.go`), which is
// audited by `internal/artifacts/scan_test.go`. This makes Job-level
// CAS writers deterministic and eliminates §P0.2 (two competing
// SUCCEEDED writers). AWAITING_ARTIFACT is NOT terminal — the artifact
// can still fail, transition to FAILED via artifact-timeout, or be
// CANCELLED.
package jobs

// Status is the canonical job state.
type Status string

const (
	StatusPending           Status = "PENDING"
	StatusLeased            Status = "LEASED"
	StatusRunning           Status = "RUNNING"
	StatusAwaitingArtifact  Status = "AWAITING_ARTIFACT"
	StatusRetryWait         Status = "RETRY_WAIT"
	StatusSucceeded         Status = "SUCCEEDED"
	StatusFailed            Status = "FAILED"
	StatusCancelled         Status = "CANCELLED"
)

// IsTerminal reports whether a job in this state has finished its lifecycle.
//
// AWAITING_ARTIFACT is intentionally NOT terminal: the artifact
// verification step can still fail (timeout, hash mismatch, missing
// upload session), in which case the lifecycle moves to FAILED.
// Treating AWAITING_ARTIFACT as terminal would cause supervisors and
// calendar APIs to mis-count pending Jobs.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	}
	return false
}
