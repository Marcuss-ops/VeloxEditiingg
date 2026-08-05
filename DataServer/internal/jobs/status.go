// Package jobs defines the canonical job domain model.
//
// jobs.Status is the single source of truth for job state constants.
// Both store.JobStatus and jobs.Status are the canonical types.
// here, so the entire codebase shares one set of statuses at compile time.
//
// State machine:
//
//	PENDING → LEASED → RUNNING → AWAITING_ARTIFACT → DELIVERING → SUCCEEDED
//	              ↓           ↓                 ↓
//	           CANCELLED  RETRY_WAIT       CANCELLED / artifact-timeout → FAILED
//	                            ↓
//	                         PENDING (retry)
//	                                ↓
//	                            FAILED
//
// AWAITING_ARTIFACT and DELIVERING are non-terminal gates between rendering
// and job success. AWAITING_ARTIFACT is used while the artifact is being
// verified; DELIVERING is used when explicit delivery attempts are pending.
// AWAITING_ARTIFACT was added between RUNNING and SUCCEEDED so the
// maybeTransitionJob roll-up can mark "all tasks succeeded" without
// writing the terminal SUCCEEDED itself. The actual SUCCEEDED flip is
// reserved for the verified-finalization path
// (`internal/artifacts/sqlite_finalize_writer.go`), audited by
// `internal/artifacts/scan_test.go`. This makes Job-level CAS writers
// deterministic (sole SUCCEEDED writer). AWAITING_ARTIFACT is NOT
// terminal — the artifact
// can still fail, transition to FAILED via artifact-timeout, or be
// CANCELLED.
package jobs

// JobState is the canonical job lifecycle state.
type JobState string

// Status is retained as a source-compatible alias. New code should use
// JobState to make the job aggregate boundary explicit.
//
// Deprecated: use JobState.
type Status = JobState

const (
	StatusPending          Status = "PENDING"
	StatusLeased           Status = "LEASED"
	StatusRunning          Status = "RUNNING"
	StatusAwaitingArtifact Status = "AWAITING_ARTIFACT"
	// StatusDelivering means the artifact is READY and one or more explicit
	// required deliveries are still pending. It is intentionally non-terminal.
	StatusDelivering Status = "DELIVERING"
	StatusRetryWait  Status = "RETRY_WAIT"
	StatusSucceeded  Status = "SUCCEEDED"
	StatusFailed     Status = "FAILED"
	StatusCancelled  Status = "CANCELLED"
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
