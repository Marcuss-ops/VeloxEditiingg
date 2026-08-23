// Package jobs defines the canonical job domain model.
//
// JobStatus is the single source of truth for job state constants.
// store.JobStatus is a type alias for this type.
// The distinct type prevents accidental comparison with statuses from
// attempts, deliveries, publications, or input assembly, even though these
// values are persisted as strings at storage and wire boundaries.
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

// JobStatus is the canonical job lifecycle state. It is the ONLY type
// for job status in this domain — former aliases JobState and Status
// have been removed. It is deliberately distinct from task-attempt,
// delivery, publication, and input-assembly statuses even though each
// is persisted as a string at storage/wire edges.
type JobStatus string

const (
	StatusPending          JobStatus = "PENDING"
	StatusLeased           JobStatus = "LEASED"
	StatusRunning          JobStatus = "RUNNING"
	StatusAwaitingArtifact JobStatus = "AWAITING_ARTIFACT"
	// StatusDelivering means the artifact is READY and one or more explicit
	// required deliveries are still pending. It is intentionally non-terminal.
	StatusDelivering JobStatus = "DELIVERING"
	StatusRetryWait  JobStatus = "RETRY_WAIT"
	StatusSucceeded  JobStatus = "SUCCEEDED"
	StatusFailed     JobStatus = "FAILED"
	StatusCancelled  JobStatus = "CANCELLED"
)

// IsTerminal reports whether a job in this state has finished its lifecycle.
//
// AWAITING_ARTIFACT is intentionally NOT terminal: the artifact
// verification step can still fail (timeout, hash mismatch, missing
// upload session), in which case the lifecycle moves to FAILED.
// Treating AWAITING_ARTIFACT as terminal would cause supervisors and
// calendar APIs to mis-count pending Jobs.
// Valid reports whether s is a known persisted job status.
func (s JobStatus) Valid() bool {
	switch s {
	case StatusPending, StatusLeased, StatusRunning, StatusAwaitingArtifact, StatusDelivering, StatusRetryWait, StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	default:
		return false
	}
}

func (s JobStatus) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	}
	return false
}
