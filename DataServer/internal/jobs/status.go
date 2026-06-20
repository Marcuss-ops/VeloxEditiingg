// Package jobs defines the canonical job domain model.
//
// jobs.Status is the single source of truth for job state constants.
// Both store.JobStatus and queue.JobStatus are type aliases pointing
// here, so the entire codebase shares one set of statuses at compile time.
//
// State machine:
//
//	PENDING → LEASED → RUNNING → SUCCEEDED
//	                   ↓
//	              RETRY_WAIT → PENDING (retry)
//	                   ↓
//	              FAILED
//	PENDING → CANCELLED
package jobs

// Status is the canonical job state.
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusLeased    Status = "LEASED"
	StatusRunning   Status = "RUNNING"
	StatusRetryWait Status = "RETRY_WAIT"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
	StatusCancelled Status = "CANCELLED"
)

// IsTerminal reports whether a job in this state has finished its lifecycle.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	}
	return false
}
