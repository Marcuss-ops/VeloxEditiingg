package taskgraph

// Status is the canonical task state.
type Status string

const (
	StatusPending   Status = "PENDING"
	StatusReady     Status = "READY"
	StatusLeased    Status = "LEASED"
	StatusRunning   Status = "RUNNING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
	StatusCancelled Status = "CANCELLED"

	// StatusTimedOut is set on a Task that the audit-mandated lease reaper
	// (PR-04 / fix/task-expiry-atomic-transition) transitions when its
	// lease TTL elapses without a worker report OR with an active attempt
	// whose worker crashed mid-flight. The corresponding TaskAttempt is
	// closed as AttemptStatusTimedOut in the same atomic tx; the reaper
	// owns the write so handleTaskResult's ingestion path NEVER
	// produces StatusTimedOut directly (only the reaper does).
	StatusTimedOut Status = "TIMED_OUT"
)

// IsTerminal reports whether a task in this state has finished its lifecycle.
//
// Audit §P0.4 / PR-04: StatusTimedOut is terminal so the Job roll-up
// (internal/taskingestion.TaskReportIngestionService.maybeTransitionJob)
// correctly observes "all tasks terminal" after a reaper pass. Treating
// StatusTimedOut as non-terminal would cause the roll-up to hang waiting
// for a worker report that the reaper already produced (the master is
// the canonical owner of the task at that point).
func (s Status) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled, StatusTimedOut:
		return true
	}
	return false
}

// AllStatuses returns every valid task status for use in queries and validation.
func AllStatuses() []Status {
	return []Status{
		StatusPending, StatusReady, StatusLeased, StatusRunning,
		StatusSucceeded, StatusFailed, StatusCancelled,
		StatusTimedOut,
	}
}
