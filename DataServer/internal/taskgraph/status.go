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
)

// IsTerminal reports whether a task in this state has finished its lifecycle.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCancelled:
		return true
	}
	return false
}

// AllStatuses returns every valid task status for use in queries and validation.
func AllStatuses() []Status {
	return []Status{
		StatusPending, StatusReady, StatusLeased, StatusRunning,
		StatusSucceeded, StatusFailed, StatusCancelled,
	}
}
