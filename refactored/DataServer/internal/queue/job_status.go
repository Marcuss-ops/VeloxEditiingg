package queue

import "strings"

// normalizeJobStatus maps any status string (including legacy) to a canonical status.
// Legacy statuses are silently promoted to their canonical equivalent.
func normalizeJobStatus(status string) JobStatus {
	s := JobStatus(strings.ToUpper(strings.TrimSpace(status)))
	switch s {
	case StatusProcessing, StatusAssigned:
		return StatusRunning
	case StatusCompleted:
		return StatusSucceeded
	case StatusError, StatusLost:
		return StatusFailed
	case StatusQueued:
		return StatusPending
	case StatusCancelling:
		return StatusCancelled
	case StatusRetrying:
		return StatusRetryWait
	default:
		return s
	}
}

// isValidJobStatusTransition validates the canonical 7-state machine:
//
//	"" / PENDING → LEASED, RUNNING, RETRY_WAIT, FAILED, CANCELLED
//	LEASED       → RUNNING, FAILED, CANCELLED
//	RUNNING      → SUCCEEDED, FAILED, RETRY_WAIT, CANCELLED
//	RETRY_WAIT   → PENDING, FAILED, CANCELLED
//	SUCCEEDED    → (terminal)
//	FAILED       → (terminal)
//	CANCELLED    → (terminal)
func isValidJobStatusTransition(from, to JobStatus) bool {
	if to == "" {
		return true
	}
	if from == to {
		return true
	}

	// Normalize legacy statuses
	from = normalizeJobStatus(string(from))
	to = normalizeJobStatus(string(to))

	if from == to {
		return true
	}

	switch from {
	case "", StatusPending:
		switch to {
		case StatusLeased, StatusRunning, StatusRetryWait, StatusFailed, StatusCancelled:
			return true
		}
	case StatusLeased:
		switch to {
		case StatusRunning, StatusFailed, StatusCancelled:
			return true
		}
	case StatusRunning:
		switch to {
		case StatusSucceeded, StatusFailed, StatusRetryWait, StatusCancelled:
			return true
		}
	case StatusRetryWait:
		switch to {
		case StatusPending, StatusFailed, StatusCancelled:
			return true
		}
	case StatusSucceeded, StatusFailed, StatusCancelled:
		// Terminal states — no transitions out
		return false
	}

	return false
}
