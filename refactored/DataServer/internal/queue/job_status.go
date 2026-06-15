package queue

import "strings"

func normalizeJobStatus(status string) JobStatus {
	return JobStatus(strings.ToUpper(strings.TrimSpace(status)))
}

func isValidJobStatusTransition(from, to JobStatus) bool {
	if to == "" {
		return true
	}
	if from == to {
		return true
	}

	switch from {
	case "", StatusPending:
		switch to {
		case StatusPending, StatusQueued, StatusProcessing, StatusAssigned, StatusLeased, StatusFailed, StatusError, StatusCancelled, StatusLost, StatusRetrying:
			return true
		}
	case StatusQueued:
		switch to {
		case StatusQueued, StatusPending, StatusProcessing, StatusAssigned, StatusLeased, StatusFailed, StatusError, StatusCancelled, StatusLost, StatusRetrying:
			return true
		}
	case StatusProcessing, StatusAssigned, StatusLeased, StatusRetrying:
		switch to {
		case StatusProcessing, StatusCompleted, StatusFailed, StatusError, StatusCancelling, StatusCancelled, StatusLost, StatusRetrying:
			return true
		}
	case StatusCancelling:
		switch to {
		case StatusCancelling, StatusCancelled, StatusLost:
			return true
		}
	case StatusCompleted, StatusError, StatusFailed, StatusCancelled, StatusLost:
		return false
	default:
		switch to {
		case StatusCompleted, StatusFailed, StatusError, StatusCancelled, StatusLost:
			return true
		}
	}

	return false
}
