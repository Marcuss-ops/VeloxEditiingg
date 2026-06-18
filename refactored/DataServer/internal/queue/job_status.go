package queue

// isValidJobStatusTransition validates the canonical 9-state machine:
//
//	"" / PENDING → LEASED, RUNNING, RETRY_WAIT, FAILED, CANCELLED
//	LEASED       → RUNNING, FAILED, CANCELLED
//	RUNNING      → RENDER_FINISHED, AWAITING_ARTIFACT, FAILED, RETRY_WAIT, CANCELLED
//	RENDER_FINISHED → AWAITING_ARTIFACT, SUCCEEDED, FAILED, CANCELLED
//	AWAITING_ARTIFACT → SUCCEEDED, FAILED, CANCELLED
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
		case StatusRenderFinished, StatusAwaitingArtifact, StatusFailed, StatusRetryWait, StatusCancelled:
			return true
		}
	case StatusRenderFinished:
		switch to {
		case StatusAwaitingArtifact, StatusSucceeded, StatusFailed, StatusCancelled:
			return true
		}
	case StatusAwaitingArtifact:
		switch to {
		case StatusSucceeded, StatusFailed, StatusCancelled:
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
