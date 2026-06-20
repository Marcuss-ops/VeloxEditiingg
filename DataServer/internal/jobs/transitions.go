package jobs

// CanTransition validates the canonical 7-state machine:
//
//	"" / PENDING → LEASED, RUNNING, RETRY_WAIT, FAILED, CANCELLED
//	LEASED       → RUNNING, FAILED, CANCELLED
//	RUNNING      → SUCCEEDED, FAILED, RETRY_WAIT, CANCELLED
//	RETRY_WAIT   → PENDING, FAILED, CANCELLED
//	SUCCEEDED    → (terminal)
//	FAILED       → (terminal)
//	CANCELLED    → (terminal)
//
// Returns true when the transition is legal; false otherwise.
// Idempotent transitions (from == to) are always legal.
func CanTransition(from, to Status) bool {
	if to == "" {
		return false
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
