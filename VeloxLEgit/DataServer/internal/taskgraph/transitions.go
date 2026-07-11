package taskgraph

// CanTransition validates the canonical task state machine:
//
//	"" / PENDING → READY, LEASED, RUNNING, FAILED, CANCELLED
//	READY        → LEASED, RUNNING, FAILED, CANCELLED
//	LEASED       → RUNNING, FAILED, CANCELLED
//	RUNNING      → SUCCEEDED, FAILED, CANCELLED
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
		case StatusReady, StatusLeased, StatusRunning, StatusPending, StatusFailed, StatusCancelled:
			return true
		}
	case StatusReady:
		switch to {
		case StatusLeased, StatusRunning, StatusFailed, StatusCancelled:
			return true
		}
	case StatusLeased:
		switch to {
		case StatusRunning, StatusFailed, StatusCancelled:
			return true
		}
	case StatusRunning:
		switch to {
		case StatusSucceeded, StatusFailed, StatusCancelled:
			return true
		}
	case StatusSucceeded, StatusFailed, StatusCancelled:
		return false
	}

	return false
}
