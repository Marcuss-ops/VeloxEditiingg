package jobs

// CanTransition validates the canonical 8-state machine:
//
//	"" / PENDING → LEASED, RUNNING, RETRY_WAIT, FAILED, CANCELLED
//	LEASED               → RUNNING, FAILED, CANCELLED
//	RUNNING              → AWAITING_ARTIFACT, SUCCEEDED (legacy direct),
//	                        FAILED, RETRY_WAIT, CANCELLED
//	AWAITING_ARTIFACT    → SUCCEEDED (verified-finalization path only,
//	                        via artifacts.FinalizeVerified CAS),
//	                        FAILED (artifact timeout / hash mismatch),
//	                        CANCELLED (worker drained / admin)
//	RETRY_WAIT           → PENDING, FAILED, CANCELLED
//	SUCCEEDED            → (terminal)
//	FAILED               → (terminal)
//	CANCELLED            → (terminal)
//
// PR-02: AWAITING_ARTIFACT was inserted between RUNNING and SUCCEEDED.
// The handler.maybeTransitionJob write of SUCCEEDED was replaced with a
// write of AWAITING_ARTIFACT so the verified-finalization path becomes
// the sole terminalizer. PR-01's FinalizeVerified CAS now reads
// status IN ('RUNNING', 'AWAITING_ARTIFACT') so both legacy direct
// SUCCEEDED (workers without artifact contract) and the new
// artifact-gated SUCCEEDED coexist.
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
		case StatusAwaitingArtifact, StatusSucceeded, StatusFailed, StatusRetryWait, StatusCancelled:
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
