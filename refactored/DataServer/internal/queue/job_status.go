package queue//	isValidJobStatusTransition validates the canonical 7-state machine:
//
//	"" / PENDING → LEASED, RUNNING, RETRY_WAIT, FAILED, CANCELLED
//	LEASED       → RUNNING, FAILED, CANCELLED
//	RUNNING      → SUCCEEDED, FAILED, RETRY_WAIT, CANCELLED
//	RETRY_WAIT   → PENDING, FAILED, CANCELLED
//	SUCCEEDED    → (terminal)
//	FAILED       → (terminal)
//	CANCELLED    → (terminal)
//
// The job stays in RUNNING while the worker renders. Render completion is
// recorded as a timestamp (render_finished_at) without changing job status.
// Attempt and artifact tables track intermediate states. The job transitions
// to SUCCEEDED only after artifact verification (ArtifactFinalizationService +
// CompleteJobTx — see grpcserver.handleArtifactUploaded).
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
