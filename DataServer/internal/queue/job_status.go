package queue

import "velox-server/internal/jobs"

// isValidJobStatusTransition validates the canonical 7-state machine.
// Delegates to jobs.CanTransition — the single source of truth for the
// job state machine. Since JobStatus = jobs.Status is a type alias,
// the parameters are directly compatible without explicit conversion.
func isValidJobStatusTransition(from, to JobStatus) bool {
	return jobs.CanTransition(from, to)
}
