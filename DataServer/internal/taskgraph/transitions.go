package taskgraph

import "velox-server/internal/statemachine"

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
	return statemachine.DefaultRegistry().CanTransition(statemachine.DomainTask, string(from), string(to))
}
