// Package placement / reasons.go
//
// Typed rejection codes used by the Matcher, metrics, and diagnostics.
// Every placement decision that does NOT produce a candidate MUST emit
// rejection records so observability can distinguish "no work available"
// from "work exists but is incompatible with this worker".
package placement

// RejectionCode is a stable, low-cardinality string suitable for log
// fields and Prometheus label values.
type RejectionCode string

const (
	// RejectWorkerNotReady — the worker has not yet signalled full readiness.
	RejectWorkerNotReady RejectionCode = "worker_not_ready"

	// RejectSessionInactive — the session is not alive (disconnected / expired).
	RejectSessionInactive RejectionCode = "session_inactive"

	// RejectWorkerDraining — the worker is draining and should not receive new work.
	RejectWorkerDraining RejectionCode = "worker_draining"

	// RejectCapacityFull — the worker has no free task slots (max_parallel_jobs reached).
	RejectCapacityFull RejectionCode = "capacity_full"

	// RejectUnsupportedExecutor — the worker does not advertise the
	// (executor_id, executor_version) pair required by the task.
	RejectUnsupportedExecutor RejectionCode = "unsupported_executor"

	// RejectMissingCapability — the worker is missing a required capability
	// string (e.g. "artifact.commit.v1").
	RejectMissingCapability RejectionCode = "missing_capability"

	// RejectInvalidTaskRequirement — the task's executor key is invalid
	// (empty ID or zero version).
	RejectInvalidTaskRequirement RejectionCode = "invalid_task_requirement"
)

// Rejection describes why a single candidate was skipped.
type Rejection struct {
	TaskID string
	Code   RejectionCode
	Detail string
}
