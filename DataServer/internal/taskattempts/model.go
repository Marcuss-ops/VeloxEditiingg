// Package taskattempts defines the canonical TaskAttempt domain model.
// A TaskAttempt represents a single execution attempt of a Task by a worker.
package taskattempts

import "time"

// AttemptStatus is the canonical attempt state.
type AttemptStatus string

const (
	AttemptStatusPending   AttemptStatus = "PENDING"
	AttemptStatusRunning   AttemptStatus = "RUNNING"
	AttemptStatusSucceeded AttemptStatus = "SUCCEEDED"
	AttemptStatusFailed    AttemptStatus = "FAILED"
	AttemptStatusCancelled AttemptStatus = "CANCELLED"
)

// IsTerminal reports whether an attempt in this state has finished.
func (s AttemptStatus) IsTerminal() bool {
	switch s {
	case AttemptStatusSucceeded, AttemptStatusFailed, AttemptStatusCancelled:
		return true
	}
	return false
}

// TaskAttempt is the canonical domain model for a single execution attempt.
//
// Uniqueness: (task_id, attempt_number) is unique.
// At most one active (non-terminal) attempt exists per task at any time.
type TaskAttempt struct {
	ID            string        `json:"id"`
	TaskID        string        `json:"task_id"`
	JobID         string        `json:"job_id"`
	AttemptNumber int           `json:"attempt_number"`
	WorkerID      string        `json:"worker_id"`
	LeaseID       string        `json:"lease_id"`
	Status        AttemptStatus `json:"status"`
	StartedAt     *time.Time    `json:"started_at,omitempty"`
	CompletedAt   *time.Time    `json:"completed_at,omitempty"`
	ErrorCode     string        `json:"error_code,omitempty"`
	ErrorMessage  string        `json:"error_message,omitempty"`
	ReportVersion int           `json:"report_version"`
	CreatedAt     time.Time     `json:"created_at"`
	UpdatedAt     time.Time     `json:"updated_at"`
}
