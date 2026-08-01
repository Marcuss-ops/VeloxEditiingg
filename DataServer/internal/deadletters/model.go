// Package deadletters defines the operator-facing dead-letter contract for
// terminal task failures.
package deadletters

import "time"

const (
	StatusOpen          = "OPEN"
	StatusReplayPending = "REPLAY_PENDING"
	StatusResolved      = "RESOLVED"
	StatusCancelled     = "CANCELLED"
)

type Task struct {
	ID                  string    `json:"id"`
	JobID               string    `json:"job_id"`
	TaskID              string    `json:"task_id"`
	LastAttemptID       string    `json:"last_attempt_id"`
	FailureClass        string    `json:"failure_class"`
	ErrorCode           string    `json:"error_code"`
	Retryable           bool      `json:"retryable"`
	PayloadSnapshotJSON string    `json:"payload_snapshot_json"`
	FirstFailedAt       time.Time `json:"first_failed_at"`
	LastFailedAt        time.Time `json:"last_failed_at"`
	ReplayCount         int       `json:"replay_count"`
	Status              string    `json:"status"`
}
