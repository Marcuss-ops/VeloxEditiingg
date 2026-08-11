package store

import "time"

// StaleExecutionCategory is the stable operator-facing category for a finding.
type StaleExecutionCategory string

const (
	StaleLeaseExpired      StaleExecutionCategory = "expired_lease"
	StaleOrphanTask        StaleExecutionCategory = "orphan_task"
	StaleCommittedArtifact StaleExecutionCategory = "committed_artifact_job_drift"
	StaleUnconfirmedSpool  StaleExecutionCategory = "unconfirmed_spool"
	StaleWorkerOffline     StaleExecutionCategory = "worker_offline"
	StaleOrphanAttempt     StaleExecutionCategory = "orphan_attempt"
	// Canonical Phase A3 reconciliation categories (shared audit ledger).
	StaleAwaitingArtifact StaleExecutionCategory = "awaiting_artifact"
	StaleDeliveryPending  StaleExecutionCategory = "delivery_pending"
	StaleWorkerLost       StaleExecutionCategory = "worker_lost"
)

// StaleExecutionFinding is a read-only change proposal emitted by Scan.
type StaleExecutionFinding struct {
	Category       StaleExecutionCategory `json:"category"`
	ResourceType   string                 `json:"resource_type"`
	ResourceID     string                 `json:"resource_id"`
	JobID          string                 `json:"job_id,omitempty"`
	TaskID         string                 `json:"task_id,omitempty"`
	AttemptID      string                 `json:"attempt_id,omitempty"`
	ArtifactID     string                 `json:"artifact_id,omitempty"`
	CommitID       string                 `json:"commit_id,omitempty"`
	WorkerID       string                 `json:"worker_id,omitempty"`
	LeaseID        string                 `json:"lease_id,omitempty"`
	OldStatus      string                 `json:"old_status,omitempty"`
	ProposedStatus string                 `json:"proposed_status,omitempty"`
	Reason         string                 `json:"reason"`
	ObservedAt     time.Time              `json:"observed_at"`
}

// StaleExecutionReport is returned by both dry-run and apply modes.
type StaleExecutionReport struct {
	GeneratedAt string                  `json:"generated_at"`
	Mode        string                  `json:"mode"`
	Findings    []StaleExecutionFinding `json:"findings"`
	Applied     []StaleExecutionFinding `json:"applied,omitempty"`
	Skipped     int                     `json:"skipped,omitempty"`
}
