package store

import (
	"context"
	"time"
)

// EventStore provides side-effect operations (logging, supplementary updates,
// history) that accompany job lifecycle transitions. These are separate from
// the atomic JobRepository operations because they touch different tables
// and are best-effort (not part of the atomic CAS transition).
//
// *SQLiteStore already implements all methods on this interface.
type EventStore interface {
	// LogJobEvent records a job lifecycle event in the job_events table.
	LogJobEvent(jobID, eventType string, extra map[string]interface{}) error
	// UpdateJobSupplementary updates secondary columns on the jobs row
	// (lease_id, error_message, failed_at, etc.) without CAS.
	UpdateJobSupplementary(jobID string, fields map[string]interface{}) error
	// AddJobHistory appends a status-change entry to job_history.
	AddJobHistory(jobID, status, workerID, message string, extra map[string]interface{}) error
	// AddJobLog writes a worker log entry to job_logs.
	AddJobLog(jobID, message, workerID string, isError bool) error
	// SetJobRequest writes the immutable request_json blob for a job.
	SetJobRequest(jobID string, requestJSON []byte) error
	// UpsertJobResult writes the result_json blob for a job.
	UpsertJobResult(jobID string, resultJSON []byte) error

	// ── Read operations used by QueryService ──

	// GetJob returns the full job row as a map (for rich Job construction).
	GetJob(ctx context.Context, jobID string) (map[string]interface{}, error)
	// GetActiveJobs returns all active jobs keyed by job_id.
	GetActiveJobs() (map[string]map[string]interface{}, error)
	// JobCounts returns status→count statistics.
	JobCounts(ctx context.Context) (map[string]int64, error)
	// ListJobsByStatus returns up to limit raw job maps matching any status.
	ListJobsByStatus(statuses []string, limit int) ([]map[string]interface{}, error)
	// DeleteJob removes a job and its related rows.
	DeleteJob(jobID string) error
	// ArchiveOldJobs deletes completed/failed jobs older than the cutoff.
	ArchiveOldJobs(olderThan time.Time) (int64, error)
	// TransitionJobStatus performs a CAS status change on a job row.
	TransitionJobStatus(ctx context.Context, jobID, expected, newStatus string, revision int) (int, error)
	// UpdateArtifactStatus updates an artifact's status (VERIFYING → READY).
	UpdateArtifactStatus(ctx context.Context, artifactID, status string) error
	// CompleteJobTx performs atomic SUCCEEDED + close attempt + outbox.
	// expectedLeaseID != "" → second-line CAS on jobs.lease_id.
	// expectedRevision > 0 → second-line CAS on jobs.revision.
	// Both guards are AND-ed with the existing "status NOT IN terminal"
	// guard. Pass "" / 0 to skip the additional CAS checks.
	CompleteJobTx(ctx context.Context, jobID string, attemptID int64, outboxPayload string, expectedLeaseID string, expectedRevision int) error
}

// Compile-time check: *SQLiteStore implements EventStore.
var _ EventStore = (*SQLiteStore)(nil)
