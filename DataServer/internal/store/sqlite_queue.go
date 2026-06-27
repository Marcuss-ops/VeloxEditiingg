package store

import (
	"time"
)

// ── Typed queue structs ──────────────────────────────────────────────────

// OrchestratorJob is the typed row from orchestrator_jobs.
// RawJSON carries the full JSON blob for any fields not captured
// by the dedicated columns.
type OrchestratorJob struct {
	JobID        string `json:"job_id"`
	Status       string `json:"status"`
	TotalSteps   int    `json:"total_steps"`
	CurrentStep  int    `json:"current_step"`
	PipelineType string `json:"pipeline_type"`
	StartedAt    string `json:"started_at,omitempty"`
	CompletedAt  string `json:"completed_at,omitempty"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
	RawJSON      string `json:"-"`
}

// DLQJob is the typed row from dlq_jobs.
type DLQJob struct {
	JobID      string `json:"job_id"`
	DeadAt     string `json:"dead_at"`
	DeadReason string `json:"dead_reason"`
	FailReason string `json:"fail_reason"`
	FailCount  int    `json:"fail_count"`
	Replayable bool   `json:"replayable"`
	CreatedAt  string `json:"created_at"`
	RawJSON    string `json:"-"`
}

// JobEvent is the typed row from job_events.
type JobEvent struct {
	Timestamp string `json:"timestamp"`
	JobID     string `json:"job_id"`
	Event     string `json:"event"`
	RawJSON   string `json:"-"`
}

// --- Orchestrator Jobs ---

// UpsertOrchestratorJob creates or updates an orchestrator job.
func (s *SQLiteStore) UpsertOrchestratorJob(jobID, status, pipelineType string, totalSteps, currentStep int, rawJSON string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`INSERT INTO orchestrator_jobs (job_id, status, total_steps, current_step, pipeline_type, created_at, updated_at, raw_json)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(job_id) DO UPDATE SET
		   status=excluded.status, total_steps=excluded.total_steps, current_step=excluded.current_step,
		   pipeline_type=excluded.pipeline_type, updated_at=excluded.updated_at, raw_json=excluded.raw_json`,
		jobID, status, totalSteps, currentStep, pipelineType, now, now, rawJSON,
	)
	return err
}

// SetOrchestratorJobTimestamps updates started_at and completed_at for a job.
func (s *SQLiteStore) SetOrchestratorJobTimestamps(jobID string, startedAt, completedAt *time.Time) error {
	var started, completed *string
	if startedAt != nil {
		st := startedAt.UTC().Format(time.RFC3339)
		started = &st
	}
	if completedAt != nil {
		ct := completedAt.UTC().Format(time.RFC3339)
		completed = &ct
	}
	_, err := s.db.Exec(
		`UPDATE orchestrator_jobs SET started_at=?, completed_at=?, updated_at=? WHERE job_id=?`,
		started, completed, time.Now().UTC().Format(time.RFC3339), jobID,
	)
	return err
}

// ListOrchestratorJobs returns all orchestrator jobs, typed.
func (s *SQLiteStore) ListOrchestratorJobs() ([]OrchestratorJob, error) {
	rows, err := s.db.Query(
		`SELECT job_id, status, total_steps, current_step, pipeline_type,
		        COALESCE(started_at, ''), COALESCE(completed_at, ''),
		        created_at, updated_at, raw_json
		 FROM orchestrator_jobs ORDER BY updated_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []OrchestratorJob
	for rows.Next() {
		var j OrchestratorJob
		if err := rows.Scan(
			&j.JobID, &j.Status, &j.TotalSteps, &j.CurrentStep, &j.PipelineType,
			&j.StartedAt, &j.CompletedAt, &j.CreatedAt, &j.UpdatedAt, &j.RawJSON,
		); err != nil {
			continue
		}
		result = append(result, j)
	}
	return result, rows.Err()
}

// GetOrchestratorJob returns a single job as raw JSON.
func (s *SQLiteStore) GetOrchestratorJob(jobID string) (string, error) {
	var raw string
	err := s.db.QueryRow(`SELECT raw_json FROM orchestrator_jobs WHERE job_id=?`, jobID).Scan(&raw)
	return raw, err
}

// DeleteOrchestratorJob removes an orchestrator job.
func (s *SQLiteStore) DeleteOrchestratorJob(jobID string) error {
	_, err := s.db.Exec(`DELETE FROM orchestrator_jobs WHERE job_id=?`, jobID)
	return err
}

// --- Dead Letter Queue ---

// UpsertDLQJob creates or updates a DLQ job.
func (s *SQLiteStore) UpsertDLQJob(jobID, deadAt, deadReason, failReason string, failCount int, replayable bool, rawJSON string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	replayInt := 0
	if replayable {
		replayInt = 1
	}
	_, err := s.db.Exec(
		`INSERT INTO dlq_jobs (job_id, dead_at, dead_reason, fail_reason, fail_count, replayable, created_at, raw_json)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(job_id) DO UPDATE SET
		   dead_at=excluded.dead_at, dead_reason=excluded.dead_reason, fail_reason=excluded.fail_reason,
		   fail_count=excluded.fail_count, replayable=excluded.replayable, raw_json=excluded.raw_json`,
		jobID, deadAt, deadReason, failReason, failCount, replayInt, now, rawJSON,
	)
	return err
}

// ListDLQJobs returns all DLQ jobs, typed.
func (s *SQLiteStore) ListDLQJobs() ([]DLQJob, error) {
	rows, err := s.db.Query(
		`SELECT job_id, dead_at, dead_reason, fail_reason, fail_count,
		        replayable, created_at, raw_json
		 FROM dlq_jobs ORDER BY dead_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []DLQJob
	for rows.Next() {
		var j DLQJob
		var replayInt int
		if err := rows.Scan(
			&j.JobID, &j.DeadAt, &j.DeadReason, &j.FailReason, &j.FailCount,
			&replayInt, &j.CreatedAt, &j.RawJSON,
		); err != nil {
			continue
		}
		j.Replayable = replayInt == 1
		result = append(result, j)
	}
	return result, rows.Err()
}

// GetDLQJob returns a single DLQ job as raw JSON.
func (s *SQLiteStore) GetDLQJob(jobID string) (string, error) {
	var raw string
	err := s.db.QueryRow(`SELECT raw_json FROM dlq_jobs WHERE job_id=?`, jobID).Scan(&raw)
	return raw, err
}

// DeleteDLQJob removes a DLQ job.
func (s *SQLiteStore) DeleteDLQJob(jobID string) error {
	_, err := s.db.Exec(`DELETE FROM dlq_jobs WHERE job_id=?`, jobID)
	return err
}

// PurgeDLQJobs removes DLQ jobs older than a given time.
func (s *SQLiteStore) PurgeDLQJobs(olderThan time.Time) (int64, error) {
	result, err := s.db.Exec(`DELETE FROM dlq_jobs WHERE dead_at < ?`, olderThan.UTC().Format(time.RFC3339))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// --- Job Events ---

// InsertJobEvent logs a job event to SQLite.
func (s *SQLiteStore) InsertJobEvent(timestamp, jobID, eventType, rawJSON string) error {
	_, err := s.db.Exec(
		`INSERT INTO job_events (timestamp, job_id, event, raw_json) VALUES (?, ?, ?, ?)`,
		timestamp, jobID, eventType, rawJSON,
	)
	return err
}

// ListJobEvents returns recent events for a job, typed.
func (s *SQLiteStore) ListJobEvents(jobID string, limit int) ([]JobEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(
		`SELECT timestamp, job_id, event, raw_json
		 FROM job_events WHERE job_id=? ORDER BY timestamp DESC LIMIT ?`,
		jobID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []JobEvent
	for rows.Next() {
		var e JobEvent
		if err := rows.Scan(&e.Timestamp, &e.JobID, &e.Event, &e.RawJSON); err != nil {
			continue
		}
		result = append(result, e)
	}
	return result, rows.Err()
}
