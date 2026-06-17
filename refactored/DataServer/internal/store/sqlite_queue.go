package store

import (
	"encoding/json"
	"time"
)

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

// ListOrchestratorJobs returns all orchestrator jobs as raw JSON.
func (s *SQLiteStore) ListOrchestratorJobs() ([]map[string]any, error) {
	rows, err := s.db.Query(`SELECT raw_json FROM orchestrator_jobs ORDER BY updated_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []map[string]any
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			result = append(result, m)
		}
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

// --- Orchestrator Outbox (PR5: transactional, never-lost events) ---

// UpsertOrchestratorJobWithOutbox writes a job and outbox entries in a single transaction.
// Includes started_at/completed_at so timestamps are atomic with state changes (PR5).
func (s *SQLiteStore) UpsertOrchestratorJobWithOutbox(
	jobID, status, pipelineType string,
	totalSteps, currentStep int,
	rawJSON, startedAt, completedAt string,
	outboxEntries []OutboxEntry,
) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339)
	_, err = tx.Exec(
		`INSERT INTO orchestrator_jobs (job_id, status, total_steps, current_step, pipeline_type, created_at, updated_at, started_at, completed_at, raw_json)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(job_id) DO UPDATE SET
		   status=excluded.status, total_steps=excluded.total_steps, current_step=excluded.current_step,
		   pipeline_type=excluded.pipeline_type, updated_at=excluded.updated_at,
		   started_at=excluded.started_at, completed_at=excluded.completed_at, raw_json=excluded.raw_json`,
		jobID, status, totalSteps, currentStep, pipelineType, now, now, startedAt, completedAt, rawJSON,
	)
	if err != nil {
		return err
	}

	for _, entry := range outboxEntries {
		_, err = tx.Exec(
			`INSERT INTO orchestrator_outbox (event_type, job_id, step_id, payload, created_at)
			 VALUES (?, ?, ?, ?, ?)`,
			entry.EventType, entry.JobID, toNullString(entry.StepID), entry.Payload, now,
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// OutboxEntry represents a single outbox message.
type OutboxEntry struct {
	EventType string
	JobID     string
	StepID    string
	Payload   string
}

// PollOrchestratorOutbox returns unprocessed outbox entries ordered by creation time.
func (s *SQLiteStore) PollOrchestratorOutbox(limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.Query(
		`SELECT id, event_type, job_id, COALESCE(step_id,''), payload, created_at
		 FROM orchestrator_outbox WHERE processed = 0
		 ORDER BY created_at ASC LIMIT ?`, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []map[string]any
	for rows.Next() {
		var id int
		var eventType, jobID, stepID, payload, createdAt string
		if err := rows.Scan(&id, &eventType, &jobID, &stepID, &payload, &createdAt); err != nil {
			continue
		}
		entries = append(entries, map[string]any{
			"id":         id,
			"event_type": eventType,
			"job_id":     jobID,
			"step_id":    stepID,
			"payload":    payload,
			"created_at": createdAt,
		})
	}
	return entries, rows.Err()
}

// MarkOutboxProcessed marks an outbox entry as processed.
func (s *SQLiteStore) MarkOutboxProcessed(id int) error {
	_, err := s.db.Exec(`UPDATE orchestrator_outbox SET processed = 1 WHERE id = ?`, id)
	return err
}

// InsertOutboxEntry inserts a single outbox entry (without touching any job).
// Used for external events like ReportStepComplete that only need to write to outbox.
func (s *SQLiteStore) InsertOutboxEntry(eventType, jobID, stepID, payload string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`INSERT INTO orchestrator_outbox (event_type, job_id, step_id, payload, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		eventType, jobID, toNullString(stepID), payload, now,
	)
	return err
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

// ListJobEvents returns recent events for a job.
func (s *SQLiteStore) ListJobEvents(jobID string, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(
		`SELECT raw_json FROM job_events WHERE job_id=? ORDER BY timestamp DESC LIMIT ?`,
		jobID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []map[string]any
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			result = append(result, m)
		}
	}
	return result, rows.Err()
}
