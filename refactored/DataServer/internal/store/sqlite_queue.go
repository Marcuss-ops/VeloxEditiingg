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

// SetOrchestratorJobTimestamps updates started_at and completed_at for a job.
func (s *SQLiteStore) SetOrchestratorJobTimestamps(jobID string, startedAt, completedAt string) error {
	_, err := s.db.Exec(
		`UPDATE orchestrator_jobs SET started_at=?, completed_at=?, updated_at=? WHERE job_id=?`,
		startedAt, completedAt, time.Now().UTC().Format(time.RFC3339), jobID,
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
