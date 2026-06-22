package store

import (
	"encoding/json"
	"time"
)

// UpsertJobResult updates the mutable operational columns and result_json blob.
// Does NOT touch request_json (immutable).
func (s *SQLiteStore) UpsertJobResult(jobID string, resultJSON []byte) error {
	var m map[string]any
	if err := json.Unmarshal(resultJSON, &m); err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	status := asString(m["status"])
	videoName := asString(m["video_name"])
	projectID := asString(m["project_id"])
	createdAt := toISO(m["created_at"])
	updatedAt := toISO(m["updated_at"])
	lastError := asString(m["last_error"])
	if lastError == "" {
		lastError = asString(m["error_message"])
	}
	completedAt := toISO(m["completed_at"])

	_, err := s.db.Exec(
		`INSERT INTO jobs (
			job_id, status, video_name, project_id, created_at, updated_at,
			last_error, completed_at,
			result_json, migrated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(job_id) DO UPDATE SET
			status=excluded.status,
			video_name=excluded.video_name,
			project_id=excluded.project_id,
			updated_at=excluded.updated_at,
			last_error=excluded.last_error,
			completed_at=excluded.completed_at,
			result_json=excluded.result_json,
			migrated_at=excluded.migrated_at`,
		jobID, status, videoName, projectID, createdAt, updatedAt,
		lastError, completedAt,
		string(resultJSON), now,
	)
	return err
}

func (s *SQLiteStore) DeleteJob(jobID string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`DELETE FROM job_logs WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM job_history WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM jobs WHERE job_id = ?`, jobID); err != nil {
		return err
	}

	return tx.Commit()
}
