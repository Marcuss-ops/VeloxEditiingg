package store

import (
	"encoding/json"
	"time"
)

func (s *SQLiteStore) ReplaceJobs(rawJobs map[string][]byte) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec("DELETE FROM job_logs"); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM job_history"); err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM jobs"); err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	for jobID, raw := range rawJobs {
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			continue
		}
		status := asString(m["status"])
		videoName := asString(m["video_name"])
		projectID := asString(m["project_id"])
		createdAt := toISO(m["created_at"])
		updatedAt := toISO(m["updated_at"])
		assignedTo := asString(m["assigned_to"])
		retryCount := asInt(m["retry_count"])
		lastError := asString(m["last_error"])
		if lastError == "" {
			lastError = asString(m["error_message"])
		}
		completedAt := toISO(m["completed_at"])

		rawStr := string(raw)
		if _, err := tx.Exec(
			`INSERT INTO jobs (
				job_id, status, video_name, project_id, created_at, updated_at,
				assigned_to, retry_count, last_error, completed_at,
				raw_json, request_json, result_json, migrated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			jobID, status, videoName, projectID, createdAt, updatedAt,
			assignedTo, retryCount, lastError, completedAt,
			rawStr, rawStr, rawStr, now,
		); err != nil {
			return err
		}

		if hist, ok := m["history"].([]any); ok {
			for _, hv := range hist {
				hm, ok := hv.(map[string]any)
				if !ok {
					continue
				}
				hraw, _ := json.Marshal(hm)
				if _, err := tx.Exec(
					`INSERT INTO job_history (job_id, status, event_ts, worker_id, message, raw_json)
					 VALUES (?, ?, ?, ?, ?, ?)`,
					jobID, asString(hm["status"]), toISO(hm["timestamp"]), asString(hm["worker_id"]), asString(hm["message"]), string(hraw),
				); err != nil {
					return err
				}
			}
		}

		if logs, ok := m["logs"].([]any); ok {
			for _, lv := range logs {
				lm, ok := lv.(map[string]any)
				if !ok {
					continue
				}
				lraw, _ := json.Marshal(lm)
				isErr := 0
				if b, ok := lm["is_error"].(bool); ok && b {
					isErr = 1
				}
				logTS := toISO(lm["timestamp"])
				if logTS == "" {
					logTS = toISO(lm["time"])
				}
				if _, err := tx.Exec(
					`INSERT INTO job_logs (job_id, log_ts, message, worker_id, is_error, raw_json)
					 VALUES (?, ?, ?, ?, ?, ?)`,
					jobID, logTS, asString(lm["message"]), asString(lm["worker_id"]), isErr, string(lraw),
				); err != nil {
					return err
				}
			}
		}
	}

	return tx.Commit()
}

// UpsertJob inserts or updates a job from its raw_json blob (legacy path).
// Also populates request_json and result_json from raw_json during migration.
func (s *SQLiteStore) UpsertJob(jobID string, rawJSON []byte) error {
	var m map[string]any
	if err := json.Unmarshal(rawJSON, &m); err != nil {
		return err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	status := asString(m["status"])
	videoName := asString(m["video_name"])
	projectID := asString(m["project_id"])
	createdAt := toISO(m["created_at"])
	updatedAt := toISO(m["updated_at"])
	assignedTo := asString(m["assigned_to"])
	retryCount := asInt(m["retry_count"])
	lastError := asString(m["last_error"])
	if lastError == "" {
		lastError = asString(m["error_message"])
	}
	completedAt := toISO(m["completed_at"])

	rawStr := string(rawJSON)
	_, err := s.db.Exec(
		`INSERT INTO jobs (
			job_id, status, video_name, project_id, created_at, updated_at,
			assigned_to, retry_count, last_error, completed_at,
			raw_json, request_json, result_json, migrated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(job_id) DO UPDATE SET
			status=excluded.status,
			video_name=excluded.video_name,
			project_id=excluded.project_id,
			updated_at=excluded.updated_at,
			assigned_to=excluded.assigned_to,
			retry_count=excluded.retry_count,
			last_error=excluded.last_error,
			completed_at=excluded.completed_at,
			raw_json=excluded.raw_json,
			migrated_at=excluded.migrated_at`,
		jobID, status, videoName, projectID, createdAt, updatedAt,
		assignedTo, retryCount, lastError, completedAt,
		rawStr, rawStr, rawStr, now,
	)
	return err
}

// UpsertJobResult updates the mutable operational columns and result_json blob.
// Does NOT touch request_json (immutable) or raw_json (legacy).
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
	assignedTo := asString(m["assigned_to"])
	retryCount := asInt(m["retry_count"])
	lastError := asString(m["last_error"])
	if lastError == "" {
		lastError = asString(m["error_message"])
	}
	completedAt := toISO(m["completed_at"])

	_, err := s.db.Exec(
		`INSERT INTO jobs (
			job_id, status, video_name, project_id, created_at, updated_at,
			assigned_to, retry_count, last_error, completed_at,
			result_json, raw_json, migrated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(job_id) DO UPDATE SET
			status=excluded.status,
			video_name=excluded.video_name,
			project_id=excluded.project_id,
			updated_at=excluded.updated_at,
			assigned_to=excluded.assigned_to,
			retry_count=excluded.retry_count,
			last_error=excluded.last_error,
			completed_at=excluded.completed_at,
			result_json=excluded.result_json,
			raw_json=excluded.raw_json,
			migrated_at=excluded.migrated_at`,
		jobID, status, videoName, projectID, createdAt, updatedAt,
		assignedTo, retryCount, lastError, completedAt,
		string(resultJSON), string(resultJSON), now,
	)
	return err
}

// SetJobRequest stores the immutable request payload in request_json.
// Only used at job creation — never updates after initial write.
func (s *SQLiteStore) SetJobRequest(jobID string, requestJSON []byte) error {
	_, err := s.db.Exec(
		`UPDATE jobs SET request_json = ? WHERE job_id = ?`,
		string(requestJSON), jobID,
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

func (s *SQLiteStore) ArchiveOldJobs(olderThan time.Time) (int64, error) {
	cutoff := olderThan.UTC().Format(time.RFC3339)
	result, err := s.db.Exec(
		`DELETE FROM jobs WHERE UPPER(status) IN ('SUCCEEDED', 'FAILED', 'CANCELLED') AND updated_at < ?`,
		cutoff,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
