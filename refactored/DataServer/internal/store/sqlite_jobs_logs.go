package store

import (
	"encoding/json"
	"time"
)

func (s *SQLiteStore) AddJobLog(jobID, message, workerID string, isError bool) error {
	now := time.Now().UTC().Format(time.RFC3339)
	isErr := 0
	if isError {
		isErr = 1
	}
	lm := map[string]any{
		"timestamp": now,
		"message":   message,
		"worker_id": workerID,
		"is_error":  isError,
	}
	lraw, _ := json.Marshal(lm)

	_, err := s.db.Exec(
		`INSERT INTO job_logs (job_id, log_ts, message, worker_id, is_error, raw_json)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		jobID, now, message, workerID, isErr, string(lraw),
	)
	return err
}

func (s *SQLiteStore) GetJobLogs(jobID string, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.Query(
		`SELECT raw_json FROM job_logs WHERE job_id = ? ORDER BY id DESC LIMIT ?`,
		jobID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]any, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err == nil {
			out = append(out, m)
		}
	}
	return out, nil
}
