package store

import (
	"database/sql"
	"encoding/json"
)

func (s *SQLiteStore) replaceJobHistoryTx(tx *sql.Tx, jobID string, history []map[string]any) error {
	if _, err := tx.Exec(`DELETE FROM job_history WHERE job_id = ?`, jobID); err != nil {
		return err
	}
	for _, hm := range history {
		hraw, _ := json.Marshal(hm)
		if _, err := tx.Exec(
			`INSERT INTO job_history (job_id, status, event_ts, worker_id, message, raw_json)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			jobID, asString(hm["status"]), toISO(hm["timestamp"]), asString(hm["worker_id"]), asString(hm["message"]), string(hraw),
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteStore) GetJobHistory(jobID string, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(
		`SELECT raw_json FROM job_history WHERE job_id = ? ORDER BY id DESC LIMIT ?`,
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
