package store

import (
	"fmt"
)

// JobEvent is the typed row from job_events.
type JobEvent struct {
	Timestamp string `json:"timestamp"`
	JobID     string `json:"job_id"`
	Event     string `json:"event"`
	RawJSON   string `json:"-"`
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
			return nil, fmt.Errorf("scan job event: %w", err)
		}
		result = append(result, e)
	}
	return result, rows.Err()
}
