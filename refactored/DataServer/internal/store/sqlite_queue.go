// Package store / sqlite_queue.go
//
// Queue-related store methods. PR 8 + PR 9 cutover removed the legacy
// multi-step orchestrator types (*queue.Orchestrator / JobStep / MultiStepJob)
// and the orchestrator_jobs + orchestrator_outbox tables; workflow state now
// lives in workflow_runs + workflow_steps + workflow_dependencies
// (migration 027), and outbox events live in outbox_events (migration 026).
//
// Only the canonical JobEvents audit log remains here. Pre-PR8 callers of
// the deleted methods should migrate to:
//
//   UpsertOrchestratorJob*        -> workflow.Repository.CreateRun / CompleteStepAndReleaseDependents
//   PollOrchestratorOutbox        -> outbox.Dispatcher.Claim (handler-driven)
//   MarkOutboxProcessed           -> outbox.Dispatcher dispatches and the store.MarkProcessed
//                                    is called automatically on handler success
//   InsertOutboxEntry             -> outbox.Store.Insert(...) with aggregate_id
package store

import (
	"encoding/json"
)

// --- Job Events ---

// InsertJobEvent logs a job event to SQLite. Timestamp is provided by the
// caller (callers typically format with `time.Now().UTC().Format(time.RFC3339)`).
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
