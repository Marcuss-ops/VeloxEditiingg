package store

import (
	"context"
	"fmt"
	"strings"
)

// ExecutionEvent is the read-only operator projection of one durable
// task_execution_events row. The table remains the canonical detailed
// timeline; callers must not mutate these values.
type ExecutionEvent struct {
	Timestamp   string
	JobID       string
	Event       string
	AttemptID   string
	EventIndex  int64
	Origin      string
	Scope       string
	Status      string
	Metadata    string
	StartedAt   string
	CompletedAt string
}

// ListExecutionEventsByJob returns the durable execution timeline for a job.
// Older databases without migration 110 remain readable and return no rows.
func (s *SQLiteStore) ListExecutionEventsByJob(ctx context.Context, jobID string, limit int) ([]ExecutionEvent, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT job_id,
		       COALESCE(NULLIF(event_name, ''), NULLIF(action, ''), component),
		       attempt_id, event_index, origin, scope,
		       status, metadata_json, started_at, completed_at,
		       CASE
		         WHEN completed_at <> '' THEN completed_at
		         WHEN started_at <> '' THEN started_at
		         ELSE created_at
		       END AS timestamp
		  FROM task_execution_events
		 WHERE job_id = ?
		 ORDER BY timestamp ASC, attempt_id ASC, event_index ASC
		 LIMIT ?`, jobID, limit)
	if err != nil {
		if isMissingExecutionEventsTable(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	result := make([]ExecutionEvent, 0)
	for rows.Next() {
		var event ExecutionEvent
		if err := rows.Scan(&event.JobID, &event.Event, &event.AttemptID,
			&event.EventIndex, &event.Origin, &event.Scope, &event.Status,
			&event.Metadata, &event.StartedAt, &event.CompletedAt, &event.Timestamp); err != nil {
			return nil, fmt.Errorf("scan execution event: %w", err)
		}
		result = append(result, event)
	}
	return result, rows.Err()
}

func isMissingExecutionEventsTable(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no such table: task_execution_events")
}
