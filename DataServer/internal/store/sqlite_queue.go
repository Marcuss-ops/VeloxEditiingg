package store

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
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

func deleteJobEventsBatch(ctx context.Context, tx *sql.Tx, cutoff string, limit int) (sql.Result, error) {
	return tx.ExecContext(ctx, `
		DELETE FROM job_events
		WHERE rowid IN (
			SELECT e.rowid FROM job_events e
			WHERE e.timestamp < ?
			  AND NOT EXISTS (
				SELECT 1 FROM jobs j
				WHERE j.job_id = e.job_id
				  AND j.status NOT IN ('SUCCEEDED','FAILED','CANCELLED')
			  )
			ORDER BY e.timestamp ASC LIMIT ?
		)`, cutoff, limit)
}

// PruneJobEvents deletes old events in bounded transactions so heartbeats and
// job polling can make progress between batches during the initial cleanup.
func (s *SQLiteStore) PruneJobEvents(ctx context.Context, days, batchSize int) error {
	if s == nil {
		return nil
	}
	if days <= 0 {
		days = s.retentionDays.JobEvents
	}
	if days <= 0 {
		return nil
	}
	if batchSize <= 0 {
		batchSize = 5000
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -days).Format(time.RFC3339Nano)
	deleted, batches := int64(0), 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("prune job events begin: %w", err)
		}
		result, err := deleteJobEventsBatch(ctx, tx, cutoff, batchSize)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("prune job events batch: %w", err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("prune job events rows affected: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("prune job events commit: %w", err)
		}
		deleted += n
		batches++
		if batches%10 == 0 {
			log.Printf("[RETENTION] job_events batches=%d deleted=%d", batches, deleted)
		}
		if n < int64(batchSize) {
			break
		}
	}
	if deleted > 0 {
		log.Printf("[RETENTION] job_events pruned=%d batches=%d retention_days=%d", deleted, batches, days)
	}
	return nil
}
