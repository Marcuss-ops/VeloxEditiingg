// Package store / sqlite_jobs_dialect.go
//
// SQLite dialect for the shared job repository.  Provides real audit
// hooks (job_history, job_events, outbox) and SQLite placeholder syntax.

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/outbox"
)

// sqliteDialect provides SQLite-specific SQL syntax and audit hooks.
type sqliteDialect struct {
	store *SQLiteStore
}

func (d sqliteDialect) Placeholder(n int) string { return "?" }

func (d sqliteDialect) Placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat(",?", n)[1:]
}

func (d sqliteDialect) CoalesceStatus() string { return "UPPER(status)" }

func (d sqliteDialect) ConflictError() error { return ErrTransitionConflict }

// ── Reader support (17-column projection with Requirements) ──────────────

func (d sqliteDialect) ProjectionColumns() string {
	return `job_id, status,
		COALESCE(video_name, ''), COALESCE(project_id, ''),
		COALESCE(revision, 0), COALESCE(max_retries, 0),
		COALESCE(created_at, ''), COALESCE(updated_at, ''),
		COALESCE(started_at, ''), COALESCE(completed_at, ''),
		COALESCE(run_id, ''), COALESCE(request_json, '{}'),
		COALESCE(job_required_resource_class, ''),
		COALESCE(job_required_temporal_mode, ''),
		COALESCE(job_required_deterministic, 0),
		COALESCE(job_required_cacheable, 0),
		COALESCE(job_required_min_bandwidth_mbps, 0.0)`
}

func (d sqliteDialect) ScanJob(row interface{ Scan(...interface{}) error }) (*jobs.Job, error) {
	var j JobRecord
	err := row.Scan(
		&j.JobID, &j.Status, &j.VideoName, &j.ProjectID,
		&j.Revision, &j.MaxRetries,
		&j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.CompletedAt,
		&j.RunID, &j.PayloadJSON,
		&j.RequiredResourceClass, &j.RequiredTemporalMode,
		&j.RequiredDeterministic, &j.RequiredCacheable, &j.RequiredMinBandwidthMbps,
	)
	if err != nil {
		return nil, err
	}
	return &jobs.Job{
		ID:          j.JobID,
		Type:        "",
		Status:      jobs.Status(j.Status),
		Attempts:    0,
		Revision:    j.Revision,
		VideoName:   j.VideoName,
		ProjectID:   j.ProjectID,
		RunID:       j.RunID,
		MaxRetries:  j.MaxRetries,
		StartedAt:   parseTimeOrZero(j.StartedAt),
		CompletedAt: parseTimeOrZero(j.CompletedAt),
		CreatedAt:   parseTimeOrZero(j.CreatedAt),
		UpdatedAt:   parseTimeOrZero(j.UpdatedAt),
		Payload:     j.PayloadJSON,
		Requirements: costmodel.JobRequirements{
			ResourceClass:    costmodel.ResourceClass(strings.TrimSpace(j.RequiredResourceClass)),
			TemporalMode:     costmodel.TemporalMode(strings.TrimSpace(j.RequiredTemporalMode)),
			Deterministic:    j.RequiredDeterministic,
			Cacheable:        j.RequiredCacheable,
			MinBandwidthMbps: j.RequiredMinBandwidthMbps,
		},
	}, nil
}

func (d sqliteDialect) ListByStatus(ctx context.Context, db *sql.DB, statuses []string, limit int) ([]jobs.Job, error) {
	if limit <= 0 {
		limit = 1000
	}
	proj := d.ProjectionColumns()
	var query string
	var args []interface{}
	if len(statuses) == 0 {
		query = `SELECT ` + proj + ` FROM jobs ORDER BY COALESCE(updated_at, created_at) DESC LIMIT ?`
		args = []interface{}{limit}
	} else {
		phs := d.Placeholders(len(statuses))
		args = make([]interface{}, len(statuses)+1)
		for i, s := range statuses {
			args[i] = s
		}
		args[len(statuses)] = limit
		query = fmt.Sprintf(
			`SELECT %s FROM jobs WHERE UPPER(status) IN (%s) ORDER BY COALESCE(updated_at, created_at) DESC LIMIT ?`,
			proj, phs,
		)
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list by status: %w", err)
	}
	defer rows.Close()
	var out []jobs.Job
	for rows.Next() {
		j, err := d.ScanJob(rows)
		if err != nil {
			continue
		}
		if j != nil {
			out = append(out, *j)
		}
	}
	return out, rows.Err()
}

func (d sqliteDialect) GetCounts(ctx context.Context, db *sql.DB) (jobs.Counts, error) {
	if d.store == nil {
		return nil, fmt.Errorf("counts: store not initialized")
	}
	raw, err := d.store.JobCounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("counts: %w", err)
	}
	if raw == nil {
		return nil, nil
	}
	out := make(jobs.Counts, len(raw))
	for k, v := range raw {
		out[jobs.Status(k)] = v
	}
	return out, nil
}

// ── Audit hooks ──────────────────────────────────────────────────────────

func (d sqliteDialect) InsertHistoryTx(ctx context.Context, tx *sql.Tx, jobID, status, workerID, message string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	raw, _ := json.Marshal(map[string]interface{}{
		"status":    status,
		"timestamp": now,
		"worker_id": workerID,
		"message":   message,
	})
	_, err := tx.ExecContext(ctx,
		`INSERT INTO job_history (job_id, status, worker_id, message, raw_json, event_ts) VALUES (?, ?, ?, ?, ?, ?)`,
		jobID, status, workerID, message, string(raw), now,
	)
	if err != nil {
		// Fallback for older schemas with different column sets.
		_, err = tx.ExecContext(ctx,
			`INSERT INTO job_history (job_id, status, raw_json, event_ts) VALUES (?, ?, ?, ?)`,
			jobID, status, string(raw), now,
		)
	}
	return err
}

func (d sqliteDialect) InsertEventTx(ctx context.Context, tx *sql.Tx, jobID, eventType string, payload map[string]interface{}) error {
	raw, err := json.Marshal(map[string]interface{}{
		"event":     eventType,
		"job_id":    jobID,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"payload":   payload,
	})
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO job_events (timestamp, job_id, event, raw_json) VALUES (?, ?, ?, ?)`,
		time.Now().UTC().Format(time.RFC3339), jobID, eventType, string(raw),
	)
	return err
}

func (d sqliteDialect) EmitOutboxTx(ctx context.Context, tx *sql.Tx, aggregateType, aggregateID, eventType string, payload []byte) error {
	return d.store.emitOutbox(ctx, tx, outbox.InsertParams{
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		EventType:     eventType,
		Payload:       payload,
	})
}
