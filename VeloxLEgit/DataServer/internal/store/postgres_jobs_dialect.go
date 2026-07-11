// Package store / postgres_jobs_dialect.go
//
// Postgres dialect for the shared job repository.  Uses $n placeholders
// and no-op audit hooks (Phase 2 doesn't support job_history / job_events
// outbox_events).

package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"velox-server/internal/jobs"
)

// postgresDialect provides Postgres-specific SQL syntax.  Audit hooks are
// no-ops because Phase 2 only writes to the jobs table.
type postgresDialect struct{}

func (d postgresDialect) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }

func (d postgresDialect) Placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		parts[i] = fmt.Sprintf("$%d", i+1)
	}
	return strings.Join(parts, ",")
}

func (d postgresDialect) CoalesceStatus() string { return "UPPER(COALESCE(status, ''))" }

var pgJobTransitionConflict = fmt.Errorf("postgres jobs: transition from-state mismatch")

func (d postgresDialect) ConflictError() error { return pgJobTransitionConflict }

// ── Reader support (12-column narrow projection, no Requirements) ────────

func (d postgresDialect) ProjectionColumns() string {
	return `job_id, COALESCE(status, ''),
		COALESCE(video_name, ''), COALESCE(project_id, ''),
		COALESCE(revision, 0), COALESCE(max_retries, 0),
		COALESCE(created_at, ''), COALESCE(updated_at, ''),
		COALESCE(started_at, ''), COALESCE(completed_at, ''),
		COALESCE(run_id, ''), COALESCE(request_json, '')`
}

func (d postgresDialect) ScanJob(row interface{ Scan(...interface{}) error }) (*jobs.Job, error) {
	var (
		jobID, status, videoName, projectID,
		createdAt, updatedAt, startedAt, completedAt, runID, requestJSON string
		revision, maxRetries int
	)
	err := row.Scan(
		&jobID, &status, &videoName, &projectID,
		&revision, &maxRetries,
		&createdAt, &updatedAt, &startedAt, &completedAt,
		&runID, &requestJSON,
	)
	if err != nil {
		return nil, err
	}
	return &jobs.Job{
		ID:          jobID,
		Status:      jobs.Status(status),
		Type:        "",
		VideoName:   videoName,
		ProjectID:   projectID,
		RunID:       runID,
		Attempts:    0,
		Revision:    revision,
		MaxRetries:  maxRetries,
		StartedAt:   parseTimeOrZero(startedAt),
		CompletedAt: parseTimeOrZero(completedAt),
		CreatedAt:   parseTimeOrZero(createdAt),
		UpdatedAt:   parseTimeOrZero(updatedAt),
		Payload:     requestJSON,
	}, nil
}

func (d postgresDialect) ListByStatus(ctx context.Context, db *sql.DB, statuses []string, limit int) ([]jobs.Job, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	// Normalize status strings (uppercase, trim blanks).
	cleaned := make([]string, 0, len(statuses))
	for _, s := range statuses {
		c := normalizeStatus(s)
		if c == "" {
			continue
		}
		cleaned = append(cleaned, c)
	}
	if len(cleaned) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 1000
	}

	rows, err := db.QueryContext(ctx,
		`SELECT `+d.ProjectionColumns()+`
		 FROM jobs
		 WHERE UPPER(COALESCE(status, '')) = ANY($1::text[])
		 ORDER BY COALESCE(updated_at, created_at) DESC
		 LIMIT $2`,
		cleaned, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres jobs: List: %w", err)
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

func (d postgresDialect) GetCounts(ctx context.Context, db *sql.DB) (jobs.Counts, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT UPPER(COALESCE(status, '')) AS s, COUNT(*)
		 FROM jobs
		 WHERE UPPER(COALESCE(status, '')) IN
		     ('PENDING','LEASED','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED','CANCELLED')
		 GROUP BY UPPER(COALESCE(status, ''))`)
	if err != nil {
		return nil, fmt.Errorf("postgres jobs: Counts: %w", err)
	}
	defer rows.Close()
	out := jobs.Counts{}
	for rows.Next() {
		var sname string
		var cnt int64
		if err := rows.Scan(&sname, &cnt); err != nil {
			continue
		}
		out[jobs.Status(sname)] = cnt
	}
	return out, rows.Err()
}

// All audit hooks are no-ops on Postgres (Phase 2).
func (d postgresDialect) InsertHistoryTx(ctx context.Context, tx *sql.Tx, jobID, status, workerID, message string) error {
	return nil
}
func (d postgresDialect) InsertEventTx(ctx context.Context, tx *sql.Tx, jobID, eventType string, payload map[string]interface{}) error {
	return nil
}
func (d postgresDialect) EmitOutboxTx(ctx context.Context, tx *sql.Tx, aggregateType, aggregateID, eventType string, payload []byte) error {
	return nil
}
