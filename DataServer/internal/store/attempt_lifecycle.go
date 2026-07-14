package store

// attempt_lifecycle.go: lifecycle row-CRUD + state-transition CAS
// on the task_attempts table. Single-row, non-transactional helpers
// (no Tx wrapper, no multi-table atomicity). The §9.5 atomic-multi-
// row flows live on SQLiteTaskRepository (sqlite_task_atomic.go
// siblings). Typed metrics + cache + cost + drift-snapshot reads
// live in attempt_metrics.go. Per-phase / per-segment sidecar
// timings live in attempt_reports.go.
// Extracted from sqlite_task_attempt_repository.go.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/taskattempts"
)

// SQLiteTaskAttemptRepository implements taskattempts.Repository against *SQLiteStore.
type SQLiteTaskAttemptRepository struct {
	store *SQLiteStore
}

// Compile-time assertion.
var _ taskattempts.Repository = (*SQLiteTaskAttemptRepository)(nil)

// NewSQLiteTaskAttemptRepository wraps a SQLiteStore as a taskattempts.Repository.
func NewSQLiteTaskAttemptRepository(store *SQLiteStore) *SQLiteTaskAttemptRepository {
	return &SQLiteTaskAttemptRepository{store: store}
}

var attemptColumns = []string{
	"id", "task_id", "job_id", "attempt_number", "worker_id", "lease_id",
	"status", "started_at", "completed_at", "error_code", "error_message",
	"report_version", "created_at", "updated_at",
	"git_sha", "worker_version", "engine_version",
	"ffmpeg_version", "config_hash", "docker_image_digest",
	"trace_id", "span_id",
}

func scanAttempt(row interface{ Scan(...interface{}) error }) (*taskattempts.TaskAttempt, error) {
	var a taskattempts.TaskAttempt
	var startedAt, completedAt sql.NullString
	var createdAt, updatedAt string
	err := row.Scan(
		&a.ID, &a.TaskID, &a.JobID, &a.AttemptNumber, &a.WorkerID, &a.LeaseID,
		&a.Status, &startedAt, &completedAt, &a.ErrorCode, &a.ErrorMessage,
		&a.ReportVersion, &createdAt, &updatedAt,
		&a.GitSHA, &a.WorkerVersion, &a.EngineVersion,
		&a.FFmpegVersion, &a.ConfigHash, &a.DockerImageDigest,
		&a.TraceID, &a.SpanID,
	)
	if err != nil {
		return nil, err
	}
	if createdAt != "" {
		if pt, e := time.Parse(time.RFC3339, createdAt); e == nil {
			a.CreatedAt = pt
		}
	}
	if updatedAt != "" {
		if pt, e := time.Parse(time.RFC3339, updatedAt); e == nil {
			a.UpdatedAt = pt
		}
	}
	if startedAt.Valid && startedAt.String != "" {
		if pt, e := time.Parse(time.RFC3339, startedAt.String); e == nil {
			a.StartedAt = &pt
		}
	}
	if completedAt.Valid && completedAt.String != "" {
		if pt, e := time.Parse(time.RFC3339, completedAt.String); e == nil {
			a.CompletedAt = &pt
		}
	}
	return &a, nil
}

// Create inserts a new attempt in PENDING state.
func (r *SQLiteTaskAttemptRepository) Create(ctx context.Context, attempt *taskattempts.TaskAttempt) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("task attempt repository: store not initialized")
	}
	if attempt.ID == "" {
		attempt.ID = uuid.NewString()
	}
	if attempt.Status == "" {
		attempt.Status = taskattempts.AttemptStatusPending
	}

	// Check for active attempt
	var count int
	err := r.store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_attempts WHERE task_id = ? AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')`,
		attempt.TaskID,
	).Scan(&count)
	if err != nil {
		return fmt.Errorf("task attempt create check: %w", err)
	}
	if count > 0 {
		return fmt.Errorf("task attempt create: %w", taskattempts.ErrActiveAttemptExists)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	_, err = r.store.db.ExecContext(ctx,
		`INSERT INTO task_attempts (
			id, task_id, job_id, attempt_number, worker_id, lease_id,
			status, report_version, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`,
		attempt.ID, attempt.TaskID, attempt.JobID, attempt.AttemptNumber,
		attempt.WorkerID, attempt.LeaseID,
		string(attempt.Status), now, now,
	)
	if err != nil {
		return fmt.Errorf("task attempt create: %w", err)
	}
	return nil
}

// Get returns a single attempt by ID, or (nil, nil) on missing.
func (r *SQLiteTaskAttemptRepository) Get(ctx context.Context, id string) (*taskattempts.TaskAttempt, error) {
	if id == "" {
		return nil, fmt.Errorf("task attempt repository: empty id")
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT `+strings.Join(attemptColumns, ",")+` FROM task_attempts WHERE id = ?`,
		id,
	)
	a, err := scanAttempt(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("task attempt get: %w", err)
	}
	return a, nil
}

// ListByTaskID returns all attempts for a task, ordered by attempt number.
func (r *SQLiteTaskAttemptRepository) ListByTaskID(ctx context.Context, taskID string) ([]taskattempts.TaskAttempt, error) {
	if taskID == "" {
		return nil, nil
	}
	rows, err := r.store.db.QueryContext(ctx,
		`SELECT `+strings.Join(attemptColumns, ",")+` FROM task_attempts WHERE task_id = ? ORDER BY attempt_number ASC`,
		taskID,
	)
	if err != nil {
		return nil, fmt.Errorf("task attempt list: %w", err)
	}
	defer rows.Close()

	var results []taskattempts.TaskAttempt
	for rows.Next() {
		a, err := scanAttempt(rows)
		if err != nil {
			continue
		}
		results = append(results, *a)
	}
	return results, rows.Err()
}

// GetActiveAttempt returns the current non-terminal attempt for a task.
func (r *SQLiteTaskAttemptRepository) GetActiveAttempt(ctx context.Context, taskID string) (*taskattempts.TaskAttempt, error) {
	if taskID == "" {
		return nil, nil
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT `+strings.Join(attemptColumns, ",")+` FROM task_attempts
		 WHERE task_id = ? AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
		 ORDER BY attempt_number DESC LIMIT 1`,
		taskID,
	)
	a, err := scanAttempt(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("task attempt get active: %w", err)
	}
	return a, nil
}

// GetByTaskIDAndWorkerAndLease returns the active attempt for the
// (task_id, worker_id, lease_id) tuple — used by the master's
// handleTaskResult identity-validation wire-fallback path (PR-02 /
// fix/canonical-attempt-identity). The canonical-attempt-id-first path
// looks up via Reader.Get(attempt_id); this method backs off when a
// legacy worker reports no canonical attempt_id (or sends the
// pre-PR-02 leaseID placeholder). Returns (nil, nil) when no active
// attempt matches.
func (r *SQLiteTaskAttemptRepository) GetByTaskIDAndWorkerAndLease(
	ctx context.Context, taskID, workerID, leaseID string,
) (*taskattempts.TaskAttempt, error) {
	if taskID == "" || workerID == "" || leaseID == "" {
		return nil, nil
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT `+strings.Join(attemptColumns, ",")+` FROM task_attempts
		 WHERE task_id = ? AND worker_id = ? AND lease_id = ?
		   AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
		 ORDER BY attempt_number DESC LIMIT 1`,
		taskID, workerID, leaseID)
	a, err := scanAttempt(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("task attempt get by identity tuple: %w", err)
	}
	return a, nil
}

// SetStatus performs a CAS status change from → to.
func (r *SQLiteTaskAttemptRepository) SetStatus(ctx context.Context, id string, from, to taskattempts.AttemptStatus, revision int) error {
	if id == "" {
		return fmt.Errorf("task attempt repository: empty id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE task_attempts
		 SET status = ?, report_version = report_version + 1, updated_at = ?
		 WHERE id = ? AND status = ? AND report_version = ?`,
		string(to), now, id, string(from), revision,
	)
	if err != nil {
		return fmt.Errorf("task attempt set status: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("task attempt set status rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("task attempt set status %s: %w", id, taskattempts.ErrStaleReport)
	}
	return nil
}

// CompleteFinal marks an attempt as terminal with worker-identity CAS tuple.
// Idempotent on already-terminal attempts.
func (r *SQLiteTaskAttemptRepository) CompleteFinal(ctx context.Context, id, workerID, leaseID string, status taskattempts.AttemptStatus, errorCode, errorMessage string, revision int) error {
	if id == "" {
		return fmt.Errorf("task attempt repository: empty id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE task_attempts
		 SET status = ?, completed_at = ?, error_code = ?, error_message = ?,
		     report_version = report_version + 1, updated_at = ?
		 WHERE id = ? AND worker_id = ? AND lease_id = ?
		   AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT')`,
		string(status), now, errorCode, errorMessage, now,
		id, workerID, leaseID,
	)
	if err != nil {
		return fmt.Errorf("task attempt complete final: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("task attempt complete final rows: %w", err)
	}
	if n == 0 {
		// Check if already terminal (idempotent)
		var existing string
		err := r.store.db.QueryRowContext(ctx,
			`SELECT status FROM task_attempts WHERE id = ?`, id,
		).Scan(&existing)
		if err == nil && taskattempts.AttemptStatus(existing).IsTerminal() {
			return nil
		}
		return fmt.Errorf("task attempt complete final %s: %w", id, taskattempts.ErrStaleReport)
	}
	return nil
}

// Delete hard-deletes an attempt.
func (r *SQLiteTaskAttemptRepository) Delete(ctx context.Context, id string) error {
	if id == "" {
		return nil
	}
	_, err := r.store.db.ExecContext(ctx, `DELETE FROM task_attempts WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("task attempt delete: %w", err)
	}
	return nil
}
