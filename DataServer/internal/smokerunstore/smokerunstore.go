// Package smokerunstore owns the smoke_runs analytics table:
//
//   - smoke_runs records the END-TO-END duration baseline for every
//     Level D smoke executed by the LevelDSmokeExecutor (fleet —
//     Step 12/15).
//   - Distinct from fleet_operations (audit trail) and
//     deployment_records (deploy lifecycle). The duration_ms column is
//     the operator's p-baseline for the dashboard's "smoke runs slower
//     than usual?" alerting.
//
// All terminal transitions are atomic UPDATE; the dashboard's "running
// smokes" view never observes a torn (status=SUCCEEDED, finished_at=NULL)
// row. This leaf depends only on database/sql, never on internal/store;
// internal/store re-exports the symbols for compatibility.
package smokerunstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const (
	// SmokeStatusPending — initial state at insert. The row
	// exists but no phase has completed.
	SmokeStatusPending = "PENDING"

	// SmokeStatusSucceeded — every phase completed: lease
	// acquired → asset downloaded → ffmpeg rendered → artifact
	// uploaded to Drive. artifact_drive_id holds the canonical
	// Drive URL.
	SmokeStatusSucceeded = "SUCCEEDED"

	// SmokeStatusFailed — any phase failed AND cleanup could
	// not recover. error_message stores the operator-readable
	// failure diagnosis.
	SmokeStatusFailed = "FAILED"
)

// ErrSmokeRunNotFound is returned by GetLatestSmokeForWorker
// when no rows exist for that worker. Distinguishes "worker
// has never been smoke-tested" from "smoke row missing despite
// expectations" — the former is normal at fleet-onboard time.
var ErrSmokeRunNotFound = errors.New("no smoke runs for worker")

// SmokeRun mirrors a single row in smoke_runs. All time fields
// are RFC3339 strings in the SQL row to keep the schema
// dialect-agnostic; Go-side conversion is at the repository
// boundary so callers see time.Time.
//
// duration_ms is the canonical end-to-end duration field for
// the analytics baseline. The smoke dashboard's p95 / p99 /
// moving-avg are all computed from this column. inserted +
// derived columns keep the dashboard queries small.
type SmokeRun struct {
	RunID           string    `json:"run_id"`
	WorkerID        string    `json:"worker_id"`
	StartedAt       time.Time `json:"started_at"`
	FinishedAt      time.Time `json:"finished_at"`
	DurationMs      int64     `json:"duration_ms"`
	AssetID         string    `json:"asset_id"`
	ArtifactDriveID string    `json:"artifact_drive_id,omitempty"`
	Status          string    `json:"status"`
	ErrorMessage    string    `json:"error_message,omitempty"`
	RequestedBy     string    `json:"requested_by"`
}

// CreateSmokeRunsTableIfNotExists is the test/dev-only bootstrap path.
// Idempotent; the DDL mirrors migration 106 exactly (modulo the inline
// CHECK (duration_ms >= 0) which both enforce — defence-in-depth against
// raw-INSERT bugs that bypass the Go-side validation).
func CreateSmokeRunsTableIfNotExists(db *sql.DB) error {
	ddl := `
CREATE TABLE IF NOT EXISTS smoke_runs (
  run_id TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL,
  duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
  asset_id TEXT NOT NULL CHECK (length(asset_id) > 0),
  artifact_drive_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED')),
  error_message TEXT,
  requested_by TEXT NOT NULL CHECK (length(requested_by) > 0)
);
CREATE INDEX IF NOT EXISTS idx_smoke_runs_worker ON smoke_runs(worker_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_smoke_runs_status ON smoke_runs(status, started_at DESC);
`
	_, err := db.Exec(ddl)
	return err
}

// InsertSmokeRun persists a new smoke run row. Status MUST be
// SmokeStatusPending at insert time (terminal statuses fail the
// CHECK constraint and confuse the dashboard's running-vs-completed
// view).
func InsertSmokeRun(ctx context.Context, db *sql.DB, rec SmokeRun) error {
	if rec.Status != SmokeStatusPending {
		return fmt.Errorf("InsertSmokeRun: initial status must be PENDING, got %q", rec.Status)
	}
	if rec.RunID == "" {
		return errors.New("InsertSmokeRun: RunID empty")
	}
	if rec.WorkerID == "" {
		return errors.New("InsertSmokeRun: WorkerID empty")
	}
	if rec.AssetID == "" {
		return errors.New("InsertSmokeRun: AssetID empty")
	}
	if rec.RequestedBy == "" {
		return errors.New("InsertSmokeRun: RequestedBy empty")
	}
	// Stamp finished_at to started_at at insert — terminal transition
	// rewrites this. Dashboard queries filter status='PENDING' to
	// find running smokes; non-terminal rows ignore finished_at.
	_, err := db.ExecContext(ctx, `
INSERT INTO smoke_runs
  (run_id, worker_id, started_at, finished_at, duration_ms,
   asset_id, artifact_drive_id, status, error_message, requested_by)
VALUES (?, ?, ?, ?, 0, ?, NULL, ?, NULL, ?)`,
		rec.RunID, rec.WorkerID,
		rec.StartedAt.UTC().Format(time.RFC3339),
		rec.StartedAt.UTC().Format(time.RFC3339),
		rec.AssetID, rec.Status, rec.RequestedBy,
	)
	return err
}

// MarkSmokeSucceeded atomically transitions a smoke_runs row to
// SUCCEEDED + stamps finished_at + duration_ms + artifact_drive_id.
func MarkSmokeSucceeded(ctx context.Context, db *sql.DB, runID string, finishedAt time.Time, durationMs int64, artifactDriveID string) error {
	if durationMs < 0 {
		return fmt.Errorf("MarkSmokeSucceeded: duration_ms must be >= 0, got %d", durationMs)
	}
	res, err := db.ExecContext(ctx, `
UPDATE smoke_runs
SET status = 'SUCCEEDED', finished_at = ?, duration_ms = ?, artifact_drive_id = ?, error_message = NULL
WHERE run_id = ? AND status = 'PENDING'`,
		finishedAt.UTC().Format(time.RFC3339), durationMs, artifactDriveID, runID,
	)
	if err == nil {
		if n, rowsErr := res.RowsAffected(); rowsErr != nil {
			return fmt.Errorf("MarkSmokeSucceeded rows: %w", rowsErr)
		} else if n != 1 {
			return fmt.Errorf("MarkSmokeSucceeded: run %s is missing or not PENDING", runID)
		}
	}
	return err
}

// MarkSmokeFailed atomically transitions a smoke_runs row to
// FAILED + stamps finished_at + duration_ms + error_message.
func MarkSmokeFailed(ctx context.Context, db *sql.DB, runID string, finishedAt time.Time, durationMs int64, errMsg string) error {
	if durationMs < 0 {
		return fmt.Errorf("MarkSmokeFailed: duration_ms must be >= 0, got %d", durationMs)
	}
	res, err := db.ExecContext(ctx, `
UPDATE smoke_runs
SET status = 'FAILED', finished_at = ?, duration_ms = ?, error_message = ?
WHERE run_id = ? AND status = 'PENDING'`,
		finishedAt.UTC().Format(time.RFC3339), durationMs, errMsg, runID,
	)
	if err == nil {
		if n, rowsErr := res.RowsAffected(); rowsErr != nil {
			return fmt.Errorf("MarkSmokeFailed rows: %w", rowsErr)
		} else if n != 1 {
			return fmt.Errorf("MarkSmokeFailed: run %s is missing or not PENDING", runID)
		}
	}
	return err
}

// GetLatestSmokeForWorker returns the row with the highest started_at
// for the worker, regardless of status. Returns ErrSmokeRunNotFound
// when no rows exist.
func GetLatestSmokeForWorker(ctx context.Context, db *sql.DB, workerID string) (*SmokeRun, error) {
	rows, err := db.QueryContext(ctx, `
SELECT run_id, worker_id, started_at, finished_at, duration_ms,
       asset_id, artifact_drive_id, status, error_message, requested_by
FROM smoke_runs
WHERE worker_id = ?
ORDER BY started_at DESC
LIMIT 1`, workerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, ErrSmokeRunNotFound
	}
	return scanSmokeRun(rows)
}

// ListRecentSmokesForWorker returns up to limit rows in started_at
// DESC order. limit <= 0 means no cap.
func ListRecentSmokesForWorker(ctx context.Context, db *sql.DB, workerID string, limit int) ([]SmokeRun, error) {
	q := `
SELECT run_id, worker_id, started_at, finished_at, duration_ms,
       asset_id, artifact_drive_id, status, error_message, requested_by
FROM smoke_runs
WHERE worker_id = ?
ORDER BY started_at DESC`
	args := []interface{}{workerID}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SmokeRun
	for rows.Next() {
		r, err := scanSmokeRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *r)
	}
	return out, rows.Err()
}

// scanSmokeRun reads one row into a SmokeRun. Pulled out so the same
// SQL row → Go struct mapping happens in both Get / List without drift.
func scanSmokeRun(rows *sql.Rows) (*SmokeRun, error) {
	var (
		r               SmokeRun
		startedAt       string
		finishedAt      string
		artifactDriveID sql.NullString
		errorMessage    sql.NullString
	)
	if err := rows.Scan(
		&r.RunID, &r.WorkerID, &startedAt, &finishedAt, &r.DurationMs,
		&r.AssetID, &artifactDriveID, &r.Status, &errorMessage, &r.RequestedBy,
	); err != nil {
		return nil, err
	}
	parsedStarted, err := parsePersistedTimestamp(startedAt, "smoke_runs.started_at")
	if err != nil {
		return nil, err
	}
	r.StartedAt = parsedStarted
	parsedFinished, err := parsePersistedTimestamp(finishedAt, "smoke_runs.finished_at")
	if err != nil {
		return nil, err
	}
	r.FinishedAt = parsedFinished
	if artifactDriveID.Valid {
		r.ArtifactDriveID = artifactDriveID.String
	}
	if errorMessage.Valid {
		r.ErrorMessage = errorMessage.String
	}
	return &r, nil
}

// parsePersistedTimestamp parses a persisted smoke_runs timestamp
// (RFC3339Nano / RFC3339 / bare space-separated). Local copy of the
// store helper so this leaf stays free of internal/store.
func parsePersistedTimestamp(value, field string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("worker runtime: invalid %s %q", field, value)
}
