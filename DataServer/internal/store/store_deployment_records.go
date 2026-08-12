package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// store_deployment_records.go owns the deployment_records table:
// schema bootstrap, insert (PENDING), status updates, latest
// lookup, history list. The struct shape mirrors the SQL columns
// 1:1 so callers can pass it through the API boundary without
// reserialisation.
//
// Step 5/15 lifecycle (out of scope here, lands in Step 6): the
// Fleet Controller calls InsertDeploymentRecord on PENDING, then
// UpdateDeploymentStatus once the worker-side prepare-host.sh
// pipeline reports back via worker_events / heartbeat. Until
// Step 6 lands, only the schema + repository plumbing exist.

const (
	// DeployStatusPending is the initial state at insert. The
	// row exists, but the worker-side update has not yet
	// completed. Rows in PENDING are the dashboard's "in-flight
	// deploys" view.
	DeployStatusPending = "PENDING"

	// DeployStatusSucceeded marks a deploy where the worker's
	// heartbeat reported image_digest == target_digest AND the
	// worker emitted a matching worker_events row signalling
	// "deploy_completed".
	DeployStatusSucceeded = "SUCCEEDED"

	// DeployStatusFailed marks a deploy that did NOT promote —
	// health check failed, cosign verify failed on the worker,
	// or the worker didn't come up at all within timeout.
	DeployStatusFailed = "FAILED"

	// DeployStatusRolledBack marks a successful forward-and-roll-
	// back cascade: target_digest was attempted and failed, so
	// the worker was rolled back to previous_digest and a new
	// deployment_records row was written with is_rollback=true
	// (the row documents the rollback, not the original
	// forward deploy which already has its own row).
	DeployStatusRolledBack = "ROLLED_BACK"
)

// ErrDeploymentNotFound is returned by GetLatestDeploymentForWorker
// when no rows exist for that worker. Maps to a 404 at the API
// boundary; callers MUST distinguish "unknown worker" from
// "known worker with no deploys yet" — the messaging differs.
var ErrDeploymentNotFound = errors.New("no deployment records for worker")

// DeploymentRecord mirrors a single row in deployment_records.
// All time fields are RFC3339 strings in the SQL row to keep
// the schema dialect-agnostic; Go-side conversion is at the
// repository boundary so callers see time.Time.
//
// IsRollback distinguishes "intentional rollback to previous_digest"
// from "forward deploy". Step 6's rollback path sets this flag
// when a previously-FAILED forward deploy triggers an automatic
// rollback. It is meaningful only with a terminal status:
//   - status=SUCCEEDED + IsRollback=false — canonical forward happy
//     path (target_digest was deployed and the worker came up).
//   - status=SUCCEEDED + IsRollback=true  — rollback itself succeeded
//     (the worker is now on previous_digest, the operator's intent).
//   - status=ROLLED_BACK + IsRollback=true — forward failed, the
//     auto-rollback transition row is logged.
//
// status=FAILED + IsRollback=true is unreachable through the
// canonical happy path; status=PENDING + IsRollback=true is
// unreachable because the Fleet Controller (Step 2) emits the
// PENDING row with IsRollback=false. Both combinations are
// syntactically allowed in the schema (the two fields are
// independent columns) but should never appear in production.
type DeploymentRecord struct {
	DeploymentID   string     `json:"deployment_id"`
	WorkerID       string     `json:"worker_id"`
	PreviousDigest string     `json:"previous_digest"`
	TargetDigest   string     `json:"target_digest"`
	StartedAt      time.Time  `json:"started_at"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	Status         string     `json:"status"`
	ErrorMessage   string     `json:"error_message,omitempty"`
	AppliedBy      string     `json:"applied_by"`
	IsRollback     bool       `json:"is_rollback"`
}

// CreateDeploymentRecordsTableIfNotExists is the test/dev-only
// bootstrap path. Production uses the migration runner from
// internal/store/migrations/sqlite/103_deployment_records.sql + 151
// (error_message) — this function is here so unit tests against an
// in-memory SQLite can stand up the table without a full migration
// sweep.
//
// Idempotent: safe to call repeatedly. The DDL uses CREATE TABLE
// IF NOT EXISTS so the migration runner's checksum tracking
// stays the source of truth (this function does NOT insert into
// schema_migrations).
//
// The DDL mirrors sqlite/103_deployment_records.sql as amended by
// 151_worker_deployment_state.sql (the error_message column the
// migration ALTERs in), modulo inline CHECK length > 0 on the
// digest columns which the migration file also carries — both
// layers enforce the digest non-emptiness invariant,
// defence-in-depth against raw-INSERT bugs that bypass the Go-side
// InsertDeploymentRecord validation).
func (s *SQLiteStore) CreateDeploymentRecordsTableIfNotExists() error {
	ddl := `
CREATE TABLE IF NOT EXISTS deployment_records (
  deployment_id TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL,
  previous_digest TEXT CHECK (previous_digest IS NULL OR length(previous_digest) > 0),
  target_digest TEXT NOT NULL CHECK (length(target_digest) > 0),
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED', 'ROLLED_BACK')),
  error_message TEXT NOT NULL DEFAULT '',
  applied_by TEXT NOT NULL,
  is_rollback INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_deployment_records_worker ON deployment_records(worker_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_deployment_records_status ON deployment_records(status, started_at DESC);
`
	if _, err := s.db.Exec(ddl); err != nil {
		return err
	}
	return s.CreateWorkerDeploymentStateTableIfNotExists()
}

// InsertDeploymentRecord persists a new deployment record. Status
// MUST be DeployStatusPending at insert time (terminal statuses
// fail the FK + CHECK constraints downstream and confuse the
// dashboard's "in-flight deploys" query).
func (s *SQLiteStore) InsertDeploymentRecord(ctx context.Context, r DeploymentRecord) error {
	if r.Status != DeployStatusPending {
		return fmt.Errorf("InsertDeploymentRecord: initial status must be PENDING, got %q", r.Status)
	}
	if r.DeploymentID == "" {
		return errors.New("InsertDeploymentRecord: DeploymentID empty")
	}
	if r.WorkerID == "" {
		return errors.New("InsertDeploymentRecord: WorkerID empty")
	}
	// Defence-in-depth: SQL CHECK (length > 0) catches empty digests
	// at the DB layer too, but rejecting eagerly here gives the
	// caller a clearer error and avoids the surprise of an SQL
	// CHECK violation if a future caller calls BEFORE running
	// ValidateImageRef upstream.
	if r.PreviousDigest == "" {
		return errors.New("InsertDeploymentRecord: PreviousDigest empty")
	}
	if r.TargetDigest == "" {
		return errors.New("InsertDeploymentRecord: TargetDigest empty")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
INSERT INTO deployment_records
	  (deployment_id, worker_id, previous_digest, target_digest, started_at, finished_at, status, error_message, applied_by, is_rollback)
VALUES (?, ?, ?, ?, ?, NULL, ?, '', ?, ?)`,
		r.DeploymentID, r.WorkerID, r.PreviousDigest, r.TargetDigest,
		r.StartedAt.UTC().Format(time.RFC3339),
		r.Status, r.AppliedBy, boolToIntSQLite(r.IsRollback),
	)
	if err != nil {
		return err
	}
	if err := upsertDeploymentStateFromRecord(ctx, tx, r, r.StartedAt); err != nil {
		return err
	}
	return tx.Commit()
}

// InsertBaselineDeploymentRecord writes an already-verified runtime state as
// a terminal ledger row. It is intentionally separate from
// InsertDeploymentRecord: ordinary rollouts must always begin PENDING, while
// a bootstrap baseline must never expose a fake in-flight deployment or run
// any worker mutation.
//
// An empty PreviousDigest is persisted as SQL NULL. NULL means rollback
// provenance is unavailable; it is never replaced with the current digest.
func (s *SQLiteStore) InsertBaselineDeploymentRecord(ctx context.Context, r DeploymentRecord) error {
	if r.Status != DeployStatusSucceeded {
		return fmt.Errorf("InsertBaselineDeploymentRecord: status must be SUCCEEDED, got %q", r.Status)
	}
	if r.DeploymentID == "" {
		return errors.New("InsertBaselineDeploymentRecord: DeploymentID empty")
	}
	if r.WorkerID == "" {
		return errors.New("InsertBaselineDeploymentRecord: WorkerID empty")
	}
	if r.TargetDigest == "" {
		return errors.New("InsertBaselineDeploymentRecord: TargetDigest empty")
	}
	finishedAt := r.FinishedAt
	if finishedAt == nil {
		finishedAt = &r.StartedAt
	}
	var previous interface{}
	if r.PreviousDigest != "" {
		previous = r.PreviousDigest
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
INSERT INTO deployment_records
	  (deployment_id, worker_id, previous_digest, target_digest, started_at, finished_at, status, error_message, applied_by, is_rollback)
VALUES (?, ?, ?, ?, ?, ?, ?, '', ?, 0)`,
		r.DeploymentID, r.WorkerID, previous, r.TargetDigest,
		r.StartedAt.UTC().Format(time.RFC3339), finishedAt.UTC().Format(time.RFC3339),
		r.Status, r.AppliedBy,
	)
	if err != nil {
		return err
	}
	if err := upsertDeploymentStateFromRecord(ctx, tx, r, finishedAt.UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

// UpdateDeploymentStatus transitions a row to a terminal status
// (SUCCEEDED, FAILED, ROLLED_BACK). `finishedAt` is required — the
// in-flight vs completed dashboard rendering relies on the row
// having a finished_at once status != PENDING.
func (s *SQLiteStore) UpdateDeploymentStatus(ctx context.Context, deploymentID, status string, finishedAt time.Time) error {
	switch status {
	case DeployStatusSucceeded, DeployStatusFailed, DeployStatusRolledBack:
	default:
		return fmt.Errorf("UpdateDeploymentStatus: status must be terminal, got %q", status)
	}
	return s.updateDeploymentTerminal(ctx, deploymentID, status, finishedAt, "", false)
}

func (s *SQLiteStore) updateDeploymentTerminal(ctx context.Context, deploymentID, status string, finishedAt time.Time, errMsg string, rollback bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	query := `UPDATE deployment_records SET status = ?, finished_at = ?, error_message = ?`
	args := []any{status, finishedAt.UTC().Format(time.RFC3339), errMsg}
	if rollback {
		query += `, is_rollback = 1`
	}
	query += ` WHERE deployment_id = ?`
	args = append(args, deploymentID)
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	n, err := readRowsAffected(res, "update deployment status")
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrDeploymentNotFound
	}
	record, err := getDeploymentRecordFrom(ctx, tx, deploymentID)
	if err != nil {
		return err
	}
	if err := upsertDeploymentStateFromRecord(ctx, tx, *record, finishedAt.UTC()); err != nil {
		return err
	}
	return tx.Commit()
}

// UpdateDeploymentRollbackFlag flips is_rollback on an existing
// row, used by Step 6's rollback path when it writes a new row
// that documents a rollback to previous_digest. Atomic with the
// deploy status transition if wrapped in a tx by the caller.
func (s *SQLiteStore) UpdateDeploymentRollbackFlag(ctx context.Context, deploymentID string, isRollback bool) error {
	res, err := s.db.ExecContext(ctx, `
UPDATE deployment_records SET is_rollback = ? WHERE deployment_id = ?`,
		boolToIntSQLite(isRollback), deploymentID,
	)
	if err != nil {
		return err
	}
	n, err := readRowsAffected(res, "update deployment rollback flag")
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrDeploymentNotFound
	}
	return nil
}

// MarkDeploymentRolledBack atomically transitions a row to the
// terminal ROLLED_BACK status AND sets is_rollback=true in a
// single UPDATE — Step 9/15 UpdateExecutor writes a SEPARATE
// row (status=PENDING, is_rollback=true from creation) for the
// rollback cascade, then transitions it on completion.
//
//	rollbackOK=true  → status=ROLLED_BACK (rollback finished
//	                   cleanly; the worker is back on
//	                   previous_digest).
//	rollbackOK=false → status=FAILED (rollback also failed;
//	                   operator intervention required; Health
//	                   derives ROLLBACK from is_rollback=true
//	                   in both cases so the operator always
//	                   sees the rollback attempt at-glance).
//
// The atomic UPDATE prevents a torn state where status was
// updated but is_rollback wasn't (or vice versa) which would
// silently make the row invisible to dashboard rollback views.
func (s *SQLiteStore) MarkDeploymentRolledBack(ctx context.Context, deploymentID string, finishedAt time.Time, rollbackOK bool) error {
	status := DeployStatusRolledBack
	if !rollbackOK {
		status = DeployStatusFailed
	}
	return s.updateDeploymentTerminal(ctx, deploymentID, status, finishedAt, "", true)
}

// GetLatestDeploymentForWorker returns the row with the highest
// started_at for the worker, regardless of status. Returns
// ErrDeploymentNotFound when no rows exist. Crucial for the
// dashboard's "what was this worker last asked to do" query.
func (s *SQLiteStore) GetLatestDeploymentForWorker(ctx context.Context, workerID string) (*DeploymentRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT deployment_id, worker_id, previous_digest, target_digest,
       started_at, finished_at, status, error_message, applied_by, is_rollback
FROM deployment_records
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
		return nil, ErrDeploymentNotFound
	}
	return scanDeploymentRecord(rows)
}

// GetLatestSuccessfulDeploymentForWorker returns the most recent verified
// deployment for the worker. It is intentionally separate from
// GetLatestDeploymentForWorker: a failed newer rollout must not erase the
// last digest that was actually accepted by the worker.
func (s *SQLiteStore) GetLatestSuccessfulDeploymentForWorker(ctx context.Context, workerID string) (*DeploymentRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT deployment_id, worker_id, previous_digest, target_digest,
       started_at, finished_at, status, error_message, applied_by, is_rollback
FROM deployment_records
WHERE worker_id = ? AND status = ?
ORDER BY started_at DESC
LIMIT 1`, workerID, DeployStatusSucceeded)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, ErrDeploymentNotFound
	}
	return scanDeploymentRecord(rows)
}

// ListDeploymentsForWorker returns up to `limit` rows in
// started_at DESC order. limit <= 0 means "no cap".
func (s *SQLiteStore) ListDeploymentsForWorker(ctx context.Context, workerID string, limit int) ([]DeploymentRecord, error) {
	q := `
SELECT deployment_id, worker_id, previous_digest, target_digest,
       started_at, finished_at, status, error_message, applied_by, is_rollback
FROM deployment_records
WHERE worker_id = ?
ORDER BY started_at DESC`
	args := []interface{}{workerID}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DeploymentRecord
	for rows.Next() {
		d, err := scanDeploymentRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *d)
	}
	return out, rows.Err()
}

// scanDeploymentRecord reads one row into a DeploymentRecord.
// Pulled out so the same SQL row → Go struct mapping happens in
// both GetLatest/List without drift. Tolerates empty started_at /
// finished_at because the schema lets finished_at be NULL.
func scanDeploymentRecord(rows *sql.Rows) (*DeploymentRecord, error) {
	var (
		r             DeploymentRecord
		startedAt     string
		finishedAt    sql.NullString
		isRollbackInt int
		errorMessage  string
		err           error
	)
	var previousDigest sql.NullString
	if err := rows.Scan(
		&r.DeploymentID, &r.WorkerID, &previousDigest, &r.TargetDigest,
		&startedAt, &finishedAt, &r.Status, &errorMessage, &r.AppliedBy, &isRollbackInt,
	); err != nil {
		return nil, err
	}
	r.PreviousDigest = previousDigest.String
	r.ErrorMessage = errorMessage
	r.StartedAt, err = parsePersistedWorkerTimestamp(startedAt, "deployment_records.started_at")
	if err != nil {
		return nil, err
	}
	if finishedAt.Valid && finishedAt.String != "" {
		parsed, err := parsePersistedWorkerTimestamp(finishedAt.String, "deployment_records.finished_at")
		if err != nil {
			return nil, err
		}
		r.FinishedAt = &parsed
	}
	r.IsRollback = isRollbackInt != 0
	return &r, nil
}

func (s *SQLiteStore) getDeploymentRecord(ctx context.Context, deploymentID string) (*DeploymentRecord, error) {
	return getDeploymentRecordFrom(ctx, s.db, deploymentID)
}

func getDeploymentRecordFrom(ctx context.Context, queryer deploymentStateQuerier, deploymentID string) (*DeploymentRecord, error) {
	rows, err := queryer.QueryContext(ctx, `
SELECT deployment_id, worker_id, previous_digest, target_digest,
       started_at, finished_at, status, error_message, applied_by, is_rollback
  FROM deployment_records WHERE deployment_id = ?`, deploymentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, ErrDeploymentNotFound
	}
	return scanDeploymentRecord(rows)
}

// boolToIntSQLite returns 1 if b is true, 0 otherwise — the
// canonical encoding for SQLite's INTEGER-as-BOOLEAN dialect.
func boolToIntSQLite(b bool) int {
	if b {
		return 1
	}
	return 0
}
