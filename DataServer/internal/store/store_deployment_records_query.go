package store

import (
	"context"
	"database/sql"
)

// store_deployment_records_query.go owns the deployment_records read
// paths (latest / latest-successful / list) and the shared row scanner
// used by both the read paths and the terminal transition read-first step.

// GetLatestDeploymentForWorker returns the row with the highest
// started_at for the worker, regardless of status. Returns
// ErrDeploymentNotFound when no rows exist. Crucial for the
// dashboard's "what was this worker last asked to do" query.
func (s *SQLiteStore) GetLatestDeploymentForWorker(ctx context.Context, workerID string) (*DeploymentRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT deployment_id, worker_id, previous_digest, target_digest,
       started_at, finished_at, status, error_code, error_message, applied_by, is_rollback
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
       started_at, finished_at, status, error_code, error_message, applied_by, is_rollback
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
       started_at, finished_at, status, error_code, error_message, applied_by, is_rollback
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
		errorCode     string
		errorMessage  string
		err           error
	)
	var previousDigest sql.NullString
	if err := rows.Scan(
		&r.DeploymentID, &r.WorkerID, &previousDigest, &r.TargetDigest,
		&startedAt, &finishedAt, &r.Status, &errorCode, &errorMessage, &r.AppliedBy, &isRollbackInt,
	); err != nil {
		return nil, err
	}
	r.PreviousDigest = previousDigest.String
	r.ErrorCode = errorCode
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
       started_at, finished_at, status, error_code, error_message, applied_by, is_rollback
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
