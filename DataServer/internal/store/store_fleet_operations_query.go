package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

// store_fleet_operations_query.go owns the fleet_operations read paths
// (list queued / list audit / get by ID) and the shared row scanner used by
// both the read paths and the terminal transition read-first step.

// ListQueuedOperations returns up to `limit` rows with status=QUEUED,
// oldest first (the tick path processes the queue in FIFO order
// so an admin's "drain now, then update 5s later" doesn't get
// answered in reverse). limit <= 0 means "no cap".
func (s *SQLiteStore) ListQueuedOperations(ctx context.Context, limit int) ([]Operation, error) {
	q := `SELECT operation_id, worker_id, op, requested_by, reason, status,
                 queued_at, started_at, finished_at, payload, error_message
          FROM fleet_operations
          WHERE status = ?
          ORDER BY queued_at ASC`
	args := []interface{}{OperationStatusQueued}
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Operation
	for rows.Next() {
		op, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *op)
	}
	return out, rows.Err()
}

// ListOperations is the audit-query path. Filters by workerID
// ("" == any) and statusFilter ("" == any). Sort queued_at DESC
// (newest first) so the dashboard's "show recent ops" view is
// the default. limit <= 0 means "no cap".
func (s *SQLiteStore) ListOperations(ctx context.Context, workerID, statusFilter string, limit int) ([]Operation, error) {
	q := `SELECT operation_id, worker_id, op, requested_by, reason, status,
                 queued_at, started_at, finished_at, payload, error_message
          FROM fleet_operations
          WHERE 1=1`
	args := []interface{}{}
	if workerID != "" {
		q += " AND worker_id = ?"
		args = append(args, workerID)
	}
	if statusFilter != "" {
		q += " AND status = ?"
		args = append(args, statusFilter)
	}
	q += " ORDER BY queued_at DESC"
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Operation
	for rows.Next() {
		op, err := scanOperation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *op)
	}
	return out, rows.Err()
}

// GetOperation fetches one row by operation_id. Returns
// ErrOperationNotFound on miss.
func (s *SQLiteStore) GetOperation(ctx context.Context, operationID string) (*Operation, error) {
	return getOperationFrom(ctx, s.db, operationID)
}

// operationStateQuerier is the read seam shared by getOperationFrom and the
// transactional transition path (a *sql.Tx satisfies it).
type operationStateQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// getOperationFrom reads one operation row through an arbitrary querier
// (store DB handle or an open transaction), so the transition API can read
// the current status inside the same tx that persists the change.
func getOperationFrom(ctx context.Context, queryer operationStateQuerier, operationID string) (*Operation, error) {
	rows, err := queryer.QueryContext(ctx, `
SELECT operation_id, worker_id, op, requested_by, reason, status,
       queued_at, started_at, finished_at, payload, error_message
FROM fleet_operations
WHERE operation_id = ?`, operationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, ErrOperationNotFound
	}
	return scanOperation(rows)
}

// scanOperation reads one row into an Operation. Tolerates
// NULL on the optional started_at / finished_at / error_message
// columns (the schema lets all three be NULL). payload is the
// marshalled JSON executor input — kept as json.RawMessage so
// the audit endpoint's JSON envelope round-trips it unchanged.
func scanOperation(rows *sql.Rows) (*Operation, error) {
	var (
		op         Operation
		queuedAt   string
		startedAt  sql.NullString
		finishedAt sql.NullString
		payload    string
		errMsg     sql.NullString
	)
	if err := rows.Scan(
		&op.OperationID, &op.WorkerID, &op.Op, &op.RequestedBy, &op.Reason,
		&op.Status, &queuedAt, &startedAt, &finishedAt, &payload, &errMsg,
	); err != nil {
		return nil, err
	}
	var err error
	op.QueuedAt, err = parsePersistedWorkerTimestamp(queuedAt, "fleet_operations.queued_at")
	if err != nil {
		return nil, err
	}
	if startedAt.Valid && startedAt.String != "" {
		parsed, err := parsePersistedWorkerTimestamp(startedAt.String, "fleet_operations.started_at")
		if err != nil {
			return nil, err
		}
		op.StartedAt = &parsed
	}
	if finishedAt.Valid && finishedAt.String != "" {
		parsed, err := parsePersistedWorkerTimestamp(finishedAt.String, "fleet_operations.finished_at")
		if err != nil {
			return nil, err
		}
		op.FinishedAt = &parsed
	}
	if payload != "" {
		if !json.Valid([]byte(payload)) {
			return nil, fmt.Errorf("fleet_operations.payload is invalid JSON")
		}
		op.Payload = json.RawMessage(payload)
	}
	if errMsg.Valid {
		op.ErrorMessage = errMsg.String
	}
	return &op, nil
}
