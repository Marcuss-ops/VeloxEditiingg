// Package store — fleet_operations ledger (Step 4/15 fleet-operator
// rollout).
//
// The fleet_operations table tracks async admin mutations against
// the worker fleet. Each row is the durable audit trail of one
// publish ("this admin queued drain on worker W at time T") plus
// its terminal outcome ("SUCCEEDED at T2 / FAILED with error E").
// The FleetController tick goroutine drains QUEUED → RUNNING via
// the OperationExecutor → SUCCEEDED / FAILED.
//
// In-flight idempotency is enforced at the DB layer by a partial
// UNIQUE INDEX (idx_fleet_ops_worker_op_inflight) on (worker_id,
// op) WHERE status IN ('QUEUED','RUNNING'). Re-issuing the same
// operation while a previous one is still in-flight is rejected
// with ErrOperationInFlight — the operator UI must wait for the
// prior run to terminate.
//
// Repository shape mirrors store_deployment_records.go:
//   * Operation struct mirrors the SQL columns 1:1 (json.RawMessage
//     payload, *time.Time for optional started_at/finished_at).
//   * CRUD + sentinel errors + boolToIntSQLite-style helpers are
//     not needed here because the schema uses TEXT for booleans.
//
// This file is scope-tight to Step 4/15. Future steps that wire
// concrete OperationExecutors (Ansible, SSH, etc.) live in the
// internal/fleet/ package and only touch the ExecutorRegistry
// surface — the storage layer stays untouched.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// OperationStatus* mirrors the schema CHECK constraint. The
// string values are the canonical vocabulary on every
// boundary (DB, JSON envelope, audit log line).
const (
	OperationStatusQueued    = "QUEUED"
	OperationStatusRunning   = "RUNNING"
	OperationStatusSucceeded = "SUCCEEDED"
	OperationStatusFailed    = "FAILED"
)

// ErrOperationNotFound is returned by GetOperation when the
// operation_id matches no row. Maps to a 404 at the API layer;
// the caller MUST distinguish "unknown operation" from
// "operation not yet seen" (audit dashboard) from "operation in
// flight" (ErrOperationInFlight) — the messaging differs and
// the operator copy is conditional on the sentinel.
var ErrOperationNotFound = errors.New("fleet_operations: operation not found")

// ErrOperationInFlight is returned by InsertOperation when the
// (worker_id, op) partial-UNIQUE-INDEX in-flight constraint
// trips. The operator UI must surface "drain is already pending
// or running on this worker — wait for it to terminate before
// re-issuing". Future steps gate retry buttons on this sentinel.
var ErrOperationInFlight = errors.New("fleet_operations: operation for (worker_id, op) is already in-flight (QUEUED or RUNNING)")

// Operation is one row of the fleet_operations ledger. Payload
// is op-dependent JSON (empty object "{}" when none was given),
// so the executor receives a stable contract regardless of op
// kind (future steps narrow it via Go type assertions inside
// the executor's Execute).
//
// Time semantics: QueuedAt is always set (the row exists, so
// it has a creation timestamp); StartedAt and FinishedAt are
// *time.Time so the omitempty JSON contract drops the field
// when the column is NULL (a row that hasn't started yet).
type Operation struct {
	OperationID  string          `json:"operation_id"`
	WorkerID     string          `json:"worker_id"`
	Op           string          `json:"op"`
	RequestedBy  string          `json:"requested_by"`
	Reason       string          `json:"reason"`
	Status       string          `json:"status"`
	QueuedAt     time.Time       `json:"queued_at"`
	StartedAt    *time.Time      `json:"started_at,omitempty"`
	FinishedAt   *time.Time      `json:"finished_at,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	ErrorMessage string          `json:"error_message,omitempty"`
}

// CreateFleetOperationsTableIfNotExists is the in-test bootstrap
// path. The production migrations/sqlite/104_fleet_operations.sql
// (and the postgres/014 variant) run via the migration runner on
// boot.
//
// Idempotent: safe to call repeatedly. CREATE TABLE / CREATE INDEX
// IF NOT EXISTS forms. Does NOT insert into schema_migrations —
// the migration runner's checksum tracking stays the source of
// truth; this function exists so unit tests against an in-memory
// SQLite can stand up the table without a full migration sweep.
//
// The DDL mirrors migrations/sqlite/104_fleet_operations.sql
// exactly. Defence-in-depth against INSERT bugs that bypass
// InsertOperation validation.
func (s *SQLiteStore) CreateFleetOperationsTableIfNotExists() error {
	ddl := `
CREATE TABLE IF NOT EXISTS fleet_operations (
    operation_id  TEXT PRIMARY KEY,
    worker_id     TEXT NOT NULL
        CHECK (length(worker_id) > 0),
    op            TEXT NOT NULL
        CHECK (op IN ('drain', 'resume', 'restart', 'update', 'rollback', 'quarantine', 'smoke')),
    requested_by  TEXT NOT NULL
        CHECK (length(requested_by) > 0),
    reason        TEXT NOT NULL
        CHECK (length(reason) > 0),
    status        TEXT NOT NULL
        CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    queued_at     TEXT NOT NULL,
    started_at    TEXT,
    finished_at   TEXT,
    payload       TEXT NOT NULL
        CHECK (length(payload) > 0),
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_fleet_ops_worker
    ON fleet_operations(worker_id, queued_at DESC);

CREATE INDEX IF NOT EXISTS idx_fleet_ops_status
    ON fleet_operations(status, queued_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_fleet_ops_worker_op_inflight
    ON fleet_operations(worker_id, op)
    WHERE status IN ('QUEUED', 'RUNNING');
`
	_, err := s.db.Exec(ddl)
	return err
}

// InsertOperation persists a new operation row, status=QUEUED.
// Translates the partial-UNIQUE-INDEX conflict to
// ErrOperationInFlight so the API layer has a clean sentinel
// to surface to the operator UI (no raw SQLITE / POSTGRES error
// strings leak into the response).
//
// Validation runs eagerly:
//   * Status MUST be QUEUED (terminal statuses confuse the
//     in-flight de-dup contract — they would lift the partial
//     UNIQUE immediately, allowing a duplicate re-issue).
//   * OperationID, WorkerID, Op, RequestedBy, Reason MUST be
//     non-empty (mirror of the schema CHECK constraints; eager
//     rejection gives the caller a clearer error than a SQL
//     CHECK violation).
//   * Payload is normalised to "{}" when nil/empty. The schema
//     CHECK refuses an empty string, but a JSON `{}` is the
//     canonical no-args marker an executor receives; this lets
//     callers omit EmptyObject boilerplate.
func (s *SQLiteStore) InsertOperation(ctx context.Context, op *Operation) error {
	if op == nil {
		return errors.New("InsertOperation: op is nil")
	}
	if op.Status != OperationStatusQueued {
		return fmt.Errorf("InsertOperation: initial status must be QUEUED, got %q", op.Status)
	}
	if op.OperationID == "" {
		return errors.New("InsertOperation: OperationID empty")
	}
	if op.WorkerID == "" {
		return errors.New("InsertOperation: WorkerID empty")
	}
	if op.Op == "" {
		return errors.New("InsertOperation: Op empty")
	}
	if op.RequestedBy == "" {
		return errors.New("InsertOperation: RequestedBy empty")
	}
	if op.Reason == "" {
		return errors.New("InsertOperation: Reason empty")
	}

	payload := op.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("{}")
	}

	_, err := s.db.ExecContext(ctx, `
INSERT INTO fleet_operations
  (operation_id, worker_id, op, requested_by, reason, status, queued_at, payload)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		op.OperationID, op.WorkerID, op.Op, op.RequestedBy, op.Reason,
		op.Status, op.QueuedAt.UTC().Format(time.RFC3339),
		string(payload),
	)
	if err != nil {
		if isInflightUniqueConflict(err) {
			return ErrOperationInFlight
		}
		return err
	}
	return nil
}

// isInflightUniqueConflict narrows SQL-level UNIQUE-INDEX
// violations to the in-flight (worker_id, op) case. Two engines
// produce different messages:
//
//   - SQLite:  "UNIQUE constraint failed: fleet_operations.worker_id, fleet_operations.op"
//   - Postgres: "duplicate key value violates unique constraint
//     \"idx_fleet_ops_worker_op_inflight\""
//
// Cross-dialect safety pins on two substrings: the index name
// (post-resolve) AND the (worker_id, op) column pair (when the
// driver suppresses the index name). Both fire only on the
// in-flight conflict — operation_id PK collisions happen at the
// driver layer without surfacing these substrings.
func isInflightUniqueConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if strings.Contains(msg, "idx_fleet_ops_worker_op_inflight") {
		return true
	}
	if strings.Contains(msg, "fleet_operations.worker_id") &&
		strings.Contains(msg, "fleet_operations.op") {
		return true
	}
	return false
}

// MarkRunning transitions QUEUED → RUNNING, atomically. The
// WHERE status='QUEUED' guard matches at most once per row, so
// a duplicate tick-call (e.g. after a controller restart) is a
// silent no-op rather than a destructive overwrite — the row
// stays in its current state.
//
// Returns nil on success AND on no-op (the row was no longer
// QUEUED when the update ran). The caller does not need to
// disambiguate; both cases are "ready to proceed to the next
// transition check".
func (s *SQLiteStore) MarkRunning(ctx context.Context, operationID string, startedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE fleet_operations
SET status = ?, started_at = ?
WHERE operation_id = ? AND status = ?`,
		OperationStatusRunning,
		startedAt.UTC().Format(time.RFC3339),
		operationID,
		OperationStatusQueued,
	)
	return err
}

// MarkSucceeded transitions RUNNING → SUCCEEDED, capturing
// finished_at. Idempotent under double-call (guard on
// status='RUNNING'); if the row is already terminal the call is
// a no-op.
func (s *SQLiteStore) MarkSucceeded(ctx context.Context, operationID string, finishedAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE fleet_operations
SET status = ?, finished_at = ?
WHERE operation_id = ? AND status = ?`,
		OperationStatusSucceeded,
		finishedAt.UTC().Format(time.RFC3339),
		operationID,
		OperationStatusRunning,
	)
	return err
}

// MarkFailed transitions RUNNING → FAILED, capturing
// finished_at + error_message. errMsg MUST be non-empty
// (otherwise the audit dashboard cannot tell a failed-with-no-
// log from a successful-completion).
func (s *SQLiteStore) MarkFailed(ctx context.Context, operationID string, finishedAt time.Time, errMsg string) error {
	if errMsg == "" {
		errMsg = "executor returned an error (no detail provided)"
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE fleet_operations
SET status = ?, finished_at = ?, error_message = ?
WHERE operation_id = ? AND status = ?`,
		OperationStatusFailed,
		finishedAt.UTC().Format(time.RFC3339),
		errMsg,
		operationID,
		OperationStatusRunning,
	)
	return err
}

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
	rows, err := s.db.QueryContext(ctx, `
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
	if queuedAt != "" {
		if t, e := time.Parse(time.RFC3339, queuedAt); e == nil {
			op.QueuedAt = t
		}
	}
	if startedAt.Valid && startedAt.String != "" {
		if t, e := time.Parse(time.RFC3339, startedAt.String); e == nil {
			op.StartedAt = &t
		}
	}
	if finishedAt.Valid && finishedAt.String != "" {
		if t, e := time.Parse(time.RFC3339, finishedAt.String); e == nil {
			op.FinishedAt = &t
		}
	}
	if payload != "" {
		op.Payload = json.RawMessage(payload)
	}
	if errMsg.Valid {
		op.ErrorMessage = errMsg.String
	}
	return &op, nil
}
