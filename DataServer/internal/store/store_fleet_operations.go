// store_fleet_operations.go owns the fleet_operations ledger model: the
// status constants, the sentinel errors, the Operation row shape and the
// schema bootstrap. Inserts live in store_fleet_operations_insert.go,
// terminal transitions in store_fleet_operations_transition.go, and reads
// in store_fleet_operations_query.go.
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
package store

import (
	"encoding/json"
	"errors"
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

// ErrOperationConcurrentTransition is returned by the fleet_operations
// transition API when the fenced UPDATE matches zero rows even though the
// row was found a moment earlier in the same transaction — a concurrent
// writer moved the row between our read and our write. The row EXISTS (so
// this is NOT ErrOperationNotFound); the transition is refused rather than
// clobbering the other writer's terminal outcome.
var ErrOperationConcurrentTransition = errors.New("fleet operation state machine: concurrent transition")

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
// runs via the migration runner on boot.
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
