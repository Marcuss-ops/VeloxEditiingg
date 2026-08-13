package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// store_fleet_operations_insert.go owns the fleet_operations insert path:
// InsertOperation (status=QUEUED) and the SQL-level in-flight conflict
// classifier. Terminal transitions live in store_fleet_operations_transition.go.

// InsertOperation persists a new operation row, status=QUEUED.
// Translates the partial-UNIQUE-INDEX conflict to
// ErrOperationInFlight so the API layer has a clean sentinel
// to surface to the operator UI (no raw SQLITE error strings
// leak into the response).
//
// Validation runs eagerly:
//   - Status MUST be QUEUED (terminal statuses confuse the
//     in-flight de-dup contract — they would lift the partial
//     UNIQUE immediately, allowing a duplicate re-issue).
//   - OperationID, WorkerID, Op, RequestedBy, Reason MUST be
//     non-empty (mirror of the schema CHECK constraints; eager
//     rejection gives the caller a clearer error than a SQL
//     CHECK violation).
//   - Payload is normalised to "{}" when nil/empty. The schema
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
// violations to the in-flight (worker_id, op) case. SQLite
// produces "UNIQUE constraint failed: fleet_operations.worker_id,
// fleet_operations.op".
//
// The check pins on two substrings: the index name
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
