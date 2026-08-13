package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// store_fleet_operations_transition.go owns every fleet_operations
// terminal transition. transitionOperation is the SINGLE transactional
// write path; MarkRunning, MarkSucceeded and MarkFailed all funnel through
// it.

// MarkRunning transitions QUEUED → RUNNING, atomically. Routes through the
// single transactional transition API (transitionOperation) so the claim
// is validated against the canonical operation machine and fenced with
// RowsAffected.
//
// The bool reports whether this call actually claimed the row. A false
// result is a guarded no-op — the row was already RUNNING (idempotent
// duplicate tick-call, e.g. after a controller restart) or already
// terminal — and the caller MUST NOT execute the external executor. A
// MISSING row is not a no-op: it surfaces ErrOperationNotFound (fail-loud,
// the old WHERE-guard silently swallowed it).
func (s *SQLiteStore) MarkRunning(ctx context.Context, operationID string, startedAt time.Time) (bool, error) {
	changed, err := s.transitionOperation(ctx, operationID, OperationStatusRunning, &startedAt, nil, "")
	if err != nil && errors.Is(err, ErrIllegalOperationTransition) {
		// The canonical machine only lets a QUEUED row claim RUNNING. An
		// already-RUNNING row is the idempotent case (handled inside
		// transitionOperation as a no-op); an already-terminal row is the
		// documented guarded no-op — another dispatcher owns the outcome,
		// never replay the external executor.
		return false, nil
	}
	return changed, err
}

// MarkSucceeded transitions RUNNING → SUCCEEDED, capturing finished_at and
// clearing error_message. Idempotent under double-call: a row already in
// SUCCEEDED is a no-op. A terminal row in any OTHER state (FAILED) is
// rejected with ErrIllegalOperationTransition — no resurrection.
func (s *SQLiteStore) MarkSucceeded(ctx context.Context, operationID string, finishedAt time.Time) error {
	_, err := s.transitionOperation(ctx, operationID, OperationStatusSucceeded, nil, &finishedAt, "")
	return err
}

// MarkFailed transitions RUNNING → FAILED, capturing finished_at +
// error_message. errMsg MUST be non-empty (otherwise the audit dashboard
// cannot tell a failed-with-no-log from a successful-completion); the
// fallback string is synthesised when the caller passes "". Idempotent
// under double-call; a terminal row in any OTHER state (SUCCEEDED) is
// rejected with ErrIllegalOperationTransition — no resurrection.
func (s *SQLiteStore) MarkFailed(ctx context.Context, operationID string, finishedAt time.Time, errMsg string) error {
	if errMsg == "" {
		errMsg = "executor returned an error (no detail provided)"
	}
	_, err := s.transitionOperation(ctx, operationID, OperationStatusFailed, nil, &finishedAt, errMsg)
	return err
}

// transitionOperation is the SINGLE transactional transition API for
// fleet_operations rows. Every Mark* method routes through it. It:
//
//  1. reads the current row inside a transaction,
//  2. treats an already-in-target-state row as an idempotent no-op
//     (double-call safety for the controller's terminal-persist retries),
//  3. validates `current → to` via the canonical operation machine
//     (store_state_machine.go) — a terminal row can never be resurrected
//     and a QUEUED row can never jump straight to a terminal status,
//  4. fences the UPDATE with the observed from-state and checks
//     RowsAffected so a concurrent transition cannot be clobbered,
//  5. commits only when every step succeeded.
//
// `changed` reports whether the row actually moved. Errors: missing row →
// ErrOperationNotFound; illegal transition → ErrIllegalOperationTransition;
// fence miss → ErrOperationConcurrentTransition.
func (s *SQLiteStore) transitionOperation(ctx context.Context, operationID, to string, startedAt, finishedAt *time.Time, errMsg string) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	// Read-first: the canonical machine needs the CURRENT status. Missing
	// rows surface ErrOperationNotFound before any write is attempted.
	op, err := getOperationFrom(ctx, tx, operationID)
	if err != nil {
		return false, err
	}
	if op.Status == to {
		// Idempotent: the row is already in the requested state.
		return false, nil
	}
	if err := ValidateOperationTransition(op.Status, to); err != nil {
		return false, err
	}

	query := `UPDATE fleet_operations SET status = ?`
	args := []any{to}
	if startedAt != nil {
		query += `, started_at = ?`
		args = append(args, startedAt.UTC().Format(time.RFC3339))
	}
	if finishedAt != nil {
		query += `, finished_at = ?`
		args = append(args, finishedAt.UTC().Format(time.RFC3339))
	}
	if errMsg == "" {
		query += `, error_message = NULL`
	} else {
		query += `, error_message = ?`
		args = append(args, errMsg)
	}
	// Fencing: re-check the from-state in the WHERE clause. RowsAffected==0
	// here means the row moved between the read and the write (concurrent
	// writer) — fail closed instead of overwriting a terminal outcome.
	query += ` WHERE operation_id = ? AND status = ?`
	args = append(args, operationID, op.Status)
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return false, err
	}
	n, err := readRowsAffected(res, "transition fleet operation")
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, fmt.Errorf("%w: operation %s moved concurrently during transition", ErrOperationConcurrentTransition, operationID)
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
