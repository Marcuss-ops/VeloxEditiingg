package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// store_deployment_records_transition.go owns every terminal
// deployment_records transition. updateDeploymentTerminal is the SINGLE
// write path for terminal rows; UpdateDeploymentStatus, MarkVerifiedSucceeded
// and MarkDeploymentRolledBack all funnel through it (MarkVerifiedSucceeded
// additionally verifies the observed digest against the row's target before
// advancing last_successful_digest).

// UpdateDeploymentStatus transitions a row to a terminal status
// (SUCCEEDED, FAILED, ROLLED_BACK). `finishedAt` is required — the
// in-flight vs completed dashboard rendering relies on the row
// having a finished_at once status != PENDING.
//
// The target-status check delegates to the canonical state machine
// (IsDeploymentStatusTerminal); the from-status enforcement lives in
// updateDeploymentTerminal via ValidateDeploymentTransition.
func (s *SQLiteStore) UpdateDeploymentStatus(ctx context.Context, deploymentID, status string, finishedAt time.Time) error {
	if !IsDeploymentStatusTerminal(status) {
		return fmt.Errorf("UpdateDeploymentStatus: status must be terminal, got %q", status)
	}
	return s.updateDeploymentTerminal(ctx, deploymentID, status, finishedAt, "", "", false)
}

// updateDeploymentTerminal is the SINGLE write path for every terminal
// deployment_records transition. It enforces the canonical deployment
// state machine (store_state_machine.go):
//
//  1. read the current row inside the transaction,
//  2. validate `current → status` via ValidateDeploymentTransition — a
//     terminal row (SUCCEEDED / FAILED / ROLLED_BACK) can never be
//     resurrected, even into a different terminal status,
//  3. fence the UPDATE with the observed from-state so a concurrent
//     transition between our read and our write cannot be clobbered,
//  4. project the NEW status into the worker_deployment_state read model
//     in the same transaction.
//
// errCode is the stable machine-routable failure code (empty for successful
// transitions); errMsg the human-readable message. Both are written to the
// journal row AND projected into the read model's last_operation_error_code /
// last_operation_error.
//
// The read-model upsert uses the in-memory post-transition record (status /
// error / is_rollback applied to the pre-transition row), so the projection
// always reflects exactly what the ledger just persisted — no second read, no
// torn journal-vs-projection state.
func (s *SQLiteStore) updateDeploymentTerminal(ctx context.Context, deploymentID, status string, finishedAt time.Time, errCode, errMsg string, rollback bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Read-first: the canonical machine needs the CURRENT status. Missing
	// rows surface ErrDeploymentNotFound before any write is attempted.
	record, err := getDeploymentRecordFrom(ctx, tx, deploymentID)
	if err != nil {
		return err
	}
	if err := ValidateDeploymentTransition(record.Status, status); err != nil {
		return err
	}

	query := `UPDATE deployment_records SET status = ?, finished_at = ?, error_code = ?, error_message = ?`
	args := []any{status, finishedAt.UTC().Format(time.RFC3339), errCode, errMsg}
	if rollback {
		query += `, is_rollback = 1`
	}
	// Fencing: re-check the from-state in the WHERE clause. RowsAffected==0
	// here means the row moved between the read and the write (concurrent
	// writer) — fail closed instead of overwriting a terminal outcome.
	query += ` WHERE deployment_id = ? AND status = ?`
	args = append(args, deploymentID, record.Status)
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	n, err := readRowsAffected(res, "update deployment status")
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: deployment %s moved concurrently during terminal transition", ErrDeploymentConcurrentTransition, deploymentID)
	}

	record.Status = status
	record.FinishedAt = &finishedAt
	record.ErrorCode = errCode
	record.ErrorMessage = errMsg
	if rollback {
		record.IsRollback = true
	}
	// ROLLED_BACK restores a previously verified digest, so it advances
	// last_successful_digest; generic SUCCEEDED and FAILED do not.
	if err := upsertDeploymentStateFromRecord(ctx, tx, *record, finishedAt.UTC(), status == DeployStatusRolledBack); err != nil {
		return err
	}
	return tx.Commit()
}

// MarkVerifiedSucceeded is the ONLY path that advances last_successful_digest
// for a forward rollout. It is the store-side enforcement of the
// VERIFYING_DIGEST phase:
//
//  1. reads the row inside a transaction (PENDING required by the canonical
//     deployment machine),
//  2. verifies the authenticated observed digest equals the record's target
//     digest — on mismatch returns ErrDeploymentDigestMismatch and applies NO
//     transition,
//  3. fences the PENDING → SUCCEEDED UPDATE with the observed from-state,
//  4. projects the terminal SUCCEEDED state into worker_deployment_state WITH
//     last_successful_digest advanced to the verified target.
//
// The generic UpdateDeploymentStatus(SUCCEEDED) path no longer advances
// last_successful_digest: an unverified success must never become the
// last-known-good digest. running_digest is never touched here — it is
// written only by authenticated heartbeats.
func (s *SQLiteStore) MarkVerifiedSucceeded(ctx context.Context, deploymentID, observedDigest string, finishedAt time.Time) error {
	if strings.TrimSpace(observedDigest) == "" {
		return errors.New("MarkVerifiedSucceeded: observed digest empty (cannot verify)")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Read-first: the canonical machine needs the CURRENT status, and the
	// digest check needs the row's target.
	record, err := getDeploymentRecordFrom(ctx, tx, deploymentID)
	if err != nil {
		return err
	}
	if err := ValidateDeploymentTransition(record.Status, DeployStatusSucceeded); err != nil {
		return err
	}
	if !DigestRefsEqual(observedDigest, record.TargetDigest) {
		return fmt.Errorf("%w: expected=%s observed=%s", ErrDeploymentDigestMismatch, record.TargetDigest, observedDigest)
	}

	res, err := tx.ExecContext(ctx, `
UPDATE deployment_records
SET status = ?, finished_at = ?, error_code = '', error_message = ''
WHERE deployment_id = ? AND status = ?`,
		DeployStatusSucceeded, finishedAt.UTC().Format(time.RFC3339), deploymentID, record.Status)
	if err != nil {
		return err
	}
	n, err := readRowsAffected(res, "mark verified deployment succeeded")
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: deployment %s moved concurrently during verified transition", ErrDeploymentConcurrentTransition, deploymentID)
	}

	record.Status = DeployStatusSucceeded
	record.FinishedAt = &finishedAt
	record.ErrorCode = ""
	record.ErrorMessage = ""
	if err := upsertDeploymentStateFromRecord(ctx, tx, *record, finishedAt.UTC(), true); err != nil {
		return err
	}
	return tx.Commit()
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
func (s *SQLiteStore) MarkDeploymentRolledBack(ctx context.Context, deploymentID string, finishedAt time.Time, rollbackOK bool, errCode string) error {
	status := DeployStatusRolledBack
	if !rollbackOK {
		status = DeployStatusFailed
	}
	return s.updateDeploymentTerminal(ctx, deploymentID, status, finishedAt, errCode, "", true)
}
