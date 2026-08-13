package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"velox-server/internal/publicationstate"
)

// store_publication_effect.go owns the publication_phase_effects side-effect
// ledger: the idempotent BEGIN/RUNNING→SUCCEEDED/FAILED lifecycle, the
// reconciliation evidence recording, and the read/validation surfaces.

func (s *SQLiteStore) BeginPublicationPhaseEffect(ctx context.Context, publicationID string, phase publicationstate.State, operation string) (key string, already bool, err error) {
	publicationID = strings.TrimSpace(publicationID)
	operation = strings.TrimSpace(operation)
	if publicationID == "" || phase == "" || operation == "" {
		return "", false, fmt.Errorf("publication phase effect: publication, phase and operation are required")
	}
	key = publicationstate.SideEffectKey(publicationID, phase, operation)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, wrapDBInfrastructure("BeginPublicationPhaseEffect begin", err)
	}
	defer tx.Rollback()
	now := nowRFC3339()
	result, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO publication_phase_effects
		(publication_id, phase, operation, idempotency_key, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, 'RUNNING', ?, ?)`, publicationID, phase, operation, key, now, now)
	if err != nil {
		return "", false, wrapDBInfrastructure("BeginPublicationPhaseEffect exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "BeginPublicationPhaseEffect insert")
	if rowsErr != nil {
		return "", false, rowsErr
	}
	if phase == publicationstate.Verifying {
		result, err = tx.ExecContext(ctx, `
			UPDATE publication_states SET verification_operation = ?
			WHERE publication_id = ? AND verification_operation = ''`, operation, publicationID)
		if err != nil {
			return "", false, wrapDBInfrastructure("BeginPublicationPhaseEffect verification operation", err)
		}
		updated, rowsErr := readRowsAffected(result, "BeginPublicationPhaseEffect verification operation")
		if rowsErr != nil {
			return "", false, rowsErr
		}
		if updated == 0 {
			var existing string
			if err := tx.QueryRowContext(ctx, `SELECT COALESCE(verification_operation, '') FROM publication_states WHERE publication_id = ?`, publicationID).Scan(&existing); err != nil {
				return "", false, wrapDBInfrastructure("BeginPublicationPhaseEffect verification lookup", err)
			}
			if existing != operation {
				return "", false, fmt.Errorf("%w: verification operation already bound to %q", ErrPublicationPhaseConflict, existing)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return "", false, wrapDBInfrastructure("BeginPublicationPhaseEffect commit", err)
	}
	return key, affected == 0, nil
}

// CompletePublicationReconciliationEffect atomically records the positive
// reconciliation evidence and the succeeded VERIFYING effect. No caller can
// expose reconciliation_verified=1 without the matching SUCCEEDED effect.
func (s *SQLiteStore) CompletePublicationReconciliationEffect(ctx context.Context, publicationID, operation string) error {
	publicationID = strings.TrimSpace(publicationID)
	operation = strings.TrimSpace(operation)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return wrapDBInfrastructure("CompletePublicationReconciliationEffect begin", err)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `UPDATE publication_states SET reconciliation_verified = 1 WHERE publication_id = ? AND state = 'VERIFYING' AND verification_operation = ? AND reconciliation_verified = 0 AND TRIM(remote_id) <> '' AND TRIM(submitted_remote_id) <> '' AND remote_id <> submitted_remote_id`, publicationID, operation)
	if err != nil {
		return wrapDBInfrastructure("CompletePublicationReconciliationEffect state", err)
	}
	affected, rowsErr := readRowsAffected(result, "CompletePublicationReconciliationEffect state")
	if rowsErr != nil {
		return rowsErr
	}
	if affected != 1 {
		return ErrPublicationPhaseConflict
	}
	result, err = tx.ExecContext(ctx, `UPDATE publication_phase_effects SET status='SUCCEEDED', error_code=NULL, updated_at=? WHERE publication_id=? AND phase='VERIFYING' AND operation=? AND status='RUNNING'`, nowRFC3339(), publicationID, operation)
	if err != nil {
		return wrapDBInfrastructure("CompletePublicationReconciliationEffect effect", err)
	}
	affected, rowsErr = readRowsAffected(result, "CompletePublicationReconciliationEffect effect")
	if rowsErr != nil {
		return rowsErr
	}
	if affected != 1 {
		return ErrPublicationPhaseConflict
	}
	if err := tx.Commit(); err != nil {
		return wrapDBInfrastructure("CompletePublicationReconciliationEffect commit", err)
	}
	return nil
}

func (s *SQLiteStore) CompletePublicationPhaseEffect(ctx context.Context, publicationID string, phase publicationstate.State, operation string, success bool, errorCode string) error {
	status := "FAILED"
	if success {
		status = "SUCCEEDED"
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE publication_phase_effects
		SET status = ?, error_code = NULLIF(?, ''), updated_at = ?
		WHERE publication_id = ? AND phase = ? AND operation = ?`,
		status, strings.TrimSpace(errorCode), nowRFC3339(),
		strings.TrimSpace(publicationID), phase, strings.TrimSpace(operation))
	if err != nil {
		return wrapDBInfrastructure("CompletePublicationPhaseEffect exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "CompletePublicationPhaseEffect")
	if rowsErr != nil {
		return rowsErr
	}
	if affected != 1 {
		return ErrPublicationPhaseConflict
	}
	return nil
}

func (s *SQLiteStore) RetryPublicationPhaseEffect(ctx context.Context, publicationID string, phase publicationstate.State, operation string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE publication_phase_effects
		SET status = 'RUNNING', error_code = NULL, updated_at = ?
		WHERE publication_id = ? AND phase = ? AND operation = ? AND status = 'FAILED'`,
		nowRFC3339(), strings.TrimSpace(publicationID), phase, strings.TrimSpace(operation))
	if err != nil {
		return wrapDBInfrastructure("RetryPublicationPhaseEffect exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "RetryPublicationPhaseEffect")
	if rowsErr != nil {
		return rowsErr
	}
	if affected > 1 {
		return ErrPublicationPhaseConflict
	}
	return nil
}

func (s *SQLiteStore) ValidatePublishedAfterReconciliation(ctx context.Context, publicationID, verificationOperation string) error {
	publicationID = strings.TrimSpace(publicationID)
	verificationOperation = strings.TrimSpace(verificationOperation)
	if publicationID == "" || verificationOperation == "" {
		return fmt.Errorf("%w: publication and verification operation are required", ErrPublicationPhaseConflict)
	}
	var state, remoteID, submittedRemoteID, dbVerificationOperation, effectStatus string
	var reconciliationVerified int
	err := s.db.QueryRowContext(ctx, `
		SELECT ps.state, COALESCE(ps.remote_id, ''), COALESCE(ps.submitted_remote_id, ''),
		       COALESCE(ps.verification_operation, ''), COALESCE(ps.reconciliation_verified, 0), COALESCE(pe.status, '')
		FROM publication_states ps
		LEFT JOIN publication_phase_effects pe
		  ON pe.publication_id = ps.publication_id AND pe.phase = 'VERIFYING' AND pe.operation = ?
		WHERE ps.publication_id = ?`, verificationOperation, publicationID).
		Scan(&state, &remoteID, &submittedRemoteID, &dbVerificationOperation, &reconciliationVerified, &effectStatus)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrPublicationStateNotFound
		}
		return wrapDBInfrastructure("ValidatePublishedAfterReconciliation scan", err)
	}
	if state != string(publicationstate.Published) || reconciliationVerified != 1 || remoteID == "" || submittedRemoteID == "" || remoteID == submittedRemoteID || dbVerificationOperation != verificationOperation || effectStatus != "SUCCEEDED" {
		return fmt.Errorf("%w: published state lacks exact reconciliation evidence", ErrPublicationPhaseConflict)
	}
	return nil
}

func (s *SQLiteStore) GetPublicationPhaseEffectStatus(ctx context.Context, publicationID string, phase publicationstate.State, operation string) (string, error) {
	var status string
	err := s.db.QueryRowContext(ctx, `SELECT status FROM publication_phase_effects WHERE publication_id = ? AND phase = ? AND operation = ?`, strings.TrimSpace(publicationID), phase, strings.TrimSpace(operation)).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", wrapDBInfrastructure("GetPublicationPhaseEffectStatus scan", err)
	}
	return status, nil
}
