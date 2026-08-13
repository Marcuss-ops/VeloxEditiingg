package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"velox-server/internal/audittrail"
	"velox-server/internal/publicationstate"

	"github.com/google/uuid"
)

// store_publication_transition.go owns the formal publication state machine:
// the CAS-guarded transitions (including the PUBLISHED promotion that
// requires reconciliation evidence) and the transition audit projection.

// TransitionPublicationState performs a CAS-guarded formal transition. A
// replay of the same target state is a no-op; a stale writer cannot overwrite
// a newer phase or retry checkpoint. PUBLISHED has a dedicated method below
// because it requires reconciliation evidence and an exact VERIFYING effect.
func (s *SQLiteStore) TransitionPublicationState(ctx context.Context, publicationID string, to publicationstate.State, errorCode string) (*PublicationState, error) {
	if to == publicationstate.Published {
		return nil, fmt.Errorf("%w: use CompletePublicationAfterReconciliation", ErrPublicationPhaseConflict)
	}
	return s.transitionPublicationState(ctx, publicationID, to, "", errorCode)
}

// CompletePublicationAfterReconciliation is the only persistence boundary
// that can promote VERIFYING to PUBLISHED. The operation key must match the
// canonical VERIFYING operation recorded for this publication, and the
// reconciliation evidence must be positive and atomic.
func (s *SQLiteStore) CompletePublicationAfterReconciliation(ctx context.Context, publicationID, verificationOperation string) (*PublicationState, error) {
	publicationID = strings.TrimSpace(publicationID)
	verificationOperation = strings.TrimSpace(verificationOperation)
	if publicationID == "" || verificationOperation == "" {
		return nil, fmt.Errorf("%w: publication and verification operation are required", ErrPublicationPhaseConflict)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, wrapDBInfrastructure("CompletePublicationAfterReconciliation begin", err)
	}
	defer tx.Rollback()

	current, err := scanPublicationState(tx.QueryRowContext(ctx, `
		SELECT publication_id, COALESCE(job_id, ''), state, COALESCE(retry_from, ''),
		       COALESCE(artifact_id, ''), COALESCE(remote_id, ''),
		       COALESCE(submitted_remote_id, ''), COALESCE(verification_operation, ''), COALESCE(reconciliation_verified, 0), COALESCE(remote_url, ''), revision, COALESCE(last_error_code, ''),
		       created_at, updated_at FROM publication_states WHERE publication_id = ?`, publicationID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPublicationStateNotFound
	}
	if err != nil {
		return nil, wrapDBInfrastructure("CompletePublicationAfterReconciliation scan", err)
	}
	if current.State != publicationstate.Verifying || !current.ReconciliationVerified || strings.TrimSpace(current.RemoteID) == "" || strings.TrimSpace(current.SubmittedRemoteID) == "" || strings.TrimSpace(current.RemoteID) == strings.TrimSpace(current.SubmittedRemoteID) || strings.TrimSpace(current.VerificationOperation) != verificationOperation {
		return nil, fmt.Errorf("%w: PUBLISHED requires VERIFYING with distinct submission and final media evidence", ErrPublicationPhaseConflict)
	}
	var effectStatus string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM publication_phase_effects WHERE publication_id=? AND phase='VERIFYING' AND operation=?`, publicationID, verificationOperation).Scan(&effectStatus); err != nil || effectStatus != "SUCCEEDED" {
		return nil, fmt.Errorf("%w: exact VERIFYING reconciliation effect is not SUCCEEDED", ErrPublicationPhaseConflict)
	}
	now := nowRFC3339()
	result, err := tx.ExecContext(ctx, `UPDATE publication_states SET state='PUBLISHED', retry_from=NULL, revision=revision+1, last_error_code=NULL, updated_at=? WHERE publication_id=? AND state='VERIFYING' AND revision=?`, now, publicationID, current.Revision)
	if err != nil {
		return nil, wrapDBInfrastructure("CompletePublicationAfterReconciliation exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "CompletePublicationAfterReconciliation")
	if rowsErr != nil {
		return nil, rowsErr
	}
	if affected != 1 {
		return nil, ErrPublicationPhaseConflict
	}
	fromState := current.State
	current.State = publicationstate.Published
	current.RetryFrom = ""
	current.Revision++
	current.LastErrorCode = ""
	current.UpdatedAt = now
	if err := appendPublicationTransitionAuditTx(ctx, tx, current, fromState, publicationstate.Published, ""); err != nil {
		return nil, wrapDBInfrastructure("CompletePublicationAfterReconciliation audit", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, wrapDBInfrastructure("CompletePublicationAfterReconciliation commit", err)
	}
	return current, nil
}

func (s *SQLiteStore) TransitionPublicationPartial(ctx context.Context, publicationID string, retryFrom publicationstate.State, errorCode string) (*PublicationState, error) {
	return s.transitionPublicationState(ctx, publicationID, publicationstate.Partial, retryFrom, errorCode)
}

func (s *SQLiteStore) transitionPublicationState(ctx context.Context, publicationID string, to, partialRetryFrom publicationstate.State, errorCode string) (*PublicationState, error) {
	publicationID = strings.TrimSpace(publicationID)
	if publicationID == "" {
		return nil, fmt.Errorf("publication_id is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, wrapDBInfrastructure("transitionPublicationState begin", err)
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `
		SELECT publication_id, COALESCE(job_id, ''), state, COALESCE(retry_from, ''),
		       COALESCE(artifact_id, ''), COALESCE(remote_id, ''),
		       COALESCE(submitted_remote_id, ''), COALESCE(verification_operation, ''), COALESCE(reconciliation_verified, 0), COALESCE(remote_url, ''),
		       revision, COALESCE(last_error_code, ''), created_at, updated_at
		FROM publication_states WHERE publication_id = ?`, publicationID)
	current, err := scanPublicationState(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPublicationStateNotFound
	}
	if err != nil {
		return nil, wrapDBInfrastructure("transitionPublicationState scan", err)
	}
	snapshot := publicationstate.Snapshot{PublicationID: current.PublicationID, State: current.State, RetryFrom: current.RetryFrom, Revision: current.Revision}
	var next publicationstate.Snapshot
	if to == publicationstate.Partial {
		next, err = snapshot.TransitionPartial(partialRetryFrom)
	} else {
		next, err = snapshot.Transition(to)
	}
	if err != nil {
		return nil, err
	}
	if next == snapshot {
		if err := tx.Commit(); err != nil {
			return nil, wrapDBInfrastructure("transitionPublicationState idempotent commit", err)
		}
		return current, nil
	}
	now := nowRFC3339()
	result, err := tx.ExecContext(ctx, `
		UPDATE publication_states
		SET state = ?, retry_from = NULLIF(?, ''), revision = ?,
		    last_error_code = NULLIF(?, ''), updated_at = ?
		WHERE publication_id = ? AND state = ? AND revision = ?`,
		next.State, next.RetryFrom, next.Revision, strings.TrimSpace(errorCode), now,
		publicationID, current.State, current.Revision)
	if err != nil {
		return nil, wrapDBInfrastructure("transitionPublicationState exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "transitionPublicationState")
	if rowsErr != nil {
		return nil, rowsErr
	}
	if affected != 1 {
		return nil, fmt.Errorf("%w: publication=%s revision=%d", ErrPublicationPhaseConflict, publicationID, current.Revision)
	}
	fromState := current.State
	current.State = next.State
	current.RetryFrom = next.RetryFrom
	current.Revision = next.Revision
	current.LastErrorCode = strings.TrimSpace(errorCode)
	current.UpdatedAt = now
	if err := appendPublicationTransitionAuditTx(ctx, tx, current, fromState, to, errorCode); err != nil {
		return nil, wrapDBInfrastructure("transitionPublicationState audit", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, wrapDBInfrastructure("transitionPublicationState commit", err)
	}
	return current, nil
}

func appendPublicationTransitionAuditTx(ctx context.Context, tx *sql.Tx, state *PublicationState, from, to publicationstate.State, errorCode string) error {
	type auditRecord struct{ action, phase string }
	var records []auditRecord
	if publicationPhaseName(from) != "" {
		switch {
		case to == publicationstate.RetryWait || to == publicationstate.Partial || to == publicationstate.Failed:
			records = append(records, auditRecord{"PUBLICATION_PHASE_FAILED", publicationPhaseName(from)})
		case to != from:
			records = append(records, auditRecord{"PUBLICATION_PHASE_SUCCEEDED", publicationPhaseName(from)})
		}
	}
	switch to {
	case publicationstate.Uploading, publicationstate.MetadataApplying, publicationstate.Verifying:
		records = append(records, auditRecord{"PUBLICATION_PHASE_STARTED", publicationPhaseName(to)})
	case publicationstate.Published:
		records = append(records, auditRecord{"PUBLICATION_COMPLETED", ""})
	}
	for _, record := range records {
		metadata, err := json.Marshal(map[string]string{
			"publication_id": state.PublicationID,
			"phase":          record.phase,
			"status":         string(to),
			"error_code":     strings.TrimSpace(errorCode),
			"remote_id":      state.RemoteID,
		})
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO audit_events (id, occurred_at, actor_type, actor_id, action, resource_type, resource_id, request_id, trace_id, before_hash, after_hash, metadata_json) VALUES (?, ?, 'service', 'delivery_runner', ?, 'publication', ?, '', '', '', '', ?)`, uuid.NewString(), nowRFC3339Nano(), record.action, state.PublicationID, audittrail.RedactMetadata(string(metadata)))
		if err != nil {
			return wrapDBInfrastructure("appendPublicationTransitionAuditTx exec", err)
		}
	}
	return nil
}
