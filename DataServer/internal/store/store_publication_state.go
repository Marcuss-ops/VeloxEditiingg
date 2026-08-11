package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/audittrail"
	"velox-server/internal/publicationstate"

	"github.com/google/uuid"
)

var (
	ErrPublicationStateNotFound = errors.New("store: publication state not found")
	ErrPublicationPhaseConflict = errors.New("store: publication phase effect conflict")
)

type PublicationState struct {
	PublicationID          string
	JobID                  string
	State                  publicationstate.State
	RetryFrom              publicationstate.State
	ArtifactID             string
	RemoteID               string
	SubmittedRemoteID      string
	VerificationOperation  string
	ReconciliationVerified bool
	RemoteURL              string
	Revision               uint64
	LastErrorCode          string
	CreatedAt              string
	UpdatedAt              string
}

// CreatePublicationState is idempotent on publication_id. The first writer
// owns the immutable initial PENDING state; retries read the existing row.
func (s *SQLiteStore) CreatePublicationState(ctx context.Context, publicationID string) error {
	publicationID = strings.TrimSpace(publicationID)
	if _, err := publicationstate.NewSnapshot(publicationID); err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO publication_states
		(publication_id, state, revision, created_at, updated_at)
		VALUES (?, 'PENDING', 0, ?, ?)`, publicationID, now, now)
	if err != nil {
		return wrapDBInfrastructure("CreatePublicationState exec", err)
	}
	return nil
}

func (s *SQLiteStore) GetPublicationState(ctx context.Context, publicationID string) (*PublicationState, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT publication_id, COALESCE(job_id, ''), state, COALESCE(retry_from, ''),
		       COALESCE(artifact_id, ''), COALESCE(remote_id, ''),
		       COALESCE(submitted_remote_id, ''), COALESCE(verification_operation, ''), COALESCE(reconciliation_verified, 0), COALESCE(remote_url, ''), revision,
		       COALESCE(last_error_code, ''), created_at, updated_at
		FROM publication_states WHERE publication_id = ?`, strings.TrimSpace(publicationID))
	state, err := scanPublicationState(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPublicationStateNotFound
	}
	if err != nil {
		return nil, wrapDBInfrastructure("GetPublicationState scan", err)
	}
	return state, nil
}

// GetPublicationIDForArtifact resolves the publication control-plane row for
// a delivery whose metadata does not carry the optional publication_id. The
// enqueue transaction creates this relation through job_id before any
// artifact can become deliverable.
func (s *SQLiteStore) GetPublicationIDForArtifact(ctx context.Context, artifactID string) (string, error) {
	var publicationID string
	err := s.db.QueryRowContext(ctx, `
		SELECT ps.publication_id
		FROM publication_states ps
		JOIN artifacts a ON a.job_id = ps.job_id
		WHERE a.id = ?
		ORDER BY ps.created_at ASC, ps.publication_id ASC
		LIMIT 1`, strings.TrimSpace(artifactID)).Scan(&publicationID)
	if errors.Is(err, sql.ErrNoRows) {
		err = s.db.QueryRowContext(ctx, `
			SELECT publication_id FROM publication_states
			WHERE (job_id IS NULL OR job_id = '')
			  AND (SELECT COUNT(*) FROM publication_states WHERE job_id IS NULL OR job_id = '') = 1
			ORDER BY created_at ASC, publication_id ASC LIMIT 1`).Scan(&publicationID)
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrPublicationStateNotFound
		}
		if err != nil {
			return "", wrapDBInfrastructure("GetPublicationIDForArtifact fallback scan", err)
		}
	}
	if err != nil {
		return "", wrapDBInfrastructure("GetPublicationIDForArtifact scan", err)
	}
	return publicationID, nil
}

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
	now := time.Now().UTC().Format(time.RFC3339)
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
	now := time.Now().UTC().Format(time.RFC3339)
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
	now := time.Now().UTC().Format(time.RFC3339)
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
	result, err = tx.ExecContext(ctx, `UPDATE publication_phase_effects SET status='SUCCEEDED', error_code=NULL, updated_at=? WHERE publication_id=? AND phase='VERIFYING' AND operation=? AND status='RUNNING'`, time.Now().UTC().Format(time.RFC3339), publicationID, operation)
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
		status, strings.TrimSpace(errorCode), time.Now().UTC().Format(time.RFC3339),
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
		time.Now().UTC().Format(time.RFC3339), strings.TrimSpace(publicationID), phase, strings.TrimSpace(operation))
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

func (s *SQLiteStore) RecordPublicationRemoteResult(ctx context.Context, publicationID string, expectedRevision uint64, expectedRemoteID, remoteID, remoteURL string) error {
	publicationID = strings.TrimSpace(publicationID)
	expectedRemoteID = strings.TrimSpace(expectedRemoteID)
	remoteID = strings.TrimSpace(remoteID)
	if publicationID == "" || expectedRemoteID == "" || remoteID == "" {
		return fmt.Errorf("store: final publication result requires publication_id, expected remote_id, and remote_id")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE publication_states SET remote_id = ?, remote_url = COALESCE(NULLIF(?, ''), remote_url), revision = revision + 1, updated_at = ?
		WHERE publication_id = ? AND state = 'VERIFYING' AND revision = ? AND remote_id = ?`,
		remoteID, strings.TrimSpace(remoteURL), time.Now().UTC().Format(time.RFC3339Nano), publicationID, expectedRevision, expectedRemoteID)
	if err != nil {
		return wrapDBInfrastructure("RecordPublicationRemoteResult exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "RecordPublicationRemoteResult")
	if rowsErr != nil {
		return rowsErr
	}
	if affected != 1 {
		return ErrPublicationPhaseConflict
	}
	return nil
}

func (s *SQLiteStore) PersistPublicationVideoCreated(ctx context.Context, publicationID, artifactID, remoteID, remoteURL string) (*PublicationState, error) {
	publicationID = strings.TrimSpace(publicationID)
	if publicationID == "" || strings.TrimSpace(remoteID) == "" {
		return nil, fmt.Errorf("store: publication video checkpoint requires publication_id and remote_id")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, wrapDBInfrastructure("PersistPublicationVideoCreated begin", err)
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `
		SELECT publication_id, COALESCE(job_id, ''), state, COALESCE(retry_from, ''),
		       COALESCE(artifact_id, ''), COALESCE(remote_id, ''),
		       COALESCE(submitted_remote_id, ''), COALESCE(verification_operation, ''), COALESCE(reconciliation_verified, 0),
		       COALESCE(remote_url, ''), revision, COALESCE(last_error_code, ''), created_at, updated_at
		FROM publication_states WHERE publication_id = ?`, publicationID)
	current, err := scanPublicationState(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPublicationStateNotFound
	}
	if err != nil {
		return nil, wrapDBInfrastructure("PersistPublicationVideoCreated scan", err)
	}
	if current.State != publicationstate.Uploading && current.State != publicationstate.VideoCreated {
		return nil, fmt.Errorf("%w: video checkpoint from %s", ErrPublicationPhaseConflict, current.State)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	nextRevision := current.Revision
	nextState := current.State
	if current.State == publicationstate.Uploading {
		nextState = publicationstate.VideoCreated
		nextRevision++
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE publication_states
		SET state = ?, retry_from = NULL, artifact_id = COALESCE(NULLIF(?, ''), artifact_id),
		    remote_id = ?, submitted_remote_id = COALESCE(NULLIF(submitted_remote_id, ''), ?),
		    remote_url = COALESCE(NULLIF(?, ''), remote_url), revision = ?, last_error_code = NULL, updated_at = ?
		WHERE publication_id = ? AND state = ? AND revision = ?`,
		nextState, artifactID, remoteID, remoteID, remoteURL, nextRevision, now, publicationID, current.State, current.Revision)
	if err != nil {
		return nil, wrapDBInfrastructure("PersistPublicationVideoCreated exec", err)
	}
	affected, rowsErr := readRowsAffected(result, "PersistPublicationVideoCreated")
	if rowsErr != nil {
		return nil, rowsErr
	}
	if affected != 1 {
		return nil, ErrPublicationPhaseConflict
	}
	current.State = nextState
	current.RetryFrom = ""
	current.ArtifactID = artifactID
	current.RemoteID = remoteID
	if current.SubmittedRemoteID == "" {
		current.SubmittedRemoteID = remoteID
	}
	if remoteURL != "" {
		current.RemoteURL = remoteURL
	}
	current.Revision = nextRevision
	current.LastErrorCode = ""
	current.UpdatedAt = now
	if err := appendPublicationTransitionAuditTx(ctx, tx, current, publicationstate.Uploading, publicationstate.VideoCreated, ""); err != nil {
		return nil, wrapDBInfrastructure("PersistPublicationVideoCreated audit", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, wrapDBInfrastructure("PersistPublicationVideoCreated commit", err)
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
		_, err = tx.ExecContext(ctx, `INSERT INTO audit_events (id, occurred_at, actor_type, actor_id, action, resource_type, resource_id, request_id, trace_id, before_hash, after_hash, metadata_json) VALUES (?, ?, 'service', 'delivery_runner', ?, 'publication', ?, '', '', '', '', ?)`, uuid.NewString(), time.Now().UTC().Format(time.RFC3339Nano), record.action, state.PublicationID, audittrail.RedactMetadata(string(metadata)))
		if err != nil {
			return wrapDBInfrastructure("appendPublicationTransitionAuditTx exec", err)
		}
	}
	return nil
}

func publicationPhaseName(state publicationstate.State) string {
	switch state {
	case publicationstate.Uploading:
		return "UPLOAD_MEDIA"
	case publicationstate.MetadataApplying:
		return "APPLY_METADATA"
	case publicationstate.LocalizationsApplying:
		return "APPLY_LOCALIZATIONS"
	case publicationstate.Verifying:
		return "VERIFY"
	default:
		return ""
	}
}

type publicationScanner interface{ Scan(...any) error }

func scanPublicationState(row publicationScanner) (*PublicationState, error) {
	var state PublicationState
	var rawState, rawRetry string
	if err := row.Scan(&state.PublicationID, &state.JobID, &rawState, &rawRetry, &state.ArtifactID, &state.RemoteID, &state.SubmittedRemoteID, &state.VerificationOperation, &state.ReconciliationVerified, &state.RemoteURL, &state.Revision, &state.LastErrorCode, &state.CreatedAt, &state.UpdatedAt); err != nil {
		return nil, err
	}
	state.State = publicationstate.State(rawState)
	state.RetryFrom = publicationstate.State(rawRetry)
	return &state, nil
}
