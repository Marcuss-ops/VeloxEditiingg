package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/publicationstate"
)

var (
	ErrPublicationStateNotFound = errors.New("store: publication state not found")
	ErrPublicationPhaseConflict = errors.New("store: publication phase effect conflict")
)

type PublicationState struct {
	PublicationID string
	JobID         string
	State         publicationstate.State
	RetryFrom     publicationstate.State
	ArtifactID    string
	RemoteID      string
	RemoteURL     string
	Revision      uint64
	LastErrorCode string
	CreatedAt     string
	UpdatedAt     string
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
	return err
}

func (s *SQLiteStore) GetPublicationState(ctx context.Context, publicationID string) (*PublicationState, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT publication_id, COALESCE(job_id, ''), state, COALESCE(retry_from, ''),
		       COALESCE(artifact_id, ''), COALESCE(remote_id, ''),
		       COALESCE(remote_url, ''), revision,
		       COALESCE(last_error_code, ''), created_at, updated_at
		FROM publication_states WHERE publication_id = ?`, strings.TrimSpace(publicationID))
	state, err := scanPublicationState(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPublicationStateNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get publication state: %w", err)
	}
	return state, nil
}

// TransitionPublicationState performs a CAS-guarded formal transition. A
// replay of the same target state is a no-op; a stale writer cannot overwrite
// a newer phase or retry checkpoint.
func (s *SQLiteStore) TransitionPublicationState(ctx context.Context, publicationID string, to publicationstate.State, errorCode string) (*PublicationState, error) {
	return s.transitionPublicationState(ctx, publicationID, to, "", errorCode)
}

// TransitionPublicationPartial records a partial result with the exact phase
// that must be retried. This is the durable form of “video created, metadata
// applied, localizations failed”; callers must not collapse it to a generic
// retry that would upload the video again.
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
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `
		SELECT publication_id, COALESCE(job_id, ''), state, COALESCE(retry_from, ''),
		       COALESCE(artifact_id, ''), COALESCE(remote_id, ''),
		       COALESCE(remote_url, ''), revision,
		       COALESCE(last_error_code, ''), created_at, updated_at
		FROM publication_states WHERE publication_id = ?`, publicationID)
	current, err := scanPublicationState(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrPublicationStateNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read publication state: %w", err)
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
		return current, tx.Commit()
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
		return nil, fmt.Errorf("transition publication state: %w", err)
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return nil, fmt.Errorf("%w: publication=%s revision=%d", ErrPublicationPhaseConflict, publicationID, current.Revision)
	}
	current.State = next.State
	current.RetryFrom = next.RetryFrom
	current.Revision = next.Revision
	current.LastErrorCode = strings.TrimSpace(errorCode)
	current.UpdatedAt = now
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return current, nil
}

// BeginPublicationPhaseEffect reserves one phase operation. Repeating the
// same call returns already=true and the same key, allowing a worker to skip
// a completed remote side effect instead of uploading again.
func (s *SQLiteStore) BeginPublicationPhaseEffect(ctx context.Context, publicationID string, phase publicationstate.State, operation string) (key string, already bool, err error) {
	publicationID = strings.TrimSpace(publicationID)
	operation = strings.TrimSpace(operation)
	if publicationID == "" || phase == "" || operation == "" {
		return "", false, fmt.Errorf("publication phase effect: publication, phase and operation are required")
	}
	key = publicationstate.SideEffectKey(publicationID, phase, operation)
	now := time.Now().UTC().Format(time.RFC3339)
	result, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO publication_phase_effects
		(publication_id, phase, operation, idempotency_key, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, 'RUNNING', ?, ?)`, publicationID, phase, operation, key, now, now)
	if err != nil {
		return "", false, err
	}
	affected, _ := result.RowsAffected()
	return key, affected == 0, nil
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
		return err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return ErrPublicationPhaseConflict
	}
	return nil
}

type publicationScanner interface{ Scan(...any) error }

func scanPublicationState(row publicationScanner) (*PublicationState, error) {
	var state PublicationState
	var rawState, rawRetry string
	if err := row.Scan(&state.PublicationID, &state.JobID, &rawState, &rawRetry, &state.ArtifactID, &state.RemoteID, &state.RemoteURL, &state.Revision, &state.LastErrorCode, &state.CreatedAt, &state.UpdatedAt); err != nil {
		return nil, err
	}
	state.State = publicationstate.State(rawState)
	state.RetryFrom = publicationstate.State(rawRetry)
	return &state, nil
}
