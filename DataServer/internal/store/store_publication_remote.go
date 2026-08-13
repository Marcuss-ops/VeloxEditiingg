package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"velox-server/internal/publicationstate"
)

// store_publication_remote.go owns the publication_states remote-result
// checkpoints: recording the final remote media identity and the
// video-created checkpoint that advances UPLOADING → VIDEO_CREATED.

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
		remoteID, strings.TrimSpace(remoteURL), nowRFC3339Nano(), publicationID, expectedRevision, expectedRemoteID)
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
	now := nowRFC3339()
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
