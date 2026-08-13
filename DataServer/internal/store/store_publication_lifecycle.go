package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"velox-server/internal/publicationstate"
)

// store_publication_lifecycle.go owns the publication_states lifecycle
// bootstrap: creating the initial PENDING row and reading it back, plus the
// artifact→publication reverse lookup used by the delivery path.

// CreatePublicationState is idempotent on publication_id. The first writer
// owns the immutable initial PENDING state; retries read the existing row.
func (s *SQLiteStore) CreatePublicationState(ctx context.Context, publicationID string) error {
	publicationID = strings.TrimSpace(publicationID)
	if _, err := publicationstate.NewSnapshot(publicationID); err != nil {
		return err
	}
	now := nowRFC3339()
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
