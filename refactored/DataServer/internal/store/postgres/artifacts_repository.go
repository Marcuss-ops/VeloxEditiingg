package postgres

import (
	"context"

	"velox-server/internal/store"
)

// ArtifactRepository is the upcoming Postgres implementation of
// store.ArtifactRepository. Scaffolding only — see package doc.
type ArtifactRepository struct {
	// dsn is reserved for future use; the real backend will open a *pgxpool.Pool
	// at construction time and stash it in unexported fields here.
	dsn string
}

// NewArtifactRepository constructs a Postgres artifact repository stub.
func NewArtifactRepository(dsn string) *ArtifactRepository {
	return &ArtifactRepository{dsn: dsn}
}

// Insert returns ErrNotImplemented until the driver is wired in.
func (r *ArtifactRepository) Insert(ctx context.Context, artifact *store.Artifact) error {
	_ = ctx
	_ = artifact
	return store.ErrNotImplemented
}

// FinalizeAndComplete returns ErrNotImplemented until the driver is wired in.
func (r *ArtifactRepository) FinalizeAndComplete(ctx context.Context, artifactID, status, storageURL, jobID, sha256 string) error {
	_, _, _, _, _ = ctx, artifactID, status, storageURL, jobID
	_ = sha256
	return store.ErrNotImplemented
}

// GetByID returns ErrNotImplemented stub. Returning (nil, ErrNotImplemented)
// keeps call sites honest: consumers will see the error and not silently
// treat missing rows as nil.
func (r *ArtifactRepository) GetByID(ctx context.Context, artifactID string) (*store.Artifact, error) {
	_, _ = ctx, artifactID
	return nil, store.ErrNotImplemented
}

// ListByJob returns ErrNotImplemented.
func (r *ArtifactRepository) ListByJob(ctx context.Context, jobID string, limit int) ([]store.Artifact, error) {
	_, _, _ = ctx, jobID, limit
	return nil, store.ErrNotImplemented
}

// Compile-time interface check — keeps the stub honest if the contract shifts.
var _ store.ArtifactRepository = (*ArtifactRepository)(nil)
