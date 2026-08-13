package repository

import (
	"context"

	"velox-server/internal/assets"
)

// AssetRepository is the narrow write-aware contract for the generic asset registry.
type AssetRepository interface {
	// Insert creates a new asset row. Returns ErrAssetAlreadyExists if the
	// SHA-256 or (storage_provider, storage_key) conflicts.
	Insert(ctx context.Context, a assets.AssetRecord) error
	// GetByID returns a single asset, or (nil, nil) on missing.
	GetByID(ctx context.Context, assetID string) (*assets.AssetRecord, error)
	// GetByIDAndWorkspace returns a single asset scoped to a workspace,
	// or (nil, nil) on missing / wrong workspace.
	GetByIDAndWorkspace(ctx context.Context, assetID string, workspaceID int64) (*assets.AssetRecord, error)
	// GetBySHA256 returns the asset with the given SHA-256, or (nil, nil).
	GetBySHA256(ctx context.Context, sha256 string) (*assets.AssetRecord, error)
	// UpdateStatus atomically transitions status (CAS on from).
	UpdateStatus(ctx context.Context, assetID, from, to string) error
	// SoftDelete sets deleted_at and status=DELETED.
	SoftDelete(ctx context.Context, assetID string) error
	// InsertSource records provenance for an asset.
	InsertSource(ctx context.Context, s assets.AssetSourceRecord) error
	// LinkToJob binds an asset to a job with a role and ordinal.
	LinkToJob(ctx context.Context, jobID, assetID, role string, ordinal int, required bool) error
	// ListByJob returns all assets linked to a job.
	ListByJob(ctx context.Context, jobID string) ([]assets.AssetRecord, error)
}
