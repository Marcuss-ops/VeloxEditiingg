// Package assets provides the canonical asset registry and the
// voiceover/scene-image payload rewrite helpers used by the job
// ingress pipeline.
//
// service.go owns the entry point: the persistence/storage
// interfaces, the AssetService struct itself, the constructor, and
// the read-side methods (Get, LinkToJob) plus the AssetRecord→Asset
// conversion helper. The registration-side pipeline (ResolveAndRegister)
// lives in registration.go; the payload rewrite pipeline lives in
// payload_rewrite.go with role-specific collectors/applicators in
// rewrite_voiceover.go and rewrite_scene_images.go.
package assets

import (
	"context"
	"fmt"

	"velox-server/internal/platform/clock"
)

// AssetRepository is the narrow persistence contract for the asset
// registry. Implemented by store.SQLiteAssetRepository.
type AssetRepository interface {
	Insert(ctx context.Context, a AssetRecord) error
	GetByID(ctx context.Context, assetID string) (*AssetRecord, error)
	GetBySHA256(ctx context.Context, sha256 string) (*AssetRecord, error)
	UpdateStatus(ctx context.Context, assetID, from, to string) error
	InsertSource(ctx context.Context, s AssetSourceRecord) error
	LinkToJob(ctx context.Context, jobID, assetID, role string, ordinal int, required bool) error
}

// BlobStore is the storage abstraction for asset blobs.
// This is a subset of store.BlobStore; any store.BlobStore implementation
// satisfies this interface automatically (verified at compile time in store/store_assets.go).
type BlobStore interface {
	StagingPath(jobID, artifactID, extension string) (string, error)
	PromoteToFinal(stagingPath, finalPath string) (string, error)
	RemoveStaging(path string) error
	FinalPath(jobID, artifactID, extension string) string
}

// AssetService is the generic asset registry service.
type AssetService struct {
	repo      AssetRepository
	blobStore BlobStore
	registry  *ResolverRegistry
	clock     clock.Clock
}

// NewAssetService creates a new generic asset service.
func NewAssetService(repo AssetRepository, blobStore BlobStore, registry *ResolverRegistry, c clock.Clock) *AssetService {
	if c == nil {
		c = clock.System{}
	}
	return &AssetService{
		repo:      repo,
		blobStore: blobStore,
		registry:  registry,
		clock:     c,
	}
}

// Get retrieves an asset by ID.
func (s *AssetService) Get(ctx context.Context, assetID string) (*Asset, error) {
	if s == nil || s.repo == nil {
		return nil, fmt.Errorf("asset service unavailable")
	}
	rec, err := s.repo.GetByID(ctx, assetID)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, nil
	}
	return s.recordToAsset(rec), nil
}

// LinkToJob binds an asset to a job with a role.
func (s *AssetService) LinkToJob(ctx context.Context, jobID, assetID, role string, ordinal int, required bool) error {
	if s == nil || s.repo == nil {
		return fmt.Errorf("asset service unavailable")
	}
	return s.repo.LinkToJob(ctx, jobID, assetID, role, ordinal, required)
}

// recordToAsset converts a storage-projection AssetRecord into the
// canonical Asset domain type. Shared by Get and any other read path
// that wants to surface an Asset.
func (s *AssetService) recordToAsset(rec *AssetRecord) *Asset {
	if rec == nil {
		return nil
	}
	return &Asset{
		AssetID:         rec.AssetID,
		Kind:            rec.Kind,
		Status:          rec.Status,
		SHA256:          rec.SHA256,
		MimeType:        rec.MimeType,
		SizeBytes:       rec.SizeBytes,
		StorageProvider: rec.StorageProvider,
		StorageKey:      rec.StorageKey,
		MetadataJSON:    rec.MetadataJSON,
		CreatedAt:       rec.CreatedAt,
		VerifiedAt:      rec.VerifiedAt,
		DeletedAt:       rec.DeletedAt,
	}
}

var _ = (*AssetService)(nil)
