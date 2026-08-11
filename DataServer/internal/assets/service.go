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

	"velox-server/internal/inputsecurity"
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
	// UpsertMediaMetadata persists the canonical one-time media probe for an
	// asset (Fase C1). No-op for non-media assets (no row is ever created).
	UpsertMediaMetadata(ctx context.Context, assetID string, rec MediaMetadataRecord) error
	// GetMediaMetadata returns the verified media metadata for an asset, or
	// (nil, nil) when no metadata row exists (metadata_verified=false).
	GetMediaMetadata(ctx context.Context, assetID string) (*MediaMetadataRecord, error)
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
	repo                AssetRepository
	blobStore           BlobStore
	registry            *ResolverRegistry
	clock               clock.Clock
	videoTrimmer        *VideoTrimmer
	security            *inputsecurity.Fetcher
	mediaMetadata        *MediaMetadataResolver
	mediaMetadataMetrics *MediaMetadataMetrics
}

// NewAssetService creates a new generic asset service.
func NewAssetService(repo AssetRepository, blobStore BlobStore, registry *ResolverRegistry, c clock.Clock) *AssetService {
	if c == nil {
		c = clock.System{}
	}
	policy := inputsecurity.DefaultPolicy()
	if registry != nil {
		if configured, ok := registry.SecurityPolicy(); ok {
			policy = configured
		}
	}
	return &AssetService{
		repo:                repo,
		blobStore:           blobStore,
		registry:            registry,
		clock:               c,
		videoTrimmer:        NewVideoTrimmer(DefaultVideoNormalization),
		security:            inputsecurity.NewFetcher(policy),
		mediaMetadata:        NewMediaMetadataResolver(),
		mediaMetadataMetrics: NewMediaMetadataMetrics(),
	}
}

// MediaMetadataMetrics exposes bounded asset-metadata probe outcome
// counters for the metrics composition root (verified / probe_failed /
// persist_failed) without exposing asset IDs, URLs or hashes.
func (s *AssetService) MediaMetadataMetrics() *MediaMetadataMetrics {
	if s == nil || s.mediaMetadataMetrics == nil {
		return nil
	}
	return s.mediaMetadataMetrics
}

// SecurityMetrics exposes bounded input rejection counters for the metrics
// composition root without exposing URLs or filesystem paths.
func (s *AssetService) SecurityMetrics() *inputsecurity.Metrics {
	if s == nil || s.security == nil {
		return nil
	}
	return s.security.Metrics()
}

// SecurityPolicy exposes the single acquisition policy to HTTP upload
// boundaries so multipart staging uses the same byte and directory controls
// as remote resolvers.
func (s *AssetService) SecurityPolicy() inputsecurity.Policy {
	if s == nil || s.security == nil {
		return inputsecurity.DefaultPolicy()
	}
	return s.security.Policy()
}

// GetMediaMetadata returns the verified media metadata for an asset, or nil
// when no metadata was captured (non-media asset, or a probe that did not
// verify). Job-time resolvers consume this registry metadata instead of
// spawning their own ffprobe (Fase C1).
func (s *AssetService) GetMediaMetadata(ctx context.Context, assetID string) (*MediaMetadata, error) {
	if s == nil || s.repo == nil {
		return nil, fmt.Errorf("asset service unavailable")
	}
	rec, err := s.repo.GetMediaMetadata(ctx, assetID)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, nil
	}
	return rec.ToDomain(), nil
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
