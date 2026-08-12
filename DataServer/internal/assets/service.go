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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

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

// VerifiedAssetRepository is the optional atomic write surface used by
// contracts that cannot tolerate a READY asset without its verified media
// metadata. Generic asset ingestion continues to use AssetRepository's
// best-effort metadata path; final_audio requires this stronger capability.
type VerifiedAssetRepository interface {
	AssetRepository
	InsertWithMediaMetadataAndSource(ctx context.Context, asset AssetRecord, metadata MediaMetadataRecord, source AssetSourceRecord) error
}

// BlobStore is the storage abstraction for asset blobs.
// This is a subset of store.BlobStore; any store.BlobStore implementation
// satisfies this interface automatically (verified at compile time in store/store_assets.go).
type BlobStore interface {
	StagingPath(jobID, artifactID, extension string) (string, error)
	PromoteToFinal(stagingPath, finalPath string) (string, error)
	RemoveStaging(path string) error
	FinalPath(jobID, artifactID, extension string) string
	// FinalDir returns the final storage root. Used to resolve a relative
	// StorageKey into a filesystem path for the canonical metadata probe
	// (Fase C2 EnsureMediaMetadata).
	FinalDir() string
}

// FinalBlobRemover is an optional cleanup capability. Registration uses it
// after an atomic metadata transaction fails so a promoted but unreferenced
// final blob is not left behind by a rejected final_audio operation.
type FinalBlobRemover interface {
	RemoveFinal(storageKey string) error
}

// AssetService is the generic asset registry service.
type AssetService struct {
	repo                 AssetRepository
	blobStore            BlobStore
	registry             *ResolverRegistry
	clock                clock.Clock
	videoTrimmer         *VideoTrimmer
	security             *inputsecurity.Fetcher
	mediaMetadata        *MediaMetadataResolver
	mediaMetadataMetrics *MediaMetadataMetrics
}

// assetSourceReader is intentionally optional so lightweight test and
// migration repositories do not need to implement source lookup. The
// production SQLite repository implements it and makes asset_sources the
// authoritative fallback when a local final blob is not present.
type assetSourceReader interface {
	ListSources(ctx context.Context, assetID string) ([]AssetSourceRecord, error)
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
		repo:                 repo,
		blobStore:            blobStore,
		registry:             registry,
		clock:                c,
		videoTrimmer:         NewVideoTrimmer(defaultVideoNormalization),
		security:             inputsecurity.NewFetcher(policy),
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

// ResolveExternalSource opens the newest registered source for an asset that
// is not available as a local final blob. The returned reader owns any
// temporary download and MUST be closed by the caller. Resolution happens at
// execution time; preflight only checks that a resolver exists for the
// registered reference.
func (s *AssetService) ResolveExternalSource(ctx context.Context, assetID string) (*Source, error) {
	if s == nil || s.repo == nil || s.registry == nil {
		return nil, fmt.Errorf("asset external source resolution unavailable")
	}
	assetID = strings.TrimSpace(assetID)
	asset, err := s.Get(ctx, assetID)
	if err != nil {
		return nil, err
	}
	if asset == nil {
		return nil, fmt.Errorf("asset %s not found", assetID)
	}
	if asset.Status != AssetStatusReady {
		return nil, fmt.Errorf("asset %s is not READY", assetID)
	}
	reader, ok := s.repo.(assetSourceReader)
	if !ok {
		return nil, fmt.Errorf("asset source registry unavailable")
	}
	sources, err := reader.ListSources(ctx, asset.AssetID)
	if err != nil {
		return nil, err
	}
	var lastErr error
	for _, source := range sources {
		reference := strings.TrimSpace(source.SourceReference)
		if reference == "" || !s.registry.SupportsReference(reference) {
			continue
		}
		resolved, resolveErr := s.registry.ResolveByInference(ctx, reference)
		if resolveErr == nil && resolved != nil && resolved.Reader != nil {
			if verifyErr := verifyExternalSource(resolved, asset); verifyErr != nil {
				_ = resolved.Reader.Close()
				lastErr = verifyErr
				continue
			}
			return resolved, nil
		}
		lastErr = resolveErr
	}
	if lastErr != nil {
		return nil, fmt.Errorf("asset %s external source unavailable: %w", assetID, lastErr)
	}
	return nil, fmt.Errorf("asset %s has no resolvable external source", assetID)
}

// verifyExternalSource validates the downloaded bytes against the canonical
// registry facts before they are streamed to a worker. Resolver downloads are
// staged in seekable temporary files, so the verification pass can rewind the
// reader without retaining the asset in memory.
func verifyExternalSource(source *Source, asset *Asset) error {
	if source == nil || source.Reader == nil || asset == nil {
		return fmt.Errorf("external source verification unavailable")
	}
	seeker, ok := source.Reader.(io.Seeker)
	if !ok {
		return fmt.Errorf("external source is not replayable")
	}
	hash := sha256.New()
	actualSize, err := io.Copy(hash, source.Reader)
	if err != nil {
		return fmt.Errorf("read external source: %w", err)
	}
	if asset.SizeBytes <= 0 || actualSize != asset.SizeBytes {
		return fmt.Errorf("external source size mismatch: got %d want %d", actualSize, asset.SizeBytes)
	}
	actualSHA := hex.EncodeToString(hash.Sum(nil))
	if !strings.EqualFold(actualSHA, strings.TrimSpace(asset.SHA256)) {
		return fmt.Errorf("external source sha256 mismatch")
	}
	if _, err := seeker.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind external source: %w", err)
	}
	return nil
}

// HasResolvableExternalSource checks source metadata without opening a
// network connection or downloading bytes.
func (s *AssetService) HasResolvableExternalSource(ctx context.Context, assetID string) bool {
	if s == nil || s.repo == nil || s.registry == nil {
		return false
	}
	reader, ok := s.repo.(assetSourceReader)
	if !ok {
		return false
	}
	sources, err := reader.ListSources(ctx, strings.TrimSpace(assetID))
	if err != nil {
		return false
	}
	for _, source := range sources {
		if s.registry.SupportsReference(source.SourceReference) {
			return true
		}
	}
	return false
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
