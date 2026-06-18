package assets

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// AssetRepository is the narrow persistence contract for the asset registry.
// Implemented by store.SQLiteAssetRepository.
type AssetRepository interface {
	Insert(ctx context.Context, a AssetRecord) error
	GetByID(ctx context.Context, assetID string) (*AssetRecord, error)
	GetBySHA256(ctx context.Context, sha256 string) (*AssetRecord, error)
	UpdateStatus(ctx context.Context, assetID, from, to string) error
	InsertSource(ctx context.Context, s AssetSourceRecord) error
	LinkToJob(ctx context.Context, jobID, assetID, role string, ordinal int, required bool) error
}

// BlobStore is the storage abstraction for asset blobs.
// Matches the shape of store.LocalBlobStore.
type BlobStore interface {
	StagingPath(jobID, artifactID, extension string) (string, error)
	PromoteToFinal(stagingPath, finalPath string) (string, error)
	RemoveStaging(path string) error
	FinalPath(jobID, artifactID, extension string) string
}

// Clock returns the current time (allows testing).
type Clock func() time.Time

// AssetService is the generic asset registry service.
type AssetService struct {
	repo      AssetRepository
	blobStore BlobStore
	registry  *ResolverRegistry
	clock     Clock
}

// NewAssetService creates a new generic asset service.
func NewAssetService(repo AssetRepository, blobStore BlobStore, registry *ResolverRegistry, clock Clock) *AssetService {
	if clock == nil {
		clock = time.Now
	}
	return &AssetService{
		repo:      repo,
		blobStore: blobStore,
		registry:  registry,
		clock:     clock,
	}
}

// ResolveAndRegister resolves a reference, content-addresses the bytes,
// deduplicates by SHA-256, stores via BlobStore, and registers in the DB.
func (s *AssetService) ResolveAndRegister(ctx context.Context, cmd ResolveAssetCommand) (*Asset, error) {
	if s == nil || s.repo == nil {
		return nil, fmt.Errorf("asset service unavailable")
	}
	reference := strings.TrimSpace(cmd.Reference)
	if reference == "" {
		return nil, fmt.Errorf("empty reference")
	}
	kind := strings.TrimSpace(cmd.Kind)
	if kind == "" {
		return nil, fmt.Errorf("empty kind")
	}

	// 1. Resolve via resolver registry → Source
	source, err := s.registry.ResolveByInference(ctx, reference)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", reference, err)
	}
	if source == nil || source.Reader == nil {
		return nil, fmt.Errorf("resolver returned no reader for %s", reference)
	}
	defer source.Reader.Close()

	// 2. Stage bytes to temp file while computing SHA-256
	ext := extensionFromName(source.SuggestedName, source.MIMEType)
	stagingPath, err := s.blobStore.StagingPath("assets", "pending", ext)
	if err != nil {
		return nil, fmt.Errorf("staging path: %w", err)
	}

	stagingFile, err := os.Create(stagingPath)
	if err != nil {
		return nil, fmt.Errorf("create staging file: %w", err)
	}

	hasher := sha256.New()
	tee := io.TeeReader(source.Reader, hasher)
	sizeBytes, err := io.Copy(stagingFile, tee)
	if err != nil {
		_ = stagingFile.Close()
		_ = s.blobStore.RemoveStaging(stagingPath)
		return nil, fmt.Errorf("stage bytes: %w", err)
	}
	if err := stagingFile.Sync(); err != nil {
		_ = stagingFile.Close()
		_ = s.blobStore.RemoveStaging(stagingPath)
		return nil, fmt.Errorf("sync staging: %w", err)
	}
	_ = stagingFile.Close()

	if sizeBytes == 0 {
		_ = s.blobStore.RemoveStaging(stagingPath)
		return nil, fmt.Errorf("empty asset from %s", reference)
	}
	sha256hex := hex.EncodeToString(hasher.Sum(nil))

	// 3. Dedup: if SHA-256 already exists, return existing asset
	if existing, err := s.repo.GetBySHA256(ctx, sha256hex); err == nil && existing != nil {
		_ = s.blobStore.RemoveStaging(stagingPath)
		return s.recordToAsset(existing), nil
	}

	// 4. Promote to final storage
	assetID := sha256hex
	finalPath := s.blobStore.FinalPath("assets", assetID, ext)
	storageKey, err := s.blobStore.PromoteToFinal(stagingPath, finalPath)
	if err != nil {
		return nil, fmt.Errorf("promote to final: %w", err)
	}

	// 5. Insert asset record
	now := s.clock().UTC().Format(time.RFC3339)
	assetRecord := AssetRecord{
		AssetID:         assetID,
		Kind:            kind,
		Status:          AssetStatusReady,
		SHA256:          sha256hex,
		MimeType:        source.MIMEType,
		SizeBytes:       sizeBytes,
		StorageProvider: "local",
		StorageKey:      storageKey,
		CreatedAt:       now,
		VerifiedAt:      now,
	}
	if err := s.repo.Insert(ctx, assetRecord); err != nil {
		return nil, fmt.Errorf("insert asset: %w", err)
	}

	// 6. Insert source record
	sourceID := generateID()
	sourceRecord := AssetSourceRecord{
		SourceID:        sourceID,
		AssetID:         assetID,
		SourceType:      source.SourceType,
		SourceReference: reference,
		MetadataJSON:    cmd.MetadataJSON,
		CreatedAt:       now,
	}
	if err := s.repo.InsertSource(ctx, sourceRecord); err != nil {
		return nil, fmt.Errorf("insert source: %w", err)
	}

	return &Asset{
		AssetID:         assetID,
		Kind:            kind,
		Status:          AssetStatusReady,
		SHA256:          sha256hex,
		MimeType:        source.MIMEType,
		SizeBytes:       sizeBytes,
		StorageProvider: "local",
		StorageKey:      storageKey,
		CreatedAt:       now,
		VerifiedAt:      now,
	}, nil
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

// ── helpers ──

func extensionFromName(name, mimeType string) string {
	name = strings.TrimSpace(name)
	if name != "" {
		if idx := strings.LastIndex(name, "."); idx >= 0 {
			ext := name[idx:]
			if ext != "" {
				return ext
			}
		}
	}
	switch {
	case strings.HasPrefix(mimeType, "audio/mpeg"):
		return ".mp3"
	case strings.HasPrefix(mimeType, "audio/wav"):
		return ".wav"
	case strings.HasPrefix(mimeType, "audio/mp4"), strings.HasPrefix(mimeType, "audio/m4a"):
		return ".m4a"
	case strings.HasPrefix(mimeType, "video/mp4"):
		return ".mp4"
	case strings.HasPrefix(mimeType, "image/jpeg"):
		return ".jpg"
	case strings.HasPrefix(mimeType, "image/png"):
		return ".png"
	case strings.HasPrefix(mimeType, "image/webp"):
		return ".webp"
	}
	return ".bin"
}

func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

var _ = (*AssetService)(nil)
