package assets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"velox-server/internal/identity"
)

// registration.go owns the asset-registration pipeline: the unique
// ResolveAndRegister method that resolves a reference, content-addresses
// the bytes, deduplicates by SHA-256, stores via BlobStore, and registers
// in the DB. The media-extension helper lives in media_extension.go;
// the per-asset-role collectors/applicators live in rewrite_voiceover.go
// and rewrite_scene_images.go.

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
	now := s.clock.Now().UTC().Format(time.RFC3339)
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
	sourceID, err := identity.NewHex128()
	if err != nil {
		return nil, fmt.Errorf("generate source ID: %w", err)
	}
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
		MetadataJSON:    "",
		CreatedAt:       now,
		VerifiedAt:      now,
	}, nil
}
