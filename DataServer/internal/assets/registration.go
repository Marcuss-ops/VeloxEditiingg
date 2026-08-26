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
	"velox-server/internal/inputsecurity"
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
	maxBytes := int64(0)
	if s.security != nil {
		maxBytes = s.security.Policy().MaxBytes
	}
	if maxBytes <= 0 {
		maxBytes = 256 * 1024 * 1024
	}
	sizeBytes, err := io.Copy(stagingFile, io.LimitReader(tee, maxBytes+1))
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
	if sizeBytes > maxBytes {
		securityErr := inputsecurity.NewError(inputsecurity.NormalizeKind(cmd.Kind), inputsecurity.ErrDownloadTooLarge, "asset exceeds the input byte limit", nil)
		if s.security != nil {
			_ = s.security.Quarantine(stagingPath, inputsecurity.NormalizeKind(cmd.Kind), inputsecurity.ErrDownloadTooLarge, securityErr.Error())
		} else {
			_ = s.blobStore.RemoveStaging(stagingPath)
		}
		return nil, securityErr
	}
	if s.security == nil {
		_ = s.blobStore.RemoveStaging(stagingPath)
		return nil, fmt.Errorf("input security validator unavailable")
	}
	validation, validationErr := s.security.ValidateFile(ctx, stagingPath, inputsecurity.NormalizeKind(cmd.Kind), source.MIMEType)
	if validationErr != nil {
		if quarantineErr := s.security.Quarantine(stagingPath, inputsecurity.NormalizeKind(cmd.Kind), inputsecurity.CodeOf(validationErr), validationErr.Error()); quarantineErr != nil {
			return nil, fmt.Errorf("validate asset: %w; quarantine: %v", validationErr, quarantineErr)
		}
		return nil, validationErr
	}
	sha256hex := hex.EncodeToString(hasher.Sum(nil))

	// 3. Contract-specific preflight runs while the bytes are still staged.
	// This is before dedup and INSERT so a malformed final_audio can never
	// become a READY registry row and only fail later at the consumer gate.
	var preflightMetadata *MediaMetadata
	if cmd.PreRegistrationValidation != nil {
		var preflightErr error
		preflightMetadata, preflightErr = cmd.PreRegistrationValidation(ctx, stagingPath, validation.MIMEType, sizeBytes)
		if preflightErr != nil {
			_ = s.blobStore.RemoveStaging(stagingPath)
			return nil, fmt.Errorf("pre-registration validation: %w", preflightErr)
		}
		if preflightMetadata == nil {
			_ = s.blobStore.RemoveStaging(stagingPath)
			return nil, fmt.Errorf("pre-registration validation: metadata result is required")
		}
		// A contract that returns verified metadata must be persisted through
		// the atomic repository surface. Refuse before promotion when the
		// configured repository cannot provide it; never leave a READY row
		// that the final-audio consumer will reject later.
		if _, ok := s.repo.(VerifiedAssetRepository); !ok {
			_ = s.blobStore.RemoveStaging(stagingPath)
			return nil, fmt.Errorf("pre-registration validation: repository lacks atomic verified-asset support")
		}
	}

	// 4. Dedup: if SHA-256 already exists, return existing asset
	if existing, err := s.repo.GetBySHA256(ctx, sha256hex); err == nil && existing != nil {
		_ = s.blobStore.RemoveStaging(stagingPath)
		sourceID, sourceErr := identity.NewHex128()
		if sourceErr != nil {
			return nil, fmt.Errorf("generate source ID: %w", sourceErr)
		}
		sourceType := strings.TrimSpace(cmd.SourceType)
		if sourceType == "" {
			sourceType = source.SourceType
		}
		if insertErr := s.repo.InsertSource(ctx, AssetSourceRecord{
			SourceID: sourceID, AssetID: existing.AssetID, SourceType: sourceType,
			SourceReference: reference, MetadataJSON: cmd.MetadataJSON,
			CreatedAt: s.clock.Now().UTC().Format(time.RFC3339),
		}); insertErr != nil {
			return nil, fmt.Errorf("insert deduplicated source: %w", insertErr)
		}
		return s.recordToAsset(existing), nil
	}

	// 5. Promote to final storage
	assetID := sha256hex
	finalPath := s.blobStore.FinalPath("assets", assetID, ext)
	storageKey, err := s.blobStore.PromoteToFinal(stagingPath, finalPath)
	if err != nil {
		return nil, fmt.Errorf("promote to final: %w", err)
	}

	// 6. Insert asset record
	now := s.clock.Now().UTC().Format(time.RFC3339)
	assetRecord := AssetRecord{
		AssetID:         assetID,
		Kind:            kind,
		Status:          AssetStatusReady,
		SHA256:          sha256hex,
		MimeType:        validation.MIMEType,
		SizeBytes:       sizeBytes,
		StorageProvider: "local",
		StorageKey:      storageKey,
		MetadataJSON:    cmd.MetadataJSON,
		CreatedAt:       now,
		VerifiedAt:      now,
	}
	if preflightMetadata != nil {
		verifiedRepo := s.repo.(VerifiedAssetRepository)
		metadataRecord, metadataErr := s.mediaMetadataRecord(assetID, preflightMetadata)
		if metadataErr != nil {
			return nil, fmt.Errorf("build verified media metadata: %w", metadataErr)
		}
		sourceID, sourceErr := identity.NewHex128()
		if sourceErr != nil {
			if remover, ok := s.blobStore.(FinalBlobRemover); ok {
				_ = remover.RemoveFinal(storageKey)
			}
			return nil, fmt.Errorf("generate source ID: %w", sourceErr)
		}
		sourceType := strings.TrimSpace(cmd.SourceType)
		if sourceType == "" {
			sourceType = source.SourceType
		}
		sourceRecord := AssetSourceRecord{
			SourceID:        sourceID,
			AssetID:         assetID,
			SourceType:      sourceType,
			SourceReference: reference,
			MetadataJSON:    cmd.MetadataJSON,
			CreatedAt:       now,
		}
		if err := verifiedRepo.InsertWithMediaMetadataAndSource(ctx, assetRecord, metadataRecord, sourceRecord); err != nil {
			if remover, ok := s.blobStore.(FinalBlobRemover); ok {
				_ = remover.RemoveFinal(storageKey)
			}
			return nil, fmt.Errorf("insert verified asset: %w", err)
		}
	} else {
		if err := s.repo.Insert(ctx, assetRecord); err != nil {
			return nil, fmt.Errorf("insert asset: %w", err)
		}

		// 7. Persist canonical media metadata (Fase C1): run the one-time
		// probe on the promoted final blob and store the verified result.
		// Best-effort for generic assets; contract-specific paths return
		// metadata above only after the atomic verified insert succeeds.
		s.persistMediaMetadata(ctx, assetID, finalPath, validation.MIMEType)
	}

	// 8. Insert source record for generic registrations. The final_audio
	// path inserted its source in the same transaction as asset+metadata.
	if preflightMetadata == nil {
		sourceID, err := identity.NewHex128()
		if err != nil {
			return nil, fmt.Errorf("generate source ID: %w", err)
		}
		sourceType := strings.TrimSpace(cmd.SourceType)
		if sourceType == "" {
			sourceType = source.SourceType
		}
		sourceRecord := AssetSourceRecord{
			SourceID:        sourceID,
			AssetID:         assetID,
			SourceType:      sourceType,
			SourceReference: reference,
			MetadataJSON:    cmd.MetadataJSON,
			CreatedAt:       now,
		}
		if err := s.repo.InsertSource(ctx, sourceRecord); err != nil {
			return nil, fmt.Errorf("insert source: %w", err)
		}
	}

	return &Asset{
		AssetID:         assetID,
		Kind:            kind,
		Status:          AssetStatusReady,
		SHA256:          sha256hex,
		MimeType:        validation.MIMEType,
		SizeBytes:       sizeBytes,
		StorageProvider: "local",
		StorageKey:      storageKey,
		MetadataJSON:    cmd.MetadataJSON,
		CreatedAt:       now,
		VerifiedAt:      now,
	}, nil
}
