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
	"velox-server/internal/platform/clock"
	"velox-shared/payload"
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

// RewriteVoiceoverPayload resolves all voiceover references in the
// payload and rewrites them to canonical velox-asset:// references.
//
// PR15.6: delegates to applyRewrite with role=RoleVoiceover; the
// collector+applicator pair are the only role-specific knowledge.
func (s *AssetService) RewriteVoiceoverPayload(ctx context.Context, payloadMap map[string]interface{}) error {
	return s.applyRewrite(ctx, payloadMap, RoleVoiceover, collectVoiceoverReferences, applyVoiceoverReferences)
}

// RewriteSceneImagePayload resolves all scene image references in the
// payload and rewrites them to canonical velox-asset:// references.
func (s *AssetService) RewriteSceneImagePayload(ctx context.Context, payloadMap map[string]interface{}) error {
	return s.applyRewrite(ctx, payloadMap, RoleSceneImage, collectSceneImageReferences, applySceneImageReferences)
}

// applyRewrite is the shared collect→resolve→apply worker used by every
// payload rewrite flavor. It iterates the references collected from the
// payload, looks each one up via the asset service, then writes the
// canonical references back through the role-specific applicator.
//
// Parameters
//   - ctx:               request context for resolver + DB calls.
//   - payloadMap:        mutable payload to rewrite (top-level and parameters
//                        sub-map, depending on the applicator).
//   - kind:              asset role label (RoleVoiceover / RoleSceneImage etc.)
//                        used when registering a freshly-resolved asset.
//   - collector:         extracts references from the payload. Must dedup
//                        internally; returns nil/empty when nothing to do.
//   - applicator:        writes the rewritten references back. Receives the
//                        already-resolved canonical reference list (each item
//                        is either a velox-asset:// scheme URL returned by
//                        ResolveAndRegister, or an already-canonical URL
//                        that was passed-through).
//
// Errors from the asset service surface to the caller verbatim. nil-nil
// short-circuits when the service is gone or the payload is nil; missing
// or empty collector result is a no-op.
func (s *AssetService) applyRewrite(
	ctx context.Context,
	payloadMap map[string]interface{},
	kind string,
	collector func(map[string]interface{}) []string,
	applicator func(map[string]interface{}, []string),
) error {
	if s == nil || payloadMap == nil || collector == nil || applicator == nil {
		return nil
	}
	references := collector(payloadMap)
	if len(references) == 0 {
		return nil
	}

	refs := make([]string, 0, len(references))
	for _, ref := range references {
		trimmed := strings.TrimSpace(ref)
		if trimmed == "" {
			continue
		}
		// Skip already-canonical velox-asset references — nothing to do.
		if strings.HasPrefix(trimmed, VeloxAssetScheme+"://") {
			refs = append(refs, trimmed)
			continue
		}
		asset, err := s.ResolveAndRegister(ctx, ResolveAssetCommand{
			Kind:      kind,
			Reference: trimmed,
		})
		if err != nil {
			return err
		}
		if asset == nil {
			continue
		}
		refs = append(refs, asset.Reference())
	}
	if len(refs) == 0 {
		return nil
	}

	applicator(payloadMap, refs)
	return nil
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

// ── voiceover payload helpers (shared with legacy bridge) ────────────────────

func collectVoiceoverReferences(payloadMap map[string]interface{}) []string {
	if payloadMap == nil {
		return nil
	}
	var candidates []string

	// PR15.6 canonical input: voiceover_paths is now the only top-level alias.
	if v, ok := payloadMap["voiceover_paths"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}

	// Legacy aliases (id/run_id/title/voiceover_path/audio_path) are dropped
	// from canonical WRITES but collectors must still tolerate legacy payloads
	// flowing through — e.g. older jobs still in SQLite, or external
	// request bodies that the HTTP boundary adapter translates separately.
	candidates = append(candidates,
		payload.FirstString(payloadMap, "voiceover", "unified_voiceover_link"),
	)
	// Below keys still tolerated on input only (read-fallback). They will
	// never be set by canonical writers in PR15.6+.
	candidates = append(candidates,
		payload.FirstString(payloadMap, "voiceover_path", "audio_path"),
	)

	if params, ok := payloadMap["parameters"].(map[string]interface{}); ok {
		if v, ok := params["voiceover_paths"]; ok {
			candidates = append(candidates, payload.NormalizeToStrings(v)...)
		}
		candidates = append(candidates, payload.FirstString(params, "voiceover_path", "audio_path", "voiceover"))
	}

	return payload.DedupeStrings(candidates)
}

// applyVoiceoverReferences writes the canonical voiceover_paths array back
// to the payload AND mirrors it into the parameters sub-map.
//
// PR15.6: writes ONLY the canonical `voiceover_paths` key (array). The
// singular `voiceover_path` and `audio_path` aliases are intentionally
// NOT written here — downstream HTTP-edge reads via RenderHTTPBoundaryJobResponse
// still tolerate them when reading legacy SQLite rows.
func applyVoiceoverReferences(payloadMap map[string]interface{}, refs []string) {
	if len(refs) == 0 || payloadMap == nil {
		return
	}
	payloadMap["voiceover_paths"] = append([]string(nil), refs...)
	if params, ok := payloadMap["parameters"].(map[string]interface{}); ok {
		params["voiceover_paths"] = append([]string(nil), refs...)
	}
}

// ── scene image payload helpers ─────────────────────────────────────────

func collectSceneImageReferences(payloadMap map[string]interface{}) []string {
	if payloadMap == nil {
		return nil
	}
	var candidates []string

	// PR15.6: scene_image_paths is the canonical input.
	if v, ok := payloadMap["scene_image_paths"]; ok {
		candidates = append(candidates, payload.NormalizeToStrings(v)...)
	}

	// From scenes array — extract image_link from each scene entry.
	// Handles both []map[string]interface{} (normalized payload) and
	// []interface{} (raw JSON from json.Unmarshal).
	switch scenes := payloadMap["scenes"].(type) {
	case []map[string]interface{}:
		for _, scene := range scenes {
			if img, ok := scene["image_link"].(string); ok {
				candidates = append(candidates, img)
			}
			if imgs, ok := scene["image_links"].([]string); ok {
				candidates = append(candidates, imgs...)
			}
		}
	case []interface{}:
		for _, item := range scenes {
			if scene, ok := item.(map[string]interface{}); ok {
				if img, ok := scene["image_link"].(string); ok {
					candidates = append(candidates, img)
				}
				if imgs, ok := scene["image_links"].([]string); ok {
					candidates = append(candidates, imgs...)
				}
			}
		}
	}

	if params, ok := payloadMap["parameters"].(map[string]interface{}); ok {
		if v, ok := params["scene_image_paths"]; ok {
			candidates = append(candidates, payload.NormalizeToStrings(v)...)
		}
	}

	return payload.DedupeStrings(candidates)
}

// applySceneImageReferences writes the canonical scene_image_paths array
// back to the payload, AND mirrors per-scene rewrites (image_link +
// image_links) AND the parameters sub-map.
//
// PR15.6: writes ONLY canonical keys; no legacy alias keys are written.
func applySceneImageReferences(payloadMap map[string]interface{}, refs []string) {
	if len(refs) == 0 || payloadMap == nil {
		return
	}

	payloadMap["scene_image_paths"] = append([]string(nil), refs...)

	switch scenes := payloadMap["scenes"].(type) {
	case []map[string]interface{}:
		for i, scene := range scenes {
			if i < len(refs) {
				scene["image_link"] = refs[i]
				scene["image_links"] = []string{refs[i]}
			}
		}
	case []interface{}:
		for i, item := range scenes {
			if i < len(refs) {
				if scene, ok := item.(map[string]interface{}); ok {
					scene["image_link"] = refs[i]
					scene["image_links"] = []string{refs[i]}
				}
			}
		}
	}

	if params, ok := payloadMap["parameters"].(map[string]interface{}); ok {
		params["scene_image_paths"] = append([]string(nil), refs...)
	}
}

var _ = (*AssetService)(nil)
