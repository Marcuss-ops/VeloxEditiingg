package assets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"velox-server/internal/identity"
	"velox-server/internal/inputsecurity"
)

// RewriteVideoClipSegments trims local master-side clip sources and rewrites
// each segment to a canonical velox-asset:// reference. Entries without a
// source_path are left untouched because a worker-only URL must never be
// interpreted as a master filesystem path.
func (s *AssetService) RewriteVideoClipSegments(ctx context.Context, payload map[string]interface{}) error {
	if s == nil || payload == nil {
		return nil
	}
	if err := s.rewriteVideoClipSegmentsMap(ctx, payload); err != nil {
		return err
	}
	for _, key := range []string{"parameters", "render_manifest"} {
		if nested, ok := payload[key].(map[string]interface{}); ok {
			if err := s.rewriteVideoClipSegmentsMap(ctx, nested); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *AssetService) rewriteVideoClipSegmentsMap(ctx context.Context, payload map[string]interface{}) error {
	if raw, ok := payload["clip_segments_json"].(string); ok && strings.TrimSpace(raw) != "" {
		var segments []map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &segments); err != nil {
			return fmt.Errorf("clip_segments_json: %w", err)
		}
		if err := s.rewriteVideoClipSegmentMaps(ctx, segments); err != nil {
			return err
		}
		encoded, err := json.Marshal(segments)
		if err != nil {
			return fmt.Errorf("encode clip_segments_json: %w", err)
		}
		payload["clip_segments_json"] = string(encoded)
	}
	switch segments := payload["clip_segments"].(type) {
	case []interface{}:
		return s.rewriteVideoClipSegmentValues(ctx, segments)
	case []map[string]interface{}:
		return s.rewriteVideoClipSegmentMaps(ctx, segments)
	}
	return nil
}

func (s *AssetService) rewriteVideoClipSegmentValues(ctx context.Context, values []interface{}) error {
	for i, raw := range values {
		segment, ok := raw.(map[string]interface{})
		if !ok {
			return fmt.Errorf("clip_segments[%d] must be an object", i)
		}
		if err := s.rewriteVideoClipSegment(ctx, segment, i); err != nil {
			return err
		}
	}
	return nil
}

func (s *AssetService) rewriteVideoClipSegmentMaps(ctx context.Context, segments []map[string]interface{}) error {
	for i, segment := range segments {
		if err := s.rewriteVideoClipSegment(ctx, segment, i); err != nil {
			return err
		}
	}
	return nil
}

func (s *AssetService) rewriteVideoClipSegment(ctx context.Context, segment map[string]interface{}, index int) error {
	if segment == nil {
		return fmt.Errorf("clip_segments[%d] is nil", index)
	}
	// Only master-local fields may authorize a trim. clip_link/uri can be
	// remote or worker-only references and must fail closed when timing data
	// asks this service to materialize a segment.
	sourcePath := firstString(segment, "source_path", "local_path")
	if sourcePath == "" {
		if _, hasStart := segment["start_seconds"]; hasStart {
			return fmt.Errorf("clip_segments[%d] requires a master-local source_path for trimming", index)
		}
		if _, hasStart := segment["start_ms"]; hasStart {
			return fmt.Errorf("clip_segments[%d] requires a master-local source_path for trimming", index)
		}
		return nil
	}
	start, ok := segmentNumber(segment, "start_seconds")
	if !ok {
		if milliseconds, found := segmentNumber(segment, "start_ms"); found {
			start = milliseconds / 1000
			ok = true
		}
	}
	end, endOK := segmentNumber(segment, "end_seconds")
	if !endOK {
		if milliseconds, found := segmentNumber(segment, "end_ms"); found {
			end = milliseconds / 1000
			endOK = true
		}
	}
	if !ok || !endOK {
		return fmt.Errorf("clip_segments[%d] requires start_seconds/end_seconds or start_ms/end_ms", index)
	}

	asset, result, err := s.TrimAndRegisterVideoSegment(ctx, sourcePath, VideoSegment{StartSeconds: start, EndSeconds: end})
	if err != nil {
		return fmt.Errorf("clip_segments[%d]: %w", index, err)
	}
	segment["asset_id"] = asset.AssetID
	segment["uri"] = asset.Reference()
	segment["clip_link"] = asset.Reference()
	segment["sha256"] = asset.SHA256
	segment["size_bytes"] = asset.SizeBytes
	segment["duration_ms"] = int64(result.Plan.DurationSeconds * 1000)
	segment["trim_mode"] = string(result.Plan.Mode)
	return nil
}

// TrimAndRegisterVideoSegment trims a master-local source and registers only
// the resulting segment through the same content-addressed BlobStore and
// AssetRepository used by ordinary assets.
func (s *AssetService) TrimAndRegisterVideoSegment(ctx context.Context, sourcePath string, segment VideoSegment) (*Asset, TrimResult, error) {
	if s == nil || s.repo == nil || s.blobStore == nil || s.videoTrimmer == nil {
		return nil, TrimResult{}, fmt.Errorf("video asset service unavailable")
	}
	sourceRef := strings.TrimSpace(sourcePath)
	if sourceRef == "" {
		return nil, TrimResult{}, fmt.Errorf("video source path is required")
	}
	materializedPath, cleanup, err := s.materializeVideoSource(ctx, sourceRef)
	if err != nil {
		return nil, TrimResult{}, err
	}
	defer cleanup()

	tmpDir := filepath.Dir(materializedPath)
	tmpOutput, err := os.CreateTemp(tmpDir, ".velox-video-segment-*.mp4")
	if err != nil {
		return nil, TrimResult{}, fmt.Errorf("create segment output: %w", err)
	}
	outputPath := tmpOutput.Name()
	if err := tmpOutput.Close(); err != nil {
		_ = os.Remove(outputPath)
		return nil, TrimResult{}, fmt.Errorf("close segment output: %w", err)
	}
	defer os.Remove(outputPath)

	result, err := s.videoTrimmer.Trim(ctx, materializedPath, outputPath, segment)
	if err != nil {
		return nil, TrimResult{}, err
	}

	asset, err := s.registerPreparedVideoFile(ctx, outputPath, sourceRef, result.Plan)
	if err != nil {
		return nil, TrimResult{}, err
	}
	return asset, result, nil
}

func (s *AssetService) materializeVideoSource(ctx context.Context, sourceRef string) (string, func(), error) {
	if info, err := os.Stat(sourceRef); err == nil {
		if !info.Mode().IsRegular() {
			return "", func() {}, fmt.Errorf("video source is not a regular file")
		}
		if s.security != nil {
			if _, err := s.security.ValidateFile(ctx, sourceRef, inputsecurity.KindUnknown, ""); err != nil {
				return "", func() {}, fmt.Errorf("validate local video source: %w", err)
			}
		}
		return sourceRef, func() {}, nil
	}
	if s.registry == nil {
		return "", func() {}, fmt.Errorf("video source %q is not a master-local file and no resolver is configured", sourceRef)
	}
	source, err := s.registry.ResolveByInference(ctx, sourceRef)
	if err != nil || source == nil || source.Reader == nil {
		if err == nil {
			err = fmt.Errorf("resolver returned no reader")
		}
		return "", func() {}, fmt.Errorf("resolve video source %q: %w", sourceRef, err)
	}
	defer source.Reader.Close()
	if s.security == nil {
		return "", func() {}, fmt.Errorf("video source security validator unavailable")
	}
	policy := s.security.Policy()
	if strings.TrimSpace(policy.TempDir) == "" {
		return "", func() {}, fmt.Errorf("video source security temp directory unavailable")
	}
	if err := os.MkdirAll(policy.TempDir, 0o700); err != nil {
		return "", func() {}, fmt.Errorf("create materialized video source directory: %w", err)
	}
	tmp, err := os.CreateTemp(policy.TempDir, ".velox-video-source-*.bin")
	if err != nil {
		return "", func() {}, fmt.Errorf("create materialized video source: %w", err)
	}
	path := tmp.Name()
	maxBytes := policy.MaxBytes
	if maxBytes <= 0 {
		_ = tmp.Close()
		_ = os.Remove(path)
		return "", func() {}, fmt.Errorf("video source security byte limit unavailable")
	}
	written, err := io.Copy(tmp, io.LimitReader(source.Reader, maxBytes+1))
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(path)
		return "", func() {}, fmt.Errorf("materialize video source: %w", err)
	}
	if written > maxBytes {
		_ = tmp.Close()
		_ = s.security.Quarantine(path, inputsecurity.KindClip, inputsecurity.ErrDownloadTooLarge, "materialized video source exceeded the input byte limit")
		return "", func() {}, inputsecurity.NewError(inputsecurity.KindClip, inputsecurity.ErrDownloadTooLarge, "materialized video source exceeds the input byte limit", nil)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(path)
		return "", func() {}, fmt.Errorf("sync materialized video source: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(path)
		return "", func() {}, fmt.Errorf("close materialized video source: %w", err)
	}
	if _, err := s.security.ValidateFile(ctx, path, inputsecurity.KindClip, source.MIMEType); err != nil {
		_ = s.security.Quarantine(path, inputsecurity.KindClip, inputsecurity.CodeOf(err), err.Error())
		return "", func() {}, fmt.Errorf("validate materialized video source: %w", err)
	}
	return path, func() { _ = os.Remove(path) }, nil
}

func (s *AssetService) registerPreparedVideoFile(ctx context.Context, preparedPath, sourcePath string, plan TrimPlan) (*Asset, error) {
	stagingPath, err := s.blobStore.StagingPath("assets", "pending", ".mp4")
	if err != nil {
		return nil, fmt.Errorf("segment staging path: %w", err)
	}
	staging, err := os.OpenFile(stagingPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create segment staging file: %w", err)
	}
	input, err := os.Open(preparedPath)
	if err != nil {
		_ = staging.Close()
		_ = s.blobStore.RemoveStaging(stagingPath)
		return nil, fmt.Errorf("open prepared segment: %w", err)
	}
	hasher := sha256.New()
	maxBytes := int64(0)
	if s.security != nil {
		maxBytes = s.security.Policy().MaxBytes
	}
	if maxBytes <= 0 {
		maxBytes = 256 * 1024 * 1024
	}
	written, copyErr := io.Copy(io.MultiWriter(staging, hasher), io.LimitReader(input, maxBytes+1))
	closeInputErr := input.Close()
	closeStagingErr := staging.Close()
	if copyErr != nil || closeInputErr != nil || closeStagingErr != nil {
		_ = s.blobStore.RemoveStaging(stagingPath)
		if copyErr != nil {
			return nil, fmt.Errorf("stage prepared segment: %w", copyErr)
		}
		if closeInputErr != nil {
			return nil, fmt.Errorf("close prepared segment: %w", closeInputErr)
		}
		return nil, fmt.Errorf("close segment staging file: %w", closeStagingErr)
	}
	if written > maxBytes {
		_ = s.security.Quarantine(stagingPath, inputsecurity.KindClip, inputsecurity.ErrDownloadTooLarge, "prepared video segment exceeded the input byte limit")
		return nil, inputsecurity.NewError(inputsecurity.KindClip, inputsecurity.ErrDownloadTooLarge, "prepared video segment exceeds the input byte limit", nil)
	}
	if written <= 0 {
		_ = s.blobStore.RemoveStaging(stagingPath)
		return nil, fmt.Errorf("prepared video segment is empty")
	}
	if s.security != nil {
		if _, err := s.security.ValidateFile(ctx, stagingPath, inputsecurity.KindClip, "video/mp4"); err != nil {
			_ = s.security.Quarantine(stagingPath, inputsecurity.KindClip, inputsecurity.CodeOf(err), err.Error())
			return nil, fmt.Errorf("validate prepared video segment: %w", err)
		}
	}
	if err := syncPreparedSegment(stagingPath); err != nil {
		_ = s.blobStore.RemoveStaging(stagingPath)
		return nil, fmt.Errorf("sync segment staging file: %w", err)
	}
	sha256Hex := hex.EncodeToString(hasher.Sum(nil))
	if existing, lookupErr := s.repo.GetBySHA256(ctx, sha256Hex); lookupErr == nil && existing != nil {
		_ = s.blobStore.RemoveStaging(stagingPath)
		return s.recordToAsset(existing), nil
	}

	finalPath := s.blobStore.FinalPath("assets", sha256Hex, ".mp4")
	storageKey, err := s.blobStore.PromoteToFinal(stagingPath, finalPath)
	if err != nil {
		_ = s.blobStore.RemoveStaging(stagingPath)
		return nil, fmt.Errorf("promote prepared segment: %w", err)
	}
	now := s.clock.Now().UTC().Format(time.RFC3339)
	metadata, _ := json.Marshal(map[string]interface{}{
		"source_name":           filepath.Base(sourcePath),
		"start_seconds":         plan.Segment.StartSeconds,
		"end_seconds":           plan.Segment.EndSeconds,
		"duration_seconds":      plan.DurationSeconds,
		"trim_mode":             plan.Mode,
		"normalized":            plan.RequiresNormalization,
		"normalization_version": "video-normalization.v1",
	})
	record := AssetRecord{
		AssetID:         sha256Hex,
		Kind:            "video_segment",
		Status:          AssetStatusReady,
		SHA256:          sha256Hex,
		MimeType:        "video/mp4",
		SizeBytes:       written,
		StorageProvider: "local",
		StorageKey:      storageKey,
		MetadataJSON:    string(metadata),
		CreatedAt:       now,
		VerifiedAt:      now,
	}
	if err := s.repo.Insert(ctx, record); err != nil {
		return nil, fmt.Errorf("register prepared segment: %w", err)
	}
	sourceID, err := identity.NewHex128()
	if err != nil {
		return nil, fmt.Errorf("generate segment source ID: %w", err)
	}
	if err := s.repo.InsertSource(ctx, AssetSourceRecord{
		SourceID:        sourceID,
		AssetID:         sha256Hex,
		SourceType:      "video_segment",
		SourceReference: sourcePath,
		MetadataJSON:    string(metadata),
		CreatedAt:       now,
	}); err != nil {
		return nil, fmt.Errorf("register segment source: %w", err)
	}
	return &Asset{
		AssetID:         sha256Hex,
		Kind:            record.Kind,
		Status:          record.Status,
		SHA256:          record.SHA256,
		MimeType:        record.MimeType,
		SizeBytes:       record.SizeBytes,
		StorageProvider: record.StorageProvider,
		StorageKey:      record.StorageKey,
		MetadataJSON:    record.MetadataJSON,
		CreatedAt:       now,
		VerifiedAt:      now,
	}, nil
}

func syncPreparedSegment(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func segmentNumber(fields map[string]interface{}, key string) (float64, bool) {
	value, ok := fields[key]
	if !ok {
		return 0, false
	}
	switch number := value.(type) {
	case float64:
		return number, number >= 0
	case float32:
		return float64(number), number >= 0
	case int:
		return float64(number), number >= 0
	case int64:
		return float64(number), number >= 0
	case json.Number:
		parsed, err := number.Float64()
		return parsed, err == nil && parsed >= 0
	default:
		return 0, false
	}
}

func firstString(fields map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := fields[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
