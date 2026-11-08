package assets

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"velox-server/internal/identity"
	"velox-server/internal/inputsecurity"

	"velox-shared/assetref"
	"velox-shared/contract"
	"velox-shared/iopool"
)

// RewriteVideoClipSegments trims local master-side clip sources and rewrites
// each segment to a canonical velox-asset:// reference. Entries without a
// source_path are left untouched because a worker-only URL must never be
// interpreted as a master filesystem path.
func (s *AssetService) RewriteVideoClipSegments(ctx context.Context, payload map[string]interface{}) error {
	if s == nil || payload == nil {
		return nil
	}
	// Fase C2: ONE probe per source across the whole rewrite. The memo is
	// hoisted here so a source referenced in the payload AND in nested
	// parameters/render_manifest maps is still probed exactly once.
	// Assumption: a source's content is stable within one rewrite (a job
	// payload); for remote sources each segment materializes a fresh temp
	// copy of the same reference, and the memoized probe is reused across
	// those copies.
	probes := make(map[string]*VideoProbe)
	if err := s.rewriteVideoClipSegmentsMap(ctx, payload, probes); err != nil {
		return err
	}
	for _, key := range []string{"parameters", "render_manifest"} {
		if nested, ok := payload[key].(map[string]interface{}); ok {
			if err := s.rewriteVideoClipSegmentsMap(ctx, nested, probes); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *AssetService) rewriteVideoClipSegmentsMap(ctx context.Context, payload map[string]interface{}, probes map[string]*VideoProbe) error {
	if raw, ok := payload["clip_segments_json"].(string); ok && strings.TrimSpace(raw) != "" {
		var segments []map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &segments); err != nil {
			return fmt.Errorf("clip_segments_json: %w", err)
		}
		if err := s.rewriteVideoClipSegmentMaps(ctx, segments, probes); err != nil {
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
		return s.rewriteVideoClipSegmentValues(ctx, segments, probes)
	case []map[string]interface{}:
		return s.rewriteVideoClipSegmentMaps(ctx, segments, probes)
	}
	return nil
}

func (s *AssetService) rewriteVideoClipSegmentValues(ctx context.Context, values []interface{}, probes map[string]*VideoProbe) error {
	for i, raw := range values {
		segment, ok := raw.(map[string]interface{})
		if !ok {
			return fmt.Errorf("clip_segments[%d] must be an object", i)
		}
		if err := s.rewriteVideoClipSegment(ctx, segment, i, probes); err != nil {
			return err
		}
	}
	return nil
}

func (s *AssetService) rewriteVideoClipSegmentMaps(ctx context.Context, segments []map[string]interface{}, probes map[string]*VideoProbe) error {
	for i, segment := range segments {
		if err := s.rewriteVideoClipSegment(ctx, segment, i, probes); err != nil {
			return err
		}
	}
	return nil
}

// registeredAssetReference extracts an asset id from a canonical
// velox-asset:// reference. Non-registry references (master-local paths,
// drive URLs) yield ("", false) and skip the Fase C2 fail-closed metadata
// gate.
func registeredAssetReference(reference string) (string, bool) {
	ref, err := assetref.ParseCanonicalWire(reference)
	if err != nil || ref.Kind() != assetref.RefKindLocal || ref.ID() == "" {
		return "", false
	}
	return ref.ID(), true
}

func (s *AssetService) rewriteVideoClipSegment(ctx context.Context, segment map[string]interface{}, index int, probes map[string]*VideoProbe) error {
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

	// Fase C2 fail-closed gate: a source that references an already-registered
	// media asset MUST have verified registry metadata before it can be
	// trimmed. EnsureMediaMetadata consumes the registry when a verified row
	// exists, otherwise it probes ONCE through the canonical component and
	// persists; an unverifiable media asset is REJECTED — metadata is never
	// invented.
	if assetID, ok := registeredAssetReference(sourcePath); ok {
		if _, err := s.EnsureMediaMetadata(ctx, assetID); err != nil {
			return fmt.Errorf("clip_segments[%d]: registered media asset %s: %w", index, assetID, err)
		}
	}

	asset, result, err := s.TrimAndRegisterVideoSegment(ctx, sourcePath, VideoSegment{StartSeconds: start, EndSeconds: end}, probes)
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
//
// The optional variadic `memo` carries per-source probes across segments of
// the SAME rewrite (Fase C2: probe UNA volta) so N segments from one source
// spawn exactly ONE ffprobe instead of N.
func (s *AssetService) TrimAndRegisterVideoSegment(ctx context.Context, sourcePath string, segment VideoSegment, memo ...map[string]*VideoProbe) (*Asset, TrimResult, error) {
	if s == nil || s.repo == nil || s.blobStore == nil || s.videoTrimmer == nil {
		return nil, TrimResult{}, fmt.Errorf("video asset service unavailable")
	}
	sourceRef := strings.TrimSpace(sourcePath)
	if sourceRef == "" {
		return nil, TrimResult{}, fmt.Errorf("video source path is required")
	}
	var sourceProbes map[string]*VideoProbe
	if len(memo) > 0 {
		sourceProbes = memo[0]
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

	// Canonical probe, reused across segments of the same source.
	var probe VideoProbe
	probeLoaded := false
	if sourceProbes != nil {
		if cached := sourceProbes[sourceRef]; cached != nil {
			probe = *cached
			probeLoaded = true
		}
	}
	if !probeLoaded {
		var probeErr error
		probe, probeErr = s.videoTrimmer.Probe(ctx, materializedPath)
		if probeErr != nil {
			return nil, TrimResult{}, probeErr
		}
		if sourceProbes != nil {
			copied := probe
			sourceProbes[sourceRef] = &copied
		}
	}

	result, err := s.videoTrimmer.TrimWithProbe(ctx, probe, materializedPath, outputPath, segment)
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
			if _, err := s.security.ValidateFileForCanonicalProbe(ctx, sourceRef, inputsecurity.KindUnknown, ""); err != nil {
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
	// A4-3: pooled 1 MiB copy buffer (velox-shared/iopool) — the
	// materialization copy can stream hundreds of MB; a fresh buffer per
	// source churns GC under concurrent intake.
	pooled := iopool.Get()
	defer iopool.Put(pooled)
	written, err := io.CopyBuffer(tmp, io.LimitReader(source.Reader, maxBytes+1), pooled[:])
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
	if _, err := s.security.ValidateFileForCanonicalProbe(ctx, path, inputsecurity.KindClip, source.MIMEType); err != nil {
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
	maxBytes := int64(0)
	if s.security != nil {
		maxBytes = s.security.Policy().MaxBytes
	}
	if maxBytes <= 0 {
		maxBytes = 256 * 1024 * 1024
	}
	sourceInfo, err := os.Stat(preparedPath)
	if err != nil {
		return nil, fmt.Errorf("open prepared segment: %w", err)
	}
	if sourceInfo.Size() > maxBytes {
		return nil, inputsecurity.NewError(inputsecurity.KindClip, inputsecurity.ErrDownloadTooLarge, "prepared video segment exceeds the input byte limit", nil)
	}
	written, sha256Hex, _, err := stagePreparedFragment(preparedPath, stagingPath, maxBytes, true)
	if err != nil {
		_ = s.blobStore.RemoveStaging(stagingPath)
		return nil, fmt.Errorf("stage prepared segment: %w", err)
	}
	if written <= 0 {
		_ = s.blobStore.RemoveStaging(stagingPath)
		return nil, fmt.Errorf("prepared video segment is empty")
	}
	if s.security != nil {
		if _, err := s.security.ValidateFileForCanonicalProbe(ctx, stagingPath, inputsecurity.KindClip, "video/mp4"); err != nil {
			_ = s.security.Quarantine(stagingPath, inputsecurity.KindClip, inputsecurity.CodeOf(err), err.Error())
			return nil, fmt.Errorf("validate prepared video segment: %w", err)
		}
	}
	if err := syncPreparedSegment(stagingPath); err != nil {
		_ = s.blobStore.RemoveStaging(stagingPath)
		return nil, fmt.Errorf("sync segment staging file: %w", err)
	}
	if plan.ExpectedOutputSizeBytes > 0 && written != plan.ExpectedOutputSizeBytes {
		_ = s.blobStore.RemoveStaging(stagingPath)
		return nil, fmt.Errorf("prepared segment size differs from worker certificate")
	}
	if plan.ExpectedOutputSHA256 != "" && sha256Hex != plan.ExpectedOutputSHA256 {
		_ = s.blobStore.RemoveStaging(stagingPath)
		return nil, fmt.Errorf("prepared segment SHA-256 differs from worker certificate")
	}
	if existing, lookupErr := s.repo.GetBySHA256(ctx, sha256Hex); lookupErr == nil && existing != nil {
		_ = s.blobStore.RemoveStaging(stagingPath)
		if strings.HasPrefix(sourcePath, "prepared-window:") {
			sourceID, sourceErr := identity.NewHex128()
			if sourceErr != nil {
				return nil, fmt.Errorf("generate prepared window source ID: %w", sourceErr)
			}
			if insertErr := s.repo.InsertSource(ctx, AssetSourceRecord{
				SourceID: sourceID, AssetID: existing.AssetID, SourceType: "video.prepare.window.v1",
				SourceReference: sourcePath, MetadataJSON: plan.PreparationCertificateJSON,
				CreatedAt: s.clock.Now().UTC().Format(time.RFC3339),
			}); insertErr != nil {
				return nil, fmt.Errorf("register prepared window source identity: %w", insertErr)
			}
		}
		return s.recordToAsset(existing), nil
	}

	finalPath := s.blobStore.FinalPath("assets", sha256Hex, ".mp4")
	storageKey, err := s.blobStore.PromoteToFinal(stagingPath, finalPath)
	if err != nil {
		_ = s.blobStore.RemoveStaging(stagingPath)
		return nil, fmt.Errorf("promote prepared segment: %w", err)
	}
	now := s.clock.Now().UTC().Format(time.RFC3339)
	// The manifest records the CANONICAL stream identity the segment was
	// prepared against, so downstream consumers (preflight, plan compiler,
	// future chunk factory) never re-derive compatibility from a second
	// authority. The historical `normalization_version:
	// "video-normalization.v1"` label is deliberately gone: it named the
	// removed duplicate authority.
	metadataFields := map[string]interface{}{
		"source_name":                 filepath.Base(sourcePath),
		"start_seconds":               plan.Segment.StartSeconds,
		"end_seconds":                 plan.Segment.EndSeconds,
		"duration_seconds":            plan.DurationSeconds,
		"duration_us":                 int64(plan.DurationSeconds * 1_000_000),
		"trim_mode":                   plan.Mode,
		"normalized":                  plan.RequiresNormalization,
		"canonical_profile_id":        plan.CanonicalProfile.ProfileID,
		"canonical_stream_profile_id": plan.CanonicalProfile.StreamProfile(),
		"canonical_profile_version":   plan.CanonicalProfile.Version,
		"preparation_revision":        s.videoTrimmer.PreparationRevision(),
	}
	if plan.PreparationCertificateJSON != "" {
		metadataFields["window_certificate"] = json.RawMessage(plan.PreparationCertificateJSON)
		metadataFields["source_start_frame"] = plan.SourceStartFrame
	}
	if plan.OutputFrameCount > 0 {
		metadataFields["frame_count"] = plan.OutputFrameCount
		metadataFields["first_frame_keyframe"] = plan.FirstFrameKeyframe
		metadataFields["closed_gop"] = plan.ClosedGOP
		metadataFields["copy_eligible"] = plan.CopyEligible
	}
	if plan.SourceSHA256 != "" {
		metadataFields["source_sha256"] = plan.SourceSHA256
		metadataFields["source_size_bytes"] = plan.SourceSizeBytes
		metadataFields["source_duration_us"] = plan.SourceDurationUS
		metadataFields["source_frame_count"] = plan.SourceFrameCount
		metadataFields["source_fps_num"] = plan.SourceFPSNum
		metadataFields["source_fps_den"] = plan.SourceFPSDen
		metadataFields["source_codec"] = plan.SourceCodec
		metadataFields["source_profile"] = plan.SourceProfile
		metadataFields["source_level"] = plan.SourceLevel
		metadataFields["source_width"] = plan.SourceWidth
		metadataFields["source_height"] = plan.SourceHeight
		metadataFields["source_pixel_format"] = plan.SourcePixelFormat
		metadataFields["source_keyframes_seconds"] = plan.SourceKeyframes
	}
	metadata, _ := json.Marshal(metadataFields)
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
	// Fase C2: persist the canonical media metadata for the segment too, so
	// later consumers (preflight, plan compiler, trimmer) read the registry
	// instead of re-probing. Best-effort by contract — a failed probe leaves
	// no metadata row and never fails the registration.
	if plan.OutputFrameCount > 0 {
		profile := plan.CanonicalProfile
		_ = s.upsertMediaMetadataRecord(ctx, sha256Hex, &MediaMetadata{
			Container: "mp4", DurationMs: int64(plan.DurationSeconds * 1000), VideoCodec: strings.ToLower(profile.Codec),
			PixelFormat: strings.ToLower(profile.PixelFormat), Width: profile.Width, Height: profile.Height,
			FPSNum: profile.FPSNum, FPSDen: profile.FPSDen, TimeBaseNum: profile.TimeBaseNum, TimeBaseDen: profile.TimeBaseDen,
		})
	} else {
		s.persistMediaMetadata(ctx, sha256Hex, finalPath, "video/mp4")
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

// RegisterPreparedWindow verifies and stores a fragment produced by the
// worker-side video.prepare.window.v1 executor. The source reference is the
// shared content-preparation identity, so a later job can find the same
// certified bytes without fetching or preparing the source again.
func (s *AssetService) RegisterPreparedWindow(
	ctx context.Context,
	preparedPath string,
	request contract.VideoPrepareWindowV1,
	certificate contract.VideoPrepareWindowCertificateV1,
) (*Asset, error) {
	if s == nil || s.repo == nil || s.blobStore == nil || s.videoTrimmer == nil {
		return nil, fmt.Errorf("prepared window registration is unavailable")
	}
	if err := certificate.ValidateFor(request); err != nil {
		return nil, err
	}
	profile, err := contract.KnownCanonicalVideoProfileV1(request.OutputProfileID)
	if err != nil {
		return nil, err
	}
	revision, err := contract.VideoWindowPreparationRevision(profile, contract.CanonicalPreparationQualityPolicyDefault)
	if err != nil || revision != request.OutputProfileRevision {
		return nil, fmt.Errorf("prepared window profile revision is not enabled")
	}
	info, err := os.Stat(preparedPath)
	if err != nil || !info.Mode().IsRegular() || info.Size() != certificate.OutputSizeBytes {
		if err == nil {
			err = fmt.Errorf("prepared window size differs from certificate")
		}
		return nil, fmt.Errorf("prepared window file: %w", err)
	}
	if s.security == nil {
		return nil, fmt.Errorf("prepared window security validator is unavailable")
	}
	if _, err := s.security.ValidateFileForCanonicalProbe(ctx, preparedPath, inputsecurity.KindClip, "video/mp4"); err != nil {
		return nil, fmt.Errorf("prepared window media validation: %w", err)
	}
	probe, err := s.videoTrimmer.Probe(ctx, preparedPath)
	if err != nil {
		return nil, err
	}
	frames, err := s.videoTrimmer.countFrames(ctx, preparedPath)
	if err != nil {
		return nil, err
	}
	evidence := certificate.OutputEvidence
	level, levelErr := strconv.Atoi(strings.ReplaceAll(profile.CodecLevel, ".", ""))
	if levelErr != nil || probe.VideoCodec != profile.Codec || probe.VideoProfile != strings.ToLower(profile.CodecProfile) ||
		probe.VideoLevel != level || probe.Width != profile.Width || probe.Height != profile.Height ||
		probe.PixelFormat != profile.PixelFormat || probe.FPSNum != profile.FPSNum || probe.FPSDen != profile.FPSDen ||
		probe.TimebaseNum != profile.TimeBaseNum || probe.TimebaseDen != profile.TimeBaseDen || frames != request.OutputFrameCount ||
		probe.AudioCodec != "" || evidence.Codec != probe.VideoCodec || evidence.Profile != probe.VideoProfile ||
		evidence.Level != probe.VideoLevel || evidence.Width != probe.Width || evidence.Height != probe.Height ||
		evidence.PixelFormat != probe.PixelFormat || evidence.FPSNum != probe.FPSNum || evidence.FPSDen != probe.FPSDen ||
		evidence.TimeBaseNum != probe.TimebaseNum || evidence.TimeBaseDen != probe.TimebaseDen || evidence.FrameCount != frames {
		return nil, fmt.Errorf("prepared window does not match the independently probed canonical profile")
	}
	if len(probe.Keyframes) == 0 || probe.Keyframes[0] > float64(profile.FPSDen)/float64(profile.FPSNum) {
		return nil, fmt.Errorf("prepared window does not begin with a keyframe")
	}
	identity, err := contract.VideoPrepareWindowCacheIdentity(request)
	if err != nil {
		return nil, err
	}
	existing, lookupErr := s.GetBySourceReference(ctx, "prepared-window:"+identity)
	if lookupErr != nil {
		return nil, fmt.Errorf("lookup prepared window identity: %w", lookupErr)
	}
	if existing != nil {
		if existing.SHA256 != certificate.OutputSHA256 || existing.SizeBytes != certificate.OutputSizeBytes {
			return nil, fmt.Errorf("prepared window identity already maps to different bytes")
		}
		return existing, nil
	}
	startSeconds, durationSeconds := float64(request.SourceStartUS)/1_000_000, float64(request.SourceDurationUS)/1_000_000
	if request.SourceDurationUS == 0 {
		startSeconds = float64(request.SourceStartFrame) * float64(request.SourceFPSDen) / float64(request.SourceFPSNum)
		durationSeconds = float64(request.SourceFrameCount) * float64(request.SourceFPSDen) / float64(request.SourceFPSNum)
	}
	plan := TrimPlan{
		Mode: TrimModeFrameAccurateReencode, Segment: VideoSegment{StartSeconds: startSeconds, EndSeconds: startSeconds + durationSeconds},
		DurationSeconds:       float64(request.OutputFrameCount) * float64(profile.FPSDen) / float64(profile.FPSNum),
		RequiresNormalization: true, CanonicalProfile: profile, OutputFrameCount: request.OutputFrameCount,
		FirstFrameKeyframe: true, ClosedGOP: certificate.ClosedGOPAttested,
		SourceSHA256: request.Source.SHA256, SourceSizeBytes: request.Source.SizeBytes,
		SourceStartFrame: request.SourceStartFrame, SourceFrameCount: request.SourceFrameCount,
		SourceFPSNum: request.SourceFPSNum, SourceFPSDen: request.SourceFPSDen,
		SourceDurationUS:     request.SourceDurationUS,
		ExpectedOutputSHA256: certificate.OutputSHA256, ExpectedOutputSizeBytes: certificate.OutputSizeBytes,
	}
	certificateJSON, err := json.Marshal(certificate)
	if err != nil {
		return nil, fmt.Errorf("encode prepared window certificate: %w", err)
	}
	plan.PreparationCertificateJSON = string(certificateJSON)
	return s.registerPreparedVideoFile(ctx, preparedPath, "prepared-window:"+identity, plan)
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
