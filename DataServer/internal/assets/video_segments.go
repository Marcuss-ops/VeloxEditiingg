package assets

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"velox-server/internal/identity"
	"velox-server/internal/inputsecurity"
	"velox-shared/contract"
)

type preparedVideoFilePlan struct {
	StartSeconds               float64
	EndSeconds                 float64
	DurationSeconds            float64
	CanonicalProfile           contract.CanonicalVideoProfileV1
	OutputFrameCount           int64
	FirstFrameKeyframe         bool
	ClosedGOP                  bool
	SourceSHA256               string
	SourceSizeBytes            int64
	SourceStartFrame           int64
	SourceDurationUS           int64
	SourceFrameCount           int64
	SourceFPSNum               int
	SourceFPSDen               int
	PreparationCertificateJSON string
	ExpectedOutputSHA256       string
	ExpectedOutputSizeBytes    int64
}

func (s *AssetService) registerPreparedVideoFile(ctx context.Context, preparedPath, sourcePath string, plan preparedVideoFilePlan) (*Asset, error) {
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
		// Content-addressed rows can outlive their local blob (for example,
		// after storage retention or a volume restore). The staged file has
		// already passed SHA-256 and media validation, so restore the exact
		// registered object before treating this as a deduplicated hit.
		blobReader, supportsRead := s.blobStore.(interface {
			ReadFinal(string) (*os.File, error)
		})
		blobMissing := existing.StorageKey == ""
		if supportsRead && existing.StorageKey != "" {
			blobMissing = !registeredBlobPresent(blobReader, existing.StorageKey, existing.SizeBytes)
		}
		if blobMissing {
			target := existing.StorageKey
			if target == "" {
				target = s.blobStore.FinalPath("assets", sha256Hex, ".mp4")
			}
			if _, promoteErr := s.blobStore.PromoteToFinal(stagingPath, target); promoteErr != nil {
				_ = s.blobStore.RemoveStaging(stagingPath)
				return nil, fmt.Errorf("restore missing prepared segment blob: %w", promoteErr)
			}
		} else {
			_ = s.blobStore.RemoveStaging(stagingPath)
		}
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
		"start_seconds":               plan.StartSeconds,
		"end_seconds":                 plan.EndSeconds,
		"duration_seconds":            plan.DurationSeconds,
		"duration_us":                 int64(plan.DurationSeconds * 1_000_000),
		"trim_mode":                   "stream_copy",
		"canonical_profile_id":        plan.CanonicalProfile.ProfileID,
		"canonical_stream_profile_id": plan.CanonicalProfile.StreamProfile(),
		"canonical_profile_version":   plan.CanonicalProfile.Version,
		"preparation_revision":        s.videoWindowVerifier.preparationRevision(),
	}
	if plan.PreparationCertificateJSON != "" {
		metadataFields["window_certificate"] = json.RawMessage(plan.PreparationCertificateJSON)
		metadataFields["source_start_frame"] = plan.SourceStartFrame
	}
	if plan.OutputFrameCount > 0 {
		metadataFields["frame_count"] = plan.OutputFrameCount
		metadataFields["first_frame_keyframe"] = plan.FirstFrameKeyframe
		metadataFields["closed_gop"] = plan.ClosedGOP
	}
	if plan.SourceSHA256 != "" {
		metadataFields["source_sha256"] = plan.SourceSHA256
		metadataFields["source_size_bytes"] = plan.SourceSizeBytes
		metadataFields["source_duration_us"] = plan.SourceDurationUS
		metadataFields["source_frame_count"] = plan.SourceFrameCount
		metadataFields["source_fps_num"] = plan.SourceFPSNum
		metadataFields["source_fps_den"] = plan.SourceFPSDen
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
	if s == nil || s.repo == nil || s.blobStore == nil || s.videoWindowVerifier == nil {
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
		if cachedPreparedWindowEvidenceMatches(existing, request, identity) {
			return existing, nil
		}
	}
	if s.security == nil {
		return nil, fmt.Errorf("prepared window security validator is unavailable")
	}
	if _, err := s.security.ValidateFileForCanonicalProbe(ctx, preparedPath, inputsecurity.KindClip, "video/mp4"); err != nil {
		return nil, fmt.Errorf("prepared window media validation: %w", err)
	}
	probe, err := s.videoWindowVerifier.probe(ctx, preparedPath)
	if err != nil {
		return nil, err
	}
	if probe.ExtradataSHA256 != contract.CanonicalVideoExtradataSHA256 {
		return nil, fmt.Errorf("prepared window avcC %s does not match canonical identity %s", probe.ExtradataSHA256, contract.CanonicalVideoExtradataSHA256)
	}
	evidence := certificate.OutputEvidence
	frames := evidence.FrameCount
	if probe.FrameCount > 0 && probe.FrameCount != frames {
		return nil, fmt.Errorf("prepared window probe frame count %d differs from worker certificate %d", probe.FrameCount, frames)
	}
	level, levelErr := strconv.Atoi(strings.ReplaceAll(profile.CodecLevel, ".", ""))
	if levelErr != nil || probe.VideoCodec != profile.Codec || probe.VideoProfile != strings.ToLower(profile.CodecProfile) ||
		probe.VideoLevel != level || probe.Width != profile.Width || probe.Height != profile.Height ||
		probe.PixelFormat != profile.PixelFormat || probe.FPSNum != profile.FPSNum || probe.FPSDen != profile.FPSDen ||
		probe.TimebaseNum != profile.TimeBaseNum || probe.TimebaseDen != profile.TimeBaseDen || frames != request.OutputFrameCount ||
		probe.ExtradataSHA256 != evidence.ExtradataSHA256 ||
		probe.AudioCodec != "" || evidence.Codec != probe.VideoCodec || evidence.Profile != probe.VideoProfile ||
		evidence.Level != probe.VideoLevel || evidence.Width != probe.Width || evidence.Height != probe.Height ||
		evidence.PixelFormat != probe.PixelFormat || evidence.FPSNum != probe.FPSNum || evidence.FPSDen != probe.FPSDen ||
		evidence.TimeBaseNum != probe.TimebaseNum || evidence.TimeBaseDen != probe.TimebaseDen || evidence.FrameCount != frames {
		return nil, fmt.Errorf("prepared window does not match the independently probed canonical profile")
	}
	if len(probe.Keyframes) == 0 || probe.Keyframes[0] > float64(profile.FPSDen)/float64(profile.FPSNum) {
		return nil, fmt.Errorf("prepared window does not begin with a keyframe")
	}
	if existing != nil {
		return existing, nil
	}
	startSeconds, durationSeconds := float64(request.SourceStartUS)/1_000_000, float64(request.SourceDurationUS)/1_000_000
	if request.SourceDurationUS == 0 {
		startSeconds = float64(request.SourceStartFrame) * float64(request.SourceFPSDen) / float64(request.SourceFPSNum)
		durationSeconds = float64(request.SourceFrameCount) * float64(request.SourceFPSDen) / float64(request.SourceFPSNum)
	}
	plan := preparedVideoFilePlan{
		StartSeconds: startSeconds, EndSeconds: startSeconds + durationSeconds,
		DurationSeconds:  float64(request.OutputFrameCount) * float64(profile.FPSDen) / float64(profile.FPSNum),
		CanonicalProfile: profile, OutputFrameCount: request.OutputFrameCount,
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

// cachedPreparedWindowEvidenceMatches permits a repeated registration to reuse
// the independently probed evidence already stored with an immutable asset.
// The old certificate is revalidated against its original window and its
// content key must match the current request; timeline placement may differ.
func cachedPreparedWindowEvidenceMatches(asset *Asset, request contract.VideoPrepareWindowV1, identity string) bool {
	if asset == nil || asset.Status != AssetStatusReady || asset.MimeType != "video/mp4" {
		return false
	}
	var metadata struct {
		Certificate contract.VideoPrepareWindowCertificateV1 `json:"window_certificate"`
	}
	if json.Unmarshal([]byte(asset.MetadataJSON), &metadata) != nil {
		return false
	}
	certificate := metadata.Certificate
	originalRequest := contract.VideoPrepareWindowV1{
		Version: contract.VideoPrepareWindowVersion,
		Source: contract.AssetRefV2{
			AssetID: certificate.SourceAssetID, SHA256: certificate.SourceSHA256,
			SizeBytes: certificate.SourceSizeBytes, Kind: "video",
		},
		SourceStartFrame: certificate.SourceStartFrame, SourceFrameCount: certificate.SourceFrameCount,
		SourceFPSNum: certificate.SourceFPSNum, SourceFPSDen: certificate.SourceFPSDen,
		SourceStartUS: certificate.SourceStartUS, SourceDurationUS: certificate.SourceDurationUS,
		OutputFrameCount: certificate.OutputFrameCount, TimelineRevision: certificate.TimelineRevision,
		TimelineSHA256: certificate.TimelineSHA256, TimelineStartFrame: certificate.TimelineStartFrame,
		OutputProfileID: certificate.OutputProfileID, OutputProfileRevision: certificate.OutputProfileRevision,
	}
	if certificate.ValidateFor(originalRequest) != nil {
		return false
	}
	cachedIdentity, err := contract.VideoPrepareWindowCacheIdentity(originalRequest)
	if err != nil || cachedIdentity != identity || asset.SHA256 != certificate.OutputSHA256 ||
		asset.SizeBytes != certificate.OutputSizeBytes || certificate.OutputFrameCount != request.OutputFrameCount ||
		certificate.OutputProfileID != request.OutputProfileID || certificate.OutputProfileRevision != request.OutputProfileRevision ||
		certificate.OutputEvidence.ExtradataSHA256 != contract.CanonicalVideoExtradataSHA256 {
		return false
	}
	return true
}

func syncPreparedSegment(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
