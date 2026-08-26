package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/publisher"
	"velox-worker-agent/internal/runtimeassets"
)

// render_batch_validate.go owns the V2 plan decoding and the asset/binding/
// media integrity checks that gate each render_batch execution phase. Every
// check is a pure verification returning a stable error code; no byte of
// output is produced here.

func decodeRenderPlanV2(spec executor.TaskSpec) (*contract.CompiledRenderPlanV2, error) {
	plan, err := contract.DecodeCompiledRenderPlanV2Payload(spec.Payload)
	if err != nil {
		return nil, fmt.Errorf("render_batch@1: decode V2 plan: %w", err)
	}
	if plan == nil {
		return nil, errors.New("render_batch@1: compiled plan JSON must be present")
	}
	return plan, nil
}

func validateBindings(plan *contract.CompiledRenderPlanV2, bindings runtimeassets.Bindings) error {
	if plan == nil || bindings == nil {
		return ErrMissingRenderBatchBindings
	}
	assetByID := make(map[string]contract.AssetRefV2, len(plan.Assets))
	for _, asset := range plan.Assets {
		assetByID[asset.AssetID] = asset
		if err := validateBinding(asset.AssetID, asset.SHA256, asset.SizeBytes, bindings); err != nil {
			return err
		}
	}
	for _, track := range plan.VideoTracks {
		for _, segment := range track.Segments {
			asset, ok := assetByID[segment.AssetID]
			if !ok {
				return fmt.Errorf("%w: segment asset_id=%q is not declared", ErrRenderBatchAssetIntegrity, segment.AssetID)
			}
			if err := validateBinding(segment.AssetID, asset.SHA256, asset.SizeBytes, bindings); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateBinding(assetID, wantSHA string, wantSize int64, bindings runtimeassets.Bindings) error {
	binding, ok := bindings[assetID]
	if !ok || strings.TrimSpace(binding.Path) == "" {
		return fmt.Errorf("%w: asset_id=%q", ErrMissingRenderBatchBindings, assetID)
	}
	if strings.TrimSpace(binding.SHA256) == "" || binding.SHA256 != wantSHA || wantSize <= 0 || binding.Size != wantSize {
		return fmt.Errorf("%w: asset_id=%q declared metadata does not match plan", ErrRenderBatchAssetIntegrity, assetID)
	}
	info, err := os.Stat(binding.Path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		if err == nil {
			err = errors.New("file is empty or not regular")
		}
		return fmt.Errorf("%w: asset_id=%q path: %v", ErrRenderBatchAssetIntegrity, assetID, err)
	}
	if info.Size() != wantSize {
		return fmt.Errorf("%w: asset_id=%q actual size=%d want=%d", ErrRenderBatchAssetIntegrity, assetID, info.Size(), wantSize)
	}
	if binding.Verified {
		return nil
	}
	file, err := os.Open(binding.Path)
	if err != nil {
		return fmt.Errorf("%w: asset_id=%q open: %v", ErrRenderBatchAssetIntegrity, assetID, err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return fmt.Errorf("%w: asset_id=%q hash: %v", ErrRenderBatchAssetIntegrity, assetID, err)
	}
	actualSHA := hex.EncodeToString(hash.Sum(nil))
	if actualSHA != wantSHA || actualSHA != binding.SHA256 {
		return fmt.Errorf("%w: asset_id=%q actual sha256=%s want=%s", ErrRenderBatchAssetIntegrity, assetID, actualSHA, wantSHA)
	}
	return nil
}

const renderBatchDurationToleranceSec = 0.050

type packetCopyVideoSignature struct {
	codec, codecTag, profile, pixelFormat string
	level, width, height                  int
	fpsNum, fpsDen                        int
	timeBaseNum, timeBaseDen              int
}

func (s packetCopyVideoSignature) equal(other packetCopyVideoSignature) bool {
	return s == other
}

func normalizeVideoCodec(codec string) string {
	switch strings.ToLower(strings.TrimSpace(codec)) {
	case "libx264", "h264_nvenc", "h264_vaapi":
		return "h264"
	default:
		return strings.ToLower(strings.TrimSpace(codec))
	}
}

// validatePacketCopySources is intentionally strict. FFmpeg's concat
// demuxer can only packet-copy complete clips with identical stream identity;
// accepting a mismatch here would make it silently re-encode or produce a
// timeline with broken timestamps. The caller receives the ordered paths only
// after every segment has passed this contract.
func validatePacketCopySources(plan *contract.CompiledRenderPlanV2, bindings runtimeassets.Bindings, probe func(context.Context, string) (publisher.MediaProbe, error), ctx context.Context) ([]string, error) {
	if plan == nil || len(plan.VideoTracks) != 1 || len(plan.VideoTracks[0].Segments) == 0 {
		return nil, fmt.Errorf("%w: exactly one non-empty video track is required", ErrCopyOnlyVideoIncompatible)
	}
	if probe == nil {
		return nil, fmt.Errorf("%w: media probe is not configured", ErrCopyOnlyVideoIncompatible)
	}
	var expected packetCopyVideoSignature
	var haveExpected bool
	paths := make([]string, 0, len(plan.VideoTracks[0].Segments))
	probed := make(map[string]publisher.MediaProbe, len(plan.VideoTracks[0].Segments))
	var timelineFrame int64
	for _, segment := range plan.VideoTracks[0].Segments {
		if segment.TimelineStartFrame != timelineFrame || segment.SourceInUS != 0 || segment.FrameCount <= 0 || segment.SourceDurationUS <= 0 {
			return nil, fmt.Errorf("%w: segment %q is not a contiguous complete clip", ErrCopyOnlyVideoIncompatible, segment.SegmentID)
		}
		binding, ok := bindings[segment.AssetID]
		if !ok || strings.TrimSpace(binding.Path) == "" {
			return nil, fmt.Errorf("%w: segment %q has no local binding", ErrCopyOnlyVideoIncompatible, segment.SegmentID)
		}
		media, ok := probed[segment.AssetID]
		if !ok {
			var err error
			media, err = probe(ctx, binding.Path)
			if err != nil {
				return nil, fmt.Errorf("%w: probe segment %q: %v", ErrCopyOnlyVideoIncompatible, segment.SegmentID, err)
			}
			probed[segment.AssetID] = media
		}
		if !media.HasVideo || media.VideoTrackCount != 1 || media.VideoCodec == "" || media.Width <= 0 || media.Height <= 0 || media.VideoFPSNum <= 0 || media.VideoFPSDen <= 0 || media.VideoTimeBaseNum <= 0 || media.VideoTimeBaseDen <= 0 || media.VideoPixelFormat == "" {
			return nil, fmt.Errorf("%w: segment %q lacks a complete video stream signature: %+v", ErrCopyOnlyVideoIncompatible, segment.SegmentID, media)
		}
		if math.Abs(media.DurationSec-float64(segment.SourceDurationUS)/1_000_000) > renderBatchDurationToleranceSec {
			return nil, fmt.Errorf("%w: segment %q duration=%0.6fs does not equal complete source duration=%0.6fs", ErrCopyOnlyVideoIncompatible, segment.SegmentID, media.DurationSec, float64(segment.SourceDurationUS)/1_000_000)
		}
		signature := packetCopyVideoSignature{
			codec: normalizeVideoCodec(media.VideoCodec), codecTag: strings.ToLower(strings.TrimSpace(media.VideoCodecTag)), profile: strings.ToLower(strings.TrimSpace(media.VideoProfile)), pixelFormat: strings.ToLower(strings.TrimSpace(media.VideoPixelFormat)),
			level: media.VideoLevel, width: media.Width, height: media.Height, fpsNum: media.VideoFPSNum, fpsDen: media.VideoFPSDen, timeBaseNum: media.VideoTimeBaseNum, timeBaseDen: media.VideoTimeBaseDen,
		}
		if !haveExpected {
			expected, haveExpected = signature, true
		} else if !expected.equal(signature) {
			return nil, fmt.Errorf("%w: segment %q stream identity differs from the first clip", ErrCopyOnlyVideoIncompatible, segment.SegmentID)
		}
		if normalizeVideoCodec(plan.Output.VideoCodec) != expected.codec || plan.Output.Width != expected.width || plan.Output.Height != expected.height || plan.Output.FPSNum != expected.fpsNum || plan.Output.FPSDen != expected.fpsDen || (plan.Output.PixelFormat != "" && strings.ToLower(plan.Output.PixelFormat) != expected.pixelFormat) {
			return nil, fmt.Errorf("%w: segment %q does not match output contract", ErrCopyOnlyVideoIncompatible, segment.SegmentID)
		}
		paths = append(paths, binding.Path)
		timelineFrame += segment.FrameCount
	}
	return paths, nil
}

func validateMediaFile(probe func(context.Context, string) (publisher.MediaProbe, error), ctx context.Context, path, label string, wantDurationUS int64, requireVideo, requireAudio bool, expectedAudio *contract.FinalAudioV2) error {
	if probe == nil {
		return errors.New("media probe is not configured")
	}
	media, err := probe(ctx, path)
	if err != nil {
		return fmt.Errorf("%s probe: %w", label, err)
	}
	if requireVideo && (!media.HasVideo || media.VideoTrackCount != 1) {
		return fmt.Errorf("%s must contain exactly one video stream", label)
	}
	if requireAudio && (!media.HasAudio || media.AudioTrackCount != 1) {
		return fmt.Errorf("%s must contain exactly one audio stream", label)
	}
	if expectedAudio != nil {
		if media.AudioCodec != expectedAudio.Codec || media.AudioSampleRateHz != expectedAudio.SampleRateHz || media.AudioChannels != expectedAudio.Channels {
			return fmt.Errorf("%s audio codec=%q sample_rate_hz=%d channels=%d want codec=%q sample_rate_hz=%d channels=%d", label, media.AudioCodec, media.AudioSampleRateHz, media.AudioChannels, expectedAudio.Codec, expectedAudio.SampleRateHz, expectedAudio.Channels)
		}
	}
	want := float64(wantDurationUS) / 1_000_000
	if media.DurationSec <= 0 || math.Abs(media.DurationSec-want) > renderBatchDurationToleranceSec {
		return fmt.Errorf("%s duration=%0.6fs want=%0.6fs tolerance=%0.3fs", label, media.DurationSec, want, renderBatchDurationToleranceSec)
	}
	return nil
}

func safeOutputJobID(jobID string) (string, error) {
	if strings.TrimSpace(jobID) == "" || jobID == "." || jobID == ".." || filepath.IsAbs(jobID) || strings.ContainsAny(jobID, "/\\\\\x00") || filepath.Base(jobID) != jobID {
		return "", errors.New("job_id must be a non-empty path-free identifier")
	}
	return jobID, nil
}
