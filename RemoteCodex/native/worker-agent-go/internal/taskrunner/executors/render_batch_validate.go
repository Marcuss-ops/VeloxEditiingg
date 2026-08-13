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
	raw, ok := spec.Payload[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	if !ok {
		return nil, errors.New("render_batch@1: compiled plan JSON must be a string")
	}
	plan, err := contract.DecodeCompiledRenderPlanV2([]byte(raw))
	if err != nil {
		return nil, fmt.Errorf("render_batch@1: decode V2 plan: %w", err)
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