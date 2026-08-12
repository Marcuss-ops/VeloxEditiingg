package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"velox-shared/contract/rendermanifest"
)

// CompileRenderPlanV2FromManifest compiles a strict render_manifest into the
// worker-facing V2 execution contract. The manifest is the producer-owned
// source of truth; no worker-side float timing or timeline reconstruction is
// involved.
//
// V2 admission is intentionally narrower than the legacy manifest contract:
// it requires exactly one verified final_audio asset so render_batch@1 can
// perform FINAL_AUDIO_COPY without choosing an audio mix or AAC encode.
func CompileRenderPlanV2FromManifest(raw map[string]any) (*CompiledRenderPlanV2, error) {
	if raw == nil {
		return nil, fmt.Errorf("compiled render plan v2: render_manifest is required")
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("compiled render plan v2: encode render_manifest: %w", err)
	}
	manifest, err := rendermanifest.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("compiled render plan v2: strict render_manifest: %w", err)
	}
	return compileRenderPlanV2Manifest(manifest)
}

func compileRenderPlanV2Manifest(manifest *rendermanifest.Manifest) (*CompiledRenderPlanV2, error) {
	if manifest == nil {
		return nil, fmt.Errorf("compiled render plan v2: nil render_manifest")
	}
	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("compiled render plan v2: render_manifest: %w", err)
	}

	finalAudio, count := rendermanifest.Asset{}, 0
	assets := make([]AssetRefV2, 0, len(manifest.Assets))
	assetByID := make(map[string]rendermanifest.Asset, len(manifest.Assets))
	for _, asset := range manifest.Assets {
		assetByID[asset.ID] = asset
		if asset.Kind == "final_audio" {
			finalAudio = asset
			count++
		}
		assets = append(assets, AssetRefV2{
			AssetID:    asset.ID,
			SHA256:     asset.SHA256,
			SizeBytes:  asset.SizeBytes,
			Kind:       asset.Kind,
			MIME:       asset.Format,
			DurationUS: millisecondsToMicroseconds(asset.DurationMS),
		})
	}
	if count != 1 {
		return nil, fmt.Errorf("compiled render plan v2: exactly one final_audio asset is required, found %d", count)
	}
	if !strings.EqualFold(strings.TrimSpace(manifest.Output.AudioCodec), "aac") {
		return nil, fmt.Errorf("compiled render plan v2: final_audio requires output audio_codec=aac, got %q", manifest.Output.AudioCodec)
	}
	if manifest.Output.AudioSampleRate != 48_000 || manifest.Output.AudioChannels != 2 {
		return nil, fmt.Errorf("compiled render plan v2: final_audio requires 48000 Hz stereo, got %d Hz/%d channels", manifest.Output.AudioSampleRate, manifest.Output.AudioChannels)
	}
	finalAudioDurationUS := millisecondsToMicroseconds(finalAudio.DurationMS)
	if finalAudioDurationUS <= 0 {
		return nil, fmt.Errorf("compiled render plan v2: final_audio duration_ms must be positive")
	}

	timelineSHA, err := manifestTimelineSHA(manifest)
	if err != nil {
		return nil, err
	}
	plan := &CompiledRenderPlanV2{
		PlanVersion:      CompiledPlanVersionV2,
		TimelineRevision: 1,
		TimelineSHA256:   timelineSHA,
		DurationUS:       finalAudioDurationUS,
		Output: OutputContractV2{
			Container:   manifest.Output.Container,
			VideoCodec:  manifest.Output.VideoCodec,
			Width:       manifest.Canvas.Width,
			Height:      manifest.Canvas.Height,
			FPSNum:      manifest.Canvas.FPSNum,
			FPSDen:      manifest.Canvas.FPSDen,
			PixelFormat: manifest.Canvas.PixelFormat,
		},
		FinalAudio: FinalAudioV2{
			Mode:             AudioModeFinalAudioCopy,
			AssetID:          finalAudio.ID,
			SHA256:           finalAudio.SHA256,
			SizeBytes:        finalAudio.SizeBytes,
			Codec:            "aac",
			SampleRateHz:     48_000,
			Channels:         2,
			DurationUS:       finalAudioDurationUS,
			TimelineRevision: 1,
			TimelineSHA256:   timelineSHA,
		},
		Assets: assets,
	}

	for _, track := range manifest.Tracks {
		if track.Kind != "video" {
			continue
		}
		videoTrack := VideoTrackV2{TrackID: track.ID, Segments: make([]VideoSegmentV2, 0, len(track.Events))}
		for index, event := range track.Events {
			asset, ok := assetByID[event.AssetID]
			if !ok || asset.Kind != "video" {
				return nil, fmt.Errorf("compiled render plan v2: tracks[%s].events[%d] must reference a video asset", track.ID, index)
			}
			sourceDurationUS := millisecondsToMicroseconds(event.DurationMS)
			if sourceDurationUS <= 0 {
				return nil, fmt.Errorf("compiled render plan v2: tracks[%s].events[%d].duration_ms must be positive", track.ID, index)
			}
			startFrame, err := timeUSToNearestFrame(millisecondsToMicroseconds(event.TimelineStartMS), manifest.Canvas.FPSNum, manifest.Canvas.FPSDen)
			if err != nil {
				return nil, fmt.Errorf("compiled render plan v2: tracks[%s].events[%d] timeline frame: %w", track.ID, index, err)
			}
			frameCount, err := timeUSToNearestFrame(sourceDurationUS, manifest.Canvas.FPSNum, manifest.Canvas.FPSDen)
			if err != nil || frameCount <= 0 {
				if err == nil {
					err = fmt.Errorf("duration quantizes to zero frames")
				}
				return nil, fmt.Errorf("compiled render plan v2: tracks[%s].events[%d] frame count: %w", track.ID, index, err)
			}
			videoTrack.Segments = append(videoTrack.Segments, VideoSegmentV2{
				SegmentID:          fmt.Sprintf("%s-segment-%06d", track.ID, index),
				AssetID:            event.AssetID,
				SHA256:             asset.SHA256,
				TimelineStartFrame: startFrame,
				FrameCount:         frameCount,
				SourceInUS:         millisecondsToMicroseconds(event.SourceStartMS),
				SourceDurationUS:   sourceDurationUS,
			})
		}
		if len(videoTrack.Segments) > 0 {
			plan.VideoTracks = append(plan.VideoTracks, videoTrack)
		}
	}
	if len(plan.VideoTracks) == 0 {
		return nil, fmt.Errorf("compiled render plan v2: at least one video track with events is required")
	}
	if err := ValidateCompiledRenderPlanV2(plan); err != nil {
		return nil, fmt.Errorf("compiled render plan v2: generated plan failed validation: %w", err)
	}
	return plan, nil
}

// CompileRenderPlanV2JSON returns canonical V2 bytes plus the transport hash.
// It is the single producer helper used before TaskSpec persistence and
// TaskOffer delivery.
func CompileRenderPlanV2JSON(raw map[string]any) ([]byte, string, error) {
	plan, err := CompileRenderPlanV2FromManifest(raw)
	if err != nil {
		return nil, "", err
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		return nil, "", err
	}
	return canonical, HashCompiledPlanV2(canonical), nil
}

func manifestTimelineSHA(manifest *rendermanifest.Manifest) (string, error) {
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("compiled render plan v2: timeline identity: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func millisecondsToMicroseconds(milliseconds int64) int64 {
	if milliseconds <= 0 {
		return milliseconds * 1000
	}
	if milliseconds > int64(^uint64(0)>>1)/1000 {
		return 0
	}
	return milliseconds * 1000
}

func timeUSToNearestFrame(timeUS int64, fpsNum, fpsDen int) (int64, error) {
	if timeUS < 0 || fpsNum <= 0 || fpsDen <= 0 {
		return 0, fmt.Errorf("invalid time/fps inputs")
	}
	numerator := new(big.Int).Mul(big.NewInt(timeUS), big.NewInt(int64(fpsNum)))
	denominator := new(big.Int).Mul(big.NewInt(1_000_000), big.NewInt(int64(fpsDen)))
	quotient, remainder := new(big.Int).QuoRem(numerator, denominator, new(big.Int))
	twoRemainder := new(big.Int).Lsh(remainder, 1)
	if twoRemainder.Cmp(denominator) >= 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() {
		return 0, fmt.Errorf("frame index overflows int64")
	}
	return quotient.Int64(), nil
}
