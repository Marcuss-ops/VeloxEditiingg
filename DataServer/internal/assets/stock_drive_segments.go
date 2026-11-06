package assets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"velox-shared/contract"
)

// PrepareDriveStockWindows downloads each Drive source once, verifies and
// probes it once, then replaces every source window with a certified,
// assembly-ready fragment in the canonical plan. Prepared fragments are
// content-addressed and cached by source identity, window and output profile.
func (s *AssetService) PrepareDriveStockWindows(ctx context.Context, planJSON string, runtimeAssets []map[string]interface{}) (string, string, error) {
	if s == nil || s.repo == nil || s.blobStore == nil || s.videoTrimmer == nil {
		return "", "", fmt.Errorf("stock window preparation capability is unavailable")
	}
	var plan contract.CompiledRenderPlanV2
	if err := json.Unmarshal([]byte(planJSON), &plan); err != nil {
		return "", "", fmt.Errorf("decode compiled plan: %w", err)
	}
	if plan.StockPool != nil {
		return planJSON, contract.HashCompiledPlanV2([]byte(planJSON)), nil
	}
	bindings := stockRuntimeBindings(runtimeAssets)
	assetsByID := make(map[string]contract.AssetRefV2, len(plan.Assets))
	for _, asset := range plan.Assets {
		assetsByID[asset.AssetID] = asset
	}
	type sourceState struct {
		path    string
		cleanup func()
		probe   VideoProbe
		sha     string
		size    int64
	}
	sources := map[string]*sourceState{}
	defer func() {
		for _, source := range sources {
			source.cleanup()
		}
	}()
	preparedByKey := map[string]Asset{}
	newAssets := map[string]contract.AssetRefV2{}
	changed := false

	for trackIndex := range plan.VideoTracks {
		for segmentIndex := range plan.VideoTracks[trackIndex].Segments {
			segment := &plan.VideoTracks[trackIndex].Segments[segmentIndex]
			if segment.StockPoolSlot {
				continue
			}
			original, ok := assetsByID[segment.AssetID]
			if !ok || !isDriveStock(original) {
				continue
			}
			changed = true
			driveID := strings.TrimSpace(original.DriveFileID)
			if driveID == "" {
				driveID = strings.TrimPrefix(strings.TrimSpace(original.AssetID), "drive:")
			}
			binding := bindings[driveID]
			if binding == nil {
				return "", "", fmt.Errorf("stock segment %q has no runtime Drive binding for file %q", segment.SegmentID, driveID)
			}
			uri := stockSourceURI(binding, driveID)
			if uri == "" {
				return "", "", fmt.Errorf("stock segment %q has no fetchable Drive source", segment.SegmentID)
			}
			state := sources[driveID]
			if state == nil {
				path, cleanup, err := s.materializeVideoSource(ctx, uri)
				if err != nil {
					return "", "", fmt.Errorf("stock file %q download: %w", driveID, err)
				}
				state = &sourceState{path: path, cleanup: cleanup}
				state.sha, state.size, err = hashFile(path)
				if err != nil {
					cleanup()
					return "", "", fmt.Errorf("stock file %q identity: %w", driveID, err)
				}
				expectedSHA := strings.TrimSpace(original.SHA256)
				if expectedSHA != "" && !strings.EqualFold(expectedSHA, state.sha) {
					cleanup()
					return "", "", fmt.Errorf("stock file %q sha256 mismatch", driveID)
				}
				if original.SizeBytes > 0 && original.SizeBytes != state.size {
					cleanup()
					return "", "", fmt.Errorf("stock file %q size mismatch: got %d want %d", driveID, state.size, original.SizeBytes)
				}
				state.probe, err = s.videoTrimmer.Probe(ctx, path)
				if err != nil {
					cleanup()
					return "", "", fmt.Errorf("stock file %q certification: %w", driveID, err)
				}
				if state.probe.FrameCount <= 0 && state.probe.FPSNum > 0 && state.probe.FPSDen > 0 {
					// Some MP4 muxers omit nb_frames. For the stock pool's certified
					// constant-frame-rate sources, derive it from the file's measured
					// duration and stream rate; never inherit the producer's estimate.
					state.probe.FrameCount = int64(math.Round(state.probe.DurationSeconds * float64(state.probe.FPSNum) / float64(state.probe.FPSDen)))
				}
				if state.probe.FrameCount <= 0 {
					cleanup()
					return "", "", fmt.Errorf("stock file %q certification could not derive a positive frame_count", driveID)
				}
				sources[driveID] = state
			}
			startUS, durationUS := segment.SourceInUS, segment.SourceDurationUS
			if startUS < 0 || durationUS <= 0 || durationUS > math.MaxInt64-startUS {
				return "", "", fmt.Errorf("stock segment %q has invalid source window: source_in_us=%d source_duration_us=%d", segment.SegmentID, startUS, durationUS)
			}
			frameUS := int64(math.Round(1_000_000 * float64(state.probe.FPSDen) / float64(state.probe.FPSNum)))
			fileDurationUS := int64(math.Round(state.probe.DurationSeconds * 1_000_000))
			if frameUS <= 0 || startUS >= fileDurationUS || startUS+durationUS > fileDurationUS+frameUS {
				return "", "", fmt.Errorf("stock segment %q window exceeds certified source duration: end_us=%d file_duration_us=%d tolerance_us=%d", segment.SegmentID, startUS+durationUS, fileDurationUS, frameUS)
			}
			wantFrames := int64(math.Round(float64(durationUS) * float64(plan.Output.FPSNum) / (1_000_000 * float64(plan.Output.FPSDen))))
			if wantFrames <= 0 || segment.FrameCount != wantFrames {
				return "", "", fmt.Errorf("stock segment %q frame count mismatch: plan=%d expected=%d from %d us at %d/%d fps", segment.SegmentID, segment.FrameCount, wantFrames, durationUS, plan.Output.FPSNum, plan.Output.FPSDen)
			}
			endUS := startUS + durationUS
			windowRef := fmt.Sprintf("velox-stock-window://%s/%s/%d/%d/%s", driveID, state.sha, startUS, durationUS, plan.Output.ProfileID)
			fragment := preparedByKey[windowRef]
			if fragment.AssetID == "" {
				cached, err := s.GetBySourceReference(ctx, windowRef)
				if err != nil {
					return "", "", fmt.Errorf("stock window cache lookup: %w", err)
				}
				if cached != nil {
					fragment = *cached
				}
			}
			if fragment.AssetID == "" {
				out, err := os.CreateTemp(filepath.Dir(state.path), ".velox-stock-window-*.mp4")
				if err != nil {
					return "", "", fmt.Errorf("create stock window output: %w", err)
				}
				outputPath := out.Name()
				if err := out.Close(); err != nil {
					_ = os.Remove(outputPath)
					return "", "", err
				}
				defer os.Remove(outputPath)
				start := float64(startUS) / 1_000_000
				end := float64(endUS) / 1_000_000
				trimmed, err := s.videoTrimmer.TrimFramesWithProbe(ctx, state.probe, state.path, outputPath, VideoSegment{StartSeconds: start, EndSeconds: end}, wantFrames)
				if err != nil {
					return "", "", fmt.Errorf("stock segment %q materialization: %w", segment.SegmentID, err)
				}
				cert, err := s.videoTrimmer.Probe(ctx, outputPath)
				if err != nil {
					return "", "", fmt.Errorf("stock segment %q output certification: %w", segment.SegmentID, err)
				}
				if cert.FrameCount != wantFrames {
					return "", "", fmt.Errorf("stock segment %q output frame count mismatch: got %d want %d", segment.SegmentID, cert.FrameCount, wantFrames)
				}
				expectedDuration := float64(wantFrames) * float64(plan.Output.FPSDen) / float64(plan.Output.FPSNum)
				frameDuration := float64(plan.Output.FPSDen) / float64(plan.Output.FPSNum)
				if math.Abs(cert.DurationSeconds-expectedDuration) > frameDuration {
					return "", "", fmt.Errorf("stock segment %q output duration mismatch: got %.6f s want %.6f s", segment.SegmentID, cert.DurationSeconds, expectedDuration)
				}
				if len(cert.Keyframes) == 0 || math.Abs(cert.Keyframes[0]) > 0.0005 {
					return "", "", fmt.Errorf("stock segment %q output does not start with a keyframe", segment.SegmentID)
				}
				profile := s.videoTrimmer.Profile()
				if !matchesCanonicalStockVideo(cert, profile) {
					return "", "", fmt.Errorf("stock segment %q output does not match canonical assembly profile", segment.SegmentID)
				}
				trimmed.Plan.OutputFrameCount = cert.FrameCount
				trimmed.Plan.DurationSeconds = float64(cert.FrameCount) * float64(profile.FPSDen) / float64(profile.FPSNum)
				trimmed.Plan.FirstFrameKeyframe = true
				trimmed.Plan.ClosedGOP = profile.ClosedGOP
				trimmed.Plan.CopyEligible = true
				trimmed.Plan.SourceSHA256 = state.sha
				trimmed.Plan.SourceSizeBytes = state.size
				trimmed.Plan.SourceDurationUS = fileDurationUS
				trimmed.Plan.SourceFrameCount = state.probe.FrameCount
				trimmed.Plan.SourceFPSNum, trimmed.Plan.SourceFPSDen = state.probe.FPSNum, state.probe.FPSDen
				trimmed.Plan.SourceCodec = state.probe.VideoCodec
				trimmed.Plan.SourceProfile = state.probe.VideoProfile
				trimmed.Plan.SourceLevel = state.probe.VideoLevel
				trimmed.Plan.SourceWidth, trimmed.Plan.SourceHeight = state.probe.Width, state.probe.Height
				trimmed.Plan.SourcePixelFormat = state.probe.PixelFormat
				trimmed.Plan.SourceKeyframes = append([]float64(nil), state.probe.Keyframes...)
				fragmentPtr, err := s.registerPreparedVideoFile(ctx, outputPath, windowRef, trimmed.Plan)
				if err != nil {
					return "", "", fmt.Errorf("register stock segment %q: %w", segment.SegmentID, err)
				}
				fragment = *fragmentPtr
			}
			preparedByKey[windowRef] = fragment
			assetRef := contract.AssetRefV2{
				AssetID: fragment.AssetID, AssetKey: fragment.AssetID, SHA256: fragment.SHA256,
				SizeBytes: fragment.SizeBytes, Kind: "video", MIME: fragment.MimeType,
				DurationUS: int64(math.Round(float64(wantFrames) * 1_000_000 * float64(plan.Output.FPSDen) / float64(plan.Output.FPSNum))),
				Width:      plan.Output.Width, Height: plan.Output.Height, ProfileID: plan.Output.ProfileID,
				FrameCount: wantFrames, FirstFrameKeyframe: true, ClosedGOP: true,
			}
			newAssets[assetRef.AssetID] = assetRef
			segment.AssetID, segment.SHA256 = assetRef.AssetID, assetRef.SHA256
			segment.SourceInUS = 0
			segment.SourceDurationUS = assetRef.DurationUS
		}
	}
	if !changed {
		return planJSON, contract.HashCompiledPlanV2([]byte(planJSON)), nil
	}
	for _, asset := range plan.Assets {
		if !isDriveStock(asset) {
			newAssets[asset.AssetID] = asset
		}
	}
	plan.Assets = make([]contract.AssetRefV2, 0, len(newAssets))
	for _, asset := range newAssets {
		plan.Assets = append(plan.Assets, asset)
	}
	if err := contract.ValidateCompiledRenderPlanV2(&plan); err != nil {
		return "", "", fmt.Errorf("prepared compiled render plan is invalid: %w", err)
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		return "", "", err
	}
	return string(canonical), contract.HashCompiledPlanV2(canonical), nil
}

func matchesCanonicalStockVideo(probe VideoProbe, profile contract.CanonicalVideoProfileV1) bool {
	if probe.Width != profile.Width || probe.Height != profile.Height || probe.FPSNum != profile.FPSNum || probe.FPSDen != profile.FPSDen ||
		probe.VideoCodec != strings.ToLower(profile.Codec) || probe.PixelFormat != strings.ToLower(profile.PixelFormat) ||
		probe.VideoProfile != strings.ToLower(profile.CodecProfile) || probe.TimebaseNum != profile.TimeBaseNum || probe.TimebaseDen != profile.TimeBaseDen {
		return false
	}
	if profile.CodecLevel != "" {
		var level float64
		if _, err := fmt.Sscanf(profile.CodecLevel, "%f", &level); err == nil && probe.VideoLevel != int(math.Round(level*10)) {
			return false
		}
	}
	return canonicalGOPAligned(probe.Keyframes, profile)
}

func isDriveStock(asset contract.AssetRefV2) bool {
	return strings.EqualFold(strings.TrimSpace(asset.Kind), "video") &&
		(strings.EqualFold(strings.TrimSpace(asset.SourceKind), "drive") || strings.TrimSpace(asset.DriveFileID) != "" || strings.HasPrefix(strings.ToLower(asset.AssetID), "drive:"))
}

func stockRuntimeBindings(assets []map[string]interface{}) map[string]map[string]interface{} {
	out := make(map[string]map[string]interface{}, len(assets)*2)
	for _, asset := range assets {
		if !strings.EqualFold(strings.TrimSpace(fmt.Sprint(asset["role"])), "stock") {
			continue
		}
		for _, key := range []string{"drive_file_id", "asset_id", "asset_key"} {
			id := strings.TrimSpace(fmt.Sprint(asset[key]))
			if id != "" && id != "<nil>" {
				out[id] = asset
			}
		}
	}
	return out
}

func stockSourceURI(binding map[string]interface{}, driveID string) string {
	for _, key := range []string{"source_uri", "url", "drive_link"} {
		if value, ok := binding[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	uri, _ := DeferredDriveReference(driveID)
	return uri
}

func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}
