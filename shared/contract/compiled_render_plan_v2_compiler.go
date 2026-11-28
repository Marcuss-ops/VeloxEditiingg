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
// it requires exactly one verified final_audio asset so the native assembler
// can perform FINAL_AUDIO_COPY without choosing an audio mix or AAC encode.
func CompileRenderPlanV2FromManifest(raw map[string]any) (*CompiledRenderPlanV2, error) {
	return CompileRenderPlanV2FromManifestWithReplacements(raw, nil)
}

// CompileRenderPlanV2FromManifestWithOverlays consumes the editorial overlay
// list without widening the native V2 schema. replace intent is compiled into
// the single video track. composite intent returns a fail-closed handoff
// error: the caller must ask Chronon for prepared fragments and then compile
// those fragments as VisualReplacement values.
func CompileRenderPlanV2FromManifestWithOverlays(raw map[string]any, overlays []Overlay) (*CompiledRenderPlanV2, error) {
	if len(overlays) == 0 {
		return CompileRenderPlanV2FromManifest(raw)
	}
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
	if err := manifest.Validate(); err != nil {
		return nil, fmt.Errorf("compiled render plan v2: render_manifest: %w", err)
	}
	baseFrames := int64(0)
	for _, track := range manifest.Tracks {
		if track.Kind != "video" {
			continue
		}
		for _, event := range track.Events {
			start, startErr := timeUSToNearestFrame(millisecondsToMicroseconds(event.TimelineStartMS), manifest.Canvas.FPSNum, manifest.Canvas.FPSDen)
			duration, durationErr := timeUSToNearestFrame(millisecondsToMicroseconds(event.DurationMS), manifest.Canvas.FPSNum, manifest.Canvas.FPSDen)
			if startErr != nil || durationErr != nil || duration <= 0 {
				return nil, fmt.Errorf("compiled render plan v2: overlay base timeline cannot be quantized to frames")
			}
			if start+duration > baseFrames {
				baseFrames = start + duration
			}
		}
	}
	if err := ValidateOverlays(overlays, baseFrames); err != nil {
		return nil, err
	}
	for _, overlay := range overlays {
		if overlay.Mode == string(OverlayModeComposite) {
			return nil, fmt.Errorf("OVERLAY_COMPOSITE_REQUIRES_CHRONON: overlay %q must be prepared as a canonical video fragment before V2 compilation", overlay.ID)
		}
	}
	replacements := make([]VisualReplacement, 0, len(overlays))
	for _, overlay := range overlays {
		replacements = append(replacements, VisualReplacement{
			ReplacementID: overlay.ID, AssetID: overlay.AssetID, SHA256: overlay.SHA256,
			TimelineStartUS: frameToUS(overlay.StartFrame, manifest.Canvas.FPSNum, manifest.Canvas.FPSDen),
			TimelineEndUS:   frameToUS(overlay.StartFrame+overlay.FrameCount, manifest.Canvas.FPSNum, manifest.Canvas.FPSDen),
		})
	}
	return compileRenderPlanV2Manifest(manifest, replacements)
}

// CompileRenderPlanV2FromManifestWithPreparedOverlays compiles a base
// manifest and applies already-rendered Chronon fragments for composite
// overlay windows. Velox only places the certified fragments in the final
// packet-copy timeline; it never composites pixels itself.
func CompileRenderPlanV2FromManifestWithPreparedOverlays(
	raw map[string]any,
	overlays []Overlay,
	fragments []PreparedOverlayFragmentArtifact,
) (*CompiledRenderPlanV2, error) {
	composites := make([]Overlay, 0, len(overlays))
	replacements := make([]Overlay, 0, len(overlays))
	for _, overlay := range overlays {
		switch overlay.Mode {
		case string(OverlayModeComposite):
			composites = append(composites, overlay)
		case string(OverlayModeReplace):
			replacements = append(replacements, overlay)
		default:
			return nil, fmt.Errorf("compiled render plan v2: overlay %q has unsupported mode %q", overlay.ID, overlay.Mode)
		}
	}
	plan, err := CompileRenderPlanV2FromManifestWithOverlays(raw, replacements)
	if err != nil {
		return nil, err
	}
	if len(composites) == 0 {
		if len(fragments) != 0 {
			return nil, fmt.Errorf("compiled render plan v2: prepared overlay fragments supplied without composite overlays")
		}
		return plan, nil
	}

	profile, err := KnownCanonicalVideoProfileV1(plan.Output.ProfileID)
	if err != nil {
		return nil, fmt.Errorf("compiled render plan v2: composite overlays require a canonical output profile: %w", err)
	}
	if len(plan.VideoTracks) != 1 || len(plan.VideoTracks[0].Segments) == 0 {
		return nil, fmt.Errorf("compiled render plan v2: composite overlays require one non-empty base video track")
	}
	base := plan.VideoTracks[0].Segments
	windows, err := ResolveOverlayWindows(composites, videoTrackFrameCount(base))
	if err != nil {
		return nil, fmt.Errorf("compiled render plan v2: composite overlay windows: %w", err)
	}
	if len(windows) != len(fragments) {
		return nil, fmt.Errorf("compiled render plan v2: %d composite windows but %d prepared fragments", len(windows), len(fragments))
	}

	prepared := make([]PreparedOverlayFragment, 0, len(fragments))
	seenAssets := make(map[string]struct{}, len(fragments))
	declaredAssets := make(map[string]struct{}, len(plan.Assets))
	for _, asset := range plan.Assets {
		declaredAssets[asset.AssetID] = struct{}{}
	}
	for _, artifact := range fragments {
		manifest := artifact.Manifest
		if err := manifest.Validate(); err != nil {
			return nil, fmt.Errorf("compiled render plan v2: prepared fragment %q: %w", artifact.WindowID, err)
		}
		if strings.TrimSpace(artifact.WindowID) == "" {
			return nil, fmt.Errorf("compiled render plan v2: prepared fragment window_id is required")
		}
		if _, exists := seenAssets[manifest.ArtifactID]; exists {
			return nil, fmt.Errorf("compiled render plan v2: duplicate prepared fragment asset_id %q", manifest.ArtifactID)
		}
		if _, exists := declaredAssets[manifest.ArtifactID]; exists {
			return nil, fmt.Errorf("compiled render plan v2: prepared fragment asset_id %q is already declared by the manifest", manifest.ArtifactID)
		}
		seenAssets[manifest.ArtifactID] = struct{}{}
		if manifest.TimelineRevision != plan.TimelineRevision || manifest.TimelineSHA256 != plan.TimelineSHA256 {
			return nil, fmt.Errorf("compiled render plan v2: prepared fragment %q is bound to a different timeline", artifact.WindowID)
		}
		if manifest.ProfileID != profile.StreamProfile() {
			return nil, fmt.Errorf("compiled render plan v2: prepared fragment %q profile %q does not match output stream profile %q", artifact.WindowID, manifest.ProfileID, profile.StreamProfile())
		}
		window, ok := overlayWindowByID(windows, artifact.WindowID)
		if !ok {
			return nil, fmt.Errorf("compiled render plan v2: prepared fragment references unknown window %q", artifact.WindowID)
		}
		windowDurationUS := frameToUS(window.StartFrame+window.FrameCount, profile.FPSNum, profile.FPSDen) - frameToUS(window.StartFrame, profile.FPSNum, profile.FPSDen)
		if manifest.TimelineStartFrame != window.StartFrame || manifest.FrameCount != window.FrameCount || manifest.DurationUS != windowDurationUS {
			return nil, fmt.Errorf("compiled render plan v2: prepared fragment %q does not exactly cover its overlay window", artifact.WindowID)
		}
		asset, err := manifest.AssetRefV2("video/mp4")
		if err != nil {
			return nil, fmt.Errorf("compiled render plan v2: prepared fragment %q: %w", artifact.WindowID, err)
		}
		asset.Width = profile.Width
		asset.Height = profile.Height
		prepared = append(prepared, PreparedOverlayFragment{
			WindowID: artifact.WindowID, AssetID: asset.AssetID, SHA256: asset.SHA256, ProfileID: asset.ProfileID,
		})
		plan.Assets = append(plan.Assets, asset)
	}
	segments, err := ApplyPreparedOverlayFragments(base, windows, prepared, profile.FPSNum, profile.FPSDen)
	if err != nil {
		return nil, fmt.Errorf("compiled render plan v2: apply prepared overlays: %w", err)
	}
	for i := range segments {
		segments[i].SegmentID = fmt.Sprintf("%s-segment-%06d", plan.VideoTracks[0].TrackID, i)
	}
	plan.VideoTracks[0].Segments = segments
	if err := ValidateCompiledRenderPlanV2(plan); err != nil {
		return nil, fmt.Errorf("compiled render plan v2: prepared overlay plan failed validation: %w", err)
	}
	return plan, nil
}

func videoTrackFrameCount(segments []VideoSegmentV2) int64 {
	var frames int64
	for _, segment := range segments {
		if end := segment.TimelineStartFrame + segment.FrameCount; end > frames {
			frames = end
		}
	}
	return frames
}

func overlayWindowByID(windows []OverlayWindow, id string) (OverlayWindow, bool) {
	for _, window := range windows {
		if window.WindowID == id {
			return window, true
		}
	}
	return OverlayWindow{}, false
}

// CompileRenderPlanV2FromManifestWithReplacements compiles the strict
// render_manifest and then applies the visual_replacements[] list: each
// replacement swaps a prepared, video-only asset into the base video
// timeline over its absolute interval. The replacement asset must already be
// declared as a `video` asset in the manifest assets[] (its identity is
// re-verified against the declared SHA256), so the compiled plan still
// carries verified asset metadata and no contract relaxation is needed.
func CompileRenderPlanV2FromManifestWithReplacements(raw map[string]any, replacements []VisualReplacement) (*CompiledRenderPlanV2, error) {
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
	return compileRenderPlanV2Manifest(manifest, replacements)
}

func compileRenderPlanV2Manifest(manifest *rendermanifest.Manifest, replacements []VisualReplacement) (*CompiledRenderPlanV2, error) {
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
	for _, r := range replacements {
		asset, ok := assetByID[r.AssetID]
		if !ok {
			return nil, visualReplacementErrorf(VisualReplacementCodeAssetInvalid, r.ReplacementID, r.TimelineStartUS, "asset_id %q is not declared in the render_manifest assets", r.AssetID)
		}
		if asset.Kind != "video" {
			return nil, visualReplacementErrorf(VisualReplacementCodeAssetInvalid, r.ReplacementID, r.TimelineStartUS, "asset_id %q must reference a video asset, got kind %q", r.AssetID, asset.Kind)
		}
		if r.SHA256 != "" && r.SHA256 != asset.SHA256 {
			return nil, visualReplacementErrorf(VisualReplacementCodeAssetInvalid, r.ReplacementID, r.TimelineStartUS, "sha256 %q does not match manifest asset %q", r.SHA256, r.AssetID)
		}
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
	output := OutputContractV2{
		Container:   manifest.Output.Container,
		VideoCodec:  manifest.Output.VideoCodec,
		Width:       manifest.Canvas.Width,
		Height:      manifest.Canvas.Height,
		FPSNum:      manifest.Canvas.FPSNum,
		FPSDen:      manifest.Canvas.FPSDen,
		PixelFormat: manifest.Canvas.PixelFormat,
	}
	if profileID := strings.TrimSpace(manifest.Output.ProfileID); profileID != "" {
		profile, profileErr := KnownCanonicalVideoProfileV1(profileID)
		if profileErr != nil {
			return nil, fmt.Errorf("compiled render plan v2: output.profile_id: %w", profileErr)
		}
		if err := profile.MatchesOutput(output); err != nil {
			return nil, fmt.Errorf("compiled render plan v2: output.profile_id: %w", err)
		}
		// The profile selection is the sole source for the strengthened
		// stream-identity fields. Do not infer these values from a flag or
		// from the generic manifest geometry.
		output.ProfileID = profile.ProfileID
		output.CodecProfile = profile.CodecProfile
		output.CodecLevel = profile.CodecLevel
		output.GOPSize = profile.GOPSize
		output.BFrames = profile.BFrames
		output.ClosedGOP = profile.ClosedGOP
		output.TimeBaseNum = profile.TimeBaseNum
		output.TimeBaseDen = profile.TimeBaseDen
	}
	plan := &CompiledRenderPlanV2{
		PlanVersion:      CompiledPlanVersionV2,
		TimelineRevision: 1,
		TimelineSHA256:   timelineSHA,
		DurationUS:       finalAudioDurationUS,
		Output:           output,
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
		if len(replacements) > 0 {
			resolved, err := ResolveVisualReplacements(videoTrack.Segments, replacements, manifest.Canvas.FPSNum, manifest.Canvas.FPSDen)
			if err != nil {
				return nil, fmt.Errorf("compiled render plan v2: tracks[%s] visual replacements: %w", track.ID, err)
			}
			for i := range resolved {
				resolved[i].SegmentID = fmt.Sprintf("%s-segment-%06d", track.ID, i)
			}
			videoTrack.Segments = resolved
		}
		if len(videoTrack.Segments) > 0 {
			plan.VideoTracks = append(plan.VideoTracks, videoTrack)
		}
	}
	if len(plan.VideoTracks) == 0 {
		return nil, fmt.Errorf("compiled render plan v2: at least one video track with events is required")
	}
	// Fail-closed declared-duration gate (plan §6). Runs AFTER the resolver
	// has validated range/overlap/bounds so a malformed timeline surfaces its
	// timeline error first; a well-formed timeline with a wrong declared media
	// duration then surfaces VISUAL_REPLACEMENT_DURATION_MISMATCH. The worker
	// re-checks the REAL probed duration later — this master-side check only
	// catches a producer that declares the wrong media up front.
	for _, r := range replacements {
		asset, ok := assetByID[r.AssetID]
		if !ok {
			continue // identity gate already rejected this above
		}
		if windowUS := r.TimelineEndUS - r.TimelineStartUS; windowUS > 0 {
			declaredUS := millisecondsToMicroseconds(asset.DurationMS)
			if diff := absInt64(declaredUS - windowUS); diff > ReplacementDurationToleranceUS {
				return nil, visualReplacementErrorf(VisualReplacementCodeDurationMismatch, r.ReplacementID, r.TimelineStartUS, "manifest asset %q declares %d us but the replacement window is %d us (tolerance %d us)", r.AssetID, declaredUS, windowUS, ReplacementDurationToleranceUS)
			}
		}
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
	return CompileRenderPlanV2JSONWithReplacements(raw, nil)
}

// CompileRenderPlanV2JSONWithOverlays returns canonical V2 bytes after
// resolving editorial replace overlays. Composite overlays use the prepared
// fragment entry point below.
func CompileRenderPlanV2JSONWithOverlays(raw map[string]any, overlays []Overlay) ([]byte, string, error) {
	plan, err := CompileRenderPlanV2FromManifestWithOverlays(raw, overlays)
	if err != nil {
		return nil, "", err
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		return nil, "", err
	}
	return canonical, HashCompiledPlanV2(canonical), nil
}

// CompileRenderPlanV2JSONWithPreparedOverlays returns canonical V2 bytes and
// their transport hash after inserting certified Chronon-composited fragments
// into the packet-copy timeline.
func CompileRenderPlanV2JSONWithPreparedOverlays(
	raw map[string]any,
	overlays []Overlay,
	fragments []PreparedOverlayFragmentArtifact,
) ([]byte, string, error) {
	plan, err := CompileRenderPlanV2FromManifestWithPreparedOverlays(raw, overlays, fragments)
	if err != nil {
		return nil, "", err
	}
	canonical, err := plan.CanonicalJSON()
	if err != nil {
		return nil, "", err
	}
	return canonical, HashCompiledPlanV2(canonical), nil
}

// CompileRenderPlanV2JSONWithReplacements returns canonical V2 bytes plus the
// transport hash after applying the supplied visual replacements.
func CompileRenderPlanV2JSONWithReplacements(raw map[string]any, replacements []VisualReplacement) ([]byte, string, error) {
	plan, err := CompileRenderPlanV2FromManifestWithReplacements(raw, replacements)
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
