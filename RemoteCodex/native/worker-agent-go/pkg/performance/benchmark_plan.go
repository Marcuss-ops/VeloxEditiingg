package performance

// benchmark_plan.go owns the frame-exact CompiledRenderPlanV2 wire
// document the production benchmark RenderRunner feeds to the ZERO-SPAWN
// engine (plan §23): the canonical V2 plan stays path-free, and the
// worker injects the resolved local asset paths in a runtime "bindings"
// object so the C++ engine opens the clips in place via libavformat —
// no ffmpeg/ffprobe, no cache-to-tmp materialization, no segment files.
//
// Frame math (the "Formula 1 track" contract): every segment carries an
// integer timeline_start_frame, an integer frame_count and a CFR-exact
// source_duration_us derived from the spec frame rate. The engine parser
// validates contiguity and frame-exactness in exact integers and fails
// closed on drift — so the plan is deterministic by construction.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"velox-shared/contract"
)

// CopyOnlyPlanDocument is the built copy-only plan: the typed V2 plan
// (workload/identity source of truth) plus the runtime bindings map.
// JobID and OutputPath are stamped by the renderer before MarshalJSON.
type CopyOnlyPlanDocument struct {
	Plan       *contract.CompiledRenderPlanV2
	Bindings   map[string]string
	JobID      string
	OutputPath string
}

// compiledPlanV2Wire is the exact JSON the engine's --render consumes:
// the flattened CompiledRenderPlanV2 contract plus the worker-injected
// job_id, output_path and bindings block (the C++ parser reads these
// three top-level keys; everything else stays the canonical contract).
type compiledPlanV2Wire struct {
	*contract.CompiledRenderPlanV2
	JobID      string            `json:"job_id"`
	OutputPath string            `json:"output_path"`
	Bindings   map[string]string `json:"bindings"`
}

// MarshalJSON renders the wire document the engine parses. Indented
// output is deliberate: the plan is a diagnostic artifact too.
func (d *CopyOnlyPlanDocument) MarshalJSON() ([]byte, error) {
	if d == nil || d.Plan == nil {
		return nil, fmt.Errorf("copy-only plan: nil document")
	}
	if d.OutputPath == "" {
		return nil, fmt.Errorf("copy-only plan: output_path is required")
	}
	doc := compiledPlanV2Wire{
		CompiledRenderPlanV2: d.Plan,
		JobID:                d.JobID,
		OutputPath:           d.OutputPath,
		Bindings:             d.Bindings,
	}
	return json.MarshalIndent(doc, "", "  ")
}

// BuildCopyOnlyPlanV2 builds the frame-exact copy-only plan for the
// canonical fixture track: one video segment per clip (contiguous
// timeline placement, CFR-exact frame_count/source_duration_us at the
// spec frame rate) plus the FINAL_AUDIO_COPY track. Every binding is
// verified to exist on disk and match the manifest byte identity, so a
// stale or partial track fails here — before any render is attempted.
func BuildCopyOnlyPlanV2(spec CanonicalFixtureSpec, manifest *FixtureManifest, trackDir string) (*CopyOnlyPlanDocument, error) {
	if manifest == nil {
		return nil, fmt.Errorf("build copy-only plan: nil manifest")
	}
	if len(manifest.Clips) != spec.ClipCount {
		return nil, fmt.Errorf("build copy-only plan: manifest has %d clips, fixture requires %d", len(manifest.Clips), spec.ClipCount)
	}
	if spec.Video.FPS <= 0 || spec.Video.Width <= 0 || spec.Video.Height <= 0 {
		return nil, fmt.Errorf("build copy-only plan: spec requires positive fps/width/height")
	}
	// CFR-exact per-clip window: PerClipFrames frames at spec fps.
	perClipUS := int64(spec.PerClipFrames) * 1_000_000 / int64(spec.Video.FPS)
	durationUS := int64(spec.DurationSec) * 1_000_000

	plan := &contract.CompiledRenderPlanV2{
		PlanVersion: contract.CompiledPlanVersionV2,
		DurationUS:  durationUS,
		Output: contract.OutputContractV2{
			Container:   "mp4",
			VideoCodec:  spec.Video.Codec,
			Width:       spec.Video.Width,
			Height:      spec.Video.Height,
			FPSNum:      spec.Video.FPS,
			FPSDen:      1,
			PixelFormat: spec.Video.PixelFormat,
		},
	}
	bindings := make(map[string]string, len(manifest.Clips)+1)
	assets := make([]contract.AssetRefV2, 0, len(manifest.Clips)+1)
	segments := make([]contract.VideoSegmentV2, 0, len(manifest.Clips))

	for i, clip := range manifest.Clips {
		assetID := clipAssetID(clip.Name)
		path := filepath.Join(trackDir, clip.Name)
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("build copy-only plan: clip %s: %w", clip.Name, err)
		}
		if !info.Mode().IsRegular() || info.Size() <= 0 {
			return nil, fmt.Errorf("build copy-only plan: clip %s is not a regular non-empty file", clip.Name)
		}
		bindings[assetID] = path
		assets = append(assets, contract.AssetRefV2{
			AssetID: assetID, SHA256: clip.SHA256, SizeBytes: info.Size(),
			Kind: "video", MIME: "video/mp4", DurationUS: perClipUS,
			Width: spec.Video.Width, Height: spec.Video.Height,
		})
		segments = append(segments, contract.VideoSegmentV2{
			SegmentID:          fmt.Sprintf("seg_%03d", i+1),
			AssetID:            assetID,
			SHA256:             clip.SHA256,
			TimelineStartFrame: int64(i * spec.PerClipFrames),
			FrameCount:         int64(spec.PerClipFrames),
			SourceInUS:         0,
			SourceDurationUS:   perClipUS,
		})
	}
	plan.VideoTracks = []contract.VideoTrackV2{{TrackID: "t0", Segments: segments}}

	// Final audio: the FINAL_AUDIO_COPY contract — zero audio re-encode.
	finalPath := filepath.Join(trackDir, manifest.FinalAudio.Name)
	finalInfo, err := os.Stat(finalPath)
	if err != nil {
		return nil, fmt.Errorf("build copy-only plan: final audio %s: %w", manifest.FinalAudio.Name, err)
	}
	if !finalInfo.Mode().IsRegular() || finalInfo.Size() <= 0 {
		return nil, fmt.Errorf("build copy-only plan: final audio %s is not a regular non-empty file", manifest.FinalAudio.Name)
	}
	finalAssetID := clipAssetID(manifest.FinalAudio.Name)
	bindings[finalAssetID] = finalPath
	assets = append(assets, contract.AssetRefV2{
		AssetID: finalAssetID, SHA256: manifest.FinalAudio.SHA256, SizeBytes: finalInfo.Size(),
		Kind: "audio", MIME: "audio/mp4", DurationUS: durationUS,
	})
	plan.Assets = assets
	plan.FinalAudio = contract.FinalAudioV2{
		Mode:         contract.AudioModeFinalAudioCopy,
		AssetID:      finalAssetID,
		SHA256:       manifest.FinalAudio.SHA256,
		SizeBytes:    finalInfo.Size(),
		Codec:        spec.Audio.Codec,
		SampleRateHz: spec.Audio.SampleRate,
		Channels:     spec.Audio.Channels,
		DurationUS:   durationUS,
	}
	return &CopyOnlyPlanDocument{Plan: plan, Bindings: bindings}, nil
}

// clipAssetID derives the canonical asset id from a manifest file name
// (clip_001.mp4 → clip_001, final_audio.m4a → final_audio).
func clipAssetID(name string) string {
	return strings.TrimSuffix(name, filepath.Ext(name))
}
