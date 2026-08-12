package performance

// benchmark_plan_test.go pins the frame-exact CompiledRenderPlanV2
// builder: the segment math the engine parser validates in exact
// integers (contiguous timeline_start_frame, CFR-exact
// frame_count/source_duration_us), the FINAL_AUDIO_COPY contract and the
// worker-injected bindings.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"velox-shared/contract"
)

// testManifest builds a manifest whose clips mirror the canonical spec:
// 24 clips × 375 frames, final audio covering the full duration.
func testManifest() *FixtureManifest {
	spec := CanonicalFixtureSpecV1()
	clips := make([]ManifestAsset, 0, spec.ClipCount)
	for i := 1; i <= spec.ClipCount; i++ {
		clips = append(clips, ManifestAsset{
			Name:        clipName(i),
			SHA256:      strings.Repeat("ab", 32),
			SizeBytes:   1024 + int64(i),
			DurationSec: spec.ClipDurationSec(),
			Frames:      spec.PerClipFrames,
		})
	}
	return &FixtureManifest{
		FixtureID:  FixtureCopyOnlyCanonical5MV1,
		SpecSHA256: spec.SpecSHA256(),
		Clips:      clips,
		FinalAudio: ManifestAsset{
			Name: "final_audio.m4a", SHA256: strings.Repeat("cd", 32),
			SizeBytes: 4096, DurationSec: float64(spec.DurationSec), Frames: spec.TotalFrames(),
		},
	}
}

func clipName(i int) string { return fmt.Sprintf("clip_%03d.mp4", i) }

// writeManifest persists the manifest into the track dir.
func writeManifest(dir string, manifest *FixtureManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "manifest.json"), data, 0o644)
}

// writeTestTrack materializes the manifest's files (content is
// irrelevant for the plan builder; only existence + size matter) and
// returns the track dir.
func writeTestTrack(t *testing.T, manifest *FixtureManifest) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, writeManifest(dir, manifest))
	for _, clip := range manifest.Clips {
		require.NoError(t, os.WriteFile(filepath.Join(dir, clip.Name), make([]byte, clip.SizeBytes), 0o644))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, manifest.FinalAudio.Name), make([]byte, manifest.FinalAudio.SizeBytes), 0o644))
	return dir
}

func TestBuildCopyOnlyPlanV2_FrameExact(t *testing.T) {
	spec := CanonicalFixtureSpecV1()
	manifest := testManifest()
	trackDir := writeTestTrack(t, manifest)

	doc, err := BuildCopyOnlyPlanV2(spec, manifest, trackDir)
	require.NoError(t, err)
	require.NotNil(t, doc)
	plan := doc.Plan
	require.Equal(t, contract.CompiledPlanVersionV2, plan.PlanVersion)
	require.Equal(t, int64(300_000_000), plan.DurationUS)
	require.Equal(t, spec.Video.Codec, plan.Output.VideoCodec)
	require.Equal(t, spec.Video.Width, plan.Output.Width)
	require.Equal(t, spec.Video.Height, plan.Output.Height)
	require.Equal(t, spec.Video.FPS, plan.Output.FPSNum)
	require.Equal(t, 1, plan.Output.FPSDen)

	// One track, 24 CFR-exact contiguous segments.
	require.Len(t, plan.VideoTracks, 1)
	segments := plan.VideoTracks[0].Segments
	require.Len(t, segments, spec.ClipCount)
	perClipUS := int64(spec.PerClipFrames) * 1_000_000 / int64(spec.Video.FPS)
	for i, seg := range segments {
		require.Equal(t, int64(i*spec.PerClipFrames), seg.TimelineStartFrame, "segment %d start frame", i)
		require.Equal(t, int64(spec.PerClipFrames), seg.FrameCount, "segment %d frame count", i)
		require.Equal(t, perClipUS, seg.SourceDurationUS, "segment %d source duration", i)
		require.Equal(t, int64(0), seg.SourceInUS)
		require.Equal(t, clipAssetID(manifest.Clips[i].Name), seg.AssetID)
	}

	// FINAL_AUDIO_COPY: zero audio re-encode.
	require.Equal(t, contract.AudioModeFinalAudioCopy, plan.FinalAudio.Mode)
	require.Equal(t, int64(300_000_000), plan.FinalAudio.DurationUS)
	require.Equal(t, "aac", plan.FinalAudio.Codec)
	require.Equal(t, 48000, plan.FinalAudio.SampleRateHz)
	require.Equal(t, 2, plan.FinalAudio.Channels)

	// Bindings: one per clip + final audio, all paths real and absolute.
	require.Len(t, doc.Bindings, spec.ClipCount+1)
	for assetID, path := range doc.Bindings {
		info, err := os.Stat(path)
		require.NoError(t, err, "binding %s", assetID)
		require.False(t, info.IsDir())
		require.True(t, filepath.IsAbs(path))
	}
	require.Contains(t, doc.Bindings, "final_audio")
	require.Contains(t, doc.Bindings, "clip_001")
	require.Contains(t, doc.Bindings, "clip_024")

	// Workload projection from the plan matches the fixture.
	workload := WorkloadFromCompiledRenderPlan(plan)
	require.Equal(t, spec.ClipCount, workload.ClipCount)
	require.Equal(t, spec.ClipCount+1, workload.AssetCount)
	require.True(t, workload.FinalAudioCopy)
}

func TestBuildCopyOnlyPlanV2_WireJSON(t *testing.T) {
	spec := CanonicalFixtureSpecV1()
	manifest := testManifest()
	trackDir := writeTestTrack(t, manifest)

	doc, err := BuildCopyOnlyPlanV2(spec, manifest, trackDir)
	require.NoError(t, err)
	doc.JobID = "bench-1"
	doc.OutputPath = filepath.Join(trackDir, "out.mp4")

	data, err := doc.MarshalJSON()
	require.NoError(t, err)
	var wire map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &wire))

	// The engine parser keys.
	require.Equal(t, float64(2), wire["plan_version"])
	require.Equal(t, "bench-1", wire["job_id"])
	require.Equal(t, doc.OutputPath, wire["output_path"])
	bindings, ok := wire["bindings"].(map[string]interface{})
	require.True(t, ok)
	require.Len(t, bindings, spec.ClipCount+1)
	require.Equal(t, float64(300_000_000), wire["duration_us"])
	output := wire["output"].(map[string]interface{})
	require.Equal(t, float64(30), output["fps_num"])

	// V2 forbids float seconds: the parser fails closed on any
	// duration_seconds key.
	require.NotContains(t, string(data), "duration_seconds")
	// The canonical plan body is path-free (the V2 contract); the only
	// paths on the wire live inside the runtime bindings object.
	planData, err := json.Marshal(doc.Plan)
	require.NoError(t, err)
	require.NotContains(t, string(planData), "clip_001.mp4")
	require.NotContains(t, string(planData), trackDir)
}

func TestBuildCopyOnlyPlanV2_RejectsBrokenTracks(t *testing.T) {
	spec := CanonicalFixtureSpecV1()
	t.Run("wrong clip count", func(t *testing.T) {
		manifest := testManifest()
		manifest.Clips = manifest.Clips[:23] // 23 != 24
		trackDir := writeTestTrack(t, manifest)
		_, err := BuildCopyOnlyPlanV2(spec, manifest, trackDir)
		require.Error(t, err)
	})
	t.Run("missing clip on disk", func(t *testing.T) {
		manifest := testManifest()
		trackDir := writeTestTrack(t, manifest)
		require.NoError(t, os.Remove(filepath.Join(trackDir, "clip_010.mp4")))
		_, err := BuildCopyOnlyPlanV2(spec, manifest, trackDir)
		require.Error(t, err)
	})
	t.Run("missing final audio on disk", func(t *testing.T) {
		manifest := testManifest()
		trackDir := writeTestTrack(t, manifest)
		require.NoError(t, os.Remove(filepath.Join(trackDir, manifest.FinalAudio.Name)))
		_, err := BuildCopyOnlyPlanV2(spec, manifest, trackDir)
		require.Error(t, err)
	})
	t.Run("nil manifest", func(t *testing.T) {
		_, err := BuildCopyOnlyPlanV2(spec, nil, t.TempDir())
		require.Error(t, err)
	})
}
