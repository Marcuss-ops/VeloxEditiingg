package executors

import (
	"strconv"
	"strings"
	"testing"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/runtimeassets"
	"velox-worker-agent/pkg/video/plan"
)

// render_command_builders_bench_test.go records the allocation baseline for
// the three ffmpeg command builders slated for a single-strings.Builder
// rewrite. The target metric is B/op and allocs/op — these functions build
// one filter graph and should not pay per-segment/per-parameter temporary
// allocations.

// benchmarkAudioMixPlan builds a realistic 14-track mix: one voiceover, one
// ducked music bed, and 12 SFX tracks with offsets and fades.
func benchmarkAudioMixPlan(nTracks int) *plan.RenderPlan {
	p := &plan.RenderPlan{
		Version:     1,
		JobID:       "bench.audio-mix",
		Canvas:      plan.CanvasSpec{Width: 1920, Height: 1080, Fps: 30},
		AudioTracks: make([]plan.AudioTrack, 0, nTracks),
	}
	for i := 0; i < nTracks; i++ {
		track := plan.AudioTrack{
			SourceURL: "/cache/audio-" + strconv.Itoa(i) + ".wav",
			Volume:    1.0,
		}
		switch {
		case i == 0:
			track.Role = "voiceover"
		case i == 1:
			track.Role = "music"
			track.Volume = 0.6
			track.DurationSeconds = 30
			track.FadeInSeconds = 1.5
			track.FadeOutSeconds = 2.0
			track.DuckingEnabled = true
		default:
			track.Role = "sfx"
			track.StartTimeOffset = float64(i) * 0.4
			track.DurationSeconds = 0.8
			track.FadeInSeconds = 0.05
			track.FadeOutSeconds = 0.1
		}
		p.AudioTracks = append(p.AudioTracks, track)
	}
	return p
}

// benchmarkComposePlan builds a timeline with nSegments video sources.
func benchmarkComposePlan(nSegments int) *plan.RenderPlan {
	p := &plan.RenderPlan{
		Version:  1,
		JobID:    "bench.compose",
		Canvas:   plan.CanvasSpec{Width: 1920, Height: 1080, Fps: 30},
		Timeline: make([]plan.TimelineItem, 0, nSegments),
	}
	for i := 0; i < nSegments; i++ {
		id := strconv.Itoa(i)
		p.Timeline = append(p.Timeline, plan.TimelineItem{
			Source:          plan.MediaSource{Type: "video", URL: "/cache/seg-" + id + ".mp4", CacheKey: "seg-" + id},
			SceneID:         "scene-" + id,
			DurationSeconds: 1.0,
		})
	}
	return p
}

// benchmarkVideoPlan builds a V2 video-only plan with nSegments one-second
// segments (30fps) and matching runtime bindings. FrameCount/SourceDurationUS
// are chosen so the frame-duration consistency check always passes.
func benchmarkVideoPlan(nSegments int) (*contract.CompiledRenderPlanV2, runtimeassets.Bindings) {
	segments := make([]contract.VideoSegmentV2, 0, nSegments)
	assets := make([]contract.AssetRefV2, 0, nSegments)
	bindings := make(runtimeassets.Bindings, nSegments)
	for i := 0; i < nSegments; i++ {
		id := strconv.Itoa(i)
		assetID := "video-" + id
		sha := strings.Repeat("a", 64)
		segments = append(segments, contract.VideoSegmentV2{
			SegmentID:          "segment-" + id,
			AssetID:            assetID,
			SHA256:             sha,
			TimelineStartFrame: int64(i) * 30,
			FrameCount:         30,
			SourceInUS:         int64(i) * 1_000_000,
			SourceDurationUS:   1_000_000,
		})
		assets = append(assets, contract.AssetRefV2{
			AssetID: assetID, SHA256: sha, SizeBytes: 1_000_000,
			Kind: "video", DurationUS: 1_000_000, Width: 1920, Height: 1080,
		})
		bindings[assetID] = runtimeassets.Binding{
			AssetID: assetID, Path: "/cache/" + assetID + ".mp4", SHA256: sha, Size: 1_000_000,
		}
	}
	p := &contract.CompiledRenderPlanV2{
		PlanVersion:      contract.CompiledPlanVersionV2,
		TimelineRevision: 1,
		DurationUS:       int64(nSegments) * 1_000_000,
		Output: contract.OutputContractV2{
			Container: "mp4", VideoCodec: "libx264", Width: 1920, Height: 1080,
			FPSNum: 30, FPSDen: 1, PixelFormat: "yuv420p",
		},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: segments}},
		Assets:      assets,
	}
	return p, bindings
}

func BenchmarkBuildAudioMixPlan_14Tracks(b *testing.B) {
	p := benchmarkAudioMixPlan(14)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := buildAudioMixPlan(executor.TaskSpec{}, p, "/tmp/bench-audio.wav"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuildComposePlan_30Segments(b *testing.B) {
	p := benchmarkComposePlan(30)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := buildComposePlan(executor.TaskSpec{}, p, "/tmp/bench-compose.mp4"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuildVideoOnlyArgs_30Segments(b *testing.B) {
	p, bindings := benchmarkVideoPlan(30)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := buildVideoOnlyArgs(p, bindings, "/tmp/bench-video.mp4"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBuildVideoOnlyArgs_100Segments(b *testing.B) {
	p, bindings := benchmarkVideoPlan(100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := buildVideoOnlyArgs(p, bindings, "/tmp/bench-video.mp4"); err != nil {
			b.Fatal(err)
		}
	}
}
