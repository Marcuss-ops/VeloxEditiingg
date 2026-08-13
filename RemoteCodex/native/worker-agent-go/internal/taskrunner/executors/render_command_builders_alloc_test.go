package executors

import (
	"testing"

	"velox-worker-agent/internal/executor"
)

// render_command_builders_alloc_test.go pins the allocation baseline of the
// three ffmpeg command builders. The filter graph is written in one pass into
// a pre-grown strings.Builder, and the per-segment/per-track integer and float
// writers (writeInt / writeFloat6) are allocation-free (they write into a
// stack scratch buffer instead of strconv.Itoa / strconv.FormatFloat /
// fmt.Sprintf), so allocs/op must NOT scale with segment or track count.
//
// These assertions catch the regression the builders were rewritten to avoid:
// reintroducing strconv.Itoa or fmt.Sprintf in the hot loops makes allocs/op
// grow linearly with the timeline instead of staying flat.

func TestFFmpegBuildersAllocCeiling(t *testing.T) {
	// Build the fixtures OUTSIDE the measured closure: AllocsPerRun counts
	// every allocation in the closure, so the plan/binding construction must
	// not be mixed in with the builder under test.
	audioPlan := benchmarkAudioMixPlan(14)
	composePlan := benchmarkComposePlan(30)
	videoPlan30, videoBind30 := benchmarkVideoPlan(30)
	videoPlan100, videoBind100 := benchmarkVideoPlan(100)

	cases := []struct {
		name      string
		build     func() error
		maxAllocs float64
	}{
		{
			name: "audio_mix_14tracks",
			build: func() error {
				_, err := buildAudioMixPlan(executor.TaskSpec{}, audioPlan, "/tmp/alloc-audio.wav")
				return err
			},
			maxAllocs: 40, // baseline 26
		},
		{
			name: "compose_30segments",
			build: func() error {
				_, err := buildComposePlan(executor.TaskSpec{}, composePlan, "/tmp/alloc-compose.mp4")
				return err
			},
			maxAllocs: 20, // baseline 9
		},
		{
			name: "video_only_30segments",
			build: func() error {
				_, err := buildVideoOnlyArgs(videoPlan30, videoBind30, "/tmp/alloc-video.mp4")
				return err
			},
			maxAllocs: 32, // baseline 17
		},
		{
			name: "video_only_100segments",
			build: func() error {
				_, err := buildVideoOnlyArgs(videoPlan100, videoBind100, "/tmp/alloc-video.mp4")
				return err
			},
			maxAllocs: 32, // baseline 18 (flat regardless of segment count)
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// AllocsPerRun runs the closure once to warm up, then measures the
			// average allocation count over 100 runs.
			allocs := testing.AllocsPerRun(100, func() {
				if err := tc.build(); err != nil {
					t.Fatal(err)
				}
			})
			if allocs > tc.maxAllocs {
				t.Fatalf("%s allocs/op = %.1f, want <= %.1f (allocation regression)", tc.name, allocs, tc.maxAllocs)
			}
		})
	}
}

// TestBuildVideoOnlyArgsAllocsFlatAcrossSegments is the flatness guard: the
// filter-graph builder must not allocate per segment. A 100-segment timeline
// should cost only the one extra final-string allocation over a 30-segment
// timeline — never a linear per-segment cost. This is what catches a
// reintroduced strconv.Itoa/fmt.Sprintf in the segment loop even when the
// absolute ceiling above still passes.
func TestBuildVideoOnlyArgsAllocsFlatAcrossSegments(t *testing.T) {
	measure := func(nSegments int) float64 {
		p, b := benchmarkVideoPlan(nSegments)
		return testing.AllocsPerRun(100, func() {
			if _, err := buildVideoOnlyArgs(p, b, "/tmp/alloc-video.mp4"); err != nil {
				t.Fatal(err)
			}
		})
	}
	small := measure(30)
	large := measure(100)
	if large > small+8 {
		t.Fatalf("allocs/op grew with segment count: 30 segments=%.1f, 100 segments=%.1f (per-segment allocation regression)", small, large)
	}
}
