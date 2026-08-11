package images

import (
	"context"
	"testing"

	"velox-worker-agent/pkg/video/plan"
)

// fakeProbe satisfies audio.Probe so Compile() can be exercised
// without spawning the real audio service.
type fakeProbe struct{ dur float64 }

func (f fakeProbe) DurationSeconds(url string) float64 { return f.dur }

// forbidProbe fails the test if DurationSeconds is ever invoked. It
// pins the Fase C1/D migration: when the upstream declares the audio
// duration (compiled render plan / asset-registry stamping), the
// compiler MUST consume it and MUST NOT spawn ffprobe on the input.
type forbidProbe struct{ t *testing.T }

func (p forbidProbe) DurationSeconds(url string) float64 {
	p.t.Fatalf("probe.DurationSeconds(%q) must not be called when audio duration is declared", url)
	return 0
}

// baseInput returns the minimal valid input map for Compile: images +
// audio_url present. Tests override fields on the returned map as
// needed.
func baseInput() map[string]interface{} {
	return map[string]interface{}{
		"images":    []interface{}{"https://example.test/a.png", "https://example.test/b.png"},
		"audio_url": "https://example.test/audio.mp3",
	}
}

// TestCompile_DeclaredAudioDuration_SkipsProbe verifies that a
// declared audio_duration_seconds (the Fase D compiled-plan / C1
// registry field) is consumed instead of the probe: the probe must
// never run, and the allocated per-image durations must sum to the
// declared length.
func TestCompile_DeclaredAudioDuration_SkipsProbe(t *testing.T) {
	input := baseInput()
	input["audio_duration_seconds"] = 10.0

	rp, err := Compile(context.Background(), "job-declared", input, "/tmp/out.mp4", forbidProbe{t})
	if err != nil {
		t.Fatalf("Compile() returned unexpected error: %v", err)
	}
	if rp == nil {
		t.Fatal("Compile() returned nil RenderPlan")
	}
	if len(rp.Timeline) != 2 {
		t.Fatalf("timeline length = %d, want 2 images", len(rp.Timeline))
	}
	var total float64
	for _, item := range rp.Timeline {
		total += item.DurationSeconds
	}
	if total != 10.0 {
		t.Fatalf("timeline total = %v, want declared 10.0 (probe must not have run)", total)
	}
	if len(rp.AudioTracks) != 1 || rp.AudioTracks[0].SourceURL != "https://example.test/audio.mp3" {
		t.Fatalf("audio tracks = %+v, want the declared audio_url", rp.AudioTracks)
	}
}

// TestCompile_DeclaredAudioDurationMs_SkipsProbe pins the millisecond
// alias of the declared duration.
func TestCompile_DeclaredAudioDurationMs_SkipsProbe(t *testing.T) {
	input := baseInput()
	input["audio_duration_ms"] = 12500.0

	rp, err := Compile(context.Background(), "job-declared-ms", input, "/tmp/out.mp4", forbidProbe{t})
	if err != nil {
		t.Fatalf("Compile() returned unexpected error: %v", err)
	}
	if rp == nil {
		t.Fatal("Compile() returned nil RenderPlan")
	}
	var total float64
	for _, item := range rp.Timeline {
		total += item.DurationSeconds
	}
	if total != 12.5 {
		t.Fatalf("timeline total = %v, want 12.5 from declared audio_duration_ms", total)
	}
}

// TestCompile_DeclaredSeconds_WinsOverMs pins the precedence between
// the two declared sources: audio_duration_seconds is authoritative
// when both are present.
func TestCompile_DeclaredSeconds_WinsOverMs(t *testing.T) {
	input := baseInput()
	input["audio_duration_seconds"] = 10.0
	input["audio_duration_ms"] = 20000.0

	rp, err := Compile(context.Background(), "job-precedence", input, "/tmp/out.mp4", forbidProbe{t})
	if err != nil {
		t.Fatalf("Compile() returned unexpected error: %v", err)
	}
	if rp == nil {
		t.Fatal("Compile() returned nil RenderPlan")
	}
	var total float64
	for _, item := range rp.Timeline {
		total += item.DurationSeconds
	}
	if total != 10.0 {
		t.Fatalf("timeline total = %v, want 10.0 (audio_duration_seconds must win over ms)", total)
	}
}

// TestCompile_NoDeclaredDuration_FallsBackToProbe pins the fallback:
// a payload without a declared duration still probes (legacy/deferred
// references remain supported).
func TestCompile_NoDeclaredDuration_FallsBackToProbe(t *testing.T) {
	input := baseInput()
	rp, err := Compile(context.Background(), "job-probe", input, "/tmp/out.mp4", fakeProbe{dur: 20.0})
	if err != nil {
		t.Fatalf("Compile() returned unexpected error: %v", err)
	}
	if rp == nil {
		t.Fatal("Compile() returned nil RenderPlan")
	}
	var total float64
	for _, item := range rp.Timeline {
		total += item.DurationSeconds
	}
	if total != 20.0 {
		t.Fatalf("timeline total = %v, want probed 20.0", total)
	}
}

// TestCompile_Canvas_PinsDefault verifies the default horizontal canvas
// and the vertical override, matching the plan contract.
func TestCompile_Canvas_PinsDefault(t *testing.T) {
	defaultC := plan.DefaultCanvas()
	verticalC := plan.VerticalCanvas()

	cases := []struct {
		name        string
		orientation string
		want        plan.CanvasSpec
	}{
		{"default_horizontal", "", defaultC},
		{"horizontal_explicit", "horizontal", defaultC},
		{"vertical", "vertical", verticalC},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := baseInput()
			input["audio_duration_seconds"] = 5.0
			if tc.orientation != "" {
				input["orientation"] = tc.orientation
			}
			rp, err := Compile(context.Background(), "job-canvas", input, "/tmp/out.mp4", forbidProbe{t})
			if err != nil {
				t.Fatalf("Compile() returned unexpected error: %v", err)
			}
			if rp.Canvas != tc.want {
				t.Fatalf("want canvas %+v, got %+v", tc.want, rp.Canvas)
			}
		})
	}
}
