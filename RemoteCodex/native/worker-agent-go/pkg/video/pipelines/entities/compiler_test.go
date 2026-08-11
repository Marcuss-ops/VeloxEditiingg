package entities

import (
	"context"
	"testing"

	"velox-worker-agent/pkg/video/plan"
)

// fakeProbe satisfies audio.Probe so Compile() can be exercised
// without spawning the real audio service.
type fakeProbe struct{ dur float64 }

func (f fakeProbe) DurationSeconds(url string) float64 { return f.dur }

// baseInput returns the minimal valid input map for Compile: script +
// audio_url present, output_format deliberately omitted so the
// default-empty path is exercised. Tests override fields on the
// returned map as needed.
func baseInput() map[string]interface{} {
	return map[string]interface{}{
		"script":    "hello world",
		"audio_url": "https://example.test/audio.mp3",
	}
}

// TestParseRequest_OutputFormat_Empty pins the regression-fixed
// semantic from closure commit 59ba4eb: when input has no
// "output_format" key, parseRequest must return OutputFormat == "" —
// NOT "youtube". This prevents a future contributor from silently
// reverting the empty-default and leaking the removed YouTube
// platform string back into worker-agent runtime.
func TestParseRequest_OutputFormat_Empty(t *testing.T) {
	req := parseRequest(baseInput())
	if req.OutputFormat != "" {
		t.Fatalf("want OutputFormat \"\" when key is absent, got %q", req.OutputFormat)
	}
}

// TestParseRequest_OutputFormat_Overrides verifies that an explicit
// "output_format" key flows verbatim into Request.OutputFormat for
// every supported value:
//
//	"youtube"   — legacy default; kept for back-compat with jobs created
//	              before the 59ba4eb closure.
//	"tiktok"    — vertical social platform, drives VerticalCanvas.
//	"instagram" — vertical social platform, drives VerticalCanvas.
//	""          — explicit empty string is equivalent to omitting the
//	              key (toString treats both as "").
func TestParseRequest_OutputFormat_Overrides(t *testing.T) {
	cases := []struct {
		name string
		val  string
	}{
		{"youtube", "youtube"},
		{"tiktok", "tiktok"},
		{"instagram", "instagram"},
		{"explicit_empty", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := baseInput()
			input["output_format"] = tc.val
			req := parseRequest(input)
			if req.OutputFormat != tc.val {
				t.Fatalf("want OutputFormat %q, got %q", tc.val, req.OutputFormat)
			}
		})
	}
}

// TestCompile_Canvas_Switch pins the Compile() canvas-selection
// semantic so the empty-default cannot regress:
//
//	""          → DefaultCanvas (the 59ba4eb closure fix)
//	"youtube"   → DefaultCanvas (legacy default fell through here)
//	"tiktok"    → VerticalCanvas
//	"instagram" → VerticalCanvas
//
// Compile() must NEVER panic, return an error, or pick VerticalCanvas
// for an empty OutputFormat. That misbehavior is exactly the regression
// the empty-default was introduced to prevent.
func TestCompile_Canvas_Switch(t *testing.T) {
	defaultC := plan.DefaultCanvas()
	verticalC := plan.VerticalCanvas()

	cases := []struct {
		name   string
		format string
		want   plan.CanvasSpec
	}{
		{"empty_default_uses_landscape_canvas", "", defaultC},
		{"youtube_legacy_uses_landscape_canvas", "youtube", defaultC},
		{"tiktok_uses_vertical_canvas", "tiktok", verticalC},
		{"instagram_uses_vertical_canvas", "instagram", verticalC},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := baseInput()
			if tc.format != "" {
				input["output_format"] = tc.format
			}
			rp, err := Compile(context.Background(), "job-test", input, "/tmp/out.mp4", fakeProbe{dur: 5.0})
			if err != nil {
				t.Fatalf("Compile() returned unexpected error: %v", err)
			}
			if rp == nil {
				t.Fatalf("Compile() returned nil RenderPlan")
			}
			if rp.Canvas != tc.want {
				t.Fatalf("want canvas %+v, got %+v", tc.want, rp.Canvas)
			}
		})
	}
}

// TestParseRequest_EntityStyle_Default pins the sibling default for
// the same toStringDefault helper that fix 59ba4eb relies on. It is a
// minimal smoke coverage so the helper's behavior cannot drift on
// either OutputFormat or EntityStyle.
func TestParseRequest_EntityStyle_Default(t *testing.T) {
	req := parseRequest(baseInput())
	if req.EntityStyle != "full_screen" {
		t.Fatalf("want EntityStyle \"full_screen\", got %q", req.EntityStyle)
	}
}

// forbidProbe fails the test if DurationSeconds is ever invoked. It
// pins the Fase C1/D migration: when the upstream declares the audio
// duration (compiled render plan / asset-registry stamping), the
// compiler MUST consume it and MUST NOT spawn ffprobe on the input.
type forbidProbe struct{ t *testing.T }

func (p forbidProbe) DurationSeconds(url string) float64 {
	p.t.Fatalf("probe.DurationSeconds(%q) must not be called when audio duration is declared", url)
	return 0
}

// TestCompile_DeclaredAudioDuration_SkipsProbe verifies that a
// declared audio_duration_seconds (the Fase D compiled-plan / C1
// registry field) is consumed instead of the probe, and that the
// produced timeline is exactly audio-length (no trailing fill after
// the declared end).
func TestCompile_DeclaredAudioDuration_SkipsProbe(t *testing.T) {
	input := baseInput()
	input["audio_duration_seconds"] = 12.0
	input["entities"] = []interface{}{
		map[string]interface{}{"name": "e1", "image_url": "https://example.test/a.png", "start": 1.0, "end": 5.0},
	}

	rp, err := Compile(context.Background(), "job-declared", input, "/tmp/out.mp4", forbidProbe{t})
	if err != nil {
		t.Fatalf("Compile() returned unexpected error: %v", err)
	}
	if rp == nil {
		t.Fatal("Compile() returned nil RenderPlan")
	}
	// Entity 5s + trailing fill 12-5=7s → total timeline == declared audio.
	var total float64
	for _, item := range rp.Timeline {
		total += item.DurationSeconds
	}
	if total != 12.0 {
		t.Fatalf("timeline total = %v, want declared 12.0 (probe must not have run)", total)
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

// TestCompile_NoDeclaredDuration_FallsBackToProbe pins the fallback:
// a payload without a declared duration still probes (legacy/deferred
// references remain supported).
func TestCompile_NoDeclaredDuration_FallsBackToProbe(t *testing.T) {
	input := baseInput()
	rp, err := Compile(context.Background(), "job-probe", input, "/tmp/out.mp4", fakeProbe{dur: 7.0})
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
	if total != 7.0 {
		t.Fatalf("timeline total = %v, want probed 7.0", total)
	}
}
