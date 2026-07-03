package enqueue

import (
	"strings"
	"testing"
)

func TestBuildNarratedClipPayload_UsesRealVoiceoverDurationNotScenePlaceholder(t *testing.T) {
	t.Parallel()

	scenes := []map[string]interface{}{
		{
			"text":                        "Jackie Chan scene",
			"stock_link":                  "https://example.com/stock.mp4",
			"clip_link":                   "https://example.com/final.mp4",
			"voiceover_link":              "https://example.com/voice.mp3",
			"duration_seconds":            99.0,
			"final_clip_duration_seconds": 2.5,
		},
	}

	entries, items, _, tracks, mode, err := buildNarratedClipPayloadWithDurationProbe(scenes, func(url string) float64 {
		if url != "https://example.com/voice.mp3" {
			t.Fatalf("unexpected probe URL: %q", url)
		}
		return 7.25
	})
	if err != nil {
		t.Fatalf("buildNarratedClipPayloadWithDurationProbe: %v", err)
	}
	if mode != "clip_stock" {
		t.Fatalf("mode = %q, want clip_stock", mode)
	}
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2", len(items))
	}
	assertDuration(t, items[0]["duration"], 7.25, "voiceover bed duration")
	assertDuration(t, items[1]["duration"], 2.5, "final clip duration")
	assertDuration(t, entries[0]["voiceover_duration_seconds"], 7.25, "canonical voiceover duration")
	assertDuration(t, entries[0]["final_clip_duration_seconds"], 2.5, "canonical final clip duration")
	assertDuration(t, entries[0]["duration_seconds"], 9.75, "total scene duration")
	if len(tracks) != 1 {
		t.Fatalf("audio tracks = %d, want 1", len(tracks))
	}
	assertDuration(t, tracks[0]["start_time_offset"], 0, "first audio offset")
}

func TestBuildNarratedClipPayload_MixedScenesKeepCanonicalOffsets(t *testing.T) {
	t.Parallel()

	scenes := []map[string]interface{}{
		{
			"stock_link":                  "stock-1.mp4",
			"clip_link":                   "final-1.mp4",
			"voiceover_link":              "voice-1.mp3",
			"voiceover_duration_seconds":  5.0,
			"final_clip_duration_seconds": 2.0,
		},
		{
			"clip_link":                   "final-2.mp4",
			"duration_seconds":            999.0,
			"final_clip_duration_seconds": 3.0,
		},
		{
			"stock_link":                  "stock-3.mp4",
			"clip_link":                   "final-3.mp4",
			"voiceover_link":              "voice-3.mp3",
			"voiceover_duration_seconds":  4.0,
			"final_clip_duration_seconds": 1.0,
		},
	}

	entries, items, _, tracks, _, err := buildNarratedClipPayloadWithDurationProbe(scenes, func(string) float64 {
		t.Fatal("probe must not run when explicit voiceover duration is present")
		return 0
	})
	if err != nil {
		t.Fatalf("build narrated payload: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3", len(entries))
	}
	if len(items) != 5 {
		t.Fatalf("items = %d, want 5 (2 + 1 + 2)", len(items))
	}
	if role, _ := items[2]["role"].(string); role != "scene_clip" {
		t.Fatalf("voiceover-free scene emitted role %q, want scene_clip", role)
	}
	if len(tracks) != 2 {
		t.Fatalf("audio tracks = %d, want 2", len(tracks))
	}
	assertDuration(t, tracks[0]["start_time_offset"], 0, "first audio offset")
	assertDuration(t, tracks[1]["start_time_offset"], 10, "third-scene audio offset")
	assertDuration(t, entries[1]["voiceover_duration_seconds"], 0, "missing voiceover duration")
	assertDuration(t, entries[1]["duration_seconds"], 3, "voiceover-free total duration")
}

func TestBuildNarratedClipPayload_ExplicitDurationWinsWithoutProbe(t *testing.T) {
	t.Parallel()

	scenes := []map[string]interface{}{
		{
			"clip_link":                   "final.mp4",
			"voiceover_link":              "voice.mp3",
			"voiceover_duration_seconds":  6.5,
			"final_clip_duration_seconds": 0.25,
			"duration_seconds":            500.0,
		},
	}

	_, items, _, tracks, _, err := buildNarratedClipPayloadWithDurationProbe(scenes, func(string) float64 {
		t.Fatal("probe must not run for explicit voiceover_duration_seconds")
		return 0
	})
	if err != nil {
		t.Fatalf("build narrated payload: %v", err)
	}
	assertDuration(t, items[0]["duration"], 6.5, "explicit voiceover duration")
	assertDuration(t, items[1]["duration"], 0.25, "short final clip duration")
	assertDuration(t, tracks[0]["start_time_offset"], 0, "audio offset")
}

func TestBuildNarratedClipPayload_RejectsUnmeasurableVoiceover(t *testing.T) {
	t.Parallel()

	scenes := []map[string]interface{}{
		{
			"clip_link":        "final.mp4",
			"voiceover_link":   "missing.mp3",
			"duration_seconds": 42.0,
		},
	}

	_, _, _, _, _, err := buildNarratedClipPayloadWithDurationProbe(scenes, func(string) float64 { return 0 })
	if err == nil {
		t.Fatal("expected an error for an unmeasurable voiceover")
	}
	if !strings.Contains(err.Error(), "voiceover duration unavailable") {
		t.Fatalf("error = %q, want voiceover duration failure", err)
	}
	if !strings.Contains(err.Error(), "scenes[0]") {
		t.Fatalf("error = %q, want scene index", err)
	}
}

func TestResolveSceneFinalClipDuration_HasUnambiguousFallbacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		scene map[string]interface{}
		want  float64
	}{
		{
			name: "canonical_long_clip",
			scene: map[string]interface{}{
				"final_clip_duration_seconds": 18.75,
				"clip_duration_seconds":       8.0,
				"duration_seconds":            99.0,
			},
			want: 18.75,
		},
		{
			name: "legacy_alias",
			scene: map[string]interface{}{
				"clip_duration_seconds": 1.25,
				"duration_seconds":      99.0,
			},
			want: 1.25,
		},
		{
			name: "generic_duration_is_not_a_clip_fallback",
			scene: map[string]interface{}{
				"duration_seconds": 99.0,
			},
			want: 4.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assertDuration(t, resolveSceneFinalClipDuration(tt.scene), tt.want, tt.name)
		})
	}
}

func assertDuration(t *testing.T, got interface{}, want float64, label string) {
	t.Helper()
	value, ok := got.(float64)
	if !ok {
		t.Fatalf("%s: got %T(%v), want float64(%v)", label, got, got, want)
	}
	if value != want {
		t.Fatalf("%s: got %v, want %v", label, value, want)
	}
}
