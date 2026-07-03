package hybrid

import (
	"context"
	"testing"
)

// TestRenderPlan_HonorsVoiceoverBedAndSceneClipRoles is the TDD-red contract
// for the role-aware compile of the hybrid.v1 pipeline. The expected behavior:
//
//   - Each scene in the input payload contributes TWO timeline items, in
//     order: first a `voiceover_bed` segment sourced from the stock clip
//     with `voiceover_duration_seconds` as DurationSeconds, then a
//     `scene_clip` segment sourced from the final clip with
//     `final_clip_duration_seconds` as DurationSeconds.
//   - The RenderPlan.Timeline therefore alternates [bed_i, clip_i] per scene.
//
// This test is INTENTIONALLY RED against the current compiler: the
// hybrid.Compile() pipeline does not yet read `role`, `voiceover_duration_seconds`
// or `final_clip_duration_seconds` from the input map. It will (a) fall back
// to the default `duration` of 4.0s for every item and (b) treat every
// `role=voiceover_bed` item as a regular `image`-typed source. The
// assertions below therefore fail until the compiler learns to honor
// the role contract. Do NOT modify the compiler as part of this test:
// the failure is the spec.
func TestRenderPlan_HonorsVoiceoverBedAndSceneClipRoles(t *testing.T) {
	input := map[string]interface{}{
		"items": []interface{}{
			// Scene 1
			map[string]interface{}{
				"role":                       "voiceover_bed",
				"url":                        "https://example.com/stock-1.mp4",
				"voiceover_duration_seconds": 6.0,
			},
			map[string]interface{}{
				"role":                        "scene_clip",
				"url":                         "https://example.com/clip-1.mp4",
				"final_clip_duration_seconds": 2.0,
			},
			// Scene 2
			map[string]interface{}{
				"role":                       "voiceover_bed",
				"url":                        "https://example.com/stock-2.mp4",
				"voiceover_duration_seconds": 6.0,
			},
			map[string]interface{}{
				"role":                        "scene_clip",
				"url":                         "https://example.com/clip-2.mp4",
				"final_clip_duration_seconds": 2.0,
			},
		},
		"voiceover_url": "https://example.com/voiceover-shared.mp3",
	}

	rp, err := Compile(context.Background(), "job-renderplan-roles", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile(role-aware payload): %v", err)
	}
	if got := len(rp.Timeline); got != 4 {
		t.Fatalf("Timeline len: want 4, got %d (compiler is not honoring the per-scene split)", got)
	}

	expected := []struct {
		role     string
		url      string
		duration float64
	}{
		{"voiceover_bed", "https://example.com/stock-1.mp4", 6.0},
		{"scene_clip", "https://example.com/clip-1.mp4", 2.0},
		{"voiceover_bed", "https://example.com/stock-2.mp4", 6.0},
		{"scene_clip", "https://example.com/clip-2.mp4", 2.0},
	}
	for i, want := range expected {
		if got := rp.Timeline[i].Source.URL; got != want.url {
			t.Errorf("Timeline[%d].Source.URL: want %q, got %q", i, want.url, got)
		}
		if got := rp.Timeline[i].DurationSeconds; got != want.duration {
			t.Errorf("Timeline[%d].DurationSeconds: want %v (from %s contract), got %v", i, want.duration, want.role, got)
		}
	}

	// Audio track invariant: the shared voiceover URL should be present.
	if got := len(rp.AudioTracks); got != 1 {
		t.Fatalf("AudioTracks len: want 1 (the shared voiceover), got %d", got)
	}
	if got := rp.AudioTracks[0].SourceURL; got != "https://example.com/voiceover-shared.mp3" {
		t.Errorf("AudioTracks[0].SourceURL: want %q, got %q", "https://example.com/voiceover-shared.mp3", got)
	}
}

func TestValidate_AllowsItemsWithoutAudio(t *testing.T) {
	input := map[string]interface{}{
		"items": []interface{}{
			map[string]interface{}{
				"type":     "video",
				"url":      "https://example.com/clip.mp4",
				"duration": 6.0,
			},
		},
	}

	if err := Validate(input); err != nil {
		t.Fatalf("Validate(items without audio): %v", err)
	}
}

func TestCompile_ItemsWithoutAudio_ProducesSilentTimeline(t *testing.T) {
	input := map[string]interface{}{
		"items": []interface{}{
			map[string]interface{}{
				"type":     "video",
				"url":      "https://example.com/clip.mp4",
				"duration": 6.0,
				"fit":      "contain",
			},
		},
	}

	plan, err := Compile(context.Background(), "job-1", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile(items without audio): %v", err)
	}
	if got := len(plan.Timeline); got != 1 {
		t.Fatalf("want 1 timeline item, got %d", got)
	}
	if got := len(plan.AudioTracks); got != 0 {
		t.Fatalf("want 0 audio tracks, got %d", got)
	}
	if got := plan.Timeline[0].Source.URL; got != "https://example.com/clip.mp4" {
		t.Fatalf("want clip url preserved, got %q", got)
	}
	if got := plan.Timeline[0].DurationSeconds; got != 6.0 {
		t.Fatalf("want duration 6.0, got %v", got)
	}
}

func TestCompile_AudioTracks_ProducesOffsetMixPlan(t *testing.T) {
	input := map[string]interface{}{
		"items": []interface{}{
			map[string]interface{}{
				"type":     "video",
				"url":      "https://example.com/stock-1.mp4",
				"duration": 3.5,
				"fit":      "contain",
			},
			map[string]interface{}{
				"type":     "video",
				"url":      "https://example.com/clip-1.mp4",
				"duration": 4.0,
				"fit":      "contain",
			},
		},
		"audio_tracks": []interface{}{
			map[string]interface{}{
				"source_url":        "https://example.com/voice-1.mp3",
				"volume":            1.0,
				"start_time_offset": 0.0,
			},
			map[string]interface{}{
				"source_url":        "https://example.com/voice-2.mp3",
				"volume":            0.8,
				"start_time_offset": 7.5,
			},
		},
	}

	plan, err := Compile(context.Background(), "job-2", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile(audio_tracks): %v", err)
	}
	if got := len(plan.AudioTracks); got != 2 {
		t.Fatalf("want 2 audio tracks, got %d", got)
	}
	if got := plan.AudioTracks[0].SourceURL; got != "https://example.com/voice-1.mp3" {
		t.Fatalf("want first audio url preserved, got %q", got)
	}
	if got := plan.AudioTracks[1].Volume; got != 0.8 {
		t.Fatalf("want second audio volume 0.8, got %v", got)
	}
	if got := plan.AudioTracks[1].StartTimeOffset; got != 7.5 {
		t.Fatalf("want second audio start offset 7.5, got %v", got)
	}
}
