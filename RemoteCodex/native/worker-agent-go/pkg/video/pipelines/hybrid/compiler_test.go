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

func TestCompile_VideoItemCanPreserveOriginalAudio(t *testing.T) {
	input := map[string]interface{}{
		"items": []interface{}{
			map[string]interface{}{
				"type":          "video",
				"url":           "https://example.com/clip-with-audio.mp4",
				"duration":      6.0,
				"include_audio": true,
			},
		},
	}

	plan, err := Compile(context.Background(), "job-original-audio", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile(video item with original audio): %v", err)
	}
	if got := len(plan.AudioTracks); got != 0 {
		t.Fatalf("want no external audio tracks, got %d", got)
	}
	if !plan.Timeline[0].IncludeAudio {
		t.Fatalf("want timeline item to preserve source audio")
	}
}

func TestCompile_VoiceoverBedAlwaysSuppressesSourceAudio(t *testing.T) {
	input := map[string]interface{}{
		"items": []interface{}{
			map[string]interface{}{
				"type":          "video",
				"url":           "https://example.com/short-stock.mp4",
				"duration":      30.0,
				"role":          "voiceover_bed",
				"include_audio": true,
			},
		},
		"audio_tracks": []interface{}{
			map[string]interface{}{
				"source_url":       "https://example.com/voiceover.mp3",
				"duration_seconds": 30.0,
				"role":             "voiceover",
			},
		},
	}

	plan, err := Compile(context.Background(), "job-voiceover-bed", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile(voiceover bed): %v", err)
	}
	if plan.Timeline[0].IncludeAudio {
		t.Fatal("voiceover_bed must not preserve source audio")
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

func TestCompile_SFXTrackPreservesRoleVolumeAndOffset(t *testing.T) {
	input := map[string]interface{}{
		"items": []interface{}{map[string]interface{}{
			"type":     "video",
			"url":      "worker-local/clip.mp4",
			"duration": 4.0,
		}},
		"audio_tracks": []interface{}{map[string]interface{}{
			"source_url":        "worker-local/effect.mp3",
			"role":              "sfx",
			"volume":            0.1,
			"start_time_offset": 2.5,
		}},
	}

	renderPlan, err := Compile(context.Background(), "job-sfx", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile(sfx): %v", err)
	}
	if len(renderPlan.AudioTracks) != 1 {
		t.Fatalf("audio tracks = %d, want 1", len(renderPlan.AudioTracks))
	}
	track := renderPlan.AudioTracks[0]
	if track.Role != "sfx" || track.Volume != 0.1 || track.StartTimeOffset != 2.5 {
		t.Fatalf("sfx track = %+v, want role=sfx volume=0.1 offset=2.5", track)
	}
}

func TestCompile_BackgroundMusic_RoleEnablesLoopFadeDucking(t *testing.T) {
	input := map[string]interface{}{
		"items": []interface{}{
			map[string]interface{}{
				"type":     "video",
				"url":      "https://example.com/clip.mp4",
				"duration": 12.0,
			},
		},
		"audio_tracks": []interface{}{
			map[string]interface{}{
				"source_url": "https://example.com/bgm.mp3",
				"volume":     0.15,
				"role":       "background_music",
			},
		},
	}

	plan, err := Compile(context.Background(), "job-bgm", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile(background_music): %v", err)
	}
	if got := len(plan.AudioTracks); got != 1 {
		t.Fatalf("want 1 audio track, got %d", got)
	}

	track := plan.AudioTracks[0]
	if track.Role != "background_music" {
		t.Errorf("role = %q, want background_music", track.Role)
	}
	if !track.Loop {
		t.Error("background_music track: Loop should be true")
	}
	if track.FadeInSeconds != 0.5 {
		t.Errorf("FadeInSeconds = %v, want 0.5", track.FadeInSeconds)
	}
	if track.FadeOutSeconds != 0.5 {
		t.Errorf("FadeOutSeconds = %v, want 0.5", track.FadeOutSeconds)
	}
	if !track.DuckingEnabled {
		t.Error("background_music track: DuckingEnabled should be true")
	}
	if track.Volume != 0.15 {
		t.Errorf("Volume = %v, want 0.15", track.Volume)
	}
}

func TestCompile_BackgroundMusic_WithoutDurationDefersToEngineVideoDuration(t *testing.T) {
	input := map[string]interface{}{
		"items": []interface{}{
			map[string]interface{}{
				"type":     "video",
				"url":      "https://example.com/video.mp4",
				"duration": 85.0,
			},
		},
		"audio_tracks": []interface{}{
			map[string]interface{}{
				"source_url": "https://example.com/music.mp3",
				"role":       "background_music",
				// Deliberately omit duration_seconds. The C++ engine owns
				// the final frame-accurate video duration and must bind the
				// loop to that value rather than accepting an infinite mix.
			},
		},
	}

	renderPlan, err := Compile(context.Background(), "job-bgm-implicit-duration", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile(background music without duration): %v", err)
	}
	if len(renderPlan.AudioTracks) != 1 {
		t.Fatalf("audio tracks = %d, want 1", len(renderPlan.AudioTracks))
	}
	track := renderPlan.AudioTracks[0]
	if !track.Loop {
		t.Fatal("background music without explicit config must be looped")
	}
	if track.DurationSeconds != 0 {
		t.Fatalf("DurationSeconds = %v, want 0 so engine applies final video duration", track.DurationSeconds)
	}
	if got := renderPlan.Timeline[0].DurationSeconds; got != 85.0 {
		t.Fatalf("video duration = %v, want 85", got)
	}
}

func TestCompile_VoiceoverRole_NoAutoLoopFadeDucking(t *testing.T) {
	input := map[string]interface{}{
		"items": []interface{}{
			map[string]interface{}{
				"type":     "video",
				"url":      "https://example.com/clip.mp4",
				"duration": 12.0,
			},
		},
		"audio_tracks": []interface{}{
			map[string]interface{}{
				"source_url": "https://example.com/voiceover.mp3",
				"volume":     1.0,
				"role":       "voiceover",
			},
		},
	}

	plan, err := Compile(context.Background(), "job-vo", input, "/tmp/out.mp4", nil)
	if err != nil {
		t.Fatalf("Compile(voiceover): %v", err)
	}
	if got := len(plan.AudioTracks); got != 1 {
		t.Fatalf("want 1 audio track, got %d", got)
	}

	track := plan.AudioTracks[0]
	if track.Loop {
		t.Error("voiceover track: Loop should be false (no auto-enable for non-bgm roles)")
	}
	if track.FadeInSeconds != 0 {
		t.Errorf("voiceover track: FadeInSeconds should be 0, got %v", track.FadeInSeconds)
	}
	if track.DuckingEnabled {
		t.Error("voiceover track: DuckingEnabled should be false")
	}
}
