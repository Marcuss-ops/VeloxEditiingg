package hybrid

import (
	"context"
	"testing"
)

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
