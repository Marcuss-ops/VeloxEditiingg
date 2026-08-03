package calendar

import (
	"testing"

	"velox-server/internal/store"
)

func TestBuildCalendarJobPayloadUsesCanonicalSceneAssets(t *testing.T) {
	event := &store.CalendarEvent{
		ID:             "calendar-1",
		Title:          "Canonical calendar",
		VoiceoverPaths: []string{"velox-asset://calendar/voice.mp3"},
		InitialClips:   []store.VideoClip{{Type: "clip", URL: "velox-asset://calendar/intro.mp4", Duration: 3}},
		StockFootage:   []store.VideoClip{{Type: "stock", URL: "velox-asset://calendar/stock.mp4", Duration: 4}},
	}

	payload := buildCalendarJobPayload(event, "run-1")
	if _, ok := payload["voiceover_paths"]; ok {
		t.Fatal("calendar payload must not emit top-level voiceover_paths")
	}
	if _, ok := payload["subtitle_tracks"]; ok {
		t.Fatal("calendar payload must not emit top-level subtitle_tracks")
	}

	scenes, ok := payload["scenes"].([]map[string]interface{})
	if !ok || len(scenes) != 2 {
		t.Fatalf("scenes = %#v, want two canonical scenes", payload["scenes"])
	}
	for i, scene := range scenes {
		if _, ok := scene["duration_seconds"].(float64); !ok {
			t.Fatalf("scene %d duration_seconds missing: %#v", i, scene)
		}
		if _, ok := scene["clip"].(map[string]interface{}); !ok {
			t.Fatalf("scene %d clip missing: %#v", i, scene)
		}
		for _, legacy := range []string{"clip_link", "image_link", "voiceover_path"} {
			if _, ok := scene[legacy]; ok {
				t.Fatalf("scene %d contains legacy field %q: %#v", i, legacy, scene)
			}
		}
	}

	tracks, ok := payload["audio_tracks"].([]map[string]interface{})
	if !ok || len(tracks) != 1 {
		t.Fatalf("audio_tracks = %#v, want one global voiceover track", payload["audio_tracks"])
	}
	if tracks[0]["source_url"] != event.VoiceoverPaths[0] || tracks[0]["role"] != "voiceover" {
		t.Fatalf("audio_tracks[0] = %#v", tracks[0])
	}
}

func TestCalendarScenesDoNotInventDuration(t *testing.T) {
	event := &store.CalendarEvent{
		InitialClips: []store.VideoClip{{Type: "clip", URL: "velox-asset://calendar/unprobed.mp4"}},
	}
	if got := calendarScenes(event); len(got) != 0 {
		t.Fatalf("unmeasured clip produced scenes: %#v", got)
	}
}
