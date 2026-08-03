package pipeline

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSubmitJobRequestCanonicalSceneAssetsRoundtrip(t *testing.T) {
	input := SubmitJobRequest{
		IdempotencyKey: "canonical-assets-001",
		Scenes: []SubmitScene{{
			Text:            "scene",
			DurationSeconds: 5,
			Clip:            &SubmitClip{URL: "velox-asset://clip/1"},
			Voiceover:       &SubmitVoiceover{URL: "velox-asset://voice/1", DurationMS: 5000},
			Subtitles:       &SubmitSubtitles{URL: "velox-asset://subtitle/1", Format: "vtt"},
		}},
	}
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	encoded := string(data)
	for _, legacy := range []string{"voiceover_paths", "subtitle_tracks", "clip_link", "image_link"} {
		if strings.Contains(encoded, legacy) {
			t.Fatalf("canonical request emitted legacy field %q: %s", legacy, encoded)
		}
	}
	var roundtrip SubmitJobRequest
	if err := json.Unmarshal(data, &roundtrip); err != nil {
		t.Fatal(err)
	}
	if roundtrip.Scenes[0].Clip == nil || roundtrip.Scenes[0].Clip.URL != "velox-asset://clip/1" {
		t.Fatalf("clip roundtrip: %#v", roundtrip.Scenes[0].Clip)
	}
	if roundtrip.Scenes[0].Voiceover == nil || roundtrip.Scenes[0].Voiceover.URL != "velox-asset://voice/1" {
		t.Fatalf("voiceover roundtrip: %#v", roundtrip.Scenes[0].Voiceover)
	}
	if roundtrip.Scenes[0].Subtitles == nil || roundtrip.Scenes[0].Subtitles.Format != "vtt" {
		t.Fatalf("subtitles roundtrip: %#v", roundtrip.Scenes[0].Subtitles)
	}
}
