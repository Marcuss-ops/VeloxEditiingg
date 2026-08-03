package pipeline

import (
	"encoding/json"
	"testing"
)

func TestNormalizeExternalJobSubmission_PerSceneAssetsAreNotPositionCoupled(t *testing.T) {
	const (
		voice0 = "velox-asset://voice/scene-0.mp3"
		voice1 = "velox-asset://voice/scene-1.mp3"
	)

	request := SubmitJobRequest{
		IdempotencyKey: "canonical-scenes-001",
		Scenes: []SubmitScene{
			{
				Text:            "scene 0",
				SceneID:         "scene-0",
				DurationSeconds: 7,
				Clip:            &SubmitClip{URL: "velox-asset://clip/scene-0.mp4", DurationMS: 7000},
				Voiceover:       &SubmitVoiceover{URL: voice0, DurationMS: 7000},
				Subtitles:       &SubmitSubtitles{URL: "velox-asset://sub/scene-0.vtt", Format: "vtt"},
			},
			{
				Text:            "scene 1",
				SceneID:         "scene-1",
				DurationSeconds: 8,
				Clip:            &SubmitClip{URL: "velox-asset://clip/scene-1.mp4", DurationMS: 8000},
				Voiceover:       &SubmitVoiceover{URL: voice1, DurationMS: 8000},
			},
		},
	}

	canonical := (&Handlers{}).NormalizeExternalJobSubmission(request)
	if canonical == nil || canonical.WorkerPayload == nil {
		t.Fatal("canonical submission returned no worker payload")
	}
	encoded, ok := canonical.WorkerPayload["scenes_json"].(string)
	if !ok || encoded == "" {
		t.Fatalf("scenes_json missing: %#v", canonical.WorkerPayload)
	}

	var scenes []map[string]interface{}
	if err := json.Unmarshal([]byte(encoded), &scenes); err != nil {
		t.Fatalf("decode scenes_json: %v", err)
	}
	if len(scenes) != 2 {
		t.Fatalf("got %d scenes, want 2", len(scenes))
	}
	for i, wantVoice := range []string{voice0, voice1} {
		voice, ok := scenes[i]["voiceover"].(map[string]interface{})
		if !ok || voice["url"] != wantVoice {
			t.Fatalf("scene %d voiceover = %#v, want nested URL %q", i, scenes[i]["voiceover"], wantVoice)
		}
		if _, present := scenes[i]["clip_link"]; present {
			t.Fatalf("scene %d contains removed clip_link alias", i)
		}
		if _, present := scenes[i]["image_link"]; present {
			t.Fatalf("scene %d contains removed image_link alias", i)
		}
	}
	if _, present := canonical.WorkerPayload["voiceover_paths"]; present {
		t.Fatalf("worker payload contains removed positional voiceover_paths: %#v", canonical.WorkerPayload["voiceover_paths"])
	}
}

func TestNormalizeExternalJobSubmissionCanonicalSubtitlesStayPerScene(t *testing.T) {
	request := SubmitJobRequest{
		IdempotencyKey: "canonical-subtitles-001",
		Scenes: []SubmitScene{{
			Text:            "subtitle scene",
			DurationSeconds: 5,
			Subtitles:       &SubmitSubtitles{URL: "velox-asset://subtitles/scene.vtt", Format: "vtt", Language: "en"},
		}},
	}
	canonical := (&Handlers{}).NormalizeExternalJobSubmission(request)
	encoded, ok := canonical.WorkerPayload["scenes_json"].(string)
	if !ok {
		t.Fatalf("scenes_json missing: %#v", canonical.WorkerPayload)
	}
	var scenes []map[string]interface{}
	if err := json.Unmarshal([]byte(encoded), &scenes); err != nil {
		t.Fatal(err)
	}
	subtitles, ok := scenes[0]["subtitles"].(map[string]interface{})
	if !ok || subtitles["url"] != "velox-asset://subtitles/scene.vtt" || subtitles["format"] != "vtt" {
		t.Fatalf("canonical subtitles = %#v", scenes[0]["subtitles"])
	}
	if _, present := canonical.WorkerPayload["subtitle_tracks"]; present {
		t.Fatalf("worker payload contains removed subtitle_tracks")
	}
}

func TestNormalizeExternalJobSubmissionDoesNotInventSceneDuration(t *testing.T) {
	request := SubmitJobRequest{
		IdempotencyKey: "duration-explicit-001",
		Scenes: []SubmitScene{{
			Text:            "measured scene",
			DurationSeconds: 7,
			Clip:            &SubmitClip{URL: "velox-asset://clip/measured.mp4", DurationMS: 7000},
		}},
	}
	canonical := (&Handlers{}).NormalizeExternalJobSubmission(request)
	encoded := canonical.WorkerPayload["scenes_json"].(string)
	var scenes []map[string]interface{}
	if err := json.Unmarshal([]byte(encoded), &scenes); err != nil {
		t.Fatal(err)
	}
	if got := scenes[0]["duration_seconds"]; got != float64(7) {
		t.Fatalf("duration_seconds = %v, want 7", got)
	}
}
