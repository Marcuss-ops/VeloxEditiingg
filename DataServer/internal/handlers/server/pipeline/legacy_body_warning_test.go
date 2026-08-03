package pipeline

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestSubmitJobRejectsRemovedLegacyFields(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name string
		body string
	}{
		{
			name: "voiceover_paths",
			body: `{"idempotency_key":"legacy-voiceover","voiceover_paths":["velox-asset://voice.mp3"],"scenes":[{"text":"scene","duration_seconds":5}]}`,
		},
		{
			name: "subtitle_tracks",
			body: `{"idempotency_key":"legacy-subtitles","subtitle_tracks":[{"source":"velox-asset://sub.vtt"}],"scenes":[{"text":"scene","duration_seconds":5}]}`,
		},
		{
			name: "scene_clip_link",
			body: `{"idempotency_key":"legacy-clip","scenes":[{"text":"scene","duration_seconds":5,"clip_link":"velox-asset://clip.mp4"}]}`,
		},
		{
			name: "scene_image_link",
			body: `{"idempotency_key":"legacy-image","scenes":[{"text":"scene","duration_seconds":5,"image_link":"velox-asset://image.jpg"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			router.POST("/api/v1/jobs", (&Handlers{}).SubmitJob())

			w := httptest.NewRecorder()
			req, err := http.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewBufferString(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%s", w.Code, http.StatusBadRequest, w.Body.String())
			}
			var response map[string]interface{}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatalf("response is not JSON: %v", err)
			}
			if response["error"] != "invalid_json" {
				t.Fatalf("error = %v, want invalid_json", response["error"])
			}
		})
	}
}

func TestCanonicalSceneFieldsDoNotEmitRemovedAliases(t *testing.T) {
	request := SubmitJobRequest{
		IdempotencyKey: "canonical-scene",
		Scenes: []SubmitScene{{
			Text:            "scene",
			DurationSeconds: 5,
			Clip:            &SubmitClip{URL: "velox-asset://clip.mp4"},
			Voiceover:       &SubmitVoiceover{URL: "velox-asset://voice.mp3"},
			Subtitles:       &SubmitSubtitles{URL: "velox-asset://sub.vtt", Format: "vtt"},
		}},
	}

	payload := submitRequestToRawPayload(&request)
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, removed := range []string{"voiceover_paths", "subtitle_tracks", "clip_link", "image_link"} {
		if bytes.Contains(encoded, []byte(`"`+removed+`"`)) {
			t.Fatalf("canonical payload contains removed field %q: %s", removed, encoded)
		}
	}
}
