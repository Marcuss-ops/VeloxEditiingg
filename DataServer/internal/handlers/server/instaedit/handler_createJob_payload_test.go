package instaedit

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func postCreateJob(t *testing.T, r *gin.Engine, token string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	var reqBody []byte
	if body != nil {
		reqBody, _ = json.Marshal(body)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/instaedit/jobs", bytes.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestCreateJob_InvalidRenderSpecJSON_Returns400(t *testing.T) {
	r := setupRouter()
	token := mintToken(t, validClaims())

	body := map[string]any{
		"project_id": "proj-1",
		"render_spec": "not-json",
		"delivery_plan": map[string]any{
			"destinations": []map[string]any{
				{"external_destination_id": "ext-1"},
			},
		},
	}
	w := postCreateJob(t, r, token, body)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid JSON, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateJob_LegacyVoiceoverPathAlias_Returns422(t *testing.T) {
	r := setupRouter()
	token := mintToken(t, validClaims())

	body := map[string]any{
		"project_id": "proj-1",
		"render_spec": map[string]any{
			"video_name":    "Legacy Alias Test",
			"voiceover_path": "/audio.mp3",
		},
		"delivery_plan": map[string]any{
			"destinations": []map[string]any{
				{"external_destination_id": "ext-1"},
			},
		},
	}
	w := postCreateJob(t, r, token, body)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for legacy alias, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateJob_UnknownTopLevelKey_Returns422(t *testing.T) {
	r := setupRouter()
	token := mintToken(t, validClaims())

	body := map[string]any{
		"project_id": "proj-1",
		"render_spec": map[string]any{
			"video_name": "Unknown Key Test",
			"scenes":     []map[string]any{},
			"unknown_key": "forbidden",
		},
		"delivery_plan": map[string]any{
			"destinations": []map[string]any{
				{"external_destination_id": "ext-1"},
			},
		},
	}
	w := postCreateJob(t, r, token, body)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for unknown key, got %d: %s", w.Code, w.Body.String())
	}
}


