package script

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	jobenqueue "velox-server/internal/jobs/enqueue"
	"velox-server/internal/store"
)

func newUnifiedGenerateTestRouter(t *testing.T) *gin.Engine {
	t.Helper()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "velox.db"))
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	seedDestinationMain(t, db)
	atomic := store.NewAtomicJobTaskCreator(db)
	enqueuer := jobenqueue.NewEnqueuer(atomic, nil, nil, noopPlanResolver{})
	cfg := &config.Config{Runtime: config.RuntimeConfig{
		DataDir:   tempDir,
		VideosDir: filepath.Join(tempDir, "videos"),
	}}
	r := gin.New()
	RegisterRoutes(r.Group("/api/script"), cfg, db, enqueuer)
	return r
}

func TestUnifiedGenerateRejectsMissingSource(t *testing.T) {
	r := newUnifiedGenerateTestRouter(t)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/script/generate", bytes.NewBufferString(`{"video_name":"missing source"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestUnifiedGenerateRejectsUnsupportedSourceType(t *testing.T) {
	r := newUnifiedGenerateTestRouter(t)
	body, _ := json.Marshal(map[string]any{
		"video_name": "unsupported",
		"source": map[string]any{"type": "unknown"},
	})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/script/generate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestLegacyGenerateFromClipsRouteIsRemoved(t *testing.T) {
	r := newUnifiedGenerateTestRouter(t)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/script/generate-from-clips", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("want 404 for retired route, got %d body=%s", w.Code, w.Body.String())
	}
}
