package script

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	"velox-server/internal/jobs"
	jobenqueue "velox-server/internal/jobs/enqueue"
	"velox-server/internal/platform/clock"
	"velox-server/internal/store"
)

// PR-04.5 + PR #8: job creation is now canonical through AtomicJobTaskCreator.
// The legacy testSubmitQueue adapter was removed after Create was dropped
// from jobs.Writer.

func TestGenerateWithImages_EnqueuesSceneImageJob(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "velox.db")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.NewSQLiteJobRepository(db)
	atomic := store.NewAtomicJobTaskCreator(db)

	cfg := &config.Config{
		Runtime: config.RuntimeConfig{
			DataDir:   tempDir,
			VideosDir: filepath.Join(tempDir, "videos"),
		},
		Database: config.DatabaseConfig{
			DBPath: dbPath,
		},
	}

	// Voiceover nil: this test exercises the basic enqueue path, no asset rewrite.
	enqueuer := jobenqueue.NewEnqueuer(atomic, nil, nil)

	r := gin.New()
	RegisterRoutes(r.Group("/api/script"), cfg, db, enqueuer)

	payload := map[string]interface{}{
		"video_name":          "Amish",
		"source_text":         "Se vuoi un test del flusso immagini+audio, questo payload usa gli esempi del messaggio.",
		"language":            "it",
		"voiceover_path":      "https://drive.google.com/file/d/17zAf__wEHsq6Wcs8Oguy7P9Ky_kH2CtV/view?usp=drive_link",
		"drive_output_folder": "https://drive.google.com/drive/u/1/folders/1W4k13-sjPCr1Lynu29D3UJSGRPFSoHal",
		"scenes": []interface{}{
			map[string]interface{}{
				"text":       "Se vi dicessi che esiste un angolo del mondo dove il tempo non è semplicemente rallentato.",
				"image_link": "https://drive.google.com/file/d/1b_bKMz0SCgIbOo_-Z5PN44DOBrFquPFM/view",
				"image_links": []interface{}{
					"https://drive.google.com/file/d/1b_bKMz0SCgIbOo_-Z5PN44DOBrFquPFM/view",
				},
			},
			map[string]interface{}{
				"text":       "Stiamo parlando degli Amish.",
				"image_link": "https://drive.google.com/file/d/1pZvMEF12yJgQ0trh8maIndU7JQnBGrkk/view",
				"image_links": []interface{}{
					"https://drive.google.com/file/d/1pZvMEF12yJgQ0trh8maIndU7JQnBGrkk/view",
				},
			},
			map[string]interface{}{
				"text":       "Il loro mondo è definito da una gerarchia sociale rigorosa e da regole non scritte.",
				"image_link": "",
			},
		},
	}
	body, _ := json.Marshal(payload)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/script/generate-with-images", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "127.0.0.1:12345"
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}

	var res map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	jobID, _ := res["job_id"].(string)
	if jobID == "" {
		t.Fatalf("expected non-empty job_id, got %#v", res["job_id"])
	}
	if got := res["video_mode"]; got != scriptSceneMode {
		t.Fatalf("want video_mode %q, got %v", scriptSceneMode, got)
	}
	if got := res["status"]; got != "PENDING" {
		t.Fatalf("want status PENDING, got %v", got)
	}
	if got := res["scene_count"]; got != float64(3) {
		t.Fatalf("want 3 scenes, got %v", got)
	}

	rawJob, err := db.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	// request_json is stored as a JSON STRING column in SQLiteJobRepository;
	// store.GetJob returns the raw column value. Parse it before reading top-level keys.
	var videoMode string
	if reqStr, ok := rawJob["request_json"].(string); ok && reqStr != "" {
		var req map[string]interface{}
		if err := json.Unmarshal([]byte(reqStr), &req); err == nil {
			videoMode, _ = req["video_mode"].(string)
		}
	}
	if videoMode != scriptSceneMode {
		t.Fatalf("want persisted video_mode %q, got %v", scriptSceneMode, videoMode)
	}
	if got := rawJob["scenes_json"]; got == "" {
		t.Fatalf("want scenes_json persisted, got empty")
	}
	if got := rawJob["stock_clip_paths"]; got != nil {
		if arr, ok := got.([]interface{}); ok && len(arr) > 0 {
			t.Fatalf("want no stock clip paths for scene_image jobs, got %v", got)
		}
	}

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/script/jobs/"+jobID, nil)
	req.RemoteAddr = "127.0.0.1:12345"
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status endpoint want 200, got %d body=%s", w.Code, w.Body.String())
	}
	var statusRes map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &statusRes); err != nil {
		t.Fatalf("decode status response: %v", err)
	}
	if statusRes["ok"] != true {
		t.Fatalf("expected ok response, got %v", statusRes["ok"])
	}
	if statusRes["job_id"] != jobID {
		t.Fatalf("expected job_id %s, got %v", jobID, statusRes["job_id"])
	}
}

func TestGenerateWithImages_UsesCreatorStageWhenConfigured(t *testing.T) {
	tempDir := t.TempDir()
	assetDBPath := filepath.Join(tempDir, "assets.db")
	assetDB, err := store.NewSQLiteStore(assetDBPath)
	if err != nil {
		t.Fatalf("new asset sqlite store: %v", err)
	}
	t.Cleanup(func() {
		_ = assetDB.Close()
	})
	assetRepo := store.NewSQLiteAssetRepository(assetDB)
	assetBlobStore := store.NewNopBlobStore(tempDir)
	assetStore := voiceoverassets.NewStore(tempDir, 0, []string{tempDir})
	assetRegistry := voiceoverassets.NewResolverRegistry(voiceoverassets.NewTypedResolversFromStore(assetStore, nil, nil)...)
	voiceoverSvc := voiceoverassets.NewAssetService(assetRepo, assetBlobStore, assetRegistry, clock.System{})

	dbPath := filepath.Join(tempDir, "velox.db")
	voicePath := filepath.Join(tempDir, "voice.mp3")
	imagePath := filepath.Join(tempDir, "scene1.png")
	if err := os.WriteFile(voicePath, []byte("voice"), 0o644); err != nil {
		t.Fatalf("write voice: %v", err)
	}
	if err := os.WriteFile(imagePath, []byte("image"), 0o644); err != nil {
		t.Fatalf("write image: %v", err)
	}
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	atomic := store.NewAtomicJobTaskCreator(db)

	mockCreator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/script/generate-with-images" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		var incoming map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&incoming); err != nil {
			t.Fatalf("decode incoming payload: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"ok":       true,
			"status":   "completed",
			"trace_id": "creator-trace-1",
			"result": map[string]interface{}{
				"title":       "Creator Video",
				"script_text": "Creator generated script",
				"scenes_json": `[
					{"text":"Scene 1","image_link":"` + imagePath + `","duration_seconds":4}
				]`,
				"voiceover_path": voicePath,
				"youtube_group":  "amish",
			},
		})
	}))
	defer mockCreator.Close()

	cfg := &config.Config{
		Runtime: config.RuntimeConfig{
			DataDir:   tempDir,
			VideosDir: filepath.Join(tempDir, "videos"),
		},
		Database: config.DatabaseConfig{
			DBPath: dbPath,
		},
		Render: config.RenderConfig{
			RemoteEngineURL:       mockCreator.URL,
			RemoteEngineTimeoutMS: 5000,
			RemoteEngineRetries:   1,
		},
	}

	// PR15.7a: creator-via-assetService path. The Enqueuer must carry the
	// voiceover service so the rewrite step runs inside Enqueue.
	enqueuer := jobenqueue.NewEnqueuer(atomic, nil, voiceoverSvc)

	r := gin.New()
	RegisterRoutes(r.Group("/api/script"), cfg, db, enqueuer)

	reqBody, _ := json.Marshal(map[string]interface{}{
		"video_name":     "Creator Video",
		"voiceover_path": "https://example.com/voice.mp3",
		"scenes": []interface{}{
			map[string]interface{}{"text": "Scene 1", "image_link": "https://example.com/scene1.png"},
		},
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/script/generate-with-images", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "127.0.0.1:12345"
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}

	var res map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if res["creator_stage"] != "remote_engine" {
		t.Fatalf("want creator_stage remote_engine, got %v", res["creator_stage"])
	}
	if res["creator_job_id"] != "creator-trace-1" {
		t.Fatalf("want creator_job_id creator-trace-1, got %v", res["creator_job_id"])
	}
	if res["job_id"] == "" {
		t.Fatalf("want worker job_id, got empty")
	}
	if res["status"] != "PENDING" {
		t.Fatalf("want worker status PENDING, got %v", res["status"])
	}

	j, jobGetErr := jobRepo.Get(context.Background(), res["job_id"].(string))
	if jobGetErr != nil {
		t.Fatalf("Get: %v", jobGetErr)
	}
	if j == nil {
		t.Fatalf("want job")
	}
	payload := jobs.ToPayloadMap(j)
	if got := payload["voiceover_path"]; got == voicePath {
		t.Fatalf("want staged voiceover path, got raw local creator path %v", got)
	}
}

func TestGenerateWithImages_BypassesCreatorForRenderReadyPayload(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "velox.db")
	voicePath := filepath.Join(tempDir, "roman_voiceover.mp3")
	if err := os.WriteFile(voicePath, []byte("voice"), 0o644); err != nil {
		t.Fatalf("write voice: %v", err)
	}
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	_ = store.NewSQLiteJobRepository(db)
	atomic := store.NewAtomicJobTaskCreator(db)

	creatorCalled := false
	mockCreator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		creatorCalled = true
		http.Error(w, `{"ok":false,"error":"creator should be bypassed"}`, http.StatusBadRequest)
	}))
	defer mockCreator.Close()

	cfg := &config.Config{
		Runtime: config.RuntimeConfig{
			DataDir:   tempDir,
			VideosDir: filepath.Join(tempDir, "videos"),
		},
		Database: config.DatabaseConfig{
			DBPath: dbPath,
		},
		Render: config.RenderConfig{
			RemoteEngineURL:       mockCreator.URL,
			RemoteEngineTimeoutMS: 5000,
			RemoteEngineRetries:   1,
		},
	}

	// PR15.7a: bypass path. Creator stage is short-circuited, no asset rewrite
	// expected; voiceover nil is fine.
	enqueuer := jobenqueue.NewEnqueuer(atomic, nil, nil)

	r := gin.New()
	RegisterRoutes(r.Group("/api/script"), cfg, db, enqueuer)

	reqBody, _ := json.Marshal(map[string]interface{}{
		"skip_creator":   true,
		"video_name":     "Roman Aqueducts Fixed Job",
		"script_text":    "Roman engineering script",
		"voiceover_path": voicePath,
		"scenes_json": `[
			{"text":"Scene 1","image_link":"https://drive.google.com/file/d/1QoPBq8z2DB9OUXyjIT3HwgKOYzihF8Mh/view","duration_seconds":5},
			{"text":"Scene 2","image_link":"https://drive.google.com/file/d/1S6NiFUeLEAQwtGZISX96nRsv6sv_p7f_/view","duration_seconds":5}
		]`,
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/script/generate-with-images", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "127.0.0.1:12345"
	r.ServeHTTP(w, req)

	if creatorCalled {
		t.Fatalf("creator must not be called for render-ready payload")
	}
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}
}
