package uploads

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/artifacts"
	"velox-server/internal/config"
	"velox-server/internal/store"
)

func TestUploadCompletedVideo_CanonicalPipeline(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "test.db")
	sqliteStore, err := store.NewSQLiteStoreFromPath(dbPath, true)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })
	db := sqliteStore.DB()

	staging := filepath.Join(tmp, "staging")
	final := filepath.Join(tmp, "final")
	bs, err := store.NewFilesystemBlobStore(staging, final)
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}

	repo := artifacts.NewSQLiteRepository(db)
	finRepo := artifacts.NewSQLiteFinalizationRepository(db)
	artifactSvc := artifacts.NewService(repo, finRepo, bs,
		store.NewSQLiteJobRepository(sqliteStore),
		store.NewSQLiteArtifactRepository(sqliteStore),
		nil)

	now := time.Now().UTC().Format(time.RFC3339)
	leaseExpiry := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)

	jobID := "upload-e2e-1"
	workerID := "worker-1"
	leaseID := "lease-abc-123"
	revision := 3

	// Seed job in RUNNING state
	_, err = db.Exec(`
		INSERT INTO jobs (job_id, status, assigned_to, lease_id, lease_expiry, revision, created_at, updated_at, migrated_at)
		VALUES (?, 'RUNNING', ?, ?, ?, ?, ?, ?, ?)`,
		jobID, workerID, leaseID, leaseExpiry, revision, now, now, now)
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}

	// Seed attempt in RENDER_FINISHED state (required by BeginUpload)
	_, err = db.Exec(`
		INSERT INTO job_attempts (job_id, attempt_number, worker_id, lease_id, status, started_at, created_at)
		VALUES (?, 1, ?, ?, 'RENDER_FINISHED', ?, ?)`,
		jobID, workerID, leaseID, now, now)
	if err != nil {
		t.Fatalf("seed attempt: %v", err)
	}

	// Seed delivery destinations
	_, err = db.Exec(`INSERT INTO delivery_destinations (destination_id, provider, name, enabled, created_at, updated_at) VALUES (1, 'youtube', 'YT', 1, ?, ?)`, now, now)
	if err != nil {
		t.Fatalf("seed yt dest: %v", err)
	}
	_, err = db.Exec(`INSERT INTO delivery_destinations (destination_id, provider, name, enabled, created_at, updated_at) VALUES (2, 'drive', 'Drive', 1, ?, ?)`, now, now)
	if err != nil {
		t.Fatalf("seed drive dest: %v", err)
	}

	cfg := &config.Config{
		Runtime: config.RuntimeConfig{
			DataDir:   tmp,
			VideosDir: filepath.Join(tmp, "completed_videos"),
		},
	}
	_ = cfg

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/video/upload-completed", UploadCompletedVideo(cfg, artifactSvc))

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("video", "rendered.mp4")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	videoBytes := []byte("fake-video-bytes-for-canonical-pipeline-test")
	if _, err := part.Write(videoBytes); err != nil {
		t.Fatalf("write video: %v", err)
	}
	writer.WriteField("job_id", jobID)
	writer.WriteField("worker_id", workerID)
	writer.WriteField("lease_id", leaseID)
	writer.WriteField("revision", "3")
	writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/video/upload-completed", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}

	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !resp["ok"].(bool) {
		t.Fatalf("expected ok=true, got resp=%#v", resp)
	}
	if resp["status"] != "SUCCEEDED" {
		t.Fatalf("expected status=SUCCEEDED, got %v", resp["status"])
	}
	if resp["artifact_id"] == nil || resp["artifact_id"] == "" {
		t.Fatalf("expected artifact_id, got resp=%#v", resp)
	}

	// Verify job is SUCCEEDED in DB
	var jobStatus string
	err = db.QueryRow(`SELECT status FROM jobs WHERE job_id = ?`, jobID).Scan(&jobStatus)
	if err != nil {
		t.Fatalf("query job status: %v", err)
	}
	if jobStatus != "SUCCEEDED" {
		t.Fatalf("job status = %s, want SUCCEEDED", jobStatus)
	}

	// Verify artifact is READY in DB
	var artStatus string
	err = db.QueryRow(`SELECT status FROM artifacts WHERE job_id = ?`, jobID).Scan(&artStatus)
	if err != nil {
		t.Fatalf("query artifact status: %v", err)
	}
	if artStatus != "READY" {
		t.Fatalf("artifact status = %s, want READY", artStatus)
	}

	// Verify SHA-256 is correct
	expectedSHA := sha256.Sum256(videoBytes)
	expectedSHAHex := hex.EncodeToString(expectedSHA[:])
	var artSHA string
	err = db.QueryRow(`SELECT sha256 FROM artifacts WHERE job_id = ?`, jobID).Scan(&artSHA)
	if err != nil {
		t.Fatalf("query artifact sha: %v", err)
	}
	if artSHA != expectedSHAHex {
		t.Fatalf("artifact sha = %s, want %s", artSHA, expectedSHAHex)
	}

	// Verify attempt is SUCCEEDED
	var attemptStatus string
	err = db.QueryRow(`SELECT status FROM job_attempts WHERE job_id = ?`, jobID).Scan(&attemptStatus)
	if err != nil {
		t.Fatalf("query attempt status: %v", err)
	}
	if attemptStatus != "SUCCEEDED" {
		t.Fatalf("attempt status = %s, want SUCCEEDED", attemptStatus)
	}
}

func TestUploadCompletedVideo_BeginUploadRejected_MissingJob(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "test.db")
	sqliteStore, err := store.NewSQLiteStoreFromPath(dbPath, true)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })
	db := sqliteStore.DB()

	staging := filepath.Join(tmp, "staging")
	final := filepath.Join(tmp, "final")
	bs, err := store.NewFilesystemBlobStore(staging, final)
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}

	repo := artifacts.NewSQLiteRepository(db)
	finRepo := artifacts.NewSQLiteFinalizationRepository(db)
	artifactSvc := artifacts.NewService(repo, finRepo, bs,
		store.NewSQLiteJobRepository(sqliteStore),
		store.NewSQLiteArtifactRepository(sqliteStore),
		nil)

	cfg := &config.Config{Runtime: config.RuntimeConfig{DataDir: tmp}}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/video/upload-completed", UploadCompletedVideo(cfg, artifactSvc))

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, _ := writer.CreateFormFile("video", "rendered.mp4")
	part.Write([]byte("fake"))
	writer.WriteField("job_id", "nonexistent-job")
	writer.WriteField("worker_id", "worker-1")
	writer.WriteField("lease_id", "lease-abc")
	writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/video/upload-completed", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestUploadCompletedVideo_MissingVideo(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "test.db")
	sqliteStore, err := store.NewSQLiteStoreFromPath(dbPath, true)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	t.Cleanup(func() { _ = sqliteStore.Close() })
	db := sqliteStore.DB()

	staging := filepath.Join(tmp, "staging")
	final := filepath.Join(tmp, "final")
	bs, err := store.NewFilesystemBlobStore(staging, final)
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}

	repo := artifacts.NewSQLiteRepository(db)
	finRepo := artifacts.NewSQLiteFinalizationRepository(db)
	artifactSvc := artifacts.NewService(repo, finRepo, bs,
		store.NewSQLiteJobRepository(sqliteStore),
		store.NewSQLiteArtifactRepository(sqliteStore),
		nil)

	cfg := &config.Config{Runtime: config.RuntimeConfig{DataDir: tmp}}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/video/upload-completed", UploadCompletedVideo(cfg, artifactSvc))

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	writer.WriteField("job_id", "some-job")
	writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/video/upload-completed", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d body=%s", w.Code, w.Body.String())
	}
}
