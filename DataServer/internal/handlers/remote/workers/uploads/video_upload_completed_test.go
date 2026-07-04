package uploads

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
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
	"velox-server/internal/store/migrations"
)

// TestUploadCompletedVideo_ArtifactsPipeline exercises the /api/v1/video/upload-completed
// handler end-to-end through artifacts.Service + FinalizationWriter. It deliberately does
// NOT cover the Completion flow (Coordinator.CommitAttempt → task_attempts
// SUCCEEDED): that path lives in the Completion package's integration tests
// because the two flows mark different tables (jobs+artifacts here;
// task_attempts in Completion) and the single-writer contract on
// jobs.status='SUCCEEDED' forbids coupling them inside one test.
func TestUploadCompletedVideo_ArtifactsPipeline(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "test.db")
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, p := range []string{
		"PRAGMA synchronous = NORMAL",
		"PRAGMA foreign_keys = ON",
	} {
		if _, err := db.Exec(p); err != nil {
			t.Fatalf("pragma: %v", err)
		}
	}

	if err := migrations.RunMigrations(db, migrations.SQLiteMigrationsFS(), "sqlite"); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	staging := filepath.Join(tmp, "staging")
	final := filepath.Join(tmp, "final")
	bs, err := store.NewFilesystemBlobStore(staging, final)
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}

	// store.NewSQLiteUploadRepository is the typed artifact_uploads +
	// artifact_upload_chunks CRUD surface. The artifacts-package
	// Service composes it with three narrow writers behind
	// UploadSessionWriter / FinalizationWriter / ArtifactReader.
	repo := store.NewSQLiteUploadRepository(db)
	artifactReader := artifacts.NewSQLiteArtifactReader(db)
	artifactSvc := artifacts.NewService(
		repo,
		artifacts.NewSQLiteUploadSessionWriter(db),
		artifacts.NewSQLiteFinalizeWriter(db, artifactReader, nil),
		artifactReader,
		bs,
		artifacts.NewSQLiteAuthReader(db),
		nil,
		// JobDeliveryCounter typed reader — required by NewService
		// post the VELOX_FFPROBE_VERIFY_ON_FINALIZE gate
		// (RW-PROD-008 A4). Test wiring uses the same SQLite
		// implementation production uses.
		artifacts.NewSQLiteJobDeliveryCounter(db),
	)

	now := time.Now().UTC().Format(time.RFC3339)

	jobID := "upload-e2e-1"
	workerID := "worker-1"
	leaseID := "lease-abc-123"
	revision := 3

	// Seed job in RUNNING state.
	// migration 048: assigned_to/lease_id/lease_expiry were dropped
	// from the jobs table. Worker + lease identity now lives
	// on task_attempts (per-attempt) — see the seedAttempt helper in
	// service_test.go for the canonical pattern.
	_, err = db.Exec(`
		INSERT INTO jobs (job_id, status, revision, created_at, updated_at, migrated_at)
		VALUES (?, 'RUNNING', ?, ?, ?, ?)`,
		jobID, revision, now, now, now)
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}

	// Seed parent task (loadAttempt JOINs task_attempts→tasks).
	taskID := jobID + "-task"
	_, err = db.Exec(`
		INSERT INTO tasks (task_id, job_id, status, created_at, updated_at)
		VALUES (?, ?, 'RUNNING', ?, ?)`,
		taskID, jobID, now, now)
	if err != nil {
		t.Fatalf("seed task: %v", err)
	}

	// Seed task_attempt in a non-terminal state. BeginUpload now
	// accepts any non-terminal attempt (terminal →
	// ErrAttemptNotRenderFinished).
	_, err = db.Exec(`
		INSERT INTO task_attempts (id, task_id, attempt_number, worker_id, lease_id, status, started_at, created_at, updated_at)
		VALUES (?, ?, 1, ?, ?, 'RUNNING', ?, ?, ?)`,
		jobID+"-attempt", taskID, workerID, leaseID, now, now, now)
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

	// task_attempts.status is NOT asserted here on purpose:
	// artifacts.FinalizeVerified marks jobs+artifacts (the canonical
	// writer surface); Completion (Coordinator.CommitAttempt) is the
	// separate path that marks task_attempts via the UoW adapter's
	// TaskAttemptRepository.MarkSucceeded. The /api/v1/video/upload-completed
	// handler triggers the artifacts path, not Completion; asserting
	// task_attempts here would couple the two flows incorrectly.
	// End-to-end coverage for task_attempts marking lives in the
	// Completion flow's integration tests.
}

func TestUploadCompletedVideo_BeginUploadRejected_MissingJob(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "test.db")
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, p := range []string{
		"PRAGMA synchronous = NORMAL",
		"PRAGMA foreign_keys = ON",
	} {
		if _, err := db.Exec(p); err != nil {
			t.Fatalf("pragma: %v", err)
		}
	}
	if err := migrations.RunMigrations(db, migrations.SQLiteMigrationsFS(), "sqlite"); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	staging := filepath.Join(tmp, "staging")
	final := filepath.Join(tmp, "final")
	bs, err := store.NewFilesystemBlobStore(staging, final)
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}

	repo := store.NewSQLiteUploadRepository(db)
	artifactReader := artifacts.NewSQLiteArtifactReader(db)
	artifactSvc := artifacts.NewService(
		repo,
		artifacts.NewSQLiteUploadSessionWriter(db),
		artifacts.NewSQLiteFinalizeWriter(db, artifactReader, nil),
		artifactReader,
		bs,
		artifacts.NewSQLiteAuthReader(db),
		nil,
		artifacts.NewSQLiteJobDeliveryCounter(db),
	)

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
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, p := range []string{
		"PRAGMA synchronous = NORMAL",
		"PRAGMA foreign_keys = ON",
	} {
		if _, err := db.Exec(p); err != nil {
			t.Fatalf("pragma: %v", err)
		}
	}
	if err := migrations.RunMigrations(db, migrations.SQLiteMigrationsFS(), "sqlite"); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	staging := filepath.Join(tmp, "staging")
	final := filepath.Join(tmp, "final")
	bs, err := store.NewFilesystemBlobStore(staging, final)
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}

	repo := store.NewSQLiteUploadRepository(db)
	artifactReader := artifacts.NewSQLiteArtifactReader(db)
	artifactSvc := artifacts.NewService(
		repo,
		artifacts.NewSQLiteUploadSessionWriter(db),
		artifacts.NewSQLiteFinalizeWriter(db, artifactReader, nil),
		artifactReader,
		bs,
		artifacts.NewSQLiteAuthReader(db),
		nil,
		artifacts.NewSQLiteJobDeliveryCounter(db),
	)

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
