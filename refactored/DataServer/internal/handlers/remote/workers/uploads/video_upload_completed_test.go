package uploads

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	driveapi "velox-server/internal/integrations/drive"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

type fakeDriveAutoUploader struct {
	mu           sync.Mutex
	uploadCalls  int
	lastFilePath string
	lastProject  string
	lastParentID string
}

func (f *fakeDriveAutoUploader) UploadVideo(ctx context.Context, filePath string, projectName string, parentFolderID string) (*driveapi.UploadResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.uploadCalls++
	f.lastFilePath = filePath
	f.lastProject = projectName
	f.lastParentID = parentFolderID

	return &driveapi.UploadResult{
		Success:     true,
		FileID:      "drive-file-123",
		WebViewLink: "https://drive.example/file/drive-file-123",
		FolderLink:  "https://drive.example/folder/" + parentFolderID,
	}, nil
}

func TestUploadCompletedVideo_AutoUploadsToYouTubeAndDrive(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "velox.db")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	ts, tsErr := queue.NewLifecycleService(jobRepo, db)
	if tsErr != nil {
		t.Fatalf("new transition service: %v", tsErr)
	}
	querySvc := queue.NewQueryService(db)
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: 3, DBStore: db}, ts, querySvc)
	if err != nil {
		t.Fatalf("new file queue: %v", err)
	}

	cfg := &config.Config{
		Runtime: config.RuntimeConfig{
			DataDir:   tempDir,
			VideosDir: filepath.Join(tempDir, "completed_videos"),
		},
	}

	jobID := "upload-e2e-1"
	jobPayload := map[string]interface{}{
		"video_name":          "Upload E2E",
		"script_text":         "Upload E2E script",
		"youtube_group":       "Amish",
		"language":            "it",
		"drive_output_folder": "folder1234567890A",
		"status":              "PROCESSING",
	}
	if err := q.SubmitJob(context.Background(), jobID, jobPayload); err != nil {
		t.Fatalf("submit job: %v", err)
	}
	if _, err := q.ClaimNextJob(context.Background(), "worker-1", nil); err != nil {
		t.Fatalf("claim job: %v", err)
	}

	// Pre-create delivery targets for YouTube and Drive (PR4: required for auto-upload)
	db.InsertDeliveryTarget(&store.DeliveryTarget{
		JobID:      jobID,
		TargetType: "youtube",
		Status:     "pending",
		Config:     `{"group_name":"Amish","language":"it","title":"Upload E2E","description":"Upload E2E script"}`,
	})
	db.InsertDeliveryTarget(&store.DeliveryTarget{
		JobID:      jobID,
		TargetType: "drive",
		Status:     "pending",
		Config:     `{"folder_id":"folder1234567890A","video_name":"Upload E2E"}`,
	})

	gin.SetMode(gin.TestMode)
	r := gin.New()
	blobStore := store.NewNopBlobStore(tempDir)
	r.POST("/api/v1/video/upload-completed", UploadCompletedVideo(cfg, q, blobStore))

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("video", "rendered.mp4")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if _, err := part.Write([]byte("fake-video-bytes")); err != nil {
		t.Fatalf("write multipart file: %v", err)
	}
	if err := writer.WriteField("job_id", jobID); err != nil {
		t.Fatalf("write job_id field: %v", err)
	}
	if err := writer.WriteField("worker_id", "worker-1"); err != nil {
		t.Fatalf("write worker_id field: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}

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
	savedVideoPath, _ := resp["video_path"].(string)
	if savedVideoPath == "" {
		t.Fatalf("expected video_path in response, got %#v", resp["video_path"])
	}

	// Wait for the job to be marked SUCCEEDED by the artifact pipeline.
	// DeliveryRunner handles youtube/drive uploads asynchronously (not tested here).
	jobDone := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		job, err := q.GetJobAsMap(context.Background(), jobID)
		if err == nil && job != nil && job["status"] == "SUCCEEDED" {
			jobDone = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !jobDone {
		job, _ := q.GetJobAsMap(context.Background(), jobID)
		t.Fatalf("job not SUCCEEDED in time: %#v", job)
	}

	job, err := q.GetJobAsMap(context.Background(), jobID)
	if err != nil {
		t.Fatalf("get job as map: %v", err)
	}
	if job["status"] != "SUCCEEDED" {
		t.Fatalf("want status SUCCEEDED, got %v", job["status"])
	}
}

func TestMaybeAutoUploadDrive_FallsBackToJobLanguage(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "velox.db")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("new sqlite store: %v", err)
	}
	if err := db.UpsertMasterFolder("drive-folder-it", "Italian Master", "https://drive.google.com/drive/folders/drive-folder-it", "it", 0, `{"type":"outro"}`); err != nil {
		t.Fatalf("upsert master folder: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	ts, tsErr := queue.NewLifecycleService(jobRepo, db)
	if tsErr != nil {
		t.Fatalf("new transition service: %v", tsErr)
	}
	querySvc := queue.NewQueryService(db)
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: 3, DBStore: db}, ts, querySvc)
	if err != nil {
		t.Fatalf("new file queue: %v", err)
	}

	jobID := "upload-drive-fallback-1"
	jobPayload := map[string]interface{}{
		"video_name": "Fallback Drive Upload",
		"language":   "it",
		"status":     "PROCESSING",
	}
	if err := q.SubmitJob(context.Background(), jobID, jobPayload); err != nil {
		t.Fatalf("submit job: %v", err)
	}

	videoPath := filepath.Join(tempDir, "rendered.mp4")
	if err := os.WriteFile(videoPath, []byte("fake-video-bytes"), 0o644); err != nil {
		t.Fatalf("write video: %v", err)
	}

	driveSvc := &fakeDriveAutoUploader{}
	targets := []store.DeliveryTarget{{
		TargetType: "drive",
		Status:     "pending",
		Config:     `{"folder_id":"drive-folder-it"}`,
	}}
	maybeAutoUploadDrive(q, driveSvc, tempDir, jobID, map[string]interface{}{}, videoPath, targets)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		driveSvc.mu.Lock()
		calls := driveSvc.uploadCalls
		driveSvc.mu.Unlock()
		if calls > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	driveSvc.mu.Lock()
	defer driveSvc.mu.Unlock()
	if driveSvc.uploadCalls != 1 {
		t.Fatalf("want 1 drive upload call, got %d", driveSvc.uploadCalls)
	}
	if driveSvc.lastParentID != "drive-folder-it" {
		t.Fatalf("want fallback parent folder drive-folder-it, got %q", driveSvc.lastParentID)
	}
}
