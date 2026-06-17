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
	ytservice "velox-server/internal/integrations/youtube"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

type fakeYouTubeAutoUploader struct {
	mu             sync.Mutex
	resolveCalls   int
	healthCalls    int
	uploadCalls    int
	lastGroupName  string
	lastLanguage   string
	lastChannelID  string
	lastUploadPath string
	lastUploadCfg  ytservice.UploadConfig
}

func (f *fakeYouTubeAutoUploader) ResolveChannelByLanguage(groupName, language string) (*ytservice.AuthChannel, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.resolveCalls++
	f.lastGroupName = groupName
	f.lastLanguage = language

	return &ytservice.AuthChannel{
		ID:       "yt-channel-it",
		Name:     "Italian Channel",
		Language: language,
	}, nil
}

func (f *fakeYouTubeAutoUploader) HealthCheck(ctx context.Context, channelID string) (map[string]interface{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.healthCalls++
	f.lastChannelID = channelID

	return map[string]interface{}{"ok": true}, nil
}

func (f *fakeYouTubeAutoUploader) UploadVideo(ctx context.Context, channelID string, videoPath string, cfg ytservice.UploadConfig) (*ytservice.UploadResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.uploadCalls++
	f.lastChannelID = channelID
	f.lastUploadPath = videoPath
	f.lastUploadCfg = cfg

	return &ytservice.UploadResult{
		ID:         "yt-video-123",
		VideoID:    "yt-video-123",
		Status:     "uploaded",
		YouTubeURL: "https://youtube.example/watch?v=yt-video-123",
	}, nil
}

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
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: 3, DBStore: db})
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

	youtubeSvc := &fakeYouTubeAutoUploader{}
	driveSvc := &fakeDriveAutoUploader{}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/api/v1/video/upload-completed", UploadCompletedVideo(cfg, q, youtubeSvc, driveSvc))

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

	jobDone := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		job, err := q.GetJobAsMap(context.Background(), jobID)
		if err == nil && job != nil {
			if job["youtube_upload_status"] == "completed" && job["drive_upload_status"] == "completed" {
				jobDone = true
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !jobDone {
		job, _ := q.GetJobAsMap(context.Background(), jobID)
		t.Fatalf("upload jobs not completed in time: %#v", job)
	}

	job, err := q.GetJobAsMap(context.Background(), jobID)
	if err != nil {
		t.Fatalf("get job as map: %v", err)
	}
	if job["status"] != "COMPLETED" {
		t.Fatalf("want status COMPLETED, got %v", job["status"])
	}
	if job["youtube_url"] != "https://youtube.example/watch?v=yt-video-123" {
		t.Fatalf("want youtube_url persisted, got %v", job["youtube_url"])
	}
	if job["drive_url"] != "https://drive.example/file/drive-file-123" {
		t.Fatalf("want drive_url persisted, got %v", job["drive_url"])
	}
	if job["drive_folder_id"] != "folder1234567890A" {
		t.Fatalf("want drive_folder_id persisted, got %v", job["drive_folder_id"])
	}

	youtubeSvc.mu.Lock()
	if youtubeSvc.resolveCalls != 1 || youtubeSvc.healthCalls != 1 || youtubeSvc.uploadCalls != 1 {
		youtubeSvc.mu.Unlock()
		t.Fatalf("unexpected youtube call counts: resolve=%d health=%d upload=%d", youtubeSvc.resolveCalls, youtubeSvc.healthCalls, youtubeSvc.uploadCalls)
	}
	if youtubeSvc.lastGroupName != "Amish" {
		youtubeSvc.mu.Unlock()
		t.Fatalf("want group Amish, got %q", youtubeSvc.lastGroupName)
	}
	if youtubeSvc.lastLanguage != "it" {
		youtubeSvc.mu.Unlock()
		t.Fatalf("want language it, got %q", youtubeSvc.lastLanguage)
	}
	if youtubeSvc.lastUploadCfg.Title != "Upload E2E" {
		youtubeSvc.mu.Unlock()
		t.Fatalf("want upload title Upload E2E, got %q", youtubeSvc.lastUploadCfg.Title)
	}
	if youtubeSvc.lastUploadCfg.Description != "Upload E2E script" {
		youtubeSvc.mu.Unlock()
		t.Fatalf("want upload description preserved, got %q", youtubeSvc.lastUploadCfg.Description)
	}
	if youtubeSvc.lastUploadCfg.PrivacyStatus != "private" {
		youtubeSvc.mu.Unlock()
		t.Fatalf("want default privacy private, got %q", youtubeSvc.lastUploadCfg.PrivacyStatus)
	}
	if youtubeSvc.lastUploadPath != savedVideoPath {
		youtubeSvc.mu.Unlock()
		t.Fatalf("want video path %q, got %q", savedVideoPath, youtubeSvc.lastUploadPath)
	}
	youtubeSvc.mu.Unlock()

	driveSvc.mu.Lock()
	defer driveSvc.mu.Unlock()
	if driveSvc.uploadCalls != 1 {
		t.Fatalf("want 1 drive upload call, got %d", driveSvc.uploadCalls)
	}
	if driveSvc.lastFilePath != savedVideoPath {
		t.Fatalf("want drive upload path %q, got %q", savedVideoPath, driveSvc.lastFilePath)
	}
	if driveSvc.lastProject != "upload_e2e" {
		t.Fatalf("want drive project Upload E2E, got %q", driveSvc.lastProject)
	}
	if driveSvc.lastParentID != "folder1234567890A" {
		t.Fatalf("want drive parent folder folder1234567890A, got %q", driveSvc.lastParentID)
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
	q, err := queue.NewFileQueue(&queue.FileQueueConfig{MaxRetries: 3, DBStore: db})
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
	maybeAutoUploadDrive(q, driveSvc, tempDir, jobID, map[string]interface{}{}, videoPath, nil)

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
