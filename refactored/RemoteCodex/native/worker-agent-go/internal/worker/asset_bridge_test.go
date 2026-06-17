package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

func TestResolveVoiceoverAudioPathDownloadsFromConfiguredMasterURL(t *testing.T) {
	tempDir := t.TempDir()
	assetBytes := []byte("ID3downloaded-voiceover")
	var mu sync.Mutex
	var authHeader string
	var requestCount int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		requestCount++
		authHeader = r.Header.Get("Authorization")
		if r.URL.Path != "/api/v1/worker-assets/asset-123" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		_, _ = w.Write(assetBytes)
	}))
	defer srv.Close()

	w := &Worker{
		config: &config.WorkerConfig{
			MasterURL: srv.URL,
			WorkDir:   tempDir,
		},
		apiClient: api.NewClient(srv.URL),
		logger:    logger.New(logger.InfoLevel, nil),
	}
	w.apiClient.SetAuthToken("worker-token-123")

	localPath, err := w.resolveVoiceoverAudioPath(context.Background(), "velox-asset://asset-123", nil)
	if err != nil {
		t.Fatalf("resolve voiceover: %v", err)
	}
	if !strings.HasPrefix(localPath, tempDir) {
		t.Fatalf("want local cached path under workdir, got %q", localPath)
	}
	if _, err := os.Stat(localPath); err != nil {
		t.Fatalf("downloaded asset missing: %v", err)
	}
	if got := readFile(t, localPath); got != string(assetBytes) {
		t.Fatalf("want downloaded content %q, got %q", string(assetBytes), got)
	}
	mu.Lock()
	defer mu.Unlock()
	if requestCount != 1 {
		t.Fatalf("want 1 request, got %d", requestCount)
	}
	if authHeader != "Bearer worker-token-123" {
		t.Fatalf("want worker auth header, got %q", authHeader)
	}
}

func TestResolveVoiceoverAudioPathRetriesTransient5xxAndFailsOn404(t *testing.T) {
	tempDir := t.TempDir()
	var mu sync.Mutex
	var attempts int

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		switch r.URL.Path {
		case "/api/v1/worker-assets/transient":
			if attempts < 3 {
				http.Error(w, "temporary", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "audio/mpeg")
			_, _ = w.Write([]byte("ID3transient"))
		case "/api/v1/worker-assets/missing":
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	w := &Worker{
		config: &config.WorkerConfig{
			MasterURL: srv.URL,
			WorkDir:   tempDir,
		},
		apiClient: api.NewClient(srv.URL),
		logger:    logger.New(logger.InfoLevel, nil),
	}
	w.apiClient.SetAuthToken("worker-token-123")

	path, err := w.resolveVoiceoverAudioPath(context.Background(), "velox-asset://transient", nil)
	if err != nil {
		t.Fatalf("resolve transient asset: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("downloaded transient asset missing: %v", err)
	}

	mu.Lock()
	if attempts != 3 {
		mu.Unlock()
		t.Fatalf("want 3 attempts for transient failure, got %d", attempts)
	}
	attempts = 0
	mu.Unlock()

	_, err = w.resolveVoiceoverAudioPath(context.Background(), "velox-asset://missing", nil)
	if err == nil {
		t.Fatal("want 404 error")
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 1 {
		t.Fatalf("want 1 attempt for 404, got %d", attempts)
	}
}

func TestResolveVoiceoverAudioPathRejectsRawJobPayloadURLs(t *testing.T) {
	w := &Worker{
		config:    &config.WorkerConfig{MasterURL: "http://master.example:8000"},
		apiClient: api.NewClient("http://master.example:8000"),
		logger:    logger.New(logger.InfoLevel, nil),
	}
	_, err := w.resolveVoiceoverAudioPath(context.Background(), "https://51.91.11.36:8000/file.mp3", nil)
	if err == nil {
		t.Fatal("want error")
	}
	_, err = w.resolveVoiceoverAudioPath(context.Background(), "http://172.17.0.1:8000/file.mp3", nil)
	if err == nil {
		t.Fatal("want error")
	}
	_, err = w.resolveVoiceoverAudioPath(context.Background(), "http://localhost:8000/file.mp3", nil)
	if err == nil {
		t.Fatal("want error")
	}
}

func TestExecuteWorkflowJobPassesResolvedLocalAudioPathToWorkflow(t *testing.T) {
	tempDir := t.TempDir()
	audioBytes := []byte("ID3workflow-audio")
	var mu sync.Mutex
	var capturedAudioPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/worker-assets/asset-456" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		_, _ = w.Write(audioBytes)
	}))
	defer srv.Close()

	oldFactory := newVideoWorkflow
	newVideoWorkflow = func(cfg *config.WorkerConfig, log *logger.Logger) videoWorkflow {
		return &fakeVideoWorkflow{
			onProcess: func(input renderJobParams) (string, error) {
				mu.Lock()
				defer mu.Unlock()
				capturedAudioPath = input.AudioPath
				if got := readFile(t, input.AudioPath); got != string(audioBytes) {
					t.Fatalf("want workflow to receive downloaded audio, got %q", got)
				}
				return input.OutputPath, nil
			},
		}
	}
	t.Cleanup(func() { newVideoWorkflow = oldFactory })

	w := &Worker{
		config: &config.WorkerConfig{
			MasterURL: srv.URL,
			WorkDir:   tempDir,
		},
		apiClient: api.NewClient(srv.URL),
		logger:    logger.New(logger.InfoLevel, nil),
	}
	w.apiClient.SetAuthToken("worker-token-123")

	job := &api.Job{
		JobID:   "job-asset-workflow",
		JobType: "process_video",
		Parameters: map[string]interface{}{
			"video_name":  "Asset Workflow",
			"script_text": "Asset workflow script",
			"audio_path":  "velox-asset://asset-456",
			"scenes_json": "[]",
			"output_path": filepath.Join(tempDir, "out", "result.mp4"),
		},
	}

	res, err := w.executeWorkflowJob(context.Background(), job, "video", "mp4")
	if err != nil {
		t.Fatalf("executeWorkflowJob: %v", err)
	}
	if got, _ := res["output_path"].(string); got != filepath.Join(tempDir, "out", "result.mp4") {
		t.Fatalf("unexpected output path: %v", res["output_path"])
	}
	mu.Lock()
	if !strings.HasPrefix(capturedAudioPath, tempDir) {
		mu.Unlock()
		t.Fatalf("want workflow to receive local cached file, got %q", capturedAudioPath)
	}
	mu.Unlock()
}

type fakeVideoWorkflow struct {
	onProcess func(input renderJobParams) (string, error)
}

func (f *fakeVideoWorkflow) SetProgressCallback(fn func(percent, scene, total int, stage string)) {}

func (f *fakeVideoWorkflow) ProcessSingleVideo(ctx context.Context, input renderJobParams, statusCallback func(string, bool)) (string, error) {
	if f.onProcess != nil {
		return f.onProcess(input)
	}
	return input.OutputPath, nil
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	return string(data)
}
