// PR-3.9 notes:
//
//   - The legacy TestExecuteWorkflowJobPassesResolvedLocalAudioPathToWorkflow
//     test has been removed. It exercised the (now-deprecated)
//     executeWorkflowJob helper that was deleted as part of removing
//     duplicate routing — every job type now resolves through
//     executor.Registry → TaskRunner.
//
//   - fakeVideoWorkflow + the newVideoWorkflow swap are GONE along
//     with the helper. The remaining tests cover resolveVoiceoverAudioPath
//     directly, which is the canonical surface for the asset-bridge.
package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
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

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	return string(data)
}
