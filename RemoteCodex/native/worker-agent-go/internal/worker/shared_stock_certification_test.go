package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"velox-shared/assetref"
	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

func sharedStockDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func waitForSharedStock(t *testing.T, description string, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

// videoProcessPIDs returns the currently running video-engine processes. The
// test compares before/after sets so unrelated processes already running on a
// developer machine do not make this download-only certification flaky.
func videoProcessPIDs() map[int]string {
	out := make(map[int]string)
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return out
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		nameBytes, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "comm"))
		if err != nil {
			continue
		}
		name := string(nameBytes)
		if name == "ffmpeg\n" || name == "velox_video_engine\n" {
			out[pid] = name
		}
	}
	return out
}

// TestSharedStockDownloadProgressCertification is the Commit 5 end-to-end
// certification: ten jobs request one stock on one worker, one HTTP transfer
// serves all ten waiters, every job observes the same byte-weighted progress,
// the next request is a verified cache hit, stale partials are cleaned, and
// this downloader-only path creates no FFmpeg/video-engine process.
func TestSharedStockDownloadProgressCertification(t *testing.T) {
	beforeVideo := videoProcessPIDs()
	defer func() {
		afterVideo := videoProcessPIDs()
		for pid, name := range afterVideo {
			if _, existed := beforeVideo[pid]; !existed {
				t.Errorf("download-only certification spawned video process pid=%d name=%q", pid, name)
			}
		}
	}()

	data := make([]byte, 256*1024)
	for i := range data {
		data[i] = byte((i * 31) % 251)
	}
	digest := sharedStockDigest(data)
	half := len(data) / 2

	var upstreamRequests atomic.Int32
	firstChunk := make(chan struct{})
	releaseBody := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseBody) }) }
	t.Cleanup(release)
	handlerErrors := make(chan error, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if upstreamRequests.Add(1) != 1 {
			handlerErrors <- fmt.Errorf("unexpected duplicate upstream request: method=%s range=%q", r.Method, r.Header.Get("Range"))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if got := r.Header.Get("Range"); got != "" {
			handlerErrors <- fmt.Errorf("cold request unexpectedly carried Range=%q", got)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			handlerErrors <- fmt.Errorf("httptest response does not support flushing")
			return
		}
		if _, err := w.Write(data[:half]); err != nil {
			handlerErrors <- fmt.Errorf("write first stock chunk: %w", err)
			return
		}
		flusher.Flush()
		close(firstChunk)
		<-releaseBody
		if _, err := w.Write(data[half:]); err != nil {
			handlerErrors <- fmt.Errorf("write final stock chunk: %w", err)
		}
	}))
	defer server.Close()

	stateDir := t.TempDir()
	worker := &Worker{
		config: &config.WorkerConfig{
			WorkerID:  "shared-stock-certification",
			MasterURL: server.URL,
			StateDir:  stateDir,
		},
		logger:   logger.New(logger.InfoLevel, io.Discard),
		stopChan: make(chan struct{}),
	}

	// Seed a stale orphan. Transfer startup cleanup must remove it without
	// touching the active stock partial or any final cache entry.
	orphan := filepath.Join(worker.assetCacheDir(), "partial", "orphan-stock.part")
	if err := os.MkdirAll(filepath.Dir(orphan), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(orphan, []byte("orphan"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(orphan, old, old); err != nil {
		t.Fatal(err)
	}

	manager := downloader.NewManager(downloader.Config{
		Concurrency:  4,
		PublishBytes: 1,
	}, &masterAssetTransferer{w: worker})
	defer manager.Close()

	type resolveResult struct {
		asset downloader.DownloadedAsset
		err   error
	}
	results := make([]resolveResult, 10)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i].asset, results[i].err = manager.Resolve(context.Background(), downloader.DownloadRequest{
				JobID:     fmt.Sprintf("shared-stock-job-%02d", i),
				TaskID:    fmt.Sprintf("shared-stock-task-%02d", i),
				AssetKey:  assetref.AssetKey("shared-stock-001"),
				AssetID:   "shared-stock-001",
				Role:      downloader.AssetRoleStock,
				SHA256:    assetref.ContentHash(digest),
				SizeBytes: int64(len(data)),
				Priority:  downloader.DefaultPriority,
			})
		}(i)
	}

	waitForSharedStock(t, "ten shared waiters", func() bool {
		snap, ok := manager.Snapshot("shared-stock-001")
		return ok && snap.SharedWaiters == 10
	})
	waitForSharedStock(t, "one upstream transfer", func() bool {
		return upstreamRequests.Load() == 1
	})

	sub, unsubscribe := manager.Subscribe("shared-stock-001")
	if sub == nil {
		t.Fatal("shared stock subscriber is nil")
	}
	defer unsubscribe()

	select {
	case <-firstChunk:
	case err := <-handlerErrors:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for shared stock progress gate")
	}

	waitForSharedStock(t, "shared byte progress", func() bool {
		snap, ok := manager.Snapshot("shared-stock-001")
		return ok && snap.State == downloader.DownloadRunning && snap.BytesDownloaded > 0 && snap.BytesDownloaded < int64(len(data))
	})

	progressBytes := int64(0)
	select {
	case snap := <-sub:
		progressBytes = snap.BytesDownloaded
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber did not receive shared stock progress")
	}
	if progressBytes <= 0 || progressBytes >= int64(len(data)) {
		t.Fatalf("subscriber bytes = %d, want an in-flight partial progress value", progressBytes)
	}

	waitForSharedStock(t, "complete first progress chunk", func() bool {
		snap, ok := manager.Snapshot("shared-stock-001")
		return ok && snap.BytesDownloaded == int64(half)
	})
	sharedBytes := int64(half)
	for i := range results {
		job := fmt.Sprintf("shared-stock-job-%02d", i)
		jobSnap := manager.JobSnapshot(job)
		if jobSnap.AssetsTotal != 1 {
			t.Fatalf("%s assets_total = %d, want 1", job, jobSnap.AssetsTotal)
		}
		if jobSnap.ActiveTransfers != 1 {
			t.Fatalf("%s active_transfers = %d, want 1", job, jobSnap.ActiveTransfers)
		}
		if jobSnap.BytesDownloaded <= 0 || jobSnap.BytesDownloaded >= int64(len(data)) {
			t.Fatalf("%s bytes_downloaded = %d, want shared in-flight progress", job, jobSnap.BytesDownloaded)
		}
		if jobSnap.BytesDownloaded != sharedBytes {
			t.Fatalf("%s bytes_downloaded = %d, want shared value %d", job, jobSnap.BytesDownloaded, sharedBytes)
		}
		wantPercent := float64(jobSnap.BytesDownloaded) / float64(len(data)) * 100
		if jobSnap.ProgressPercent != wantPercent {
			t.Fatalf("%s progress = %.4f, want %.4f (byte weighted)", job, jobSnap.ProgressPercent, wantPercent)
		}
	}

	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("stale orphan partial remains after transfer cleanup, stat err=%v", err)
	}
	release()
	wg.Wait()

	for i, result := range results {
		if result.err != nil {
			t.Fatalf("job %d resolve: %v", i, result.err)
		}
		if i > 0 && result.asset.LocalPath != results[0].asset.LocalPath {
			t.Fatalf("job %d path = %q, want shared path %q", i, result.asset.LocalPath, results[0].asset.LocalPath)
		}
	}
	if got := upstreamRequests.Load(); got != 1 {
		t.Fatalf("upstream request count = %d, want exactly 1 for ten jobs", got)
	}
	select {
	case err := <-handlerErrors:
		t.Fatal(err)
	default:
	}

	// The eleventh request uses the same verified metadata and must resolve
	// from cache without starting another upstream transfer.
	cacheHit, err := manager.Resolve(context.Background(), downloader.DownloadRequest{
		JobID:     "shared-stock-cache-hit",
		TaskID:    "shared-stock-cache-hit-task",
		AssetKey:  assetref.AssetKey("shared-stock-001"),
		AssetID:   "shared-stock-001",
		Role:      downloader.AssetRoleStock,
		SHA256:    assetref.ContentHash(digest),
		SizeBytes: int64(len(data)),
		Priority:  downloader.DefaultPriority,
	})
	if err != nil {
		t.Fatalf("cache-hit resolve: %v", err)
	}
	if !cacheHit.CacheHit {
		t.Fatal("eleventh resolve must report CacheHit=true")
	}
	if got := upstreamRequests.Load(); got != 1 {
		t.Fatalf("upstream request count after cache hit = %d, want 1", got)
	}
	if cacheHit.LocalPath != results[0].asset.LocalPath {
		t.Fatalf("cache-hit path = %q, want %q", cacheHit.LocalPath, results[0].asset.LocalPath)
	}
}
