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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"velox-worker-agent/internal/downloader"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

func testAssetDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func testTransferWorker(masterURL, stateDir string) *Worker {
	return &Worker{
		config: &config.WorkerConfig{
			WorkerID:  "resilience-worker",
			MasterURL: masterURL,
			StateDir:  stateDir,
		},
		logger:   logger.New(logger.InfoLevel, io.Discard),
		stopChan: make(chan struct{}),
	}
}

func TestMasterAssetTransfererResumesAfterInterruptedHTTPResponse(t *testing.T) {
	data := []byte("media payload that survives an interrupted transfer")
	digest := testAssetDigest(data)
	var requests atomic.Int32
	firstInterrupted := make(chan struct{})
	serverError := make(chan error, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		if n == 1 {
			if got := r.Header.Get("Range"); got != "" {
				serverError <- fmt.Errorf("first request unexpectedly had Range=%q", got)
				return
			}
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				serverError <- fmt.Errorf("test server does not support hijacking")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				serverError <- fmt.Errorf("hijack: %w", err)
				return
			}
			_, _ = fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nContent-Type: audio/mpeg\r\n\r\n", len(data))
			_, _ = conn.Write(data[:7])
			_ = conn.Close()
			// The client may observe connection close before the writer
			// goroutine flushes its partial; the test waits for the file.
			close(firstInterrupted)
			return
		}

		wantRange := "bytes=7-"
		if got := r.Header.Get("Range"); got != wantRange {
			serverError <- fmt.Errorf("resume Range=%q, want %q", got, wantRange)
			return
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 7-%d/%d", len(data)-1, len(data)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[7:])
	}))
	defer srv.Close()

	stateDir := t.TempDir()
	worker := testTransferWorker(srv.URL, stateDir)
	transferer := &masterAssetTransferer{w: worker}
	req := downloader.DownloadRequest{
		AssetID:   "resume-asset",
		AssetKey:  "resume-asset",
		SHA256:    digest,
		SizeBytes: int64(len(data)),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultCh := make(chan error, 1)
	go func() {
		_, err := transferer.Transfer(ctx, context.Background(), req, nil)
		resultCh <- err
	}()
	select {
	case <-firstInterrupted:
	case err := <-serverError:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for interrupted first response")
	}
	part := assetPartialPath(worker.assetCacheDir(), req.AssetID, req.SHA256)
	deadline := time.Now().Add(2 * time.Second)
	for {
		info, statErr := os.Stat(part)
		if statErr == nil && info.Size() == 7 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("partial during interruption = size/error %v/%v, want 7 bytes", func() int64 {
				if statErr != nil {
					return -1
				}
				return info.Size()
			}(), statErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-resultCh; err == nil {
		t.Fatal("interrupted transfer should return cancellation error")
	}

	// A fresh worker instance reuses the same StateDir and resumes the
	// persisted partial. This models a worker process restart.
	restarted := testTransferWorker(srv.URL, stateDir)
	result, err := (&masterAssetTransferer{w: restarted}).Transfer(context.Background(), context.Background(), req, nil)
	if err != nil {
		t.Fatalf("resumed transfer after restart: %v", err)
	}
	got, err := os.ReadFile(result.LocalPath)
	if err != nil {
		t.Fatalf("read resumed final: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("resumed bytes = %q, want %q", got, data)
	}
	if requests.Load() != 2 {
		t.Fatalf("HTTP requests = %d, want 2", requests.Load())
	}
	select {
	case err := <-serverError:
		t.Fatal(err)
	default:
	}
}

func TestMasterAssetTransfererRestartsWhenServerIgnoresRange(t *testing.T) {
	data := []byte("complete response after ignored range")
	digest := testAssetDigest(data)
	var sawRange atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			sawRange.Store(true)
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	stateDir := t.TempDir()
	worker := testTransferWorker(srv.URL, stateDir)
	req := downloader.DownloadRequest{AssetID: "ignored-range", AssetKey: "ignored-range", SHA256: digest, SizeBytes: int64(len(data))}
	part := assetPartialPath(worker.assetCacheDir(), req.AssetID, req.SHA256)
	if err := os.MkdirAll(filepath.Dir(part), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(part, data[:5], 0o644); err != nil {
		t.Fatal(err)
	}

	result, err := (&masterAssetTransferer{w: worker}).Transfer(context.Background(), context.Background(), req, nil)
	if err != nil {
		t.Fatalf("transfer with ignored Range: %v", err)
	}
	got, err := os.ReadFile(result.LocalPath)
	if err != nil {
		t.Fatalf("read final: %v", err)
	}
	if !strings.EqualFold(string(got), string(data)) {
		t.Fatalf("final bytes = %q, want %q", got, data)
	}
	if !sawRange.Load() {
		t.Fatal("expected initial request to advertise Range for existing partial")
	}
	if _, err := os.Stat(part); !os.IsNotExist(err) {
		t.Fatalf("partial remains after successful restart, err=%v", err)
	}
}

func TestMasterAssetTransfererRestartsAfterRangeNotSatisfiable(t *testing.T) {
	data := []byte("range not satisfiable restarts cleanly")
	digest := testAssetDigest(data)
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if got := r.Header.Get("Range"); got != "" {
			t.Errorf("restart request Range=%q, want empty after 416 reset", got)
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	stateDir := t.TempDir()
	worker := testTransferWorker(srv.URL, stateDir)
	req := downloader.DownloadRequest{AssetID: "range-reset", AssetKey: "range-reset", SHA256: digest, SizeBytes: int64(len(data))}
	part := assetPartialPath(worker.assetCacheDir(), req.AssetID, req.SHA256)
	if err := os.MkdirAll(filepath.Dir(part), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(part, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := (&masterAssetTransferer{w: worker}).Transfer(context.Background(), context.Background(), req, nil)
	if err != nil {
		t.Fatalf("transfer after 416: %v", err)
	}
	got, err := os.ReadFile(result.LocalPath)
	if err != nil || string(got) != string(data) {
		t.Fatalf("final after 416 = %q/%v, want %q", got, err, data)
	}
	if requests.Load() != 2 {
		t.Fatalf("requests = %d, want 2", requests.Load())
	}
}

func TestMasterAssetTransfererRejectsMalformedContentRange(t *testing.T) {
	data := []byte("malformed range then clean restart")
	digest := testAssetDigest(data)
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			if got := r.Header.Get("Range"); got != "bytes=2-" {
				t.Errorf("first malformed-range request Range=%q, want bytes=2-", got)
			}
			w.Header().Set("Content-Type", "audio/mpeg")
			w.Header().Set("Content-Range", "bytes 3-9/10")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(data[3:])
			return
		}
		if got := r.Header.Get("Range"); got != "" {
			t.Errorf("retry after malformed Content-Range Range=%q, want empty", got)
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	stateDir := t.TempDir()
	worker := testTransferWorker(srv.URL, stateDir)
	req := downloader.DownloadRequest{AssetID: "malformed-range", AssetKey: "malformed-range", SHA256: digest, SizeBytes: int64(len(data))}
	part := assetPartialPath(worker.assetCacheDir(), req.AssetID, req.SHA256)
	if err := os.MkdirAll(filepath.Dir(part), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(part, data[:2], 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := (&masterAssetTransferer{w: worker}).Transfer(context.Background(), context.Background(), req, nil)
	if err != nil {
		t.Fatalf("transfer after malformed Content-Range: %v", err)
	}
	got, err := os.ReadFile(result.LocalPath)
	if err != nil || string(got) != string(data) {
		t.Fatalf("final after malformed range = %q/%v, want %q", got, err, data)
	}
	if requests.Load() != 2 {
		t.Fatalf("requests = %d, want exactly 2 after malformed range restart", requests.Load())
	}
	if _, err := os.Stat(part); !os.IsNotExist(err) {
		t.Fatalf("partial remains after clean restart, err=%v", err)
	}
}

func TestWorkerStopPreservesPartialForRestart(t *testing.T) {
	data := []byte("worker stop restart keeps a resumable partial")
	digest := testAssetDigest(data)
	var requests atomic.Int32
	interrupted := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("server does not support hijacking")
				return
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_, _ = fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nContent-Type: audio/mpeg\r\n\r\n", len(data))
			_, _ = conn.Write(data[:9])
			_ = conn.Close()
			close(interrupted)
			return
		}
		if got := r.Header.Get("Range"); got != "bytes=9-" {
			t.Errorf("restart Range=%q, want bytes=9-", got)
			return
		}
		w.Header().Set("Content-Type", "audio/mpeg")
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 9-%d/%d", len(data)-1, len(data)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[9:])
	}))
	defer srv.Close()

	stateDir := t.TempDir()
	first := testTransferWorker(srv.URL, stateDir)
	req := downloader.DownloadRequest{AssetID: "stop-restart", AssetKey: "stop-restart", SHA256: digest, SizeBytes: int64(len(data))}
	resolveResult := make(chan error, 1)
	go func() {
		_, err := first.assetDownloadManager().Resolve(context.Background(), req)
		resolveResult <- err
	}()
	select {
	case <-interrupted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first worker transfer")
	}
	part := assetPartialPath(first.assetCacheDir(), req.AssetID, req.SHA256)
	deadline := time.Now().Add(2 * time.Second)
	for {
		info, statErr := os.Stat(part)
		if statErr == nil && info.Size() == 9 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker partial = size/error %v/%v, want 9 bytes", func() int64 {
				if statErr != nil {
					return -1
				}
				return info.Size()
			}(), statErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	first.Stop()
	if err := <-resolveResult; err == nil {
		t.Fatal("Resolve should fail when Worker.Stop cancels the manager")
	}

	restarted := testTransferWorker(srv.URL, stateDir)
	defer restarted.Stop()
	asset, err := restarted.assetDownloadManager().Resolve(context.Background(), req)
	if err != nil {
		t.Fatalf("Resolve after worker restart: %v", err)
	}
	got, err := os.ReadFile(asset.LocalPath)
	if err != nil {
		t.Fatalf("read restarted asset: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("restarted asset = %q, want %q", got, data)
	}
	if requests.Load() != 2 {
		t.Fatalf("HTTP requests = %d, want 2", requests.Load())
	}
}
