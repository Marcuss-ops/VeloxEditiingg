package worker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/api"
)

func TestProtectedAssetsErrorsAfterStartupBlockCleanupUntilRecovery(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch atomic.AddInt32(&calls, 1) {
		case 1:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(snapshotJSON(1, time.Now().UTC().Format(time.RFC3339Nano), 1, []string{"initial"}))
		case 2, 3:
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(snapshotJSON(2, time.Now().UTC().Format(time.RFC3339Nano), 1, []string{"recovered"}))
		}
	}))
	defer server.Close()

	poller := NewProtectedAssetsPoller(api.NewClient(server.URL), time.Hour)
	poller.SnapshotMaxAge = time.Minute
	if err := poller.TickOnce(context.Background()); err != nil {
		t.Fatalf("initial 200: %v", err)
	}
	cache, err := workercache.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	cachePath := filepath.Join(t.TempDir(), "evictable.mp4")
	if err := os.WriteFile(cachePath, []byte("old-cache"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cache.Store(context.Background(), workercache.Entry{
		AssetKey: "old-cache", LocalPath: cachePath, DownloadComplete: true,
	}); err != nil {
		t.Fatal(err)
	}
	loop := &workercache.CleanupLoop{
		Cache: cache, Policy: workercache.CleanupPolicy{RecentUseGrace: 0, SnapshotMaxAge: time.Minute},
		Snapshot: poller, Barrier: poller, Interval: time.Hour,
	}

	if _, err := loop.TickOnce(context.Background()); err != nil {
		t.Fatalf("cleanup with valid snapshot: %v", err)
	}
	if _, err := os.Stat(cachePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("initial valid snapshot did not remove evictable file: %v", err)
	}
	// Recreate the row/file for the failure phase.
	if err := os.WriteFile(cachePath, []byte("old-cache"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cache.Store(context.Background(), workercache.Entry{
		AssetKey: "old-cache", LocalPath: cachePath, DownloadComplete: true,
	}); err != nil {
		t.Fatal(err)
	}

	if err := poller.TickOnce(context.Background()); err == nil {
		t.Fatal("503 unexpectedly succeeded")
	}
	if _, err := loop.TickOnce(context.Background()); err == nil {
		t.Fatal("cleanup proceeded after 503; want fail-safe error")
	}
	if _, err := os.Stat(cachePath); err != nil {
		t.Fatalf("503 cleanup removed evictable file: %v", err)
	}
	if _, found, err := cache.Find(context.Background(), "old-cache"); err != nil || !found {
		t.Fatalf("503 cleanup changed cache index: found=%v err=%v", found, err)
	}
	if poller.Snapshot() == nil || !poller.IsReady() {
		t.Fatal("503 lost startup readiness or last snapshot")
	}

	if err := poller.TickOnce(context.Background()); err == nil {
		t.Fatal("second 503 unexpectedly succeeded")
	}
	if _, err := loop.TickOnce(context.Background()); err == nil {
		t.Fatal("cleanup proceeded after consecutive 503")
	}
	if _, err := os.Stat(cachePath); err != nil {
		t.Fatalf("consecutive 503 cleanup removed evictable file: %v", err)
	}

	if err := poller.TickOnce(context.Background()); err != nil {
		t.Fatalf("recovery 200: %v", err)
	}
	if _, err := loop.TickOnce(context.Background()); err != nil {
		t.Fatalf("cleanup after recovery: %v", err)
	}
	if _, err := os.Stat(cachePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered 200 did not re-enable cleanup; file stat=%v", err)
	}
	if _, found, err := cache.Find(context.Background(), "old-cache"); err != nil || found {
		t.Fatalf("recovered 200 did not remove cache index row: found=%v err=%v", found, err)
	}
}
