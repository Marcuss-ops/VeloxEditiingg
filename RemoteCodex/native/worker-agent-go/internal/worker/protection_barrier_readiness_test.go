package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/api"
)

func TestProtectedAssetsBarrier_UpdatesReadinessOnlyAfterValidSnapshot(t *testing.T) {
	telemetry.ResetForTest()
	t.Cleanup(telemetry.ResetForTest)
	var call int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		call++
		if call == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(snapshotJSON(7, time.Now().UTC().Format(time.RFC3339Nano), 1, []string{"ready-asset"}))
	}))
	defer server.Close()

	poller := NewProtectedAssetsPoller(api.NewClient(server.URL), time.Hour)
	poller.SnapshotMaxAge = time.Minute
	if err := poller.TickOnce(context.Background()); err == nil {
		t.Fatal("401 unexpectedly succeeded")
	}
	before := telemetry.GlobalReady().Snapshot()
	if before.CacheProtectionReady {
		t.Fatal("401 opened cache protection readiness")
	}
	if before.ProtectedSnapshotAgeSeconds != 0 {
		t.Fatalf("401 changed snapshot age to %d", before.ProtectedSnapshotAgeSeconds)
	}

	if err := poller.TickOnce(context.Background()); err != nil {
		t.Fatalf("valid snapshot: %v", err)
	}
	after := telemetry.GlobalReady().Snapshot()
	if !after.CacheProtectionReady {
		t.Fatal("valid snapshot did not set cache_protection_ready")
	}
	if after.ProtectedSnapshotAgeSeconds < 0 || after.ProtectedSnapshotAgeSeconds > 2 {
		t.Fatalf("snapshot age=%d, want near zero", after.ProtectedSnapshotAgeSeconds)
	}
	if !poller.IsReady() {
		t.Fatal("valid snapshot did not open protection barrier")
	}
}

func TestProtectedAssetsBarrier_RearmsAfterRegistrationLoss(t *testing.T) {
	telemetry.ResetForTest()
	t.Cleanup(telemetry.ResetForTest)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(snapshotJSON(9, time.Now().UTC().Format(time.RFC3339Nano), 1, []string{"session-asset"}))
	}))
	defer server.Close()

	client := api.NewClient(server.URL)
	client.SetAuthToken("session-token")
	poller := NewProtectedAssetsPoller(client, 20*time.Millisecond)
	poller.SnapshotMaxAge = time.Minute
	telemetry.MarkRegistered(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- poller.Run(ctx) }()

	deadline := time.Now().Add(time.Second)
	for !poller.IsReady() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !poller.IsReady() || !telemetry.GlobalReady().Snapshot().CacheProtectionReady {
		t.Fatal("initial authenticated snapshot did not open barrier and readiness")
	}

	telemetry.MarkRegistered(false)
	client.ClearAuthToken()
	deadline = time.Now().Add(time.Second)
	for poller.IsReady() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if poller.IsReady() || telemetry.GlobalReady().Snapshot().CacheProtectionReady {
		t.Fatal("registration loss left protection barrier/readiness open")
	}

	client.SetAuthToken("new-session-token")
	telemetry.MarkRegistered(true)
	deadline = time.Now().Add(time.Second)
	for !poller.IsReady() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if !poller.IsReady() || !telemetry.GlobalReady().Snapshot().CacheProtectionReady {
		t.Fatal("fresh authenticated snapshot did not reopen barrier/readiness")
	}
}

func TestProtectedAssetsBarrier_CleanupBlocksBetweenSessions(t *testing.T) {
	telemetry.ResetForTest()
	t.Cleanup(telemetry.ResetForTest)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(snapshotJSON(10, time.Now().UTC().Format(time.RFC3339Nano), 1, []string{"protected"}))
	}))
	defer server.Close()

	client := api.NewClient(server.URL)
	client.SetAuthToken("session-token")
	poller := NewProtectedAssetsPoller(client, 20*time.Millisecond)
	poller.SnapshotMaxAge = time.Minute
	telemetry.MarkRegistered(true)

	cache, err := workercache.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	path := filepath.Join(t.TempDir(), "evictable.mp4")
	if err := os.WriteFile(path, []byte("cache"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cache.Store(context.Background(), workercache.Entry{AssetKey: "evictable", LocalPath: path, DownloadComplete: true}); err != nil {
		t.Fatal(err)
	}

	jobDone := make(chan struct{}, 2)
	events := make(chan error, 4)
	loop := &workercache.CleanupLoop{
		Cache: cache, Snapshot: poller, Barrier: poller, Interval: time.Hour, JobDone: jobDone,
		OnTick: func(_ workercache.CleanupStats, err error) { events <- err },
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pollDone := make(chan error, 1)
	cleanupDone := make(chan error, 1)
	go func() { pollDone <- poller.Run(ctx) }()
	go func() { cleanupDone <- loop.Run(ctx) }()

	select {
	case err := <-events:
		if err != nil {
			t.Fatalf("initial cleanup: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("initial cleanup did not run after first valid snapshot")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("initial cleanup did not evict file: err=%v", err)
	}

	if err := os.WriteFile(path, []byte("cache"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cache.Store(context.Background(), workercache.Entry{AssetKey: "evictable", LocalPath: path, DownloadComplete: true}); err != nil {
		t.Fatal(err)
	}
	telemetry.MarkRegistered(false)
	client.ClearAuthToken()
	deadline := time.Now().Add(time.Second)
	for poller.IsReady() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if poller.IsReady() {
		t.Fatal("barrier remained open after session loss")
	}
	jobDone <- struct{}{}
	select {
	case err := <-events:
		t.Fatalf("cleanup emitted a tick while the barrier was closed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("cleanup modified cache during disconnected session: %v", err)
	}

	client.SetAuthToken("new-session-token")
	telemetry.MarkRegistered(true)
	deadline = time.Now().Add(time.Second)
	for !poller.IsReady() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !poller.IsReady() {
		t.Fatal("fresh session did not reopen protection barrier")
	}
	jobDone <- struct{}{}
	select {
	case err := <-events:
		if err != nil {
			t.Fatalf("cleanup after session recovery: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup did not run after session recovery")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("recovered cleanup did not evict file: err=%v", err)
	}
	cancel()
	<-pollDone
	<-cleanupDone
}

func TestProtectedAssetsBarrier_503PreservesReadinessAndLastSnapshotAge(t *testing.T) {
	telemetry.ResetForTest()
	t.Cleanup(telemetry.ResetForTest)
	var call int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		call++
		if call == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(snapshotJSON(8, time.Now().UTC().Add(-10*time.Second).Format(time.RFC3339Nano), 1, []string{"preserve"}))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	poller := NewProtectedAssetsPoller(api.NewClient(server.URL), time.Hour)
	poller.SnapshotMaxAge = 2 * time.Minute
	if err := poller.TickOnce(context.Background()); err != nil {
		t.Fatalf("initial valid snapshot: %v", err)
	}
	before := telemetry.GlobalReady().Snapshot()
	if !before.CacheProtectionReady || before.ProtectedSnapshotAgeSeconds < 9 {
		t.Fatalf("initial readiness=%+v, want ready with age >=9s", before)
	}
	good := poller.Snapshot()
	if err := poller.TickOnce(context.Background()); err == nil {
		t.Fatal("503 unexpectedly succeeded")
	}
	after := telemetry.GlobalReady().Snapshot()
	if !after.CacheProtectionReady {
		t.Fatal("503 cleared cache_protection_ready")
	}
	if after.ProtectedSnapshotAgeSeconds < before.ProtectedSnapshotAgeSeconds {
		t.Fatalf("503 made snapshot appear newer: before=%d after=%d", before.ProtectedSnapshotAgeSeconds, after.ProtectedSnapshotAgeSeconds)
	}
	if poller.Snapshot() != good {
		t.Fatal("503 replaced last valid snapshot")
	}
}
