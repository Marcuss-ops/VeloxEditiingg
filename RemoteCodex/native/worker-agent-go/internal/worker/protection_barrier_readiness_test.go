package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"velox-worker-agent/internal/telemetry"
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
