package worker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"velox-worker-agent/pkg/api"
)

func TestProtectedAssetsBarrier_WaitsUntilFirstValidSnapshot(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(snapshotJSON(2, "2026-08-04T12:00:00Z", 1, []string{"protected-1"}))
	}))
	defer srv.Close()

	poller := NewProtectedAssetsPoller(api.NewClient(srv.URL), time.Hour)
	poller.SnapshotMaxAge = 0
	if poller.IsReady() {
		t.Fatal("barrier must start closed")
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := poller.WaitReady(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitReady before snapshot = %v, want deadline exceeded", err)
	}
	if err := poller.TickOnce(context.Background()); err == nil {
		t.Fatal("401 poll unexpectedly succeeded")
	}
	if poller.IsReady() {
		t.Fatal("401 must not open barrier")
	}
	if err := poller.TickOnce(context.Background()); err != nil {
		t.Fatalf("valid retry: %v", err)
	}
	if !poller.IsReady() {
		t.Fatal("first valid snapshot must open barrier")
	}
	readyCtx, readyCancel := context.WithTimeout(context.Background(), time.Second)
	defer readyCancel()
	if err := poller.WaitReady(readyCtx); err != nil {
		t.Fatalf("WaitReady after valid snapshot: %v", err)
	}
}

func TestProtectedAssetsBarrier_503KeepsLastGoodSnapshot(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(snapshotJSON(1, "2026-08-04T12:00:00Z", 1, []string{"keep-me"}))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	poller := NewProtectedAssetsPoller(api.NewClient(srv.URL), time.Hour)
	poller.SnapshotMaxAge = 0
	if err := poller.TickOnce(context.Background()); err != nil {
		t.Fatalf("initial valid poll: %v", err)
	}
	good := poller.Snapshot()
	if good == nil || !poller.IsReady() {
		t.Fatal("valid initial snapshot did not open barrier")
	}
	if err := poller.TickOnce(context.Background()); err == nil {
		t.Fatal("503 poll unexpectedly succeeded")
	}
	if poller.Snapshot() != good {
		t.Fatal("503 must preserve last-good snapshot pointer")
	}
	if !poller.IsReady() {
		t.Fatal("later 503 must not close barrier")
	}
}
