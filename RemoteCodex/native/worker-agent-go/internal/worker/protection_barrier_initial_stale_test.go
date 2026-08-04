package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"velox-worker-agent/pkg/api"
)

func TestProtectedAssetsPoller_InitialStaleSnapshotDoesNotOpenBarrier(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(snapshotJSON(1, time.Now().UTC().Add(-10*time.Minute).Format(time.RFC3339Nano), 1, []string{"stale"}))
	}))
	defer server.Close()

	poller := NewProtectedAssetsPoller(api.NewClient(server.URL), time.Hour)
	poller.SnapshotMaxAge = time.Minute
	if err := poller.TickOnce(context.Background()); err == nil {
		t.Fatal("stale first snapshot unexpectedly succeeded")
	}
	if poller.IsReady() {
		t.Fatal("stale first snapshot must not open protection barrier")
	}
	if poller.Snapshot() != nil {
		t.Fatal("stale first snapshot must not be retained")
	}
}
