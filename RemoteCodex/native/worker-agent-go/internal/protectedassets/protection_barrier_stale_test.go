package protectedassets

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"velox-worker-agent/pkg/api"
)

func TestProtectedAssetsPoller_StaleResponseKeepsLastGoodSnapshot(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(snapshotJSON(2, "2026-08-04T12:00:00Z", 1, []string{"fresh"}))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(snapshotJSON(1, "2026-08-04T11:59:00Z", 1, []string{"stale"}))
	}))
	defer server.Close()

	poller := NewProtectedAssetsPoller(api.NewClient(server.URL), time.Hour)
	poller.SnapshotMaxAge = 0
	if err := poller.TickOnce(context.Background()); err != nil {
		t.Fatalf("initial poll: %v", err)
	}
	good := poller.Snapshot()
	if err := poller.TickOnce(context.Background()); err == nil {
		t.Fatal("stale response unexpectedly succeeded")
	}
	if poller.Snapshot() != good {
		t.Fatalf("stale response replaced last-good snapshot")
	}
	if got := poller.Snapshot().ProtectedAssetKeys; len(got) != 1 || got[0] != "fresh" {
		t.Fatalf("snapshot after stale response=%v, want [fresh]", got)
	}
}
