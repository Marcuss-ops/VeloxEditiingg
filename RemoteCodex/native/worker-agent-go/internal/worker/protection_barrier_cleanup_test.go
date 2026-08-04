package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"velox-worker-agent/internal/workercache"
	"velox-worker-agent/pkg/api"
)

func TestProtectedAssetsErrorsKeepCleanupBehindBarrier(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch atomic.AddInt32(&calls, 1) {
		case 1:
			w.WriteHeader(http.StatusUnauthorized)
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()

	poller := NewProtectedAssetsPoller(api.NewClient(server.URL), time.Hour)
	poller.SnapshotMaxAge = 0
	cache, err := workercache.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	var ticks atomic.Int32
	loop := &workercache.CleanupLoop{
		Cache:    cache,
		Snapshot: poller,
		Barrier:  poller,
		Interval: time.Hour,
		OnTick: func(workercache.CleanupStats, error) {
			ticks.Add(1)
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- loop.Run(ctx) }()

	for i := 0; i < 2; i++ {
		if err := poller.TickOnce(context.Background()); err == nil {
			t.Fatalf("poll %d unexpectedly succeeded", i+1)
		}
	}
	if poller.IsReady() {
		t.Fatal("401/503 responses must not open the protection barrier")
	}

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cleanup Run returned nil after barrier cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup Run did not stop while waiting on barrier")
	}
	if got := ticks.Load(); got != 0 {
		t.Fatalf("cleanup ticks=%d after 401/503; want zero", got)
	}
}
