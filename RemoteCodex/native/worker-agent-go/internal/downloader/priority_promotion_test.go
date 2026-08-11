package downloader

import (
	"testing"
	"time"

	"velox-shared/assetref"
)

func TestScheduler_PromotionReordersQueuedPrefetch(t *testing.T) {
	s := newScheduler(1, time.Now)
	s.Start()
	defer s.Close()
	started := make(chan string, 3)
	gate := make(chan struct{})
	if !s.Enqueue(assetref.AssetKey("blocker"), 1000, time.Now(), func() { started <- "blocker"; <-gate }) {
		t.Fatal("enqueue blocker")
	}
	if !s.Enqueue(assetref.AssetKey("prefetch"), 100, time.Now(), func() { started <- "prefetch" }) {
		t.Fatal("enqueue prefetch")
	}
	if !s.Enqueue(assetref.AssetKey("other"), 200, time.Now(), func() { started <- "other" }) {
		t.Fatal("enqueue other")
	}
	if !s.Promote(assetref.AssetKey("prefetch"), PriorityForeground) {
		t.Fatal("promote returned false")
	}
	select {
	case got := <-started:
		if got != "blocker" {
			t.Fatalf("first started=%s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("blocker did not start")
	}
	close(gate)
	select {
	case got := <-started:
		if got != "prefetch" {
			t.Fatalf("promoted item started=%s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("promoted item did not start")
	}
}
