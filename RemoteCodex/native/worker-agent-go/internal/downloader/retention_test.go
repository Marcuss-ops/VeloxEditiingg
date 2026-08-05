package downloader

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestTransferRegistry_PruneTerminalKeepsLiveAndBoundsHistory(t *testing.T) {
	registry := newTransferRegistry()
	for i := 0; i < 5; i++ {
		t := newTransfer(context.Background(), fmt.Sprintf("terminal-%d", i), DownloadRequest{AssetKey: "terminal"}, context.Background(), func() time.Time { return time.Unix(int64(i+1), 0) }, "x", int64(i))
		t.finish(TransferResult{LocalPath: "/verified"}, nil)
		registry.transfers[t.Key] = t
	}
	live := newTransfer(context.Background(), "live", DownloadRequest{AssetKey: "live"}, context.Background(), time.Now, "live", 99)
	registry.transfers[live.Key] = live

	registry.PruneTerminal(2)
	terminalCount := 0
	livePresent := false
	registry.Each(func(key string, t *Transfer) {
		if key == live.Key {
			livePresent = true
		}
		if t.isTerminal() {
			terminalCount++
		}
	})
	if !livePresent {
		t.Fatal("PruneTerminal evicted a live transfer")
	}
	if terminalCount != 2 {
		t.Fatalf("terminal transfers retained = %d, want 2", terminalCount)
	}
}
