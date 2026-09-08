package downloader

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// fakeTransferer is the byte-pipeline fake used by the manager tests. Each
// test wires its own check/transfer behaviour; zero-value means always-miss
// check and a transfer that completes instantly with a deterministic path.
type fakeTransferer struct {
	check    func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (CacheCheckResult, error)
	transfer func(ctx context.Context, reportCtx context.Context, req DownloadRequest, onProgress func(downloadedBytes int64)) (TransferResult, error)

	transferCalls atomic.Int32
}

func (f *fakeTransferer) Check(ctx context.Context, reportCtx context.Context, req DownloadRequest) (CacheCheckResult, error) {
	if f.check != nil {
		return f.check(ctx, reportCtx, req)
	}
	return CacheCheckResult{}, nil
}

func (f *fakeTransferer) Transfer(ctx context.Context, reportCtx context.Context, req DownloadRequest, onProgress func(downloadedBytes int64)) (TransferResult, error) {
	f.transferCalls.Add(1)
	if f.transfer != nil {
		return f.transfer(ctx, reportCtx, req, onProgress)
	}
	return TransferResult{LocalPath: "/fake/" + string(req.AssetKey) + ".mp4", Bytes: req.SizeBytes, SHA256: "sha"}, nil
}

// newTestManager builds a manager with a fixed clock and one dispatcher.
func newTestManager(t *testing.T, tf *fakeTransferer) *Manager {
	t.Helper()
	m := NewManager(Config{Concurrency: 1, Now: func() time.Time { return time.Unix(1_700_000_000, 0) }}, tf)
	t.Cleanup(m.Close)
	return m
}

// waitFor polls a predicate until it holds or the test times out.
func waitFor(t *testing.T, what string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestManager_CacheHit: an asset already verified must resolve as CACHE_HIT,
// with zero bytes downloaded, no upstream request (Transfer never called) and
// an immediate READY.
