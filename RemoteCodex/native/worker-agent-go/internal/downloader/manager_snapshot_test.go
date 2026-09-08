package downloader

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"velox-shared/assetref"
)

func TestManager_JobSnapshot_TwoJobsSharedProgress(t *testing.T) {
	progress := make(chan struct{})
	release := make(chan struct{})
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, onProgress func(downloadedBytes int64)) (TransferResult, error) {
			select {
			case <-progress:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			onProgress(1024)
			select {
			case <-release:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			return TransferResult{LocalPath: "/s.mp4", Bytes: 4096, SHA256: "sha"}, nil
		},
	}
	m := newTestManager(t, tf)

	results := make([]error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, results[i] = m.Resolve(context.Background(), DownloadRequest{
				JobID: fmt.Sprintf("job-%d", i), TaskID: fmt.Sprintf("task-%d", i),
				AssetKey: "sh", AssetID: "sh", SizeBytes: 4096, Priority: DefaultPriority,
			})
		}(i)
	}
	waitFor(t, "two waiters on one transfer", func() bool {
		snap, ok := m.Snapshot("sh")
		return ok && snap.SharedWaiters == 2
	})
	close(progress)
	waitFor(t, "shared progress visible", func() bool {
		snap, _ := m.Snapshot("sh")
		return snap.BytesDownloaded == 1024
	})

	for _, jobID := range []string{"job-0", "job-1"} {
		js := m.JobSnapshot(jobID)
		if js.AssetsTotal != 1 {
			t.Fatalf("%s assets = %d, want 1", jobID, js.AssetsTotal)
		}
		if js.BytesTotal != 4096 || js.BytesDownloaded != 1024 {
			t.Fatalf("%s bytes = %d/%d, want 1024/4096", jobID, js.BytesDownloaded, js.BytesTotal)
		}
		if js.ProgressPercent != 25 {
			t.Fatalf("%s progress = %.1f%%, want 25%%", jobID, js.ProgressPercent)
		}
		if js.ActiveTransfers != 1 {
			t.Fatalf("%s active = %d, want 1", jobID, js.ActiveTransfers)
		}
	}
	close(release)
	wg.Wait()
	for i := range results {
		if results[i] != nil {
			t.Fatalf("resolve[%d]: %v", i, results[i])
		}
	}
}

// TestManager_JobSnapshot_ByteWeighted: job progress is weighted on bytes, not
// file counts. One 1 MiB asset READY plus one 5 GiB asset still downloading
// must report ~0.02%, never 50%.
func TestManager_JobSnapshot_ByteWeighted(t *testing.T) {
	bigRelease := make(chan struct{})
	tf := &fakeTransferer{
		check: func(ctx context.Context, reportCtx context.Context, req DownloadRequest) (CacheCheckResult, error) {
			if req.AssetID == "small" {
				return CacheCheckResult{CacheHit: true, LocalPath: "/c/small.mp4"}, nil
			}
			return CacheCheckResult{}, nil
		},
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, _ func(downloadedBytes int64)) (TransferResult, error) {
			select {
			case <-bigRelease:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			return TransferResult{LocalPath: "/big.mp4", Bytes: req.SizeBytes, SHA256: "sha"}, nil
		},
	}
	m := newTestManager(t, tf)

	const smallBytes = int64(1 << 20) // 1 MiB
	const bigBytes = int64(5) << 30   // 5 GiB
	results := make([]error, 2)
	var wg sync.WaitGroup
	for i, id := range []string{"small", "big"} {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			size := smallBytes
			if id == "big" {
				size = bigBytes
			}
			_, results[i] = m.Resolve(context.Background(), DownloadRequest{
				JobID: "job-0", AssetKey: assetref.AssetKey(id), AssetID: id, SizeBytes: size, Priority: DefaultPriority,
			})
		}(i, id)
	}
	waitFor(t, "big asset downloading with waiter", func() bool {
		snap, ok := m.Snapshot("big")
		return ok && snap.State == DownloadRunning && snap.SharedWaiters == 1
	})

	js := m.JobSnapshot("job-0")
	if js.AssetsTotal != 2 {
		t.Fatalf("assets total = %d, want 2", js.AssetsTotal)
	}
	if js.AssetsReady != 1 || js.AssetsDownloading != 1 {
		t.Fatalf("ready=%d downloading=%d, want 1/1", js.AssetsReady, js.AssetsDownloading)
	}
	if js.BytesTotal != smallBytes+bigBytes {
		t.Fatalf("bytes_total = %d, want %d", js.BytesTotal, smallBytes+bigBytes)
	}
	if js.BytesDownloaded != smallBytes {
		t.Fatalf("bytes_downloaded = %d, want %d (only the small ready asset counts)", js.BytesDownloaded, smallBytes)
	}
	if js.ProgressPercent >= 1 {
		t.Fatalf("progress = %.4f%%, want < 1%% (byte-weighted, not file-count)", js.ProgressPercent)
	}

	close(bigRelease)
	wg.Wait()
	for i := range results {
		if results[i] != nil {
			t.Fatalf("resolve[%d]: %v", i, results[i])
		}
	}
}

// TestManager_ContentHashSingleFlight_DifferentAssetKeys proves the
// single-flight-by-content_hash rule: two jobs on DIFFERENT asset keys but the
// same verified SHA-256 coalesce onto one shared transfer (one upstream), and
// each waiter receives the same local path.
func TestManager_ContentHashSingleFlight_DifferentAssetKeys(t *testing.T) {
	release := make(chan struct{})
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, _ func(downloadedBytes int64)) (TransferResult, error) {
			select {
			case <-release:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			return TransferResult{LocalPath: "/shared/content-blob.mp4", Bytes: req.SizeBytes, SHA256: assetref.ContentHash("shared")}, nil
		},
	}
	m := newTestManager(t, tf)

	const sharedSHA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	type out struct {
		asset DownloadedAsset
		err   error
	}
	results := make([]out, 2)
	keys := []string{"asset-a", "asset-b"}
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i].asset, results[i].err = m.Resolve(context.Background(), DownloadRequest{
				JobID: fmt.Sprintf("job-%d", i), TaskID: fmt.Sprintf("task-%d", i),
				AssetKey: assetref.AssetKey(keys[i]), AssetID: keys[i],
				SHA256: assetref.ContentHash(sharedSHA), SizeBytes: 2048, Priority: DefaultPriority,
			})
		}(i)
	}

	// Both waiters must be on ONE live transfer before the gate is released,
	// regardless of which asset key won the registry slot.
	waitFor(t, "two waiters on one content-hash transfer", func() bool {
		m.registry.mu.Lock()
		defer m.registry.mu.Unlock()
		live, waiters := 0, 0
		for _, tr := range m.registry.transfers {
			if tr.isTerminal() {
				continue
			}
			live++
			tr.mu.Lock()
			waiters += len(tr.waiters)
			tr.mu.Unlock()
		}
		return live == 1 && waiters == 2
	})
	close(release)
	wg.Wait()

	for i := range results {
		if results[i].err != nil {
			t.Fatalf("resolve[%d]: %v", i, results[i].err)
		}
		if results[i].asset.LocalPath != "/shared/content-blob.mp4" {
			t.Fatalf("resolve[%d] path = %q, want shared path", i, results[i].asset.LocalPath)
		}
	}
	if got := tf.transferCalls.Load(); got != 1 {
		t.Fatalf("transfer calls = %d, want exactly 1 (content-hash single-flight)", got)
	}
}
