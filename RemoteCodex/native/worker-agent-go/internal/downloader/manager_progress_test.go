package downloader

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestManager_Progress_ThroughputAndETA(t *testing.T) {
	clk := newManualClock(time.Unix(1_700_000_000, 0))
	step1 := make(chan struct{})
	step2 := make(chan struct{})
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, onProgress func(downloadedBytes int64)) (TransferResult, error) {
			select {
			case <-step1:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			onProgress(512)
			select {
			case <-step2:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			onProgress(1024)
			return TransferResult{LocalPath: "/p.mp4", Bytes: 1024, SHA256: "sha"}, nil
		},
	}
	m := NewManager(Config{
		Concurrency:  1,
		Now:          clk.Now,
		PublishBytes: 1, // publish on every progress call (deterministic)
	}, tf)

	done := make(chan error, 1)
	go func() {
		_, err := m.Resolve(context.Background(), DownloadRequest{
			JobID: "j0", AssetKey: "p1", AssetID: "p1", SizeBytes: 2048, Priority: DefaultPriority,
		})
		done <- err
	}()
	waitFor(t, "transfer downloading", func() bool {
		snap, ok := m.Snapshot("p1")
		return ok && snap.State == DownloadRunning
	})
	close(step1)
	waitFor(t, "first progress step reported", func() bool {
		snap, ok := m.Snapshot("p1")
		return ok && snap.BytesDownloaded == 512
	})
	// One second elapses while the second half of the transfer streams.
	clk.Advance(time.Second)
	close(step2)
	waitFor(t, "second progress step reported", func() bool {
		snap, ok := m.Snapshot("p1")
		return ok && snap.BytesDownloaded == 1024
	})
	mid, _ := m.Snapshot("p1")
	if mid.ThroughputBytesPerSecond < 500 || mid.ThroughputBytesPerSecond > 525 {
		t.Fatalf("throughput = %.1f B/s, want ~512 B/s", mid.ThroughputBytesPerSecond)
	}
	if mid.ETASeconds != 2 {
		t.Fatalf("ETA = %d s, want 2 (1024 remaining at 512 B/s)", mid.ETASeconds)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resolve hung")
	}

	snap, ok := m.Snapshot("p1")
	if !ok || snap.State != DownloadReady {
		t.Fatalf("final snapshot = %#v, want READY", snap)
	}
}

// TestManager_ProgressThrottle_ByteThreshold: with a time throttle that never
// fires, subscriber snapshots are published only on >= PublishBytes jumps
// (2048 here) — never once per 32KB-style chunk. The first report and the
// terminal transition still publish immediately.
func TestManager_ProgressThrottle_ByteThreshold(t *testing.T) {
	subReady := make(chan struct{})
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, onProgress func(downloadedBytes int64)) (TransferResult, error) {
			select {
			case <-subReady:
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
			for _, b := range []int64{512, 1024, 1536, 2048, 2560, 3072, 3584, 4096} {
				select {
				case <-ctx.Done():
					return TransferResult{}, ctx.Err()
				default:
				}
				onProgress(b)
			}
			return TransferResult{LocalPath: "/t.mp4", Bytes: 4096, SHA256: "sha"}, nil
		},
	}
	m := NewManager(Config{
		Concurrency:     1,
		Now:             func() time.Time { return time.Unix(1_700_000_000, 0) },
		PublishInterval: time.Hour, // never fires: byte throttle only
		PublishBytes:    2048,
	}, tf)

	done := make(chan error, 1)
	go func() {
		_, err := m.Resolve(context.Background(), DownloadRequest{
			JobID: "j0", AssetKey: "t1", AssetID: "t1", SizeBytes: 4096, Priority: DefaultPriority,
		})
		done <- err
	}()
	waitFor(t, "transfer downloading", func() bool {
		snap, ok := m.Snapshot("t1")
		return ok && snap.State == DownloadRunning
	})
	ch, unsub := m.Subscribe("t1")
	if ch == nil {
		t.Fatal("subscribe returned nil channel")
	}
	close(subReady)

	var downloaded []int64
	for snap := range ch {
		if snap.State == DownloadRunning {
			downloaded = append(downloaded, snap.BytesDownloaded)
		}
		if snap.State.Terminal() {
			break
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("resolve hung")
	}
	unsub()

	// First report publishes (512), then only the 2048-byte jump (2560); the
	// 512-byte intermediates must be suppressed. Terminal READY (4096) is not
	// DOWNLOADING, so it never lands in this slice.
	want := []int64{512, 2560}
	if len(downloaded) != len(want) {
		t.Fatalf("throttled DOWNLOADING publishes = %v, want %v", downloaded, want)
	}
	for i := range want {
		if downloaded[i] != want[i] {
			t.Fatalf("throttled DOWNLOADING publishes = %v, want %v", downloaded, want)
		}
	}
}

// TestManager_CheckpointThrottle: the durable checkpoint hook fires on the
// first report, then per CheckpointBytes (2048), then once for the terminal
// transition — throttled coarser than publishes.
func TestManager_CheckpointThrottle(t *testing.T) {
	var ckptMu sync.Mutex
	var ckptBytes []int64
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, reportCtx context.Context, req DownloadRequest, onProgress func(downloadedBytes int64)) (TransferResult, error) {
			for _, b := range []int64{512, 1024, 1536, 2048, 2560, 3072, 3584, 4096} {
				select {
				case <-ctx.Done():
					return TransferResult{}, ctx.Err()
				default:
				}
				onProgress(b)
			}
			return TransferResult{LocalPath: "/c.mp4", Bytes: 4096, SHA256: "sha"}, nil
		},
	}
	m := NewManager(Config{
		Concurrency:        1,
		Now:                func() time.Time { return time.Unix(1_700_000_000, 0) },
		CheckpointInterval: time.Hour, // never fires: byte throttle only
		CheckpointBytes:    2048,
		OnCheckpoint: func(snap DownloadSnapshot, reportCtx context.Context) {
			ckptMu.Lock()
			ckptBytes = append(ckptBytes, snap.BytesDownloaded)
			ckptMu.Unlock()
		},
	}, tf)

	if _, err := m.Resolve(context.Background(), DownloadRequest{
		JobID: "j0", AssetKey: "c1", AssetID: "c1", SizeBytes: 4096, Priority: DefaultPriority,
	}); err != nil {
		t.Fatalf("resolve: %v", err)
	}

	ckptMu.Lock()
	defer ckptMu.Unlock()
	want := []int64{512, 2560, 4096} // first, 2048-jump, terminal READY
	if len(ckptBytes) != len(want) {
		t.Fatalf("checkpoints = %v, want %v", ckptBytes, want)
	}
	for i := range want {
		if ckptBytes[i] != want[i] {
			t.Fatalf("checkpoints = %v, want %v", ckptBytes, want)
		}
	}
}

// TestManager_JobSnapshot_TwoJobsSharedProgress: two jobs on the same asset
// each get a per-job snapshot reflecting the SAME shared transfer progress
// (one upstream, two snapshots).
