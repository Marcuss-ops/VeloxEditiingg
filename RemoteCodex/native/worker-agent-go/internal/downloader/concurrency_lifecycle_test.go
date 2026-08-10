package downloader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"velox-shared/assetref"
)

func TestTransferRegistry_ConcurrentGetOrCreateAndPrune(t *testing.T) {
	registry := newTransferRegistry()
	const sharedCallers = 64

	var created atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < sharedCallers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = registry.GetOrCreate("shared", func() *Transfer {
				created.Add(1)
				return newTransfer(
					context.Background(),
					assetref.AssetKey("shared"),
					DownloadRequest{AssetKey: "shared"},
					context.Background(),
					time.Now,
					"shared-transfer",
					1,
				)
			})
		}()
	}
	wg.Wait()

	if got := created.Load(); got != 1 {
		t.Fatalf("factory calls = %d, want exactly 1", got)
	}
	shared := registry.Get("shared")
	if shared == nil {
		t.Fatal("shared transfer missing after concurrent GetOrCreate")
	}

	const terminalTransfers = 128
	const maxRetained = 8
	start := make(chan struct{})
	var errorsMu sync.Mutex
	var callbackErrors []error
	recordError := func(err error) {
		errorsMu.Lock()
		callbackErrors = append(callbackErrors, err)
		errorsMu.Unlock()
	}

	for i := 0; i < terminalTransfers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			key := assetref.AssetKey(fmt.Sprintf("terminal-%03d", i))
			transfer, _ := registry.GetOrCreate(key, func() *Transfer {
				transfer := newTransfer(
					context.Background(),
					key,
					DownloadRequest{AssetKey: key},
					context.Background(),
					time.Now,
					string(key),
					int64(i),
				)
				transfer.finish(TransferResult{LocalPath: "/verified"}, nil)
				return transfer
			})
			if transfer == nil || !transfer.isTerminal() {
				recordError(fmt.Errorf("transfer %q was not terminal after creation", key))
			}
		}(i)
	}

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 64; j++ {
				registry.Each(func(_ assetref.AssetKey, transfer *Transfer) {
					if transfer == nil {
						recordError(fmt.Errorf("Each returned nil transfer"))
					}
				})
				registry.PruneTerminal(maxRetained)
			}
		}()
	}
	close(start)
	wg.Wait()

	if len(callbackErrors) != 0 {
		t.Fatalf("concurrent registry callbacks reported %d errors: %v", len(callbackErrors), callbackErrors[0])
	}
	registry.PruneTerminal(maxRetained)

	if got := registry.Get("shared"); got != shared {
		t.Fatal("PruneTerminal evicted or replaced the live shared transfer")
	}
	terminalCount := 0
	registry.Each(func(_ assetref.AssetKey, transfer *Transfer) {
		if transfer.isTerminal() {
			terminalCount++
		}
	})
	if terminalCount > maxRetained {
		t.Fatalf("terminal transfers retained = %d, want <= %d", terminalCount, maxRetained)
	}
}

func TestManager_ConcurrentWaiterCancellationKeepsRemainingWaiters(t *testing.T) {
	release := make(chan struct{})
	tf := &fakeTransferer{
		transfer: func(ctx context.Context, _ context.Context, req DownloadRequest, _ func(int64)) (TransferResult, error) {
			select {
			case <-release:
				return TransferResult{LocalPath: "/shared/concurrent.mp4", Bytes: req.SizeBytes, SHA256: "sha"}, nil
			case <-ctx.Done():
				return TransferResult{}, ctx.Err()
			}
		},
	}
	m := newTestManager(t, tf)

	const waiterCount = 12
	type result struct {
		asset DownloadedAsset
		err   error
	}
	results := make([]result, waiterCount)
	contexts := make([]context.Context, waiterCount)
	cancels := make([]context.CancelFunc, waiterCount)
	start := make(chan struct{})
	var cancelledWaiters atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < waiterCount; i++ {
		contexts[i], cancels[i] = context.WithCancel(context.Background())
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i].asset, results[i].err = m.Resolve(contexts[i], DownloadRequest{
				JobID:     fmt.Sprintf("job-%02d", i),
				TaskID:    fmt.Sprintf("task-%02d", i),
				AssetKey:  "shared-concurrent",
				AssetID:   "shared-concurrent",
				SizeBytes: 4096,
				Priority:  DefaultPriority,
			})
			if results[i].err != nil && errors.Is(results[i].err, context.Canceled) {
				cancelledWaiters.Add(1)
			}
		}(i)
	}
	close(start)

	waitFor(t, "all concurrent waiters", func() bool {
		snapshot, ok := m.Snapshot("shared-concurrent")
		return ok && snapshot.SharedWaiters == waiterCount
	})
	waitFor(t, "one shared transfer", func() bool { return tf.transferCalls.Load() == 1 })

	for i := 0; i < waiterCount; i += 2 {
		cancels[i]()
	}
	waitFor(t, "remaining waiters", func() bool {
		snapshot, ok := m.Snapshot("shared-concurrent")
		return ok && snapshot.SharedWaiters == waiterCount/2 && cancelledWaiters.Load() == waiterCount/2
	})

	close(release)
	wg.Wait()
	for i := 0; i < waiterCount; i++ {
		if i%2 == 0 {
			if results[i].err == nil || !errors.Is(results[i].err, context.Canceled) {
				t.Errorf("cancelled waiter %d error = %v, want context.Canceled", i, results[i].err)
			}
			continue
		}
		if results[i].err != nil {
			t.Errorf("surviving waiter %d error = %v", i, results[i].err)
		}
		if results[i].asset.LocalPath != "/shared/concurrent.mp4" {
			t.Errorf("surviving waiter %d path = %q", i, results[i].asset.LocalPath)
		}
	}
	if got := tf.transferCalls.Load(); got != 1 {
		t.Fatalf("transfer calls = %d, want exactly 1", got)
	}
	final, ok := m.Snapshot("shared-concurrent")
	if !ok || final.State != DownloadReady {
		t.Fatalf("final snapshot = %#v, want READY", final)
	}
	if final.SharedWaiters != 0 {
		t.Fatalf("final shared waiters = %d, want 0", final.SharedWaiters)
	}
	for _, cancel := range cancels {
		cancel()
	}
}

func TestManager_TerminalTransferIsReplacedOnNextResolve(t *testing.T) {
	var checks atomic.Int32
	var transfers atomic.Int32
	tf := &fakeTransferer{
		check: func(context.Context, context.Context, DownloadRequest) (CacheCheckResult, error) {
			checks.Add(1)
			return CacheCheckResult{}, nil
		},
		transfer: func(_ context.Context, _ context.Context, req DownloadRequest, _ func(int64)) (TransferResult, error) {
			generation := transfers.Add(1)
			return TransferResult{
				LocalPath: fmt.Sprintf("/verified/%d.mp4", generation),
				Bytes:     req.SizeBytes,
				SHA256:    assetref.ContentHash(fmt.Sprintf("sha-%d", generation)),
			}, nil
		},
	}
	m := newTestManager(t, tf)
	request := DownloadRequest{JobID: "reuse-job", AssetKey: "reuse-asset", AssetID: "reuse-asset", SizeBytes: 512}

	first, err := m.Resolve(context.Background(), request)
	if err != nil {
		t.Fatalf("first resolve: %v", err)
	}
	firstTransfer := m.registry.Get(request.AssetKey)
	firstSnapshot, ok := m.Snapshot(request.AssetKey)
	if !ok || firstTransfer == nil || firstSnapshot.State != DownloadReady {
		t.Fatalf("first transfer state = %#v, want READY", firstSnapshot)
	}

	second, err := m.Resolve(context.Background(), request)
	if err != nil {
		t.Fatalf("second resolve: %v", err)
	}
	secondTransfer := m.registry.Get(request.AssetKey)
	secondSnapshot, ok := m.Snapshot(request.AssetKey)
	if !ok || secondTransfer == nil || secondSnapshot.State != DownloadReady {
		t.Fatalf("second transfer state = %#v, want READY", secondSnapshot)
	}
	if firstTransfer == secondTransfer {
		t.Fatal("terminal transfer was reused; expected a fresh transfer")
	}
	if secondSnapshot.TransferGeneration <= firstSnapshot.TransferGeneration {
		t.Fatalf("transfer generation moved backwards: first=%d second=%d", firstSnapshot.TransferGeneration, secondSnapshot.TransferGeneration)
	}
	if first.LocalPath != "/verified/1.mp4" || second.LocalPath != "/verified/2.mp4" {
		t.Fatalf("resolved paths = %q, %q; want distinct transfer outcomes", first.LocalPath, second.LocalPath)
	}
	if checks.Load() != 2 || transfers.Load() != 2 {
		t.Fatalf("cache checks/transfers = %d/%d, want 2/2", checks.Load(), transfers.Load())
	}
}

func TestTransfer_ConcurrentSubscriberUnsubscribeAndTerminalCleanup(t *testing.T) {
	transfer := newTransfer(
		context.Background(),
		assetref.AssetKey("subscriber-cleanup"),
		DownloadRequest{AssetKey: "subscriber-cleanup", SizeBytes: 1024},
		context.Background(),
		time.Now,
		"subscriber-transfer",
		1,
	)
	transfer.setState(DownloadRunning)

	terminalChannel, terminalUnsubscribe := transfer.subscribe()
	const churners = 32
	const subscriptionsPerChurner = 100
	start := make(chan struct{})
	continueChurn := make(chan struct{})
	var churnersReady atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < churners; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			churnersReady.Add(1)
			for j := 0; j < subscriptionsPerChurner; j++ {
				if j == 1 {
					<-continueChurn
				}
				channel, unsubscribe := transfer.subscribe()
				if channel == nil {
					continue
				}
				select {
				case <-channel:
				default:
				}
				unsubscribe()
				unsubscribe()
			}
		}()
	}
	close(start)
	waitFor(t, "all subscriber churners started", func() bool {
		return churnersReady.Load() == churners
	})

	transfer.mu.Lock()
	remainingBeforeTerminal := len(transfer.subscribers)
	transfer.mu.Unlock()
	if remainingBeforeTerminal != 1 {
		t.Fatalf("subscriber registry before terminal publish = %d, want only the held subscriber", remainingBeforeTerminal)
	}

	terminalDone := make(chan struct{})
	go func() {
		transfer.finish(TransferResult{LocalPath: "/verified/subscriber-cleanup.mp4", Bytes: 1024}, nil)
		close(terminalDone)
	}()
	close(continueChurn)
	wg.Wait()
	<-terminalDone

	select {
	case snapshot, open := <-terminalChannel:
		if !open {
			t.Fatal("held subscriber closed without receiving terminal snapshot")
		}
		if snapshot.State != DownloadReady {
			t.Fatalf("terminal snapshot state = %s, want READY", snapshot.State)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("held subscriber did not receive terminal snapshot")
	}
	if _, open := <-terminalChannel; open {
		t.Fatal("held subscriber channel remained open after terminal publish")
	}
	terminalUnsubscribe()
	terminalUnsubscribe()

	transfer.mu.Lock()
	remaining := len(transfer.subscribers)
	transfer.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("subscriber registry retained %d channels after terminal cleanup", remaining)
	}
}
