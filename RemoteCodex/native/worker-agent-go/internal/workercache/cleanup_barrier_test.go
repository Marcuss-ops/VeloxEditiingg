package workercache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type testProtectionBarrier struct {
	mu      sync.Mutex
	ready   bool
	started chan struct{}
	release chan struct{}
}

func (b *testProtectionBarrier) WaitReady(ctx context.Context) error {
	closeOnce(b.started)
	select {
	case <-b.release:
		b.mu.Lock()
		b.ready = true
		b.mu.Unlock()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *testProtectionBarrier) IsReady() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ready
}

func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func TestCleanupLoop_RunWaitsForProtectionBarrier(t *testing.T) {
	cache, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	barrier := &testProtectionBarrier{started: make(chan struct{}), release: make(chan struct{})}
	var ticks int
	loop := &CleanupLoop{
		Cache:    cache,
		Policy:   CleanupPolicy{CleanupInterval: time.Hour},
		Interval: time.Hour,
		Barrier:  barrier,
		OnTick:   func(CleanupStats, error) { ticks++ },
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- loop.Run(ctx) }()

	select {
	case <-barrier.started:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not wait on protection barrier")
	}
	time.Sleep(20 * time.Millisecond)
	if ticks != 0 {
		t.Fatalf("cleanup ticked before barrier opened: %d", ticks)
	}
	close(barrier.release)
	// The first tick should run immediately after the barrier opens.
	deadline := time.After(time.Second)
	for ticks == 0 {
		select {
		case <-deadline:
			t.Fatal("cleanup did not run after barrier opened")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup loop did not stop after cancellation")
	}
	if ticks != 1 {
		t.Fatalf("ticks=%d, want exactly one initial tick after barrier", ticks)
	}
}

func TestCleanupLoop_RunBarrierCancellationIsFailSafe(t *testing.T) {
	cache, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	barrier := &testProtectionBarrier{started: make(chan struct{}), release: make(chan struct{})}
	ticks := 0
	loop := &CleanupLoop{Cache: cache, Barrier: barrier, Interval: time.Hour, OnTick: func(CleanupStats, error) { ticks++ }}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = loop.Run(ctx) }()
	<-barrier.started
	cancel()
	// The goroutine must leave WaitReady; no cleanup tick may happen.
	time.Sleep(30 * time.Millisecond)
	if ticks != 0 {
		t.Fatalf("ticks=%d, want 0 when barrier wait is canceled", ticks)
	}
}
