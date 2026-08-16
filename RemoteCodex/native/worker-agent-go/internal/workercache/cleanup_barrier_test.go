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
	var ticksMu sync.Mutex
	var ticks int
	readTicks := func() int {
		ticksMu.Lock()
		defer ticksMu.Unlock()
		return ticks
	}
	loop := &CleanupLoop{
		Cache:    cache,
		Policy:   CleanupPolicy{CleanupInterval: time.Hour},
		Interval: time.Hour,
		Barrier:  barrier,
		OnTick: func(PressureEvictionStats, error) {
			ticksMu.Lock()
			ticks++
			ticksMu.Unlock()
		},
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
	if got := readTicks(); got != 0 {
		t.Fatalf("cleanup ticked before barrier opened: %d", got)
	}
	close(barrier.release)
	// The first tick should run immediately after the barrier opens.
	deadline := time.After(time.Second)
	for readTicks() == 0 {
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
	if got := readTicks(); got != 1 {
		t.Fatalf("ticks=%d, want exactly one initial tick after barrier", got)
	}
}

func TestCleanupLoop_RunBarrierCancellationIsFailSafe(t *testing.T) {
	cache, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	barrier := &testProtectionBarrier{started: make(chan struct{}), release: make(chan struct{})}
	var ticksMu sync.Mutex
	ticks := 0
	loop := &CleanupLoop{Cache: cache, Barrier: barrier, Interval: time.Hour, OnTick: func(PressureEvictionStats, error) {
		ticksMu.Lock()
		ticks++
		ticksMu.Unlock()
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- loop.Run(ctx) }()
	<-barrier.started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup loop did not stop after barrier cancellation")
	}
	// The goroutine must leave WaitReady; no cleanup tick may happen.
	ticksMu.Lock()
	got := ticks
	ticksMu.Unlock()
	if got != 0 {
		t.Fatalf("ticks=%d, want 0 when barrier wait is canceled", got)
	}
}
