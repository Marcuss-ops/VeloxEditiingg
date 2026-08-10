package main

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

func TestDiskWatcherStartIsSingleFlight(t *testing.T) {
	watchDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	stopped := make(chan struct{})
	var starts atomic.Int32
	watcher := &diskWatcher{
		onStart: func() {
			if starts.Add(1) == 1 {
				close(started)
			}
		},
		onStop: func() {
			close(stopped)
		},
	}
	cfg := &config.WorkerConfig{MinDiskFreeMB: 1}
	log := logger.New(logger.ErrorLevel, os.Stderr)

	const callers = 32
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			watcher.start(ctx, cfg, filepath.Clean(watchDir), log)
		}()
	}
	wg.Wait()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("disk watcher did not start")
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("disk watcher started %d times, want 1", got)
	}

	cancel()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("disk watcher did not stop after context cancellation")
	}

	// Once is bound to this runtime object's lifetime: a later call after
	// cancellation must not create a second watcher.
	watcher.start(context.Background(), cfg, watchDir, log)
	if got := starts.Load(); got != 1 {
		t.Fatalf("disk watcher restarted %d times after cancellation, want 1", got)
	}
}
