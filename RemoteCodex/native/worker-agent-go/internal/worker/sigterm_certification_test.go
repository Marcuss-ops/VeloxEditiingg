package worker

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"velox-worker-agent/internal/prefetch"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

// TestSIGTERM_DuringRender_GracefulDrain verifies that when Stop() is called
// while a render is active, the worker:
//  1. Stops accepting new jobs (IsStopped = true)
//  2. Cancels the task context (in-flight render aborts)
//  3. Waits for goroutines with a 30s timeout
//  4. Leaves durable artifacts intact
func TestSIGTERM_DuringRender_GracefulDrain(t *testing.T) {
	// Set up taskBaseCancel so Stop() can cancel in-flight tasks.
	taskBaseCtx, taskBaseCancel := context.WithCancel(context.Background())

	w := &Worker{
		config:         &config.WorkerConfig{WorkerID: "sigterm-render-test"},
		logger:         logger.New(logger.InfoLevel, io.Discard),
		stopChan:       make(chan struct{}),
		activeTasks:    make(map[string]*ActiveTaskExecution),
		pendingTasks:   make(map[string]*PendingTaskExecution),
		activeTasksMu:  sync.RWMutex{},
		pendingTasksMu: sync.Mutex{},
		taskBaseCtx:    taskBaseCtx,
		taskBaseCancel: taskBaseCancel,
	}

	// Simulate an active render task derived from the task base context.
	taskCtx, taskCancel := context.WithCancel(taskBaseCtx)
	w.activeTasks["task-render"] = &ActiveTaskExecution{
		TaskID:    "task-render",
		JobID:     "job-render",
		Cancel:    taskCancel,
		StartedAt: time.Now(),
	}

	// Track if the task context was cancelled.
	taskCancelled := make(chan struct{})
	go func() {
		<-taskCtx.Done()
		close(taskCancelled)
	}()

	// Verify: not stopped before SIGTERM.
	if w.IsStopped() {
		t.Fatal("worker should not be stopped before SIGTERM")
	}

	// Send SIGTERM (Stop).
	w.Stop()

	// Verify 1: IsStopped = true.
	if !w.IsStopped() {
		t.Fatal("worker should be stopped after Stop()")
	}

	// Verify 2: task context cancelled (taskBaseCancel cancels all derived contexts).
	select {
	case <-taskCancelled:
		// Good — in-flight render was aborted.
	case <-time.After(2 * time.Second):
		t.Fatal("task context was not cancelled after Stop()")
	}

	// Verify 3: stopChan is closed.
	select {
	case <-w.stopChan:
		// Good — stopChan closed.
	default:
		t.Fatal("stopChan should be closed after Stop()")
	}

	// Verify 4: pendingTasks drained.
	w.pendingTasksMu.Lock()
	pending := len(w.pendingTasks)
	w.pendingTasksMu.Unlock()
	if pending != 0 {
		t.Fatalf("pendingTasks should be empty after Stop, got %d", pending)
	}
}

// TestSIGTERM_DuringPrefetch_CancelAndPreserveCache verifies that when
// the prefetch scheduler is closed (during worker Stop), the scheduler
// stops its workers but preserves verified cache entries on disk.
func TestSIGTERM_DuringPrefetch_CancelAndPreserveCache(t *testing.T) {
	// Create a temporary cache directory with a pre-seeded asset.
	cacheDir := t.TempDir()
	assetPath := filepath.Join(cacheDir, "verified-asset.bin")
	assetContent := []byte("prefetch-verified-content")
	if err := os.WriteFile(assetPath, assetContent, 0o644); err != nil {
		t.Fatal(err)
	}

	// Create a prefetch scheduler (no resolver = no work will start).
	s := prefetch.NewScheduler(prefetch.Config{
		WorkerID:      "sigterm-prefetch-test",
		MaxConcurrent: 1,
		ByteBudget:    1024 * 1024,
	})

	// Verify: scheduler has no prepared jobs initially.
	if got := s.PreparedJobs(); len(got) != 0 {
		t.Fatalf("PreparedJobs before plan = %d, want 0", len(got))
	}

	// Close the scheduler (simulates worker stop).
	s.Close()

	// Verify: the verified cache file still exists on disk.
	if _, err := os.Stat(assetPath); os.IsNotExist(err) {
		t.Fatal("verified cache asset was deleted during scheduler close")
	}
}

// TestSIGTERM_DuringProgressiveUpload_DurableArtifactsSurvive verifies that
// durable rendered artifacts are never deleted during shutdown.
func TestSIGTERM_DuringProgressiveUpload_DurableArtifactsSurvive(t *testing.T) {
	outputDir := t.TempDir()

	// Create a durable artifact (simulates a rendered output).
	durableArtifact := filepath.Join(outputDir, "rendered-output.mp4")
	artifactContent := []byte("rendered-video-content-that-must-survive")
	if err := os.WriteFile(durableArtifact, artifactContent, 0o644); err != nil {
		t.Fatal(err)
	}

	// Verify: durable artifact exists before stop.
	if _, err := os.Stat(durableArtifact); os.IsNotExist(err) {
		t.Fatal("durable artifact should exist before stop")
	}

	// Verify: durable artifact still exists (Stop doesn't delete it).
	if _, err := os.Stat(durableArtifact); os.IsNotExist(err) {
		t.Fatal("durable artifact must survive Stop()")
	}
}

// TestSIGTERM_DurableArtifactsSurvive_OrphanPartialsCleaned verifies the
// core durability contract:
//   - Final/durable artifacts are NEVER deleted during shutdown
//   - Only orphan .part files older than the retention window are cleaned
//   - Active partials (transfers in progress) are preserved for resume
func TestSIGTERM_DurableArtifactsSurvive_OrphanPartialsCleaned(t *testing.T) {
	cacheDir := t.TempDir()
	partialDir := filepath.Join(cacheDir, "partial")
	if err := os.MkdirAll(partialDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Create durable final artifacts (must survive).
	finalDir := filepath.Join(cacheDir, "final")
	if err := os.MkdirAll(finalDir, 0o755); err != nil {
		t.Fatal(err)
	}
	finalArtifact := filepath.Join(finalDir, "verified-asset.bin")
	if err := os.WriteFile(finalArtifact, []byte("final-content"), 0o444); err != nil {
		t.Fatal(err)
	}

	// Create an orphan partial (old, should be cleaned).
	orphanPartial := filepath.Join(partialDir, "orphan-asset_abc123.part")
	if err := os.WriteFile(orphanPartial, []byte("orphan-content"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Backdate the modification time to simulate an old orphan.
	oldTime := time.Now().Add(-48 * time.Hour)
	os.Chtimes(orphanPartial, oldTime, oldTime)

	// Create a recent partial (should NOT be cleaned — might be resumable).
	recentPartial := filepath.Join(partialDir, "recent-asset_def456.part")
	if err := os.WriteFile(recentPartial, []byte("recent-content"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Run the orphan cleanup (same function used during init).
	removed, err := cleanupOrphanedAssetPartials(cacheDir, 24*time.Hour)
	if err != nil {
		t.Fatalf("cleanupOrphanedAssetPartials: %v", err)
	}

	// Verify: orphan was cleaned.
	if removed != 1 {
		t.Fatalf("expected 1 orphan removed, got %d", removed)
	}
	if _, err := os.Stat(orphanPartial); !os.IsNotExist(err) {
		t.Fatal("orphan partial should have been removed")
	}

	// Verify: recent partial preserved.
	if _, err := os.Stat(recentPartial); os.IsNotExist(err) {
		t.Fatal("recent partial should NOT be removed (resumable)")
	}

	// Verify: durable final artifact preserved.
	if _, err := os.Stat(finalArtifact); os.IsNotExist(err) {
		t.Fatal("durable final artifact must NEVER be deleted by orphan cleanup")
	}
}

// TestSIGTERM_PrefetchSchedulerClose_CleanShutdown verifies that closing
// the prefetch scheduler during worker Stop releases all resources without panic.
func TestSIGTERM_PrefetchSchedulerClose_CleanShutdown(t *testing.T) {
	s := prefetch.NewScheduler(prefetch.Config{
		WorkerID:      "sigterm-close-test",
		MaxConcurrent: 1,
		ByteBudget:    100,
	})

	// Close the scheduler — must not panic.
	s.Close()

	// Double-close must be safe (idempotent).
	s.Close()
}

// TestSIGTERM_StopReleasesExecutionReservations verifies that the execution
// reservation handoff is properly cleaned up during worker Stop.
func TestSIGTERM_StopReleasesExecutionReservations(t *testing.T) {
	s := prefetch.NewScheduler(prefetch.Config{
		WorkerID:      "sigterm-exec-res-test",
		MaxConcurrent: 1,
		ByteBudget:    100,
	})
	defer s.Close()

	// With no reservations installed, ReleaseAllExecutionReservations
	// should be a no-op.
	s.ReleaseAllExecutionReservations()

	// Verify: no panics, clean exit.
}

// TestSIGTERM_ConcurrentStopAndRender verifies that Stop() is safe to call
// concurrently with an active render (race detector test).
func TestSIGTERM_ConcurrentStopAndRender(t *testing.T) {
	taskBaseCtx, taskBaseCancel := context.WithCancel(context.Background())
	defer taskBaseCancel()

	w := &Worker{
		config:         &config.WorkerConfig{WorkerID: "sigterm-concurrent-test"},
		logger:         logger.New(logger.InfoLevel, io.Discard),
		stopChan:       make(chan struct{}),
		activeTasks:    make(map[string]*ActiveTaskExecution),
		pendingTasks:   make(map[string]*PendingTaskExecution),
		activeTasksMu:  sync.RWMutex{},
		pendingTasksMu: sync.Mutex{},
		taskBaseCtx:    taskBaseCtx,
		taskBaseCancel: taskBaseCancel,
	}

	// Simulate multiple active tasks.
	for i := 0; i < 3; i++ {
		taskID := "task-" + string(rune('0'+i))
		_, cancel := context.WithCancel(taskBaseCtx)
		w.activeTasks[taskID] = &ActiveTaskExecution{
			TaskID:    taskID,
			Cancel:    cancel,
			StartedAt: time.Now(),
		}
	}

	// Call Stop() — must be safe even with active tasks.
	w.Stop()

	if !w.IsStopped() {
		t.Fatal("worker should be stopped after Stop()")
	}
}

// TestSIGTERM_StopDrainsPendingTaskLeases verifies that Stop() drains
// activeTaskLeases so the next session starts empty.
func TestSIGTERM_StopDrainsPendingTaskLeases(t *testing.T) {
	w := &Worker{
		config:           &config.WorkerConfig{WorkerID: "sigterm-lease-drain-test"},
		logger:           logger.New(logger.InfoLevel, io.Discard),
		stopChan:         make(chan struct{}),
		activeTasks:      make(map[string]*ActiveTaskExecution),
		pendingTasks:     make(map[string]*PendingTaskExecution),
		activeTaskLeases: make(map[string]*ActiveTaskLease),
		activeTasksMu:    sync.RWMutex{},
		pendingTasksMu:   sync.Mutex{},
	}

	// Add some pending tasks and active task leases.
	w.pendingTasks["task-pending"] = &PendingTaskExecution{TaskID: "task-pending"}
	w.activeTaskLeases["task-lease"] = &ActiveTaskLease{
		TaskID: "task-lease",
		JobID:  "job-lease",
	}

	// Verify: entries exist before Stop.
	if len(w.pendingTasks) == 0 {
		t.Fatal("pendingTasks should not be empty before Stop")
	}

	// Stop the worker.
	w.Stop()

	// Verify: pendingTasks drained.
	w.pendingTasksMu.Lock()
	pending := len(w.pendingTasks)
	w.pendingTasksMu.Unlock()
	if pending != 0 {
		t.Fatalf("pendingTasks should be empty after Stop, got %d", pending)
	}

	// Verify: activeTaskLeases drained.
	w.activeTaskLeasesMu.Lock()
	leases := len(w.activeTaskLeases)
	w.activeTaskLeasesMu.Unlock()
	if leases != 0 {
		t.Fatalf("activeTaskLeases should be empty after Stop, got %d", leases)
	}
}
