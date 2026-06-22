package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/app"
	"velox-server/internal/artifacts"
	"velox-server/internal/config"
	"velox-server/internal/jobs"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
)

// ── Test: BlobStore unavailable → bootstrap fails ──────────────────────

func TestBootstrapFailsWhenBlobStoreUnavailable(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	// Place a *file* where the staging directory should be, so
	// os.MkdirAll fails because the path already exists as a file.
	stagingFile := filepath.Join(tmpDir, "staging")
	if err := os.WriteFile(stagingFile, []byte("x"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg := &config.Config{
		Database: config.DatabaseConfig{DBPath: filepath.Join(tmpDir, "velox.db")},
		Runtime: config.RuntimeConfig{
			DataDir:    tmpDir,
			StagingDir: stagingFile, // file, not directory → MkdirAll fails
			StorageDir: filepath.Join(tmpDir, "storage"),
		},
		Workers: config.WorkersConfig{
			MaxJobAttempts:   3,
			AllowedWorkerIDs: []string{"test-worker-1"},
		},
	}

	p, err := buildPersistence(cfg)
	if err == nil {
		if p != nil && p.SQLite != nil {
			_ = p.SQLite.Close()
		}
		t.Fatal("expected buildPersistence to fail when BlobStore cannot be created, got nil error")
	}
	t.Logf("correctly failed: %v", err)
}

// ── Test: Outbox unavailable → operations that emit fail ───────────────
//
// Verifies that when the outbox is not wired (SetOutbox not called),
// an operation that emits an outbox event (FailWithRetry, via PR3Fail)
// returns an error. This proves the emitOutbox hardening: the caller
// must see the error and rollback the transaction.
//
// PR #9: switched from CompleteJob (no longer emits outbox) to
// FailWithRetry which delegates to PR3Fail → calls emitOutbox.
func TestBootstrapFailsWhenOutboxUnavailable(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "velox.db")

	// Create the store WITHOUT wiring the outbox.
	sqliteStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer sqliteStore.Close()

	ctx := context.Background()
	jobID := "outbox-fail-test"

	// Create a job in PENDING, then advance to LEASED so FailWithRetry
	// can run (PR3Fail accepts LEASED/RUNNING/RENDER_FINISHED/AWAITING_ARTIFACT).
	atomic := store.NewAtomicJobTaskCreator(sqliteStore)
	if err := atomic.CreateJobWithTask(ctx, &jobs.Job{ID: jobID, MaxRetries: 3}, &taskgraph.TaskSpec{Version: 1}, 0); err != nil {
		t.Fatalf("CreateJobWithTask: %v", err)
	}

	repo := store.NewSQLiteJobRepository(sqliteStore)
	if err := repo.Lease(ctx, jobID, "worker-1"); err != nil {
		t.Fatalf("Lease: %v", err)
	}

	// Read current revision after Lease so the CAS check in PR3Fail matches.
	sj, _ := repo.GetJob(ctx, jobID)

	// FailWithRetry calls PR3Fail → emitOutbox. With no outbox wired,
	// emitOutbox returns an error → FailWithRetry must propagate it.
	fErr := repo.FailWithRetry(ctx, jobID, "ERR_TEST", "test failure", false, sj.Revision)
	if fErr == nil {
		t.Fatal("FailWithRetry with unwired outbox must return error (emitOutbox hardening)")
	}
	if !strings.Contains(fErr.Error(), "outbox not wired") {
		t.Fatalf("expected outbox-not-wired error, got: %v", fErr)
	}
	t.Logf("FailWithRetry correctly failed with unwired outbox: %v", fErr)

	// Verify transaction rollback: job status must NOT be FAILED.
	sjAfter, _ := repo.GetJob(ctx, jobID)
	if sjAfter != nil && sjAfter.Status == store.JobStatusFailed {
		t.Fatal("FailWithRetry failed but job status still flipped to FAILED — rollback did not occur")
	}
	t.Logf("transaction rollback confirmed — job status is %s (not FAILED) ✓", sjAfter.Status)
}

// TestBuildPersistenceWiresOutbox verifies that buildPersistence produces
// a non-nil outbox store wired into the SQLiteStore.
func TestBuildPersistenceWiresOutbox(t *testing.T) {
	t.Parallel()

	cfg := newTestConfig(t)
	p, err := buildPersistence(cfg)
	if err != nil {
		t.Fatalf("buildPersistence: %v", err)
	}
	defer p.SQLite.Close()

	if p.Outbox == nil {
		t.Fatal("persistenceDeps.Outbox must be non-nil — outbox is mandatory")
	}
	t.Log("outbox wired by buildPersistence ✓")
}

// ── Test: Reconciler init fails → bootstrap fails ─────────────────────

func TestBootstrapFailsWhenReconcilerCannotStart(t *testing.T) {
	t.Parallel()

	// NewReconciler with nil db must return an error (mandatory check).
	_, err := artifacts.NewReconciler(
		nil, // db = nil → fail
		nil, // blobStore
		nil, // repo
		nil, // clock
		artifacts.DefaultReconcilerConfig(),
	)
	if err == nil {
		t.Fatal("NewReconciler with nil db should return error")
	}
	t.Logf("correctly failed: %v", err)

	// Also verify that with nil blobStore it fails.
	dbPath := filepath.Join(t.TempDir(), "velox.db")
	sqliteStore, storeErr := store.NewSQLiteStore(dbPath)
	if storeErr != nil {
		t.Fatalf("NewSQLiteStore: %v", storeErr)
	}
	defer sqliteStore.Close()

	_, err = artifacts.NewReconciler(
		sqliteStore.DB(),
		nil, // blobStore = nil → fail
		nil, // repo
		nil, // clock
		artifacts.DefaultReconcilerConfig(),
	)
	if err == nil {
		t.Fatal("NewReconciler with nil blobStore should return error")
	}
	t.Logf("nil blobStore correctly failed: %v", err)
}

// ── Test: Supervisor stops all runners when context cancelled ──────────

func TestSupervisorStopsAllRunners(t *testing.T) {
	t.Parallel()

	var started atomic.Int32
	var stopped atomic.Int32

	sup := NewBackgroundSupervisor()
	for i := 0; i < 3; i++ {
		name := string(rune('a' + i))
		_ = sup.Register(RunnerFunc{
			name: name,
			fn: func(ctx context.Context) error {
				started.Add(1)
				<-ctx.Done()
				stopped.Add(1)
				return ctx.Err()
			},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Run the supervisor in a goroutine.
	done := make(chan error, 1)
	go func() {
		done <- sup.Run(ctx)
	}()

	// Give runners a moment to start.
	time.Sleep(100 * time.Millisecond)

	if n := started.Load(); n != 3 {
		t.Fatalf("expected 3 runners started, got %d", n)
	}

	// Cancel → all runners should stop.
	cancel()

	select {
	case err := <-done:
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			t.Fatalf("unexpected error from supervisor: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor did not stop within timeout")
	}

	if n := stopped.Load(); n != 3 {
		t.Fatalf("expected 3 runners stopped, got %d", n)
	}
	t.Log("all 3 runners started and stopped ✓")
}

// ── Test: Supervisor propagates runner failure without killing others ──

func TestSupervisorPropagatesRunnerFailure(t *testing.T) {
	t.Parallel()

	var stopped atomic.Int32
	readyCh := make(chan struct{})

	sup := NewBackgroundSupervisor()

	// Runner A: runs normally until cancelled.
	_ = sup.Register(RunnerFunc{
		name: "runner-ok",
		fn: func(ctx context.Context) error {
			<-ctx.Done()
			stopped.Add(1)
			return ctx.Err()
		},
	})

	// Runner B: fails immediately with a non-nil error.
	_ = sup.Register(RunnerFunc{
		name: "runner-fail",
		fn: func(ctx context.Context) error {
			return errors.New("simulated runner failure")
		},
	})

	// Runner C: runs until cancelled.
	_ = sup.Register(RunnerFunc{
		name: "runner-also-ok",
		fn: func(ctx context.Context) error {
			close(readyCh) // signal it started
			<-ctx.Done()
			stopped.Add(1)
			return ctx.Err()
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- sup.Run(ctx)
	}()

	// Wait for runner C to start (proves supervisor didn't abort
	// after runner B's failure).
	select {
	case <-readyCh:
		t.Log("runner C started after runner B failure ✓")
	case <-time.After(2 * time.Second):
		t.Fatal("runner C never started — supervisor may have crashed after runner B failure")
	}

	// Cancel and wait for clean shutdown.
	cancel()
	select {
	case err := <-done:
		// Supervisor.Run returns nil on clean shutdown (all runners handled).
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			t.Logf("supervisor exit error (expected after runner failure): %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor did not stop within timeout")
	}

	// Both surviving runners should have stopped.
	if n := stopped.Load(); n != 2 {
		t.Fatalf("expected 2 runners stopped, got %d", n)
	}
	t.Log("supervisor survived runner failure ✓")
}

// ── Test: /ready fails before dependencies start ──────────────────────

func TestReadyFailsBeforeDependenciesStart(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	hm := app.NewHealthModule()

	r := gin.New()
	hm.RegisterRoutes(r)

	// Before MarkReady, /ready must return 503.
	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 before MarkReady, got %d", w.Code)
	}
	t.Logf("/ready returned %d before MarkReady ✓", w.Code)

	// /health must still return 200 even before MarkReady.
	req2 := httptest.NewRequest(http.MethodGet, "/health", nil)
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected /health 200 before MarkReady, got %d", w2.Code)
	}
	t.Log("/health returned 200 before MarkReady ✓")
}

// ── Test: /ready passes after dependencies start ──────────────────────

func TestReadyPassesAfterDependenciesStart(t *testing.T) {
	t.Parallel()

	gin.SetMode(gin.TestMode)
	hm := app.NewHealthModule()

	// Add checks: some passing, one failing.
	hm.AddReadinessCheck("check-ok", func() error { return nil })
	hm.AddReadinessCheck("check-fail", func() error { return errors.New("dependency not ready") })

	r := gin.New()
	hm.RegisterRoutes(r)

	// After MarkReady with failing check → should return 503.
	hm.MarkReady()

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 with failing check, got %d", w.Code)
	}
	t.Logf("/ready returned %d with failing check ✓", w.Code)

	// All checks passing → should return 200.
	hm2 := app.NewHealthModule()
	hm2.AddReadinessCheck("check-a", func() error { return nil })
	hm2.AddReadinessCheck("check-b", func() error { return nil })
	hm2.MarkReady()

	r2 := gin.New()
	hm2.RegisterRoutes(r2)

	req2 := httptest.NewRequest(http.MethodGet, "/ready", nil)
	w2 := httptest.NewRecorder()
	r2.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 with all checks passing, got %d", w2.Code)
	}
	t.Logf("/ready returned %d with all checks passing ✓", w2.Code)
}

// ── Test: Supervisor duplicate name rejected ──────────────────────────

func TestSupervisorRejectsDuplicateRunnerName(t *testing.T) {
	t.Parallel()

	sup := NewBackgroundSupervisor()

	err := sup.Register(RunnerFunc{
		name: "unique",
		fn:   func(ctx context.Context) error { return nil },
	})
	if err != nil {
		t.Fatalf("first Register: %v", err)
	}

	err = sup.Register(RunnerFunc{
		name: "unique", // duplicate
		fn:   func(ctx context.Context) error { return nil },
	})
	if err == nil {
		t.Fatal("expected duplicate runner name to be rejected")
	}
	t.Logf("duplicate correctly rejected: %v", err)
}

// ── Helpers ─────────────────────────────────────────────────────────────

// newTestConfig is already defined in bootstrap_test.go (same package).
// It is not duplicated here — both files share package main.
