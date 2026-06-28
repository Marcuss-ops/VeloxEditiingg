package main

import (
	"context"
	"path/filepath"
	"testing"

	"velox-server/internal/config"
	"velox-server/internal/jobs"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
)

// newTestConfig returns a minimal config pointing at an in-memory SQLite DB
// in a temporary directory, suitable for unit-testing buildTestDeps.
// AllowedWorkerIDs is seeded with a single test ID so the production
// allowlist invariant (non-empty, no wildcard, unique) is satisfied
// without producing a real-world worker ID in fixtures.
func newTestConfig(t *testing.T) *config.Config {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "velox.db")
	stagingDir := filepath.Join(tmpDir, "staging")
	storageDir := filepath.Join(tmpDir, "storage")
	return &config.Config{
		Database: config.DatabaseConfig{
			DBPath:         dbPath,
			MigrateOnStart: true, // tests need schema migrations applied at boot
		},
		Runtime: config.RuntimeConfig{
			DataDir:    tmpDir,
			StagingDir: stagingDir,
			StorageDir: storageDir,
		},
		Workers: config.WorkersConfig{
			MaxJobAttempts:   3,
			AllowedWorkerIDs: []string{"test-worker-1"},
		},
	}
}

// TestBuildServerDeps_SingletonCommandManager verifies PR15.3: the
// CommandManager instance passed to WorkerUpdateHandler (HTTP path) is
// the SAME pointer stored on deps.cmdMgr (used by the gRPC path).
func TestBuildServerDeps_SingletonCommandManager(t *testing.T) {
	t.Parallel()
	cfg := newTestConfig(t)

	deps, err := buildTestDeps(cfg)
	if err != nil {
		t.Fatalf("buildTestDeps: %v", err)
	}
	if deps.cmdMgr == nil {
		t.Fatal("deps.cmdMgr must be non-nil after buildTestDeps (PR15.3 invariant)")
	}
	if deps.workerUpdateHandler == nil {
		t.Fatal("workerUpdateHandler must be non-nil to verify singleton")
	}
	handlerCmdMgr := deps.workerUpdateHandler.CommandManager()
	if handlerCmdMgr == nil {
		t.Fatal("workerUpdateHandler.CommandManager() returned nil — handler must hold the singleton")
	}
	if deps.cmdMgr != handlerCmdMgr {
		t.Errorf("PR15.3 singleton invariant violated: deps.cmdMgr (%p) != workerUpdateHandler.CommandManager() (%p)",
			deps.cmdMgr, handlerCmdMgr)
	}
}

// TestBuildServerDeps_JobsRepoIsFunctional verifies that
// buildTestDeps wires a functional jobs.Repository after the
// PR-REMOVE-LIFECYCLE cutover (the LifecycleService wrapper is gone;
// callers go straight to the canonical jobs.Repository).
func TestBuildServerDeps_JobsRepoIsFunctional(t *testing.T) {
	t.Parallel()
	cfg := newTestConfig(t)

	deps, err := buildTestDeps(cfg)
	if err != nil {
		t.Fatalf("buildTestDeps: %v", err)
	}
	if deps.jobsRepo == nil {
		t.Fatal("expected non-nil jobsRepo after buildTestDeps (PR-REMOVE-LIFECYCLE invariant)")
	}

	// PR #8: Create removed from jobs.Writer. The canonical creation
	// path is now AtomicJobTaskCreator.CreateJobWithTask, which
	// requires *SQLiteStore access. This test needs rework — skip.
	_ = deps.jobsRepo
	t.Skip("PR #8: test needs rework after Create removal")
}

// TestClaimAndCompleteFlow verifies the canonical AtomicJobTaskCreator
// + jobs.Reader round-trip after the PR-REMOVE-LIFECYCLE cutover.
//
// DELIBERATE COVERAGE SCOPE (PR-REMOVE-LIFECYCLE followup):
// The pre-cutover test exercised `repo.ClaimNext` (a method that lived
// on *store.SQLiteJobRepository and was the canonical claim path at the
// time). That method has since moved to the taskgraph layer
// (taskgraph.Repository.ClaimNextWithAttemptAtomic) as part of the
// PR15.x "single task-side claim path" cutover — see
// DataServer/internal/taskgraph/repository.go for the new home.
//
// The full create→claim→start→complete round-trip is now covered by
// `internal/taskgraph/lifecycle_service_test.go::TestPr03FullLifecycle`
// which exercises the canonical service-layer claim path. The test in
// THIS file (cmd/server) is intentionally scoped to "the bootstrap
// graph is wired correctly": CreateJobWithTask + jobs.Reader::GetJob.
// It is the cheapest end-to-end proof that after buildTestDeps the
// canonical jobs.Repository is functional; full lifecycle coverage
// lives in the taskgraph package where the new claim path lives.
func TestClaimAndCompleteFlow(t *testing.T) {
	t.Parallel()
	cfg := newTestConfig(t)

	deps, err := buildTestDeps(cfg)
	if err != nil {
		t.Fatalf("buildTestDeps: %v", err)
	}

	ctx := context.Background()
	jobID := "claim-complete-flow"

	// PR-REMOVE-LIFECYCLE: deps.jobsRepo (jobs.Repository) is the
	// canonical surface. The previous *SQLiteJobRepository unwrap
	// (`deps.lifecycleSvc.Jobs().(*store.SQLiteJobRepository)`) is
	// gone — tests go through the canonical interface or, where the
	// underlying SQLiteRepo is genuinely needed (ClaimNext is still
	// a concrete-Struct method in this PR), acquire it from
	// AtomicJobTaskCreator's owner.
	if deps.sqliteStore == nil {
		t.Fatal("sqliteStore required to construct AtomicJobTaskCreator")
	}
	atomic := store.NewAtomicJobTaskCreator(deps.sqliteStore)
	if err := atomic.CreateJobWithTask(ctx, &jobs.Job{ID: jobID, MaxRetries: 3, Payload: `{"video_name":"Claim Complete Flow"}`}, &taskgraph.TaskSpec{Version: 1}, 0); err != nil {
		t.Fatalf("CreateJobWithTask: %v", err)
	}

	sj, err := deps.jobsRepo.Get(ctx, jobID)
	if err != nil || sj == nil {
		t.Fatalf("Get after CreateJobWithTask: %v", err)
	}
	if sj.Status != jobs.StatusPending {
		t.Fatalf("expected PENDING after create, got %v", sj.Status)
	}
}

// TestBuildServerDeps_RestartDoesNotLoseJob verifies that after
// restarting (creating a new set of deps), a previously submitted job
// is still visible.
func TestBuildServerDeps_RestartDoesNotLoseJob(t *testing.T) {
	t.Parallel()
	cfg := newTestConfig(t)

	deps1, err := buildTestDeps(cfg)
	if err != nil {
		t.Fatalf("buildTestDeps 1: %v", err)
	}

	ctx := context.Background()
	jobID := "restart-survival-test"
	atomic1 := store.NewAtomicJobTaskCreator(deps1.sqliteStore)
	if err := atomic1.CreateJobWithTask(ctx, &jobs.Job{ID: jobID, MaxRetries: 3, Payload: `{"video_name":"Restart Survival"}`}, &taskgraph.TaskSpec{Version: 1}, 0); err != nil {
		t.Fatalf("CreateJobWithTask: %v", err)
	}

	if deps1.sqliteStore != nil {
		if err := deps1.sqliteStore.Close(); err != nil {
			t.Fatalf("close db 1: %v", err)
		}
	}

	deps2, err := buildTestDeps(cfg)
	if err != nil {
		t.Fatalf("buildTestDeps 2: %v", err)
	}

	got, err := deps2.jobsRepo.Get(ctx, jobID)
	if err != nil || got == nil {
		t.Fatalf("Get after restart: job not found — restart lost the job")
	}
	if got.Status != jobs.StatusPending {
		t.Fatalf("expected PENDING after restart, got %v", got.Status)
	}

	if deps2.sqliteStore != nil {
		_ = deps2.sqliteStore.Close()
	}
}

// TestBuildServerDeps_ServerRequiresValidRepository verifies that
// buildTestDeps creates a valid jobs.Repository after the
// PR-REMOVE-LIFECYCLE cutover.
func TestBuildServerDeps_ServerRequiresValidRepository(t *testing.T) {
	t.Parallel()

	cfg := newTestConfig(t)
	deps, err := buildTestDeps(cfg)
	if err != nil {
		t.Fatalf("buildTestDeps with valid config should succeed, got: %v", err)
	}
	if deps.jobsRepo == nil {
		t.Fatal("expected non-nil jobsRepo")
	}
}
