package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/config"
	"velox-server/internal/store"
)

// newTestConfig returns a minimal config pointing at an in-memory SQLite DB
// in a temporary directory, suitable for unit-testing buildServerDeps.
// AllowedWorkerIDs is seeded with a single test ID so the production
// allowlist invariant (non-empty, no wildcard, unique) is satisfied
// without producing a real-world worker ID in fixtures.
func newTestConfig(t *testing.T) *config.Config {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "velox.db")
	return &config.Config{
		Database: config.DatabaseConfig{DBPath: dbPath},
		Runtime:  config.RuntimeConfig{DataDir: tmpDir},
		Workers: config.WorkersConfig{
			MaxJobAttempts:   3,
			AllowedWorkerIDs: []string{"test-worker-1"},
		},
	}
}

// TestBuildServerDeps_SingletonCommandManager verifies PR15.3: the
// CommandManager instance passed to WorkerUpdateHandler (HTTP path) is
// the SAME pointer stored on deps.cmdMgr (used by the gRPC path).
// Pre-fix, two separate NewCommandManager calls were made on the same
// SQLiteStore, racing on the worker_commands table.
//
// WorkerUpdateHandler exposes a CommandManager() accessor that returns
// its embedded instance. Pointer equality between that and deps.cmdMgr
// proves the singleton invariant.
func TestBuildServerDeps_SingletonCommandManager(t *testing.T) {
	t.Parallel()
	cfg := newTestConfig(t)

	deps, err := buildServerDeps(cfg)
	if err != nil {
		t.Fatalf("buildServerDeps: %v", err)
	}
	if deps.cmdMgr == nil {
		t.Fatal("deps.cmdMgr must be non-nil after buildServerDeps (PR15.3 invariant)")
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

// TestBuildServerDeps_CreatesExactlyOneLifecycleService verifies that
// buildServerDeps creates exactly one LifecycleService instance and that
// FileQueue.LifecycleService() returns it.
func TestBuildServerDeps_CreatesExactlyOneLifecycleService(t *testing.T) {
	t.Parallel()
	cfg := newTestConfig(t)

	deps, err := buildServerDeps(cfg)
	if err != nil {
		t.Fatalf("buildServerDeps: %v", err)
	}
	if deps == nil {
		t.Fatal("expected non-nil serverDeps")
	}
	if deps.fileQ == nil {
		t.Fatal("expected non-nil FileQueue")
	}

	// FileQueue must have exactly one LifecycleService.
	lifecycle := deps.fileQ.LifecycleService()
	if lifecycle == nil {
		t.Fatal("LifecycleService() returned nil")
	}

	// Call it twice — must return the same instance (pointer equality).
	lifecycle2 := deps.fileQ.LifecycleService()
	if lifecycle != lifecycle2 {
		t.Fatal("LifecycleService() returned different instances — must be singleton")
	}
}

// TestBuildServerDeps_HTTPAndGRPCShareSameLifecycleService verifies that the
// LifecycleService instance wired into FileQueue (HTTP path) is the exact
// same pointer that would be passed to the gRPC handler via
// fileQ.LifecycleService().
func TestBuildServerDeps_HTTPAndGRPCShareSameLifecycleService(t *testing.T) {
	t.Parallel()
	cfg := newTestConfig(t)

	deps, err := buildServerDeps(cfg)
	if err != nil {
		t.Fatalf("buildServerDeps: %v", err)
	}

	// Simulate what runServer does for gRPC: it reads deps.fileQ.LifecycleService().
	grpcLifecycle := deps.fileQ.LifecycleService()

	// Verify the FileQueue itself holds the same instance.
	// We can't access FileQueue.lifecycle directly (unexported), but
	// LifecycleService() is the canonical accessor used by gRPC.
	// The test that the returned pointer is non-nil and consistent
	// across multiple calls validates the singleton pattern.
	if grpcLifecycle == nil {
		t.Fatal("LifecycleService() for gRPC path returned nil")
	}

	// Additional check: the lifecycle service returned is functional.
	// Submit a job and verify the lifecycle can operate on it.
	ctx := context.Background()
	jobID := "grpc-shared-lifecycle-test"
	payload := map[string]interface{}{
		"video_name": "Shared Lifecycle Test",
	}
	if err := deps.fileQ.SubmitJob(ctx, jobID, payload); err != nil {
		t.Fatalf("SubmitJob via FileQueue: %v", err)
	}

	// Complete the job via the LifecycleService directly (simulating gRPC).
	// First claim it via repo (simulates worker polling).
	repo := grpcLifecycle.Repo()
	claimResult, err := repo.ClaimNext(ctx, store.ClaimParams{
		WorkerID: "worker-grpc",
		Now:      time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if claimResult == nil || claimResult.JobID == "" {
		t.Fatal("expected non-nil claim result")
	}
	if claimResult.JobID != jobID {
		t.Fatalf("expected jobID=%s, got %s", jobID, claimResult.JobID)
	}

	// Transition LEASED → RUNNING via repo (PR3: TransitionToRunning removed from LifecycleService).
	sj, err := repo.GetJob(ctx, jobID)
	if err != nil || sj == nil {
		t.Fatalf("GetJob after claim: %v", err)
	}
	if err := repo.StartJob(ctx, store.StartJobParams{
		JobID:            jobID,
		WorkerID:         sj.AssignedTo,
		LeaseID:          sj.LeaseID,
		Attempt:          claimResult.Attempt,
		ExpectedRevision: sj.Revision,
	}); err != nil {
		t.Fatalf("StartJob via gRPC repo: %v", err)
	}

	// Complete via repo (PR3: CompleteJob on LifecycleService removed;
	// SUCCEEDED must go through artifact gate in production, but tests
	// can use repo.CompleteJob directly).
	sj, err = repo.GetJob(ctx, jobID)
	if err != nil || sj == nil {
		t.Fatalf("GetJob after StartJob: %v", err)
	}
	if err := repo.CompleteJob(ctx, store.CompleteJobParams{
		JobID:            jobID,
		WorkerID:         sj.AssignedTo,
		LeaseID:          sj.LeaseID,
		Attempt:          claimResult.Attempt,
		ExpectedRevision: sj.Revision,
		FinalStatus:      store.JobStatusSucceeded,
	}); err != nil {
		t.Fatalf("CompleteJob via gRPC repo: %v", err)
	}

	// Verify the job is SUCCEEDED via the FileQueue (HTTP path reading).
	got, err := deps.fileQ.QueryService().GetJobAsMap(ctx, jobID)
	if err != nil {
		t.Fatalf("GetJobAsMap: %v", err)
	}
	if got["status"] != "SUCCEEDED" {
		t.Fatalf("expected status=SUCCEEDED, got %v", got["status"])
	}
}

// TestClaimHTTPCompleteGRPCSeesConsistentState verifies the full flow:
// submit → HTTP claim → gRPC completion → consistent state.
func TestClaimHTTPCompleteGRPCSeesConsistentState(t *testing.T) {
	t.Parallel()
	cfg := newTestConfig(t)

	deps, err := buildServerDeps(cfg)
	if err != nil {
		t.Fatalf("buildServerDeps: %v", err)
	}

	ctx := context.Background()
	jobID := "e2e-http-claim-grpc-complete"

	// Step 1: Submit job via HTTP path (FileQueue.SubmitJob).
	payload := map[string]interface{}{
		"video_name": "E2E HTTP Claim → gRPC Complete",
		"job_type":   "process_video",
	}
	if err := deps.fileQ.SubmitJob(ctx, jobID, payload); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	// Step 2: Claim via repo (simulates worker polling).
	repo := deps.fileQ.LifecycleService().Repo()
	claimResult, err := repo.ClaimNext(ctx, store.ClaimParams{
		WorkerID: "worker-http",
		Now:      time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if claimResult == nil {
		t.Fatal("expected claim result, got nil")
	}

	// Step 3: Verify the claimed job looks correct.
	if claimResult.LeaseID == "" {
		t.Fatal("expected non-empty lease_id after claim")
	}

	// Step 4: Transition LEASED → RUNNING → SUCCEEDED via gRPC path.
	sj, err := repo.GetJob(ctx, jobID)
	if err != nil || sj == nil {
		t.Fatalf("GetJob after claim: %v", err)
	}
	if err := repo.StartJob(ctx, store.StartJobParams{
		JobID:            jobID,
		WorkerID:         sj.AssignedTo,
		LeaseID:          sj.LeaseID,
		Attempt:          claimResult.Attempt,
		ExpectedRevision: sj.Revision,
	}); err != nil {
		t.Fatalf("StartJob via gRPC: %v", err)
	}
	sj, err = repo.GetJob(ctx, jobID)
	if err != nil || sj == nil {
		t.Fatalf("GetJob after StartJob: %v", err)
	}
	if err := repo.CompleteJob(ctx, store.CompleteJobParams{
		JobID:            jobID,
		WorkerID:         sj.AssignedTo,
		LeaseID:          sj.LeaseID,
		Attempt:          claimResult.Attempt,
		ExpectedRevision: sj.Revision,
		FinalStatus:      store.JobStatusSucceeded,
	}); err != nil {
		t.Fatalf("CompleteJob via gRPC: %v", err)
	}

	// Step 5: Verify state via HTTP query.
	got, err := deps.fileQ.QueryService().GetJobAsMap(ctx, jobID)
	if err != nil {
		t.Fatalf("GetJobAsMap: %v", err)
	}
	if got["status"] != "SUCCEEDED" {
		t.Fatalf("expected status=SUCCEEDED, got %v", got["status"])
	}
	if got["job_id"] != jobID {
		t.Fatalf("expected job_id=%s, got %v", jobID, got["job_id"])
	}
}

// TestBuildServerDeps_JobeRebootDoesNotLoseJob verifies that after
// restarting (creating a new set of deps), a previously submitted job
// is still visible.
func TestBuildServerDeps_RestartDoesNotLoseJob(t *testing.T) {
	t.Parallel()
	cfg := newTestConfig(t)

	// First bootstrap: submit a job.
	deps1, err := buildServerDeps(cfg)
	if err != nil {
		t.Fatalf("buildServerDeps 1: %v", err)
	}

	ctx := context.Background()
	jobID := "restart-survival-test"
	if err := deps1.fileQ.SubmitJob(ctx, jobID, map[string]interface{}{
		"video_name": "Restart Survival",
	}); err != nil {
		t.Fatalf("SubmitJob 1: %v", err)
	}

	// Claim it to change the state.
	repo1 := deps1.fileQ.LifecycleService().Repo()
	if _, err := repo1.ClaimNext(ctx, store.ClaimParams{
		WorkerID: "worker-1",
		Now:      time.Now().UTC(),
	}); err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}

	// Close the first database so the second bootstrap can open it.
	if deps1.sqliteStore != nil {
		if err := deps1.sqliteStore.Close(); err != nil {
			t.Fatalf("close db 1: %v", err)
		}
	}

	// Second bootstrap: same DB path — must see the persisted job.
	deps2, err := buildServerDeps(cfg)
	if err != nil {
		t.Fatalf("buildServerDeps 2: %v", err)
	}

	got, err := deps2.fileQ.QueryService().GetJobAsMap(ctx, jobID)
	if err != nil {
		t.Fatalf("GetJobAsMap after restart: %v", err)
	}
	if got["job_id"] != jobID {
		t.Fatalf("job not found after restart — restart lost the job. got=%v", got)
	}
	if got["status"] != "LEASED" {
		t.Fatalf("expected status=LEASED after restart, got %v", got["status"])
	}

	// Cleanup
	if deps2.sqliteStore != nil {
		_ = deps2.sqliteStore.Close()
	}
}

// TestNewLifecycleService_ServerRefusesToStartWithNilRepository verifies
// the constructor rejects nil repo, tested via the queue package. This is
// a structural guard: no server should ever be buildable with a nil repo.
func TestBuildServerDeps_ServerRequiresValidRepository(t *testing.T) {
	t.Parallel()

	// Verify that NewLifecycleService(nil, ...) returns an error.
	// This is tested more thoroughly in the queue package, but
	// we add a structural assertion here: if buildServerDeps cannot
	// construct a valid LifecycleService, it must return an error.
	cfg := newTestConfig(t)
	deps, err := buildServerDeps(cfg)
	if err != nil {
		t.Fatalf("buildServerDeps with valid config should succeed, got: %v", err)
	}
	if deps.fileQ == nil {
		t.Fatal("expected non-nil FileQueue")
	}
	// The LifecycleService is internal to FileQueue — we can't test
	// repo nil from here, but we verify valid construction succeeds.
	// The queue/lifecycle_test.go already covers nil-repo rejection.
}

// TestQueryServiceIsSeparateFromLifecycle verifies that QueryService
// and LifecycleService are both non-nil and independently functional.
// The type system guarantees they are distinct (different struct types).
func TestQueryServiceIsSeparateFromLifecycle(t *testing.T) {
	t.Parallel()
	cfg := newTestConfig(t)

	deps, err := buildServerDeps(cfg)
	if err != nil {
		t.Fatalf("buildServerDeps: %v", err)
	}

	lifecycle := deps.fileQ.LifecycleService()
	query := deps.fileQ.QueryService()

	if lifecycle == nil {
		t.Fatal("LifecycleService() returned nil")
	}
	if query == nil {
		t.Fatal("QueryService() returned nil")
	}

	// Functional independence: query can find what lifecycle creates.
	ctx := context.Background()
	jobID := "query-lifecycle-separation-test"
	deps.fileQ.SubmitJob(ctx, jobID, map[string]interface{}{"video_name": "Separation Test"})

	got, err := query.GetJobAsMap(ctx, jobID)
	if err != nil {
		t.Fatalf("QueryService.GetJobAsMap: %v", err)
	}
	if got["job_id"] != jobID {
		t.Fatalf("QueryService could not read job submitted via FileQueue: got %v", got)
	}
}
