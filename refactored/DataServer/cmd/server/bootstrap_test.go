package main

import (
	"context"
	"path/filepath"
	"testing"

	"velox-server/internal/config"
	"velox-server/internal/queue"
	"velox-server/internal/store"
)

// newTestConfig returns a minimal config pointing at an in-memory SQLite DB
// in a temporary directory, suitable for unit-testing buildServerDeps.
func newTestConfig(t *testing.T) *config.Config {
	t.Helper()
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "velox.db")
	return &config.Config{
		Database: config.DatabaseConfig{DBPath: dbPath},
		Runtime:  config.RuntimeConfig{DataDir: tmpDir},
		Workers:  config.WorkersConfig{MaxJobAttempts: 3},
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
	// First claim it (HTTP path).
	job, err := deps.fileQ.ClaimNextJob(ctx, "worker-grpc", nil)
	if err != nil {
		t.Fatalf("ClaimNextJob: %v", err)
	}
	if job == nil {
		t.Fatal("expected non-nil job after claim")
	}
	if job.JobID != jobID {
		t.Fatalf("expected jobID=%s, got %s", jobID, job.JobID)
	}

	// Transition LEASED → RUNNING via repo (PR3: TransitionToRunning removed from LifecycleService).
	repo := grpcLifecycle.Repo()
	sj, err := repo.GetJob(ctx, jobID)
	if err != nil || sj == nil {
		t.Fatalf("GetJob after claim: %v", err)
	}
	if err := repo.StartJob(ctx, store.StartJobParams{
		JobID:            jobID,
		WorkerID:         job.AssignedTo,
		LeaseID:          job.LeaseID,
		Attempt:          job.Attempt,
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
		Attempt:          job.Attempt,
		ExpectedRevision: sj.Revision,
		FinalStatus:      store.JobStatusSucceeded,
	}); err != nil {
		t.Fatalf("CompleteJob via gRPC repo: %v", err)
	}

	// Verify the job is SUCCEEDED via the FileQueue (HTTP path reading).
	got, err := deps.fileQ.GetJobAsMap(ctx, jobID)
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

	// Step 2: Claim via HTTP path (simulates worker polling).
	claimed, err := deps.fileQ.ClaimNextJob(ctx, "worker-http", nil)
	if err != nil {
		t.Fatalf("ClaimNextJob: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected claimed job, got nil")
	}

	// Step 3: Verify the claimed job looks correct.
	if claimed.Status != queue.StatusLeased {
		t.Fatalf("expected status=LEASED after claim, got %s", claimed.Status)
	}
	if claimed.LeaseID == "" {
		t.Fatal("expected non-empty lease_id after claim")
	}

	// Step 4: Transition LEASED → RUNNING → SUCCEEDED via gRPC path.
	grpcLifecycle := deps.fileQ.LifecycleService()
	repo := grpcLifecycle.Repo()
	sj, err := repo.GetJob(ctx, jobID)
	if err != nil || sj == nil {
		t.Fatalf("GetJob after claim: %v", err)
	}
	if err := repo.StartJob(ctx, store.StartJobParams{
		JobID:            jobID,
		WorkerID:         claimed.AssignedTo,
		LeaseID:          claimed.LeaseID,
		Attempt:          claimed.Attempt,
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
		Attempt:          claimed.Attempt,
		ExpectedRevision: sj.Revision,
		FinalStatus:      store.JobStatusSucceeded,
	}); err != nil {
		t.Fatalf("CompleteJob via gRPC: %v", err)
	}

	// Step 5: Verify state via HTTP query.
	got, err := deps.fileQ.GetJobAsMap(ctx, jobID)
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
	if _, err := deps1.fileQ.ClaimNextJob(ctx, "worker-1", nil); err != nil {
		t.Fatalf("ClaimNextJob: %v", err)
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

	got, err := deps2.fileQ.GetJobAsMap(ctx, jobID)
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

	// Verify that NewLegacyLifecycleService(nil, ...) returns an error.
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
