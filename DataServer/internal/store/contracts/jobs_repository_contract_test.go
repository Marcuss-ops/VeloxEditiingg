package contracts

import (
	"context"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
)

// NewSQLiteJobRepositoryFactory returns a fresh SQLite-backed *SQLiteJobRepository
// and its companion AtomicJobTaskCreator (canonical job-create path since PR #8
// dead-code removal).
func NewSQLiteJobRepositoryFactory(t *testing.T) (*store.SQLiteJobRepository, *store.AtomicJobTaskCreator, func()) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "contract_jobs.db")
	dbStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	cleanup := func() { _ = dbStore.Close() }
	return store.NewSQLiteJobRepository(dbStore), store.NewAtomicJobTaskCreator(dbStore), cleanup
}

// makeTestTaskSpec returns a minimal TaskSpec suitable for contract-test
// job creation via AtomicJobTaskCreator.CreateJobWithTask.
func makeTestTaskSpec() *taskgraph.TaskSpec {
	return &taskgraph.TaskSpec{
		ExecutorID: "test-executor",
		Version:    1,
	}
}

// prepareJob is a contract-test helper that creates a job via the canonical
// AtomicJobTaskCreator path.
func prepareJob(t *testing.T, atomic *store.AtomicJobTaskCreator, job *jobs.Job) {
	t.Helper()
	if err := atomic.CreateJobWithTask(context.Background(), job, makeTestTaskSpec(), 0); err != nil {
		t.Fatalf("CreateJobWithTask: %v", err)
	}
}

// pendingJobID returns the JobID of the most recently-created PENDING job, or
// t.Fatal if there isn't one. Centralised so each test doesn't reinvent the
// List-then-pick logic.
func pendingJobID(t *testing.T, repo *store.SQLiteJobRepository, ctx context.Context) string {
	t.Helper()
	pending, err := repo.List(ctx, jobs.Filter{Statuses: []jobs.Status{jobs.Status("PENDING")}, Limit: 1})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(pending) == 0 {
		t.Fatal("no pending jobs available")
	}
	return pending[0].ID
}

// JobRepositoryContract runs the cross-backend test suite for jobs.
// Spec §5: identical behaviour across SQLite (today) and Postgres (PR-2b).
func JobRepositoryContract(t *testing.T, factory func(t *testing.T) (*store.SQLiteJobRepository, *store.AtomicJobTaskCreator, func())) {
	t.Run("CreateJobWithTask then Get round-trip", func(t *testing.T) {
		repo, atomic, cleanup := factory(t)
		defer cleanup()
		ctx := context.Background()
		jobID := "job_roundtrip_" + randSuffix()
		prepareJob(t, atomic, &jobs.Job{
			ID:         jobID,
			VideoName:  "test.mp4",
			ProjectID:  "p1",
			MaxRetries: 3,
			Payload:    `{"job_type":"render"}`,
		})
		got, err := repo.Get(ctx, jobID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got == nil {
			t.Fatal("expected job projection, got nil")
		}
		if got.VideoName != "test.mp4" || got.ProjectID != "p1" || got.MaxRetries != 3 {
			t.Errorf("round-trip mismatch: %+v", got)
		}
		if got.Status != jobs.Status("PENDING") {
			t.Errorf("expected PENDING post-create, got %q", got.Status)
		}
	})

	t.Run("Get missing returns (nil, nil)", func(t *testing.T) {
		repo, _, cleanup := factory(t)
		defer cleanup()
		got, err := repo.Get(context.Background(), "job_does_not_exist")
		if err != nil {
			t.Fatalf("Get missing: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil for missing id, got %+v", got)
		}
	})

	t.Run("ClaimNext returns ErrNoClaimableJob on empty queue", func(t *testing.T) {
		repo, _, cleanup := factory(t)
		defer cleanup()
		got, err := repo.ClaimNext(context.Background(), "worker-1", nil)
		if err == nil {
			t.Fatalf("expected ErrNoClaimableJob on empty queue, got %+v", got)
		}
		if got != nil {
			t.Errorf("expected nil result on empty queue, got %+v", got)
		}
	})

	t.Run("ClaimNext end-to-end persists lease and updates status", func(t *testing.T) {
		repo, atomic, cleanup := factory(t)
		defer cleanup()
		ctx := context.Background()
		jobID := "job_claim_e2e_" + randSuffix()
		prepareJob(t, atomic, &jobs.Job{ID: jobID, MaxRetries: 3})
		got, err := repo.ClaimNext(ctx, "worker-1", nil)
		if err != nil || got == nil {
			t.Fatalf("ClaimNext: %v %v", got, err)
		}
		// Spec §5 atomicity: re-read the persisted row to confirm the claim
		// committed end-to-end (status moved). PR #9: lease_id column dropped —
		// lease identity now lives in result_json + job_attempts.
		persisted, err := repo.Get(ctx, got.JobID)
		if err != nil || persisted == nil {
			t.Fatalf("Get after claim: %v %v", persisted, err)
		}
		if persisted.Status == jobs.Status("PENDING") {
			t.Errorf("expected status to leave PENDING after claim, still %q", persisted.Status)
		}
	})

	t.Run("Transition CAS success advances status and increments revision", func(t *testing.T) {
		repo, atomic, cleanup := factory(t)
		defer cleanup()
		ctx := context.Background()
		jobID := "job_trans_" + randSuffix()
		prepareJob(t, atomic, &jobs.Job{ID: jobID, MaxRetries: 3})
		j, err := repo.Get(ctx, jobID)
		if err != nil || j == nil {
			t.Fatalf("Get pre-transition: %v %v", j, err)
		}
		if err := repo.Transition(ctx, store.TransitionParams{
			JobID:          j.ID,
			ExpectedStatus: store.JobStatusPending,
			NewStatus:      store.JobStatusLeased,
			Revision:       j.Revision,
		}); err != nil {
			t.Fatalf("Transition: %v", err)
		}
		got, err := repo.Get(ctx, j.ID)
		if err != nil || got == nil {
			t.Fatalf("Get after transition: %v %v", got, err)
		}
		if got.Status != jobs.Status("LEASED") {
			t.Errorf("expected LEASED, got %q", got.Status)
		}
		if got.Revision != j.Revision+1 {
			t.Errorf("expected revision %d, got %d", j.Revision+1, got.Revision)
		}
	})

	t.Run("Transition CAS conflict returns ErrTransitionConflict", func(t *testing.T) {
		repo, atomic, cleanup := factory(t)
		defer cleanup()
		ctx := context.Background()
		jobID := "job_cas_" + randSuffix()
		prepareJob(t, atomic, &jobs.Job{ID: jobID, MaxRetries: 3})
		jID := pendingJobID(t, repo, ctx)
		j, err := repo.Get(ctx, jID)
		if err != nil || j == nil {
			t.Fatalf("Get: %v %v", j, err)
		}
		// First transition succeeds.
		if err := repo.Transition(ctx, store.TransitionParams{
			JobID:          j.ID,
			ExpectedStatus: store.JobStatusPending,
			NewStatus:      store.JobStatusLeased,
			Revision:       j.Revision,
		}); err != nil {
			t.Fatalf("first transition: %v", err)
		}
		// Second transition with the stale expectation must reject.
		err = repo.Transition(ctx, store.TransitionParams{
			JobID:          j.ID,
			ExpectedStatus: store.JobStatusPending, // stale: now LEASED
			NewStatus:      store.JobStatusFailed,
			Revision:       j.Revision, // stale: not the new revision
		})
		if err == nil {
			t.Error("expected ErrTransitionConflict, got nil")
		}
	})

	t.Run("ClaimNext round-trip populates Requirements from dedicated columns", func(t *testing.T) {
		repo, atomic, cleanup := factory(t)
		defer cleanup()
		ctx := context.Background()
		jobID := "job_req_fifo_" + randSuffix()
		req := costmodel.JobRequirements{
			ResourceClass:    costmodel.ResourceGPU,
			TemporalMode:     costmodel.TemporalWindowed,
			Deterministic:    true,
			Cacheable:        true,
			MinBandwidthMbps: 100,
		}
		prepareJob(t, atomic, &jobs.Job{
			ID:           jobID,
			MaxRetries:   3,
			Requirements: req,
		})
		got, err := repo.ClaimNext(ctx, "worker-fifo-req", nil)
		if err != nil || got == nil {
			t.Fatalf("ClaimNext: %v %v", got, err)
		}
		if got.JobID != jobID {
			t.Errorf("JobID: want %q got %q", jobID, got.JobID)
		}
		if got.Requirements.ResourceClass != req.ResourceClass {
			t.Errorf("ResourceClass: want %q got %q", req.ResourceClass, got.Requirements.ResourceClass)
		}
		if got.Requirements.TemporalMode != req.TemporalMode {
			t.Errorf("TemporalMode: want %q got %q", req.TemporalMode, got.Requirements.TemporalMode)
		}
		if got.Requirements.Deterministic != req.Deterministic {
			t.Errorf("Deterministic: want %v got %v", req.Deterministic, got.Requirements.Deterministic)
		}
		if got.Requirements.Cacheable != req.Cacheable {
			t.Errorf("Cacheable: want %v got %v", req.Cacheable, got.Requirements.Cacheable)
		}
		if got.Requirements.MinBandwidthMbps != req.MinBandwidthMbps {
			t.Errorf("MinBandwidthMbps: want %v got %v", req.MinBandwidthMbps, got.Requirements.MinBandwidthMbps)
		}
	})

	t.Run("ClaimNextForProfile round-trip populates Requirements from dedicated columns", func(t *testing.T) {
		repo, atomic, cleanup := factory(t)
		defer cleanup()
		ctx := context.Background()
		jobID := "job_req_rank_" + randSuffix()
		req := costmodel.JobRequirements{
			ResourceClass:    costmodel.ResourceCPU,
			TemporalMode:     costmodel.TemporalFrameLocal,
			Deterministic:    false,
			Cacheable:        true,
			MinBandwidthMbps: 0,
		}
		prepareJob(t, atomic, &jobs.Job{
			ID:           jobID,
			MaxRetries:   3,
			Requirements: req,
		})
		// BuildWorkerProfile with nil capabilities yields the CPU +
		// frame_local defaults; matches the on-disk requirements so
		// ClaimNextForProfile's eligibility gate (cost.Eligible) is
		// free to admit the candidate.
		profile := costmodel.BuildWorkerProfile("worker-rank-req", true, false, "online", 0, 4, nil)
		got, err := repo.ClaimNextForProfile(ctx, "worker-rank-req", nil, profile, 20)
		if err != nil || got == nil {
			t.Fatalf("ClaimNextForProfile: %v %v", got, err)
		}
		if got.JobID != jobID {
			t.Errorf("JobID: want %q got %q", jobID, got.JobID)
		}
		if got.Requirements.ResourceClass != req.ResourceClass {
			t.Errorf("ResourceClass: want %q got %q", req.ResourceClass, got.Requirements.ResourceClass)
		}
		if got.Requirements.TemporalMode != req.TemporalMode {
			t.Errorf("TemporalMode: want %q got %q", req.TemporalMode, got.Requirements.TemporalMode)
		}
		if got.Requirements.Deterministic != req.Deterministic {
			t.Errorf("Deterministic: want %v got %v", req.Deterministic, got.Requirements.Deterministic)
		}
		if got.Requirements.Cacheable != req.Cacheable {
			t.Errorf("Cacheable: want %v got %v", req.Cacheable, got.Requirements.Cacheable)
		}
		if got.Requirements.MinBandwidthMbps != req.MinBandwidthMbps {
			t.Errorf("MinBandwidthMbps: want %v got %v", req.MinBandwidthMbps, got.Requirements.MinBandwidthMbps)
		}
	})

	t.Run("ClaimNext on empty Requirements returns DefaultRequirements", func(t *testing.T) {
		repo, atomic, cleanup := factory(t)
		defer cleanup()
		ctx := context.Background()
		jobID := "job_req_legacy_" + randSuffix()
		// PR #6: zero Requirements written to dedicated columns as
		// empty strings / zero values. ClaimNext returns
		// DefaultRequirements() — the safe permissive fallback.
		prepareJob(t, atomic, &jobs.Job{
			ID:           jobID,
			MaxRetries:   3,
			Requirements: costmodel.DefaultRequirements(),
		})
		got, err := repo.ClaimNext(ctx, "worker-legacy-req", nil)
		if err != nil || got == nil {
			t.Fatalf("ClaimNext: %v %v", got, err)
		}
		def := costmodel.DefaultRequirements()
		if got.Requirements.ResourceClass != def.ResourceClass ||
			got.Requirements.TemporalMode != def.TemporalMode ||
			got.Requirements.Deterministic != def.Deterministic ||
			got.Requirements.Cacheable != def.Cacheable ||
			got.Requirements.MinBandwidthMbps != def.MinBandwidthMbps {
			t.Errorf("empty Requirements mismatch: got %+v want %+v", got.Requirements, def)
		}
	})

	t.Run("List with empty statuses returns nil on empty DB", func(t *testing.T) {
		repo, _, cleanup := factory(t)
		defer cleanup()
		got, err := repo.List(context.Background(), jobs.Filter{Statuses: nil, Limit: 10})
		if err != nil {
			t.Fatalf("List nil statuses: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil for empty statuses on empty DB, got %+v", got)
		}
	})
}

// TestJobRepositoryContract_SQLite drives the suite against SQLite.
func TestJobRepositoryContract_SQLite(t *testing.T) {
	JobRepositoryContract(t, NewSQLiteJobRepositoryFactory)
}
