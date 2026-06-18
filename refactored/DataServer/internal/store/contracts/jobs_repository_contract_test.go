package contracts

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/store"
)

// NewSQLiteJobRepositoryFactory returns a fresh SQLite-backed JobRepository.
func NewSQLiteJobRepositoryFactory(t *testing.T) (store.JobRepository, func()) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "contract_jobs.db")
	dbStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	cleanup := func() { _ = dbStore.Close() }
	return store.NewSQLiteJobRepository(dbStore), cleanup
}

// pendingJobID returns the JobID of the most recently-created PENDING job, or
// t.Fatal if there isn't one. Centralised so each test doesn't reinvent the
// ListByStatus-then-pick logic.
func pendingJobID(t *testing.T, repo store.JobRepository, ctx context.Context) string {
	t.Helper()
	pending, err := repo.ListByStatus(ctx, []store.JobStatus{store.JobStatusPending}, 1)
	if err != nil {
		t.Fatalf("ListByStatus: %v", err)
	}
	if len(pending) == 0 {
		t.Fatal("no pending jobs available")
	}
	return pending[0].JobID
}

// JobRepositoryContract runs the cross-backend test suite for jobs.
// Spec §5: identical behaviour across SQLite (today) and Postgres (PR-2b).
func JobRepositoryContract(t *testing.T, factory func(t *testing.T) (store.JobRepository, func())) {
	t.Run("CreateJob then GetJob round-trip", func(t *testing.T) {
		repo, cleanup := factory(t)
		defer cleanup()
		ctx := context.Background()
		jobID := "job_roundtrip_" + randSuffix()
		err := repo.CreateJob(ctx, store.CreateJobParams{
			JobID:      jobID,
			VideoName:  "test.mp4",
			ProjectID:  "p1",
			MaxRetries: 3,
			Payload:    map[string]interface{}{"job_type": "render"},
		})
		if err != nil {
			t.Fatalf("CreateJob: %v", err)
		}
		got, err := repo.GetJob(ctx, jobID)
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		if got == nil {
			t.Fatal("expected job projection, got nil")
		}
		if got.VideoName != "test.mp4" || got.ProjectID != "p1" || got.MaxRetries != 3 {
			t.Errorf("round-trip mismatch: %+v", got)
		}
		if got.Status != store.JobStatusPending {
			t.Errorf("expected PENDING post-create, got %q", got.Status)
		}
	})

	t.Run("GetJob missing returns (nil, nil)", func(t *testing.T) {
		repo, cleanup := factory(t)
		defer cleanup()
		got, err := repo.GetJob(context.Background(), "job_does_not_exist")
		if err != nil {
			t.Fatalf("GetJob missing: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil for missing id, got %+v", got)
		}
	})

	t.Run("ClaimNext returns ErrNoClaimableJob on empty queue", func(t *testing.T) {
		repo, cleanup := factory(t)
		defer cleanup()
		got, err := repo.ClaimNext(context.Background(), store.ClaimParams{
			WorkerID: "worker-1",
			Now:      time.Now().UTC(),
		})
		if err == nil {
			t.Fatalf("expected ErrNoClaimableJob on empty queue, got %+v", got)
		}
		if got != nil {
			t.Errorf("expected nil result on empty queue, got %+v", got)
		}
	})

	t.Run("ClaimNext end-to-end persists lease and updates status", func(t *testing.T) {
		repo, cleanup := factory(t)
		defer cleanup()
		ctx := context.Background()
		jobID := "job_claim_e2e_" + randSuffix()
		if err := repo.CreateJob(ctx, store.CreateJobParams{
			JobID: jobID, MaxRetries: 3,
		}); err != nil {
			t.Fatalf("CreateJob: %v", err)
		}
		got, err := repo.ClaimNext(ctx, store.ClaimParams{
			WorkerID: "worker-1",
			Now:      time.Now().UTC(),
		})
		if err != nil || got == nil {
			t.Fatalf("ClaimNext: %v %v", got, err)
		}
		// Spec §5 atomicity: re-read the persisted row to confirm the claim
		// committed end-to-end (status moved, lease populated).
		persisted, err := repo.GetJob(ctx, got.JobID)
		if err != nil || persisted == nil {
			t.Fatalf("GetJob after claim: %v %v", persisted, err)
		}
		if persisted.LeaseID == "" {
			t.Error("expected LeaseID populated on persisted row after claim (atomicity breach?)")
		}
		if persisted.Status == store.JobStatusPending {
			t.Errorf("expected status to leave PENDING after claim, still %q", persisted.Status)
		}
	})

	t.Run("Transition CAS success advances status and increments revision", func(t *testing.T) {
		repo, cleanup := factory(t)
		defer cleanup()
		ctx := context.Background()
		jobID := "job_trans_" + randSuffix()
		if err := repo.CreateJob(ctx, store.CreateJobParams{JobID: jobID, MaxRetries: 3}); err != nil {
			t.Fatalf("CreateJob: %v", err)
		}
		j, err := repo.GetJob(ctx, jobID)
		if err != nil || j == nil {
			t.Fatalf("GetJob pre-transition: %v %v", j, err)
		}
		if err := repo.Transition(ctx, store.TransitionParams{
			JobID:          j.JobID,
			ExpectedStatus: store.JobStatusPending,
			NewStatus:      store.JobStatusLeased,
			Revision:       j.Revision,
		}); err != nil {
			t.Fatalf("Transition: %v", err)
		}
		got, err := repo.GetJob(ctx, j.JobID)
		if err != nil || got == nil {
			t.Fatalf("GetJob after transition: %v %v", got, err)
		}
		if got.Status != store.JobStatusLeased {
			t.Errorf("expected PROCESSING, got %q", got.Status)
		}
		if got.Revision != j.Revision+1 {
			t.Errorf("expected revision %d, got %d", j.Revision+1, got.Revision)
		}
	})

	t.Run("Transition CAS conflict returns ErrTransitionConflict", func(t *testing.T) {
		repo, cleanup := factory(t)
		defer cleanup()
		ctx := context.Background()
		jobID := "job_cas_" + randSuffix()
		if err := repo.CreateJob(ctx, store.CreateJobParams{JobID: jobID, MaxRetries: 3}); err != nil {
			t.Fatalf("CreateJob: %v", err)
		}
		jID := pendingJobID(t, repo, ctx)
		j, err := repo.GetJob(ctx, jID)
		if err != nil || j == nil {
			t.Fatalf("GetJob: %v %v", j, err)
		}
		// First transition succeeds.
		if err := repo.Transition(ctx, store.TransitionParams{
			JobID:          j.JobID,
			ExpectedStatus: store.JobStatusPending,
			NewStatus:      store.JobStatusLeased,
			Revision:       j.Revision,
		}); err != nil {
			t.Fatalf("first transition: %v", err)
		}
		// Second transition with the stale expectation must reject.
		err = repo.Transition(ctx, store.TransitionParams{
			JobID:          j.JobID,
			ExpectedStatus: store.JobStatusPending, // stale: now PROCESSING
			NewStatus:      store.JobStatusFailed,
			Revision:       j.Revision, // stale: not the new revision
		})
		if err == nil {
			t.Error("expected ErrTransitionConflict, got nil")
		}
	})

	t.Run("ListByStatus with empty statuses returns nil", func(t *testing.T) {
		repo, cleanup := factory(t)
		defer cleanup()
		got, err := repo.ListByStatus(context.Background(), nil, 10)
		if err != nil {
			t.Fatalf("ListByStatus nil: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil for empty statuses, got %+v", got)
		}
	})
}

// TestJobRepositoryContract_SQLite drives the suite against SQLite.
func TestJobRepositoryContract_SQLite(t *testing.T) {
	JobRepositoryContract(t, NewSQLiteJobRepositoryFactory)
}
