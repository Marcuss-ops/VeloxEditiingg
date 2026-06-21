// Package contracts / job_repository_contract_test.go
//
// Cross-backend contract suite for jobs.Repository (jobs.Reader +
// jobs.Writer). Companion to the pre-existing jobs_repository_contract_test.go
// which is SQLite-specific (uses *store.SQLiteJobRepository concrete type
// + store.CreateJobParams + store.JobRecord projection + job_history).
//
// This suite targets the narrow jobs.Repository interface and tests
// behavioral invariants that ANY jobs.Repository implementation must
// satisfy. SQLite and Postgres adapters drive the same scenarios.
//
// Behavioral equivalents:
//
//   Insert+Get    → Create + Get: round-trip preserves ID + status
//   Missing       → Get for unknown id returns (nil, nil)
//   List status   → Create 2 + List with []Status{StatusPending} returns both
//   List empty    → List with empty Statuses returns nil
//   Counts        → Counts aggregates correctly per status
//   Lease         → Create + Lease flips PENDING → LEASED and populates lease_id
//   Lease twice   → Lease again fails (CAS predicate rejects)
//   Fail          → Create + Fail transitions to FAILED; second Fail rejects (terminal)
//   SetStatus     → Create + SetStatus on valid from → succeeds; SetStatus with stale from fails
//   Cancel        → Create + Cancel succeeds; second Cancel fails (terminal)
//   Delete        → Create + Delete + Get returns nil

package contracts

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"velox-server/internal/jobs"
	"velox-server/internal/store"
)

// JobRepositoryContractCrossBackend runs the cross-backend invariant
// suite for jobs.Repository. newRepo must return a freshly-initialized
// backend (factory owns schema isolation per-test).
//
// Renamed from JobRepositoryContract to coexist with the PRE-EXISTING
// JobRepositoryContract declared in jobs_repository_contract_test.go:
// that one is SQLite-bounded (factory returns concrete *store.SQLiteJobRepository,
// suite exercises CreateJob / Transition / ListByStatus with store-package
// types). This one targets the narrow jobs.Repository interface so
// SQLite and Postgres compose identically.
func JobRepositoryContractCrossBackend(t *testing.T, newRepo func(t *testing.T) (jobs.Repository, func())) {
	t.Helper()

	ctx := context.Background()

	t.Run("Create+Get round-trip", func(t *testing.T) {
		repo, cleanup := newRepo(t)
		defer cleanup()

		jobID := "job_rt_" + randSuffix()
		err := repo.Create(ctx, &jobs.Job{
			ID:         jobID,
			Status:     jobs.StatusPending,
			VideoName:  "rt_video.mp4",
			ProjectID:  "p1",
			MaxRetries: 3,
			Payload:    `{"type":"render"}`,
		})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		got, err := repo.Get(ctx, jobID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got == nil {
			t.Fatal("expected job projection, got nil")
		}
		if got.ID != jobID {
			t.Errorf("ID: got %q want %q", got.ID, jobID)
		}
		if got.VideoName != "rt_video.mp4" {
			t.Errorf("VideoName: got %q", got.VideoName)
		}
		if got.ProjectID != "p1" {
			t.Errorf("ProjectID: got %q", got.ProjectID)
		}
		if got.MaxRetries != 3 {
			t.Errorf("MaxRetries: got %d want 3", got.MaxRetries)
		}
		if got.Status != jobs.StatusPending {
			t.Errorf("status post-Create: got %q want PENDING", got.Status)
		}
	})

	t.Run("Get missing returns (nil, nil)", func(t *testing.T) {
		repo, cleanup := newRepo(t)
		defer cleanup()

		got, err := repo.Get(ctx, "job_does_not_exist_"+randSuffix())
		if err != nil {
			t.Fatalf("Get missing: unexpected error %v", err)
		}
		if got != nil {
			t.Errorf("expected nil for missing id, got %+v", got)
		}
	})

	t.Run("List with non-empty status filter returns matches", func(t *testing.T) {
		repo, cleanup := newRepo(t)
		defer cleanup()

		prefix := "job_list_" + randSuffix()
		for i := 0; i < 2; i++ {
			err := repo.Create(ctx, &jobs.Job{
				ID:        prefix + "_" + uuid.NewString()[:8],
				Status:    jobs.StatusPending,
				VideoName: "v",
			})
			if err != nil {
				t.Fatalf("Create #%d: %v", i, err)
			}
		}

		list, err := repo.List(ctx, jobs.Filter{Statuses: []jobs.Status{jobs.StatusPending}, Limit: 100})
		if err != nil {
			t.Fatalf("List pending: %v", err)
		}
		if len(list) < 2 {
			// Other tests in the suite may have left PENDING rows behind
			// (factory per-test isolation only inside a single test).
			// We assert at least 2 of OUR prefix.
			t.Errorf("expected at least 2 results, got %d", len(list))
		}
		matchCount := 0
		for _, j := range list {
			if j.ID != "" && (j.ID == prefix+"_a" || j.ID == prefix+"_b" || (len(j.ID) > len(prefix) && j.ID[:len(prefix)] == prefix)) {
				matchCount++
			}
		}
		if matchCount < 2 {
			t.Errorf("expected both created jobs in list, matched %d", matchCount)
		}
	})

	t.Run("List with empty filter returns nil", func(t *testing.T) {
		repo, cleanup := newRepo(t)
		defer cleanup()

		got, err := repo.List(ctx, jobs.Filter{}) // no Statuses
		if err != nil {
			t.Fatalf("List empty: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil for empty filter, got %d items", len(got))
		}
	})

	t.Run("Counts includes pending jobs we just created", func(t *testing.T) {
		repo, cleanup := newRepo(t)
		defer cleanup()

		prefix := "job_counts_" + randSuffix()
		for i := 0; i < 3; i++ {
			if err := repo.Create(ctx, &jobs.Job{
				ID: prefix + "_" + uuid.NewString()[:8],
			}); err != nil {
				t.Fatalf("Create #%d: %v", i, err)
			}
		}
		counts, err := repo.Counts(ctx)
		if err != nil {
			t.Fatalf("Counts: %v", err)
		}
		if counts[jobs.StatusPending] < 3 {
			t.Errorf("expected at least 3 PENDING in counts, got %d", counts[jobs.StatusPending])
		}
	})

	t.Run("Lease flips PENDING to LEASED", func(t *testing.T) {
		repo, cleanup := newRepo(t)
		defer cleanup()

		jobID := "job_lease_" + randSuffix()
		if err := repo.Create(ctx, &jobs.Job{ID: jobID, MaxRetries: 5}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := repo.Lease(ctx, jobID, "worker-1"); err != nil {
			t.Fatalf("Lease: %v", err)
		}
		got, err := repo.Get(ctx, jobID)
		if err != nil {
			t.Fatalf("Get post-lease: %v", err)
		}
		if got.Status != jobs.StatusLeased {
			t.Errorf("expected LEASED, got %q", got.Status)
		}
		if got.LeaseID == "" {
			t.Errorf("expected LeaseID populated")
		}
		if got.WorkerID != "worker-1" {
			t.Errorf("expected WorkerID=worker-1, got %q", got.WorkerID)
		}
		if got.Attempts < 1 {
			t.Errorf("expected retry_count to bump, got %d", got.Attempts)
		}
	})

	t.Run("Lease twice fails (CAS predicate)", func(t *testing.T) {
		repo, cleanup := newRepo(t)
		defer cleanup()

		jobID := "job_lease2_" + randSuffix()
		if err := repo.Create(ctx, &jobs.Job{ID: jobID, MaxRetries: 5}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := repo.Lease(ctx, jobID, "worker-1"); err != nil {
			t.Fatalf("first Lease: %v", err)
		}
		if err := repo.Lease(ctx, jobID, "worker-2"); err == nil {
			t.Error("expected second Lease to fail (job no longer PENDING), got nil")
		}
	})

	t.Run("Fail transitions to FAILED", func(t *testing.T) {
		repo, cleanup := newRepo(t)
		defer cleanup()

		jobID := "job_fail_" + randSuffix()
		if err := repo.Create(ctx, &jobs.Job{ID: jobID}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := repo.Fail(ctx, jobID, "intentional"); err != nil {
			t.Fatalf("Fail: %v", err)
		}
		got, err := repo.Get(ctx, jobID)
		if err != nil {
			t.Fatalf("Get post-fail: %v", err)
		}
		if got.Status != jobs.StatusFailed {
			t.Errorf("expected FAILED, got %q", got.Status)
		}
	})

	t.Run("Fail on terminal job fails", func(t *testing.T) {
		repo, cleanup := newRepo(t)
		defer cleanup()

		jobID := "job_failterm_" + randSuffix()
		if err := repo.Create(ctx, &jobs.Job{ID: jobID}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := repo.Fail(ctx, jobID, "first"); err != nil {
			t.Fatalf("first Fail: %v", err)
		}
		if err := repo.Fail(ctx, jobID, "second"); err == nil {
			t.Error("expected Fail on terminal job to fail, got nil")
		}
	})

	t.Run("SetStatus with valid from succeeds; stale from fails", func(t *testing.T) {
		repo, cleanup := newRepo(t)
		defer cleanup()

		jobID := "job_setstatus_" + randSuffix()
		if err := repo.Create(ctx, &jobs.Job{ID: jobID}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		// After Create, status is PENDING with revision=0. First SetStatus
		// flips to LEASED (revision becomes 1). Second SetStatus uses stale
		// revision=0 → CAS miss.
		if err := repo.SetStatus(ctx, jobID, jobs.StatusPending, jobs.StatusLeased); err != nil {
			t.Fatalf("first SetStatus: %v", err)
		}
		// Now status is LEASED with revision=1. Attempt to "SetStatus PENDING"
		// again from a now-stale view must reject.
		if err := repo.SetStatus(ctx, jobID, jobs.StatusPending, jobs.StatusLeased); err == nil {
			t.Error("expected stale SetStatus to fail, got nil")
		}
		// For real: PENDING → LEASED worked at first; trying the same transition
		// again from a stale-from-perspective fails because revision moved.
	})

	t.Run("Cancel succeeds; second Cancel fails (terminal)", func(t *testing.T) {
		repo, cleanup := newRepo(t)
		defer cleanup()

		jobID := "job_cancel_" + randSuffix()
		if err := repo.Create(ctx, &jobs.Job{ID: jobID}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		// Use negative revision as orchestrator-initiated (no CAS) per
		// postgres_jobs_repository.go's Cancel semantics.
		if err := repo.Cancel(ctx, jobID, "stop", -1); err != nil {
			t.Fatalf("first Cancel: %v", err)
		}
		if err := repo.Cancel(ctx, jobID, "stop-again", -1); err == nil {
			t.Error("expected second Cancel on terminal to fail, got nil")
		}
	})

	t.Run("Delete removes the row", func(t *testing.T) {
		repo, cleanup := newRepo(t)
		defer cleanup()

		jobID := "job_del_" + randSuffix()
		if err := repo.Create(ctx, &jobs.Job{ID: jobID}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := repo.Delete(ctx, jobID); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		got, err := repo.Get(ctx, jobID)
		if err != nil {
			t.Fatalf("Get post-delete: %v", err)
		}
		if got != nil {
			t.Errorf("expected nil after Delete, got %+v", got)
		}
	})

	t.Run("ClaimNext returns ErrNoClaimableJob on empty queue", func(t *testing.T) {
		repo, cleanup := newRepo(t)
		defer cleanup()

		// Don't Create anything — fresh per-test store. ClaimNext should
		// hit ErrNoClaimableJob.
		got, err := repo.ClaimNext(ctx, "worker-claimtest", nil)
		if err == nil {
			t.Fatalf("expected ErrNoClaimableJob on empty, got %+v", got)
		}
		if got != nil {
			t.Errorf("expected nil claim on empty queue, got %+v", got)
		}
		if err != store.ErrNoClaimableJob {
			t.Errorf("expected ErrNoClaimableJob, got %v", err)
		}
	})

	t.Run("ClaimNext end-to-end returns the claimed job", func(t *testing.T) {
		repo, cleanup := newRepo(t)
		defer cleanup()

		jobID := "job_claim_" + randSuffix()
		if err := repo.Create(ctx, &jobs.Job{ID: jobID}); err != nil {
			t.Fatalf("Create: %v", err)
		}
		got, err := repo.ClaimNext(ctx, "worker-1", nil)
		if err != nil {
			t.Fatalf("ClaimNext: %v", err)
		}
		if got == nil {
			t.Fatal("expected ClaimNextResult, got nil")
		}
		if got.JobID != jobID {
			t.Errorf("claimed JobID: got %q want %q", got.JobID, jobID)
		}
		if got.LeaseID == "" {
			t.Error("expected LeaseID populated post-claim")
		}
		// Verify the persisted row reflects the claim (status flipped).
		persisted, err := repo.Get(ctx, got.JobID)
		if err != nil {
			t.Fatalf("Get post-claim: %v", err)
		}
		if persisted.Status != jobs.StatusLeased {
			t.Errorf("expected persisted status LEASED, got %q", persisted.Status)
		}
		if persisted.LeaseID == "" {
			t.Error("expected persisted LeaseID populated")
		}
	})
}
