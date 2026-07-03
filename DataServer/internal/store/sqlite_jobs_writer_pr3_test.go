// sqlite_jobs_writer_pr3_test.go — PR3 rewrite against the canonical
// repo.Transition(ctx, TransitionParams) API.
//
// Background: fix/remove-job-lease-ops deleted the PR3 surfaces
// (PR3Start, PR3RenewLease, PR3RecordRenderFinished, PR3Fail,
// PR3RequeueExpiredLeases). The job state machine is now driven by
// a single CAS UPDATE in store.TransitionJobStatus, exposed via
// SQLiteJobRepository.Transition:
//
//	UPDATE jobs
//	   SET status = ?, revision = revision+1, updated_at = ?
//	 WHERE job_id = ? AND status = ? AND revision = ?
//
// This file maps each PR3 BEHAVIORAL assertion onto the canonical
// API where it still applies, and DROPS assertions whose surface
// no longer exists.
//
// ── DROPPED (no equivalent in canonical) ──────────────────────────
//
//   - NullAttemptLegacy tests           — attempt column dropped (PR #9, m. 048)
//   - SkipRevisionCAS tests             — single-CAS rule: caller passes rev
//   - EmitEvent tests                   — canonical Transition does NOT write
//                                         to job_events (history/events are
//                                         written by separate completion paths
//                                         on the Task/JobEvent domain, not by
//                                         this CAS UPDATE)
//   - Atomicity_NoOrphanEvents tests    — same: no history/event writes from
//                                         Transition, so there cannot be
//                                         orphan events to assert against
//   - HistoryAndEventVerification      — DROP for the same reason
//   - COALESCE(revision,0) tests        — schema is NOT NULL DEFAULT 0
//                                         (see openTransitionTestDB)
//   - WrongAttempt tests                — attempt column no longer exists at
//                                         this layer; identity lives in
//                                         job_attempts + tasks (PR #9)
//
// ── KEPT (rewritten on TransitionParams) ──────────────────────────
//
//   - TOCTOU across competing *sql.DB connections → §1
//   - Multi-step lifecycle (PENDING→…→SUCCEEDED) → §2
//   - Terminal-state lock → §3
//   - Parallel concurrent Transition: exactly one wins → §4
//   - Revision bump visible to next caller → §5
//   - Cancelled context returns error, no commit → §6
//   - Multi-job independent revisions → §7
//
// All tests reuse openTransitionTestDB / seedJob / readJobStatus
// declared in sqlite_jobs_writer_repository_test.go.

package store

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// ── §1. TOCTOU across competing connections ────────────────────────────────

// TestTransition_TOCTOU_ConcurrentConnection verifies the canonical
// CAS detects a competing UPDATE from a second *sql.DB connection:
// the caller's stale revision MUST produce ErrTransitionConflict,
// and the row MUST remain at the post-bump value (no half-state).
//
// Origin: PR3RecordRenderFinished_TOCTOU_ConcurrentRevisionBump.
//
// The schema is shared across the two connections because both
// open the SAME DSN `file::memory:?cache=shared&_busy_timeout=5000` — SQLite backs
// this with a process-wide shared in-memory cache.
func TestTransition_TOCTOU_ConcurrentConnection(t *testing.T) {
	_, repo := openTransitionTestDB(t)
	ctx := context.Background()

	seedJob(t, repo.store.db, "job-toc", "LEASED", 5)

	// Open a second *sql.DB to the SAME shared in-memory DSN. The
	// schema set up by openTransitionTestDB is visible to db2.
	db2, err := sql.Open("sqlite3", "file::memory:?cache=shared&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })

	// Bump revision from db2 BEFORE calling repo.Transition. The
	// caller's `Revision: 5` is now stale relative to the row.
	res, err := db2.Exec(
		`UPDATE jobs SET revision = revision + 1, updated_at = ? WHERE job_id = ?`,
		time.Now().UTC().Format(time.RFC3339), "job-toc",
	)
	if err != nil {
		t.Fatalf("db2 concurrent bump: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("concurrent bump affected %d rows, expected 1", n)
	}

	// Caller submits the stale CAS — must fail with ErrTransitionConflict.
	err = repo.Transition(ctx, TransitionParams{
		JobID:          "job-toc",
		ExpectedStatus: JobStatusLeased,
		NewStatus:      JobStatusRunning,
		Revision:       5,
	})
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("expected ErrTransitionConflict under TOCTOU, got %v", err)
	}

	status, rev := readJobStatus(t, repo.store.db, "job-toc")
	if status != "LEASED" {
		t.Errorf("post-TOCTOU status: got %q, want LEASED (no half-state)", status)
	}
	if rev != 6 {
		t.Errorf("post-TOCTOU revision: got %d, want 6 (db2's bump)", rev)
	}
}

// ── §2. Lifecycle happy-path ───────────────────────────────────────────────

// TestTransition_LifecycleHappyPath drives a single job through
// the canonical lifecycle: PENDING → LEASED → RUNNING →
// AWAITING_ARTIFACT → SUCCEEDED. Each Transition must commit
// atomically with a +1 revision bump.
//
// Origin: PR3RecordRenderFinished_HappyPath + PR3Start_NullRevision
// (PR3 only modeled single-step transitions; multi-step is owed by
// canonical).
func TestTransition_LifecycleHappyPath(t *testing.T) {
	_, repo := openTransitionTestDB(t)
	ctx := context.Background()

	seedJob(t, repo.store.db, "job-1", "PENDING", 0)

	steps := []struct {
		from JobStatus
		to   JobStatus
		pre  int // expected pre-state revision (caller's view)
		post int // expected post-state revision
	}{
		{JobStatusPending, JobStatusLeased, 0, 1},
		{JobStatusLeased, JobStatusRunning, 1, 2},
		{JobStatusRunning, JobStatusAwaitingArtifact, 2, 3},
		{JobStatusAwaitingArtifact, JobStatusSucceeded, 3, 4},
	}
	for _, s := range steps {
		err := repo.Transition(ctx, TransitionParams{
			JobID:          "job-1",
			ExpectedStatus: s.from,
			NewStatus:      s.to,
			Revision:       s.pre,
		})
		if err != nil {
			t.Fatalf("%s -> %s (rev=%d): %v", s.from, s.to, s.pre, err)
		}
		status, rev := readJobStatus(t, repo.store.db, "job-1")
		if status != string(s.to) || rev != s.post {
			t.Errorf("after %s -> %s: got %q/rev=%d, want %q/rev=%d",
				s.from, s.to, status, rev, s.to, s.post)
		}
	}
}

// ── §3. Terminal-state lock ────────────────────────────────────────────────

// TestTransition_TerminalStatesRejectFurtherChanges locks the
// terminal-state invariant: SUCCEEDED, FAILED, CANCELLED reject
// any further Transition whose ExpectedStatus doesn't match the
// actual row. The CAS WHERE clause matches zero rows on a terminal
// row + non-matching ExpectedStatus, yielding ErrTransitionConflict.
//
// Origin: PR3RecordRenderFinished_Idempotent_AlreadySucceeded. The
// "no-op on SUCCEEDED" semantic was correct; canonical surfaces it
// as an ACTIVE rejection via ErrTransitionConflict rather than a
// silent short-circuit (no idempotent short-circuit exists — that's
// the property under test).
func TestTransition_TerminalStatesRejectFurtherChanges(t *testing.T) {
	_, repo := openTransitionTestDB(t)
	ctx := context.Background()

	for _, term := range []string{"SUCCEEDED", "FAILED", "CANCELLED"} {
		t.Run(term, func(t *testing.T) {
			seedJob(t, repo.store.db, "j-"+term, term, 7)

			// Any non-matching ExpectedStatus MUST yield ErrTransitionConflict.
			err := repo.Transition(ctx, TransitionParams{
				JobID:          "j-" + term,
				ExpectedStatus: JobStatusLeased, // wrong, regardless of NewStatus
				NewStatus:      JobStatusRunning,
				Revision:       7,
			})
			if !errors.Is(err, ErrTransitionConflict) {
				t.Errorf("[%s] expected ErrTransitionConflict, got %v", term, err)
			}
			s, r := readJobStatus(t, repo.store.db, "j-"+term)
			if s != term || r != 7 {
				t.Errorf("[%s] post-rejection: got %q/rev=%d, want %s/rev=7", term, s, r, term)
			}
		})
	}
}

// ── §4. N-way parallel contention ──────────────────────────────────────────

// TestTransition_ParallelConcurrent_ExactlyOneWins validates the
// concurrency claim: N goroutines submit a Transition against the
// same row with a barrier-synchronized start. EXACTLY one wins
// (no double-commit), the rest see ErrTransitionConflict.
//
// Origin: PR3RecordRenderFinished_TOCTOU (per-connection), scaled
// to N-way + a barrier to remove timing-dependent lerps. The
// achievable writer count for SQLite under cache=shared is bounded
// (`file::memory:?cache=shared&_busy_timeout=5000` allows a small number of writers
// before SQLITE_BUSY surfaces); we use a moderate N to keep this
// race-stability focused on CAS semantics, not lock-time tuning.
func TestTransition_ParallelConcurrent_ExactlyOneWins(t *testing.T) {
	_, repo := openTransitionTestDB(t)
	ctx := context.Background()

	seedJob(t, repo.store.db, "job-parallel", "LEASED", 0)

	const N = 16
	var wg sync.WaitGroup
	results := make([]error, N)
	start := make(chan struct{})

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx] = repo.Transition(ctx, TransitionParams{
				JobID:          "job-parallel",
				ExpectedStatus: JobStatusLeased,
				NewStatus:      JobStatusRunning,
				Revision:       0,
			})
		}(i)
	}
	close(start) // release all goroutines simultaneously
	wg.Wait()

	var winners, conflicts, others int
	for _, e := range results {
		switch {
		case e == nil:
			winners++
		case errors.Is(e, ErrTransitionConflict):
			conflicts++
		default:
			others++
			t.Errorf("unexpected error class: %v", e)
		}
	}
	if winners != 1 {
		t.Errorf("expected exactly 1 winner under N=%d contention, got %d (conflicts=%d, other=%d)",
			N, winners, conflicts, others)
	}
	if conflicts != N-1 {
		t.Errorf("expected conflicts=%d, got %d", N-1, conflicts)
	}

	s, r := readJobStatus(t, repo.store.db, "job-parallel")
	if s != "RUNNING" || r != 1 {
		t.Errorf("post-race row: got %q/rev=%d, want RUNNING/rev=1", s, r)
	}
}

// ── §5. Revision bump is observable to the next caller ─────────────────────

// TestTransition_RevisionBumpsVisibleToNextRead verifies that the
// revision incremented by a successful Transition is IMMEDIATELY
// visible to the next caller's CAS — you can't accidentally
// re-Transition with the pre-bump revision. This locks the
// optimistic-locking contract.
//
// Companion to TestTransition_StaleRevisionConflict (in repository_test).
func TestTransition_RevisionBumpsVisibleToNextRead(t *testing.T) {
	_, repo := openTransitionTestDB(t)
	ctx := context.Background()

	seedJob(t, repo.store.db, "job-bumps", "LEASED", 0)

	// First Transition: LEASED -> RUNNING at rev 0 succeeds.
	if err := repo.Transition(ctx, TransitionParams{
		JobID:          "job-bumps",
		ExpectedStatus: JobStatusLeased,
		NewStatus:      JobStatusRunning,
		Revision:       0,
	}); err != nil {
		t.Fatalf("first Transition: %v", err)
	}
	s, r := readJobStatus(t, repo.store.db, "job-bumps")
	if s != "RUNNING" || r != 1 {
		t.Fatalf("after first Transition: got %q/rev=%d, want RUNNING/rev=1", s, r)
	}

	// Second Transition with the OLD revision MUST fail (CAS stale).
	err := repo.Transition(ctx, TransitionParams{
		JobID:          "job-bumps",
		ExpectedStatus: JobStatusRunning,
		NewStatus:      JobStatusSucceeded,
		Revision:       0, // stale
	})
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("second Transition with stale rev=0: expected ErrTransitionConflict, got %v", err)
	}

	// Third Transition with the bumped rev=1 succeeds.
	if err := repo.Transition(ctx, TransitionParams{
		JobID:          "job-bumps",
		ExpectedStatus: JobStatusRunning,
		NewStatus:      JobStatusSucceeded,
		Revision:       1,
	}); err != nil {
		t.Fatalf("third Transition at rev=1: %v", err)
	}
	s, r = readJobStatus(t, repo.store.db, "job-bumps")
	if s != "SUCCEEDED" || r != 2 {
		t.Errorf("after third Transition: got %q/rev=%d, want SUCCEEDED/rev=2", s, r)
	}
}

// ── §6. Cancelled context does not produce half-row orphan ─────────────────

// TestTransition_CancelledContext_NoHalfRowOrphan verifies that
// the canonical single-UPDATE CAS does NOT produce a half-row
// orphan under ctx cancellation. The driver's behavior on
// pre-cancelled ctx is NOT deterministic — it may surface
// context.Canceled OR race past cancellation and return nil
// (had already dispatched the SQL). What IS deterministic is
// that the row, whatever the call's return value, MUST be in one
// of the two atomic outcomes: {LEASED, 0} (rejected) or
// {RUNNING, 1} (committed). Never any intermediate tuple.
//
// Origin: PR3 had no equivalent. This locks the P1 #12 invariant
// (no half-row orphan) under the cancellation dimension.
//
// NOTE: We deliberately do NOT assert on the call's return value
// because pre-cancellation behaviour is driver-dependent. The
// SQL atomicity claim IS deterministic, regardless of which
// branch the driver took.
func TestTransition_CancelledContext_NoHalfRowOrphan(t *testing.T) {
	_, repo := openTransitionTestDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel BEFORE the call

	seedJob(t, repo.store.db, "job-cancel-ctx", "LEASED", 0)

	// Return value deliberately ignored — see docstring.
	_ = repo.Transition(ctx, TransitionParams{
		JobID:          "job-cancel-ctx",
		ExpectedStatus: JobStatusLeased,
		NewStatus:      JobStatusRunning,
		Revision:       0,
	})

	s, r := readJobStatus(t, repo.store.db, "job-cancel-ctx")
	atomic := (s == "LEASED" && r == 0) || (s == "RUNNING" && r == 1)
	if !atomic {
		t.Errorf("cancelled ctx produced non-atomic row: got %q/rev=%d, want {LEASED/0} or {RUNNING/1}", s, r)
	}
}

// ── §7. Multi-job independent revisions ────────────────────────────────────

// TestTransition_MultipleJobs_IndependentRevisions verifies that
// successive Transitions on DIFFERENT jobs are independent: each
// commits against its OWN row's revision, no cross-row interference.
//
// Origin: PR3RecordRenderFinished_MultipleJobsSequential.
func TestTransition_MultipleJobs_IndependentRevisions(t *testing.T) {
	_, repo := openTransitionTestDB(t)
	ctx := context.Background()

	jobs := []struct {
		id  string
		rev int
	}{
		{"job-A", 0},
		{"job-B", 5},
		{"job-C", 12},
	}
	for _, j := range jobs {
		seedJob(t, repo.store.db, j.id, "LEASED", j.rev)
	}
	for _, j := range jobs {
		if err := repo.Transition(ctx, TransitionParams{
			JobID:          j.id,
			ExpectedStatus: JobStatusLeased,
			NewStatus:      JobStatusRunning,
			Revision:       j.rev,
		}); err != nil {
			t.Fatalf("Transition %s: %v", j.id, err)
		}
		s, r := readJobStatus(t, repo.store.db, j.id)
		if s != "RUNNING" || r != j.rev+1 {
			t.Errorf("%s post-Transition: got %q/rev=%d, want RUNNING/rev=%d",
				j.id, s, r, j.rev+1)
		}
	}
}
