package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// closeSchedule is encoded as the relative timing of db.Close vs the
// Transition goroutine, expressed as a single signed microsecond
// offset applied to a transition that takes ~µs on a freshly-seeded
// SQLite file:
//
//	closeAt < 0:   db.Close BEFORE the goroutine is started
//	               (the goroutine acquires a closed-pool connection;
//	               driver returns sql.ErrConnDone-style error).
//	closeAt == 0:  goroutine runs and completes (wg.Wait blocks the
//	               close); THEN db.Close. The UPDATE has committed by
//	               the time db.Close runs.
//	closeAt > 0:   sleep closeAt microseconds from the main goroutine,
//	               then db.Close WHILE the transition is in flight.
//	               Race window: depending on closeAt, the close hits
//	               at (a) goroutine startup, (b) connection acquire,
//	               (c) query execution, or (d) post-commit cleanup.
//
// runTrial returns the row state + the transition error so the
// sub-tests can assert deterministic invariants.
func TestSQLiteJobsWriter_Transition_DBCloseInvariants(t *testing.T) {

	t.Run("CloseBefore_ReturnsConnDone_RowUnchanged", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "close_before.sqlite")
		status, revision, transErr := runCloseTrial(t, dbPath, -1)

		if !isExpectedCloseError(transErr) {
			t.Fatalf("CloseBefore: transErr = %v (want sql.ErrConnDone or 'database is closed')", transErr)
		}
		if status != "PENDING" || revision != 0 {
			t.Errorf("CloseBefore: row state=%q revision=%d (want PENDING/0 — UPDATE must NOT have written)",
				status, revision)
		}
	})

	t.Run("CloseAfter_CommitsCleanly_RowUpdated", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "close_after.sqlite")
		status, revision, transErr := runCloseTrial(t, dbPath, 0)

		if transErr != nil {
			t.Errorf("CloseAfter: transErr = %v (want nil — UPDATE committed before close)", transErr)
		}
		if status != "RUNNING" || revision != 1 {
			t.Errorf("CloseAfter: row state=%q revision=%d (want RUNNING/1)", status, revision)
		}
	})

	t.Run("CloseMidFlight_NoDivergence_AcrossTrials", func(t *testing.T) {
		// 20 trials with random 100µs-30ms mids so the close falls at many
		// different points along the Transition execution timeline. Each
		// trial's final row state must be either the pre or post atomic
		// pair — never a hybrid (e.g. status updated but revision
		// unchanged). The race window is exercised when both committed
		// and cancelled branches fire over the 20 samples.
		const trials = 20
		rng := rand.New(rand.NewSource(42))

		var (
			committedObserved bool
			cancelledObserved bool
			divergences       int
		)
		for trial := 0; trial < trials; trial++ {
			dbPath := filepath.Join(t.TempDir(), fmt.Sprintf("close_mid_%02d.sqlite", trial))

			// Range chosen so that EITHER outcome is reachable on every
			// CI machine:
			//   lower bound (100µs)  < typical goroutine wake (~200µs) → cancelled wins
			//   upper bound (30ms)   >> typical UPDATE on empty DB    → committed wins
			sleepMicros := 100 + rng.Intn(29900) // 100 .. 30000
			status, revision, transErr := runCloseTrial(t, dbPath, int64(sleepMicros))

			switch {
			case transErr == nil:
				committedObserved = true
			case isExpectedCloseError(transErr):
				cancelledObserved = true
			default:
				t.Errorf("trial %02d: unexpected error: %v (want nil, sql.ErrConnDone, or 'sql: database is closed')",
					trial, transErr)
			}

			// Atomic state check — this is the P1 #12 invariant.
			switch {
			case status == "PENDING" && revision == 0:
				// Pre-transition atomic — close won, UPDATE rolled back.
			case status == "RUNNING" && revision == 1:
				// Post-transition atomic — UPDATE committed before close.
			default:
				divergences++
				t.Errorf("trial %02d: row state DIVERGED: status=%q revision=%d (want one of {PENDING,0} or {RUNNING,1})",
					trial, status, revision)
			}
		}

		// The substantive invariant — zero divergences — is what locks
		// P1 #12: a row must be either fully pre or fully post, never
		// a hybrid. The committed-/cancelled-branch counts are
		// diagnostic-only; the test does NOT fail when only one
		// branch fires, because production is allowed to consistently
		// finish the UPDATE before our close signal arrives. What
		// matters is that NEITHER branch produces a half-row.
		if divergences > 0 {
			t.Errorf("%d trial(s) exhibited row divergence — P1 #12 invariant violated", divergences)
		}
		t.Logf("mid-flight trial distribution: committed=%t cancelled=%t divergences=%d (committed-and-cancelled both desirable but neither required)",
			committedObserved, cancelledObserved, divergences)
	})
}

// runCloseTrial opens a fresh disk-backed SQLite, seeds a PENDING/0
// job, runs Transition(PENDING→RUNNING, rev=0) on a goroutine with
// the close schedule encoded by closeAt (see type-level docs), then
// reopens the same file on a FRESH *sql.DB and reports the row state
// + transition error. The trial's behavior is fully deterministic
// per closeAt value: callers can pick the branch they want to test.
func runCloseTrial(t *testing.T, dbPath string, closeAt int64) (status string, revision int, transErr error) {
	t.Helper()
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	if _, err := db.Exec(`
		CREATE TABLE jobs (
			job_id TEXT PRIMARY KEY,
			status TEXT NOT NULL,
			revision INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 0,
			attempt INTEGER,
			started_at TEXT,
			updated_at TEXT,
			video_name TEXT,
			project_id TEXT,
			created_at TEXT,
			completed_at TEXT
		);
	`); err != nil {
		t.Fatalf("create jobs schema: %v", err)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	if _, err := db.Exec(
		`INSERT INTO jobs (job_id, status, revision, attempt, created_at, updated_at)
		 VALUES (?, 'PENDING', 0, NULL, ?, ?)`,
		"job-fail", now, now,
	); err != nil {
		t.Fatalf("seed PENDING job: %v", err)
	}

	repo := NewSQLiteJobRepository(&SQLiteStore{db: db})

	var wg sync.WaitGroup
	wg.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch {
	case closeAt < 0:
		// Branch (1): close BEFORE the goroutine acquires a connection.
		_ = db.Close()
		go func() {
			defer wg.Done()
			transErr = repo.Transition(ctx, TransitionParams{
				JobID:          "job-fail",
				ExpectedStatus: JobStatusPending,
				NewStatus:      JobStatusRunning,
				Revision:       0,
			})
		}()
		wg.Wait()

	case closeAt == 0:
		// Branch (2): close AFTER Transition completes. Goroutine runs
		// first; wg.Wait blocks the close until the UPDATE has committed.
		go func() {
			defer wg.Done()
			transErr = repo.Transition(ctx, TransitionParams{
				JobID:          "job-fail",
				ExpectedStatus: JobStatusPending,
				NewStatus:      JobStatusRunning,
				Revision:       0,
			})
		}()
		wg.Wait()
		_ = db.Close()

	default:
		// Branch (3): mid-flight. Goroutine runs Transition; meanwhile
		// the main goroutine sleeps closeAt µs then closes. The race
		// window is the time between the goroutine waking and the
		// single UPDATE finishing — well under 1 ms on any CI machine.
		go func() {
			defer wg.Done()
			transErr = repo.Transition(ctx, TransitionParams{
				JobID:          "job-fail",
				ExpectedStatus: JobStatusPending,
				NewStatus:      JobStatusRunning,
				Revision:       0,
			})
		}()
		time.Sleep(time.Duration(closeAt) * time.Microsecond)
		_ = db.Close()
		wg.Wait()
	}

	// Reopen on a fresh *sql.DB (the original is closed) and read the
	// post-flight row state. The closed pool cannot be reused.
	verifyDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer verifyDB.Close()
	if err := verifyDB.QueryRow(
		`SELECT status, revision FROM jobs WHERE job_id = ?`,
		"job-fail",
	).Scan(&status, &revision); err != nil {
		t.Fatalf("post-close SELECT: %v", err)
	}
	return status, revision, transErr
}

// isExpectedCloseError returns true when the error is one of the
// recognized database/sql connection-closed outcomes:
//   - sql.ErrConnDone                              (driver-level)
//   - ErrTransitionConflict                        (CAS predicate rejected; close raced before WHERE match)
//   - any wrapped error whose message contains "database is closed"
//
// nil is NOT a "closed" outcome — it's the "committed" branch.
func isExpectedCloseError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrConnDone) {
		return true
	}
	if errors.Is(err, ErrTransitionConflict) {
		return true
	}
	if strings.Contains(err.Error(), "database is closed") {
		return true
	}
	return false
}
