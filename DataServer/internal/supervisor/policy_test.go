// Package supervisor / policy_test.go
//
// Tests for ClassifyError + FailureTracker + the canonical
// DB-closed injection pattern that proves an *sql.DB handle on a
// real sqlite-backed store maps to ErrInfrastructure after the
// SQLite driver's "database is closed" / sql.ErrConnDone surfaces.
package supervisor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// ────────────────────────────────────────────────────────────────────────
// ClassifyError
// ────────────────────────────────────────────────────────────────────────

func TestClassifyError_Nil_ReturnsNil(t *testing.T) {
	if got := ClassifyError(nil); got != nil {
		t.Errorf("ClassifyError(nil) = %v, want nil", got)
	}
}

func TestClassifyError_SqlConnDone_IsInfrastructure(t *testing.T) {
	got := ClassifyError(sql.ErrConnDone)
	if !IsInfrastructure(got) {
		t.Errorf("ClassifyError(sql.ErrConnDone) = %v, want ErrInfrastructure", got)
	}
	if IsElementScoped(got) || IsLeaseLost(got) {
		t.Errorf("ClassifyError cross-bucket match: %v", got)
	}
}

func TestClassifyError_DatabaseIsClosed_IsInfrastructure(t *testing.T) {
	got := ClassifyError(fmt.Errorf("sql: database is closed"))
	if !IsInfrastructure(got) {
		t.Errorf("ClassifyError(database is closed) = %v, want ErrInfrastructure", got)
	}
}

func TestClassifyError_DeadlineExceeded_IsInfrastructure(t *testing.T) {
	got := ClassifyError(context.DeadlineExceeded)
	if !IsInfrastructure(got) {
		t.Errorf("ClassifyError(DeadlineExceeded) = %v, want ErrInfrastructure", got)
	}
}

func TestClassifyError_ContextCanceled_IsElementScoped(t *testing.T) {
	// ctx cancellation in a tick function is usually the
	// supervisor shutting down — the runner's loop body sees
	// the cancelled ctx on the next iteration and exits cleanly.
	// Map to element-scoped so the tracker does not count it.
	got := ClassifyError(context.Canceled)
	if !IsElementScoped(got) {
		t.Errorf("ClassifyError(Canceled) = %v, want ErrElementScoped", got)
	}
}

func TestClassifyError_TransitionConflict_IsLeaseLost(t *testing.T) {
	got := ClassifyError(fmt.Errorf("completion: transition conflict: lease_id mismatch"))
	if !IsLeaseLost(got) {
		t.Errorf("ClassifyError(transition conflict) = %v, want ErrLeaseLost", got)
	}
}

func TestClassifyError_GenericError_IsElementScoped(t *testing.T) {
	got := ClassifyError(fmt.Errorf("provider returned 503"))
	if !IsElementScoped(got) {
		t.Errorf("ClassifyError(generic) = %v, want ErrElementScoped", got)
	}
}

func TestClassifyError_PreservesUnwrapChain(t *testing.T) {
	inner := sql.ErrConnDone
	got := ClassifyError(fmt.Errorf("tick wrap: %w", inner))
	if !errors.Is(got, ErrInfrastructure) {
		t.Errorf("expected errors.Is chain to ErrInfrastructure")
	}
	if !errors.Is(got, sql.ErrConnDone) {
		t.Errorf("expected errors.Is chain to sql.ErrConnDone via inner")
	}
}

// ────────────────────────────────────────────────────────────────────────
// FailureTracker
// ────────────────────────────────────────────────────────────────────────

func TestFailureTracker_NoErrorResets(t *testing.T) {
	tk := NewFailureTracker(DefaultRetryPolicy())
	if err := tk.Record(sql.ErrConnDone); err != nil {
		t.Errorf("first infra error should not escalate: %v", err)
	}
	if tk.Consecutive() != 1 {
		t.Errorf("consecutive = %d, want 1", tk.Consecutive())
	}
	if err := tk.Record(nil); err != nil {
		t.Errorf("nil error should not escalate: %v", err)
	}
	if tk.Consecutive() != 0 {
		t.Errorf("after reset, consecutive = %d, want 0", tk.Consecutive())
	}
}

func TestFailureTracker_ElementScopedDoesNotCount(t *testing.T) {
	tk := NewFailureTracker(RetryPolicy{ConsecutiveErrorThreshold: 3})
	for i := 0; i < 10; i++ {
		elementErr := fmt.Errorf("%w: bad row %d", ErrElementScoped, i)
		if err := tk.Record(elementErr); err != nil {
			t.Fatalf("element-scoped error #%d should NOT escalate: %v", i, err)
		}
	}
	if tk.Consecutive() != 0 {
		t.Errorf("element-scoped must not increment counter: got %d", tk.Consecutive())
	}
}

func TestFailureTracker_LeaseLostDoesNotCount(t *testing.T) {
	tk := NewFailureTracker(RetryPolicy{ConsecutiveErrorThreshold: 3})
	for i := 0; i < 10; i++ {
		leaseErr := fmt.Errorf("%w: casual conflict %d", ErrLeaseLost, i)
		if err := tk.Record(leaseErr); err != nil {
			t.Fatalf("lease-lost error #%d should NOT escalate: %v", i, err)
		}
	}
	if tk.Consecutive() != 0 {
		t.Errorf("lease-lost must not increment counter: got %d", tk.Consecutive())
	}
}

func TestFailureTracker_EscalatesAfterThreshold(t *testing.T) {
	tk := NewFailureTracker(RetryPolicy{ConsecutiveErrorThreshold: 3})

	if err := tk.Record(sql.ErrConnDone); err != nil {
		t.Errorf("error 1 should not escalate: %v", err)
	}
	if err := tk.Record(sql.ErrConnDone); err != nil {
		t.Errorf("error 2 should not escalate: %v", err)
	}
	err := tk.Record(sql.ErrConnDone)
	if err == nil {
		t.Fatal("error 3 should escalate")
	}
	if !IsInfrastructure(err) {
		t.Errorf("expected ErrInfrastructure on escalation: %v", err)
	}
	if tk.Consecutive() != 3 {
		t.Errorf("consecutive = %d, want 3", tk.Consecutive())
	}
}

func TestFailureTracker_ResetWindowRefreshesStreak(t *testing.T) {
	tk := NewFailureTracker(RetryPolicy{
		ConsecutiveErrorThreshold: 5,
		ResetWindow:              1 * time.Second,
	})
	now := time.Now()
	mockNow := func() time.Time { return now }
	tk.WithClock(mockNow)

	// First wave: 2 errors within the window.
	now = now.Add(0) // t=0
	if err := tk.Record(sql.ErrConnDone); err != nil {
		t.Errorf("first wave error 1 should not escalate: %v", err)
	}
	now = now.Add(500 * time.Millisecond) // t=500ms
	if err := tk.Record(sql.ErrConnDone); err != nil {
		t.Errorf("first wave error 2 should not escalate: %v", err)
	}
	if tk.Consecutive() != 2 {
		t.Errorf("after 2 errors at t=500ms, consecutive = %d, want 2", tk.Consecutive())
	}
	// Jump past the window.
	now = now.Add(5 * time.Second) // t=5.5s
	if err := tk.Record(sql.ErrConnDone); err != nil {
		t.Errorf("post-window error should restart streak (not escalate): %v", err)
	}
	if tk.Consecutive() != 1 {
		t.Errorf("after window reset, consecutive = %d, want 1", tk.Consecutive())
	}
}

func TestFailureTracker_MixedErrors_OnlyInfraCounts(t *testing.T) {
	tk := NewFailureTracker(RetryPolicy{ConsecutiveErrorThreshold: 3})

	// pattern: infra, infra, element, infra, infra
	// expected: counter=4 — element doesn't reset; counter only
	// resets on a clean tick.
	if err := tk.Record(sql.ErrConnDone); err != nil {
		t.Errorf("infra #1 unexpected escalate: %v", err)
	}
	if err := tk.Record(sql.ErrConnDone); err != nil {
		t.Errorf("infra #2 unexpected escalate: %v", err)
	}
	elementErr := fmt.Errorf("%w: row bad", ErrElementScoped)
	if err := tk.Record(elementErr); err != nil {
		t.Errorf("element error should not escalate: %v", err)
	}
	if err := tk.Record(sql.ErrConnDone); err != nil {
		t.Errorf("infra #3 unexpected escalate: %v", err)
	}
	if tk.Consecutive() != 3 {
		t.Errorf("consecutive = %d, want 3 (infra-only counter)", tk.Consecutive())
	}
	// 4th infra: escalate.
	err := tk.Record(sql.ErrConnDone)
	if err == nil {
		t.Fatal("4th consecutive infra should escalate")
	}
	if !IsInfrastructure(err) {
		t.Errorf("expected ErrInfrastructure: %v", err)
	}
}

func TestFailureTracker_PanickedCounts(t *testing.T) {
	tk := NewFailureTracker(RetryPolicy{ConsecutiveErrorThreshold: 2})
	if err := tk.Record(fmt.Errorf("%w: handler boom", ErrPanicked)); err != nil {
		t.Errorf("first panic should not escalate: %v", err)
	}
	err := tk.Record(fmt.Errorf("%w: handler boom again", ErrPanicked))
	if err == nil {
		t.Fatal("second panic should escalate")
	}
	if !IsInfrastructure(err) {
		t.Errorf("expected ErrInfrastructure on panic escalation: %v", err)
	}
}

// ────────────────────────────────────────────────────────────────────────
// DB-closed injection test (the canonical entry point for the
// user's spec point 4: inject a DB that fails closed and verify
// the runner returns ErrInfrastructure after N attempts).
// ────────────────────────────────────────────────────────────────────────

// openTrackerTestDB is a minimal sqlite-backed *sql.DB used
// exclusively to validate the ClassifyError classification of
// "database is closed" — no schema, no migrations, just open +
// closed.
func openTrackerTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "tracker_db_closed_test.db")
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable fk: %v", err)
	}
	return db
}

// TestClosedDB_ClassifiedAsInfrastructure verifies the canonical
// injection pattern: open a real sqlite-backed *sql.DB, run an
// arbitrary Query that succeeds, close the DB, run the same
// Query — observe that the resulting error classifies as
// ErrInfrastructure. This is the building block every runner's
// tick() function will rely on.
func TestClosedDB_ClassifiedAsInfrastructure(t *testing.T) {
	db := openTrackerTestDB(t)
	// Sanity: while open, a query succeeds.
	var one int
	if err := db.QueryRow(`SELECT 1`).Scan(&one); err != nil {
		t.Fatalf("pre-close query failed: %v", err)
	}
	if one != 1 {
		t.Fatalf("pre-close query returned %d, want 1", one)
	}
	// Close the DB and confirm the next query surfaces a
	// connection error that classifies as ErrInfrastructure.
	db.Close()
	_, err := db.Exec(`SELECT 1`)
	if err == nil {
		t.Fatal("query against closed DB should fail")
	}
	classified := ClassifyError(err)
	if !IsInfrastructure(classified) {
		t.Errorf("expected ErrInfrastructure from closed-DB query, got: %v (raw=%v)", classified, err)
	}
}

// TestClosedDB_RunnerReturnsErrInfrastructureAfterN attempts
// simulates the runner-side of the contract: drive N consecutive
// closed-DB ticks through a FailureTracker and assert the
// tracker fires ErrInfrastructure at the threshold.
func TestClosedDB_RunnerReturnsErrInfrastructureAfterN(t *testing.T) {
	db := openTrackerTestDB(t)
	db.Close() // simulate pre-failure inject — every subsequent query fails closed

	// Build a closed-DB standing query that surfaces as
	// "database is closed" or sql.ErrConnDone depending on the
	// driver — both classification paths must converge on
	// ErrInfrastructure.
	probe := func() error {
		_, err := db.Exec(`SELECT 1`)
		return err
	}

	// Lower the threshold to 3 so the test runs fast while still
	// exercising the consecutive-error contract.
	tracker := NewFailureTracker(RetryPolicy{
		ConsecutiveErrorThreshold: 3,
		ResetWindow:              0,
	})

	var (
		escalated      atomic.Bool
		cleanTicks     atomic.Int32
		totalTicks     atomic.Int32
	)
	for i := 0; i < 10; i++ {
		err := probe()
		classified := ClassifyError(err)
		totalTicks.Add(1)
		if classified == nil {
			cleanTicks.Add(1)
		} else if recordErr := tracker.Record(classified); recordErr != nil {
			escalated.Store(true)
			break
		}
	}
	if !escalated.Load() {
		t.Fatalf("FailureTracker never escalated after %d closed-DB ticks (clean=%d)",
			totalTicks.Load(), cleanTicks.Load())
	}
	if cleanTicks.Load() != 0 {
		t.Errorf("expected zero clean ticks against closed DB, got %d", cleanTicks.Load())
	}
	if tk := tracker.Consecutive(); tk < 3 {
		t.Errorf("expected at least 3 consecutive infra errors before escalation, got %d", tk)
	}
}
