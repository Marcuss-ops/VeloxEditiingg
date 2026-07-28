// Package store / sqlite_tx_manager_test.go
//
// Failure-injection tests for the TxManager.RunInTx helper. Each
// keystone pins one contract branch of the helper so a future
// regression in the BeginTx/Commit/Rollback lifecycle is caught
// deterministically.
//
// The test schema (a single-row cas_test table) deliberately mirrors
// the lease/CAS surface (id, state, runner_id, lease_id, updated_at)
// so the CAS queries look like real call-site SQL, not synthetic
// fixtures.
//
// Failure-injection keystones:
//
//   1. TestTxManager_HappyPath_commit_visible
//        → commits on fn success; writes visible after RunInTx returns.
//
//   2. TestTxManager_RollsBackOnFnError_no_writes
//        → rolls back when fn returns non-nil; ALL writes done inside
//          fn (pre-failure sub-ops included) are NOT visible.
//
//   3. TestTxManager_ConflictPropagation_returns_ErrTransitionConflict
//        → CAS rows-affected=0 inside fn surfaces ErrTransitionConflict
//          through RunInTx as a typed error; writes are NOT committed.
//
//   4. TestTxManager_ContextCancelMidFn_rolls_back
//        → ctx cancellation during fn surfaces context.Canceled and
//          triggers rollback; partial writes NOT visible.
//
//   5. TestTxManager_PanicSafety_rolls_back
//        → a panic inside fn does NOT leak tx state; std lib *sql.Tx
//          rollback path cleans up automatically.
//
//   6. TestTxManager_NilGuards_return_clean_error
//        → NewTxManager(nil).RunInTx(...) and RunInTx with nil
//          callback return typed sentinel errors instead of panicking.
package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// setupTxManagerDB opens a temp SQLite DB with a single-row test
// table mirroring the lease/CAS surface row layout
// (id, state, runner_id, lease_id, updated_at). Cleanup is wired
// into t.Cleanup so test runs don't leak file descriptors.
func setupTxManagerDB(t *testing.T) *SQLiteStore {
	t.Helper()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "tx_manager_test.sqlite")
	dbStore, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { _ = dbStore.Close() })

	if _, err := dbStore.DB().Exec(`
		CREATE TABLE cas_test (
			id INTEGER PRIMARY KEY,
			state TEXT NOT NULL DEFAULT 'A',
			runner_id TEXT NOT NULL DEFAULT '',
			lease_id TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL DEFAULT ''
		)`); err != nil {
		t.Fatalf("create cas_test: %v", err)
	}
	if _, err := dbStore.DB().Exec(
		`INSERT INTO cas_test (id, state) VALUES (1, 'A')`,
	); err != nil {
		t.Fatalf("seed cas_test row: %v", err)
	}
	return dbStore
}

// TestTxManager_HappyPath_commit_visible pins the success branch:
// fn returns nil → tx.Commit() runs → writes visible after RunInTx.
func TestTxManager_HappyPath_commit_visible(t *testing.T) {
	t.Parallel()
	dbStore := setupTxManagerDB(t)
	mgr := NewTxManager(dbStore)

	err := mgr.RunInTx(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`UPDATE cas_test SET state = 'B', updated_at = ? WHERE id = 1 AND state = 'A'`,
			"happy",
		)
		return err
	})
	if err != nil {
		t.Fatalf("RunInTx: want nil, got %v", err)
	}

	var state, updatedAt string
	if err := dbStore.DB().QueryRow(
		`SELECT state, updated_at FROM cas_test WHERE id = 1`,
	).Scan(&state, &updatedAt); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if state != "B" {
		t.Errorf("state = %q, want B (RunInTx must commit on fn success)", state)
	}
	if updatedAt != "happy" {
		t.Errorf("updated_at = %q, want happy", updatedAt)
	}
}

// TestTxManager_RollsBackOnFnError_no_writes pins the rollback
// branch when fn returns a non-nil error mid-execution: the
// pre-failure sub-write IS also rolled back (atomic boundary).
func TestTxManager_RollsBackOnFnError_no_writes(t *testing.T) {
	t.Parallel()
	dbStore := setupTxManagerDB(t)
	mgr := NewTxManager(dbStore)
	bomb := errors.New("simulated fn failure mid-tx")

	err := mgr.RunInTx(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		// FIRST write — would be visible on commit.
		if _, err := tx.ExecContext(ctx,
			`UPDATE cas_test SET runner_id = 'X' WHERE id = 1`); err != nil {
			return err
		}
		// SECOND write — also pre-failure, atomic-boundary inviolable.
		if _, err := tx.ExecContext(ctx,
			`UPDATE cas_test SET state = 'C' WHERE id = 1`); err != nil {
			return err
		}
		// THEN signal failure.
		return bomb
	})
	if !errors.Is(err, bomb) {
		t.Fatalf("RunInTx err = %v, want bomb (cb err must bubble up unchanged)", err)
	}

	var state, runner string
	if err := dbStore.DB().QueryRow(
		`SELECT state, runner_id FROM cas_test WHERE id = 1`,
	).Scan(&state, &runner); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if state != "A" {
		t.Errorf("state = %q, want A (pre-failure writes must NOT be visible after fn error)", state)
	}
	if runner != "" {
		t.Errorf("runner_id = %q, want '' (pre-failure writes must NOT be visible after fn error)", runner)
	}
}

// TestTxManager_ConflictPropagation_returns_ErrTransitionConflict pins
// the typed-CAS-signal surface: when fn returns ErrTransitionConflict
// (the canonical RowsAffected==0 signal), RunInTx rolls back AND
// surfaces the typed error verbatim so callers can errors.Is on it.
func TestTxManager_ConflictPropagation_returns_ErrTransitionConflict(t *testing.T) {
	t.Parallel()
	dbStore := setupTxManagerDB(t)
	mgr := NewTxManager(dbStore)

	err := mgr.RunInTx(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		// CAS guard that DOES NOT match (state is 'A', but WHERE says 'Z').
		result, err := tx.ExecContext(ctx,
			`UPDATE cas_test SET state = 'B' WHERE id = 1 AND state = 'Z'`,
		)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n == 0 {
			return ErrTransitionConflict
		}
		return nil
	})
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("RunInTx err = %v, want ErrTransitionConflict so callers can errors.Is on it", err)
	}
	if !strings.Contains(err.Error(), ErrTransitionConflict.Error()) {
		t.Errorf("err message = %q, want it to chain the ErrTransitionConflict.Error() sentinel", err.Error())
	}

	// Row unchanged (rolled back).
	var state string
	if err := dbStore.DB().QueryRow(
		`SELECT state FROM cas_test WHERE id = 1`,
	).Scan(&state); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if state != "A" {
		t.Errorf("state = %q, want A (rows must NOT be mutated by a CAS-miss fn)", state)
	}
}

// TestTxManager_ContextCancelMidFn_rolls_back pins the ctx
// propagation contract: cancelling ctx during fn causes subsequent
// ctx-aware sql operations to fail with context.Canceled, which fn
// returns, which RunInTx converts to a rollback.
func TestTxManager_ContextCancelMidFn_rolls_back(t *testing.T) {
	t.Parallel()
	dbStore := setupTxManagerDB(t)
	mgr := NewTxManager(dbStore)

	ctx, cancel := context.WithCancel(context.Background())
	err := mgr.RunInTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		// FIRST write — pre-cancellation, atomic boundary will undo this on rollback.
		if _, err := tx.ExecContext(ctx,
			`UPDATE cas_test SET runner_id = 'Y' WHERE id = 1`); err != nil {
			return err
		}
		// Trigger cancellation mid-fn.
		cancel()
		// Sub-Exec with cancelled ctx returns context.Canceled.
		_, err := tx.ExecContext(ctx,
			`UPDATE cas_test SET state = 'C' WHERE id = 1`)
		return err
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunInTx err = %v, want context.Canceled", err)
	}

	// No writes visible.
	var state, runner string
	if err := dbStore.DB().QueryRow(
		`SELECT state, runner_id FROM cas_test WHERE id = 1`,
	).Scan(&state, &runner); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if state != "A" {
		t.Errorf("state = %q, want A (cancel-rollback must undo ALL pre-cancel writes)", state)
	}
	if runner != "" {
		t.Errorf("runner_id = %q, want ''", runner)
	}
}

// TestTxManager_PanicSafety_rolls_back pins the panic-safety
// contract: a panic inside fn must NOT leak tx state. std lib
// *sql.Tx is documented to roll back on Commit-not-called, and the
// rollback cleanup runs at *sql.Tx destruction (a panic unwinds
// stack to the BeginTx callers' frame, exposing the tx to the GC;
// the rollback is the canonical cleanup path).
//
// We pin this with `defer recover()` so the panic doesn't crash the
// test runner — the assertion is that the row is NOT mutated.
func TestTxManager_PanicSafety_rolls_back(t *testing.T) {
	t.Parallel()
	dbStore := setupTxManagerDB(t)
	mgr := NewTxManager(dbStore)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from fn to propagate")
		}
		var state, runner string
		if err := dbStore.DB().QueryRow(
			`SELECT state, runner_id FROM cas_test WHERE id = 1`,
		).Scan(&state, &runner); err != nil {
			t.Fatalf("read row: %v", err)
		}
		if state != "A" || runner != "" {
			t.Errorf("panic-leak: state=%q runner=%q, want A and '' (panic must not commit)", state, runner)
		}
	}()

	_ = mgr.RunInTx(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		_, _ = tx.ExecContext(ctx,
			`UPDATE cas_test SET runner_id = 'PANIC' WHERE id = 1`)
		panic("simulated fn panic")
	})
}

// TestTxManager_NilGuards_return_clean_error pins the misuse-prevention
// contract: NewTxManager(nil) and RunInTx with nil callback must
// return typed sentinel errors instead of panicking, so a misconfig
// at boot surfaces a clean error rather than a runtime crash.
func TestTxManager_NilGuards_return_clean_error(t *testing.T) {
	t.Parallel()

	t.Run("nil_store", func(t *testing.T) {
		mgr := NewTxManager(nil)
		err := mgr.RunInTx(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
			t.Fatal("callback must NOT run when store is nil")
			return nil
		})
		if err == nil {
			t.Fatal("want error, got nil")
		}
		if !strings.Contains(err.Error(), "not initialized") {
			t.Errorf("err = %q, want it to mention uninitialized state", err.Error())
		}
	})

	t.Run("nil_callback", func(t *testing.T) {
		dbStore := setupTxManagerDB(t)
		mgr := NewTxManager(dbStore)
		err := mgr.RunInTx(context.Background(), nil)
		if err == nil {
			t.Fatal("want error, got nil")
		}
		if !strings.Contains(err.Error(), "nil callback") {
			t.Errorf("err = %q, want it to mention nil callback", err.Error())
		}
	})
}

// TestTxManager_ConcurrentCallersOnlyOneWins pins the
// multi-runner correctness contract: concurrent RunInTx calls on the
// same row produce exactly one winner. The losing CAS surface
// ErrTransitionConflict as-is; the helper's no-retry policy holds.
func TestTxManager_ConcurrentCallersOnlyOneWins(t *testing.T) {
	t.Parallel()
	dbStore := setupTxManagerDB(t)
	mgr := NewTxManager(dbStore)

	const concurrency = 8
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		winners  int
		noWinners int
	)
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			err := mgr.RunInTx(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
				// CAS guard: only one runner's UPDATE can match the
				// 'A' state at a time.
				result, err := tx.ExecContext(ctx,
					`UPDATE cas_test SET state = 'B', runner_id = 'W' WHERE id = 1 AND state = 'A'`,
				)
				if err != nil {
					return err
				}
				if n, _ := result.RowsAffected(); n == 0 {
					return ErrTransitionConflict
				}
				return nil
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				winners++
			case errors.Is(err, ErrTransitionConflict):
				noWinners++
			default:
				t.Errorf("unexpected err: %v", err)
			}
		}()
	}
	wg.Wait()

	if winners != 1 {
		t.Errorf("winners = %d, want exactly 1 (single-writer CAS invariant)", winners)
	}
	if noWinners != concurrency-1 {
		t.Errorf("losers = %d, want %d (CAS-loss callers must all see ErrTransitionConflict)",
			noWinners, concurrency-1)
	}

	// Final state: exactly one winner wrote the row.
	var state, runner string
	if err := dbStore.DB().QueryRow(
		`SELECT state, runner_id FROM cas_test WHERE id = 1`,
	).Scan(&state, &runner); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if state != "B" || runner != "W" {
		t.Errorf("state=%q runner=%q, want B and W (the winning tx's writes)", state, runner)
	}
}
