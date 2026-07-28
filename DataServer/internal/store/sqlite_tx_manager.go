// Package store / sqlite_tx_manager.go
//
// TxManager encapsulates the Snapshot→CAS→Commit transactional
// primitive shared by every state-transition + lease method in the
// store package. The original BeginTx / defer Rollback / ExecContext
// CAS / RowsAffected==0 sentinel / Commit boilerplate appeared ~19
// times across the forwarding + delivery + completion-coordinator
// call sites. TxManager.RunInTx collapses that to a single, named
// helper so the SQL queries inside the closure focus on what changes
// per method, not on lifecycle invariants.
//
// YAGNI: NO retry loop. The historical choice at Velox is that
// ErrTransitionConflict surfaces to the caller for explicit
// contention handling (the runner decides to reclaim, retry, or
// escalate). Driver-level retries would silently mask Cas storms
// behind a one-line helper and make contention invisible to the
// caller. If a future site needs bounded CAS-storm retry, prefer a
// dedicated method (see completion/coordinator.go::RecordAttemptCommitsCAS
// for the existing CAS-storm primitive pattern) over a hidden retry
// here.
//
// Lifecycle invariants guaranteed by RunInTx:
//
//   - BeginTx failure aborts before callback runs; callback is NOT
//     called. The BeginTx error is returned wrapped as
//     "TxManager.RunInTx begin: %w".
//
//   - Callback receives the ctx AND *sql.Tx; it returns non-nil on
//     any CAS miss, SQL error, or sub-operation failure.
//
//   - On callback error, tx.Rollback() is invoked. The callback's
//     NON-NIL return value is the canonical cause; rollback errors
//     are reported in the wrapped message but do NOT mask it.
//
//   - On callback success, tx.Commit() is invoked. Commit failure is
//     returned wrapped as "TxManager.RunInTx commit: %w".
package store

import (
	"context"
	"database/sql"
	"fmt"
)

// TxManager wraps the SQLite connection bound to *SQLiteStore and
// exposes a single transactional primitive: RunInTx. Construct via
// NewTxManager (factory pattern, mirrors NewAtomicJobTaskCreator).
//
// TxManager is intentionally stateless — every RunInTx call opens a
// fresh *sql.Tx via BeginTx. The state belongs to the closure
// arguments; mixing state across calls would re-introduce the
// connection-pool contention that BeginTx per call prevents.
type TxManager struct {
	store *SQLiteStore
}

// NewTxManager constructs a TxManager bound to the given store.
// Cheap; callers typically construct one per call site
//
//	NewTxManager(s).RunInTx(ctx, func(ctx context.Context, tx *sql.Tx) error { ... })
//
// because the type carries no per-instance state (every RunInTx opens
// a fresh *sql.Tx). A long-lived *TxManager per *SQLiteStore is also
// safe; no synchronization needed.
func NewTxManager(s *SQLiteStore) *TxManager {
	return &TxManager{store: s}
}

// RunInTx opens a *sql.Tx, invokes fn(ctx, tx), and commits iff fn
// returns nil. Any non-nil return from fn rolls back the transaction;
// the fn's error is propagated to the caller. Commit failures are
// surfaced verbatim (wrapped). Rollback failures are reported in the
// wrapped message but do NOT mask the original fn error.
//
// Behavior contracts (locked by sqlite_tx_manager_test.go):
//
//   1. fn returns nil → tx.Commit() runs; if Commit fails, RunInTx
//      returns the Commit error. The fn's writes are NOT visible.
//   2. fn returns non-nil → tx.Rollback() runs; if Rollback fails,
//      RunInTx returns the fn error wrapped with the Rollback error
//      in an extra annotation ("rollback also failed: ...") so the
//      diagnostic is preserved without losing the root cause.
//   3. fn panics → tx.Rollback runs via the deferred recover-cleanup
//      path (std lib tx semantics); panic propagates unchanged.
//      (We do NOT add an explicit recover — std lib *sql.Tx handles
//      this; the explicit defer-rollback pattern in
//      pre-RunInTx callers is itself the proven panic-safety shape.)
//
// Caller invariants:
//
//   - fn MUST NOT call tx.Commit() or tx.Rollback() directly. RunInTx
//     owns the transactional state machine; nested commits/rollbacks
//     would corrupt the deferred-rollback guard.
//   - fn SHOULD return ErrTransitionConflict on RowsAffected==0 from
//     the CAS UPDATE; this is the canonical typed signal callers
//     route on for contention handling.
//   - fn SHOULD use the supplied ctx (not a parent ctx) for every
//     sub-Exec / sub-Query call so context cancellation propagates
//     uniformly through RunInTx.
func (m *TxManager) RunInTx(ctx context.Context, fn func(ctx context.Context, tx *sql.Tx) error) error {
	if m == nil || m.store == nil || m.store.db == nil {
		return fmt.Errorf("TxManager.RunInTx: not initialized")
	}
	if fn == nil {
		return fmt.Errorf("TxManager.RunInTx: nil callback")
	}

	tx, err := m.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("TxManager.RunInTx begin: %w", err)
	}

	fnErr := fn(ctx, tx)
	if fnErr != nil {
		// Caller signaled insufficient state for the COMMIT boundary.
		// Best-effort rollback — std-lib guarantees the tx is rolled
		// back even if Rollback() returns an error here, so the
		// cleanup path is fail-safe at the *sql.Tx layer; the
		// Rollback return value is purely diagnostic.
		if rbErr := tx.Rollback(); rbErr != nil {
			// Preserve the root cause. The cb error wins; rbErr
			// is appended for operator diagnosis.
			return fmt.Errorf("TxManager.RunInTx: callback returned error (rollback also failed: %v): %w", rbErr, fnErr)
		}
		return fnErr
	}

	if cErr := tx.Commit(); cErr != nil {
		return fmt.Errorf("TxManager.RunInTx commit: %w", cErr)
	}
	return nil
}
