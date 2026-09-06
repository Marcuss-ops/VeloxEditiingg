// COMPATIBILITY:
// Owner:        P0.4 store-facade migration
// Remove after: 2026-12-31 (sunset extended by the 2026-09 facade-migration audit: these
// shims are pure delegation with zero logic drift risk; re-audit at the
// 2026-Q4 sweep — see docs/adr/0008-soft-deprecate-vs-remove-pivot.md)
// Read-only:    yes

package store

import "velox-server/internal/completionstore"

// SQLiteCompletionStore is re-exported from the completionstore package,
// which owns the SQLite completion-protocol implementation.
type SQLiteCompletionStore = completionstore.SQLiteCompletionStore

// NewSQLiteCompletionStore is re-exported from the completionstore package.
var NewSQLiteCompletionStore = completionstore.NewSQLiteCompletionStore
