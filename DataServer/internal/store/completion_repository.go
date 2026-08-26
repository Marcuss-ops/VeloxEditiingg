// COMPATIBILITY:
// Owner:        P0.4 store-facade migration
// Remove after: 2026-09-30
// Read-only:    yes

package store

import "velox-server/internal/completionstore"

// SQLiteCompletionStore is re-exported from the completionstore package,
// which owns the SQLite completion-protocol implementation.
type SQLiteCompletionStore = completionstore.SQLiteCompletionStore

// NewSQLiteCompletionStore is re-exported from the completionstore package.
var NewSQLiteCompletionStore = completionstore.NewSQLiteCompletionStore
