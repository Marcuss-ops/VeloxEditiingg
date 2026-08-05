package artifacts

import (
	"database/sql"

	"velox-server/internal/store"
)

// NewSQLiteUploadSessionWriter is retained as a compatibility constructor for
// package-local callers. The SQL implementation lives in internal/store;
// artifacts exposes only the consumer port and never owns a database handle.
func NewSQLiteUploadSessionWriter(db *sql.DB) UploadSessionWriter {
	return store.NewSQLiteUploadSessionWriter(db)
}
