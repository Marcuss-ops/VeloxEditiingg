package artifacts

import "velox-server/internal/store"

// COMPATIBILITY:
// Owner:        P0.4 artifacts-store migration
// Remove after: 2026-09-30
// Read-only:    yes (delegates to the store writer; no second write path)
func NewSQLiteUploadSessionWriter(inner *store.SQLiteUploadSessionWriter) UploadSessionWriter {
	if inner == nil {
		panic("artifacts: NewSQLiteUploadSessionWriter requires a non-nil store writer")
	}
	return inner
}
