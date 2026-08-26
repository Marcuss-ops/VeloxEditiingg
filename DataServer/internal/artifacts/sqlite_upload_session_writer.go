package artifacts

import "velox-server/internal/artifactsstore"

// COMPATIBILITY:
// Owner:        P0.4 artifacts-store migration
// Remove after: 2026-09-30
// Read-only:    yes (delegates to the store writer; no second write path)
func NewSQLiteUploadSessionWriter(inner *artifactsstore.SQLiteUploadSessionWriter) UploadSessionWriter {
	if inner == nil {
		panic("artifacts: NewSQLiteUploadSessionWriter requires a non-nil artifactsstore writer")
	}
	return inner
}
