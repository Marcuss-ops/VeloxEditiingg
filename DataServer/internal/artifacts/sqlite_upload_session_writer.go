package artifacts

import "velox-server/internal/artifactsstore"

// COMPATIBILITY:
// Owner:        P0.4 artifacts-store migration
// Remove after: 2026-12-31 (sunset extended by the 2026-09 facade-migration audit: these
// shims are pure delegation with zero logic drift risk; re-audit at the
// 2026-Q4 sweep — see docs/adr/0008-soft-deprecate-vs-remove-pivot.md)
// Read-only:    yes (delegates to the store writer; no second write path)
func NewSQLiteUploadSessionWriter(inner *artifactsstore.SQLiteUploadSessionWriter) UploadSessionWriter {
	if inner == nil {
		panic("artifacts: NewSQLiteUploadSessionWriter requires a non-nil artifactsstore writer")
	}
	return inner
}
