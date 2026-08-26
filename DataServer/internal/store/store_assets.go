// COMPATIBILITY:
// Owner:        P0.4 store-facade migration
// Remove after: 2026-09-30
// Read-only:    yes

package store

import (
	"fmt"
	"strings"

	"velox-server/internal/assets"
	"velox-server/internal/repository"
)

// store_assets.go owns the asset domain model and the SQLiteAssetRepository
// type: the assets-package re-exports, the job_assets projection shape, the
// sentinel errors, the constructor, and the compile-time interface guards.
// Write paths live in store_assets_write.go and read paths in
// store_assets_read.go.

// Re-export asset domain types from the assets package so callers within the
// store package (and downstream consumers) don't need to import assets directly.
type AssetRecord = assets.AssetRecord
type AssetSourceRecord = assets.AssetSourceRecord
type MediaMetadataRecord = assets.MediaMetadataRecord

// JobAssetRecord is the storage projection of a job_assets row.
type JobAssetRecord struct {
	JobID     string
	AssetID   string
	Role      string
	Ordinal   int
	Required  bool
	CreatedAt string
}

// AssetRepository is re-exported from the repository leaf package.
type AssetRepository = repository.AssetRepository

// ErrAssetAlreadyExists is returned when an insert violates a uniqueness constraint.
var ErrAssetAlreadyExists = fmt.Errorf("store: asset already exists")

// ErrAssetConflict is returned on CAS status transition mismatch.
var ErrAssetConflict = fmt.Errorf("store: asset transition conflict")

// SQLiteAssetRepository implements AssetRepository against SQLite.
type SQLiteAssetRepository struct {
	store *SQLiteStore
}

// NewSQLiteAssetRepository wraps a SQLiteStore as an AssetRepository.
func NewSQLiteAssetRepository(store *SQLiteStore) *SQLiteAssetRepository {
	return &SQLiteAssetRepository{store: store}
}

var _ AssetRepository = (*SQLiteAssetRepository)(nil)
var _ assets.AssetRepository = (*SQLiteAssetRepository)(nil)
var _ assets.VerifiedAssetRepository = (*SQLiteAssetRepository)(nil)

// Compile-time guard: every store.BlobStore implementation satisfies assets.BlobStore.
// This ensures the subset interface in assets doesn't drift from the canonical definition in store.
var _ assets.BlobStore = (*FilesystemBlobStore)(nil)
var _ assets.BlobStore = (*NopBlobStore)(nil)

func isUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		strings.Contains(msg, "PRIMARY KEY constraint failed") ||
		strings.Contains(msg, "constraint failed: UNIQUE") ||
		strings.Contains(msg, "constraint failed: PRIMARY KEY")
}
