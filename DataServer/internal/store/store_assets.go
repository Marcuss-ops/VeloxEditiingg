package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/assets"
)

// Re-export asset domain types from the assets package so callers within the
// store package (and downstream consumers) don't need to import assets directly.
type AssetRecord = assets.AssetRecord
type AssetSourceRecord = assets.AssetSourceRecord

// JobAssetRecord is the storage projection of a job_assets row.
type JobAssetRecord struct {
	JobID     string
	AssetID   string
	Role      string
	Ordinal   int
	Required  bool
	CreatedAt string
}

// AssetRepository is the narrow write-aware contract for the generic asset registry.
type AssetRepository interface {
	// Insert creates a new asset row. Returns ErrAssetAlreadyExists if the
	// SHA-256 or (storage_provider, storage_key) conflicts.
	Insert(ctx context.Context, a AssetRecord) error
	// GetByID returns a single asset, or (nil, nil) on missing.
	GetByID(ctx context.Context, assetID string) (*AssetRecord, error)
	// GetBySHA256 returns the asset with the given SHA-256, or (nil, nil).
	GetBySHA256(ctx context.Context, sha256 string) (*AssetRecord, error)
	// UpdateStatus atomically transitions status (CAS on from).
	UpdateStatus(ctx context.Context, assetID, from, to string) error
	// SoftDelete sets deleted_at and status=DELETED.
	SoftDelete(ctx context.Context, assetID string) error
	// InsertSource records provenance for an asset.
	InsertSource(ctx context.Context, s AssetSourceRecord) error
	// LinkToJob binds an asset to a job with a role and ordinal.
	LinkToJob(ctx context.Context, jobID, assetID, role string, ordinal int, required bool) error
	// ListByJob returns all assets linked to a job.
	ListByJob(ctx context.Context, jobID string) ([]AssetRecord, error)
}

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

func (r *SQLiteAssetRepository) Insert(ctx context.Context, a AssetRecord) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("asset repository: store not initialized")
	}
	if a.AssetID == "" {
		return fmt.Errorf("asset repository: empty asset_id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if a.CreatedAt == "" {
		a.CreatedAt = now
	}
	_, err := r.store.db.ExecContext(ctx,
		`INSERT INTO assets (asset_id, kind, status, sha256, mime_type, size_bytes,
		                     storage_provider, storage_key, metadata_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.AssetID, a.Kind, a.Status, a.SHA256, a.MimeType, a.SizeBytes,
		a.StorageProvider, a.StorageKey, nullIfEmpty(a.MetadataJSON), a.CreatedAt,
	)
	if err != nil {
		if isUniqueConstraintError(err) {
			return fmt.Errorf("asset %s: %w", a.AssetID, ErrAssetAlreadyExists)
		}
		return fmt.Errorf("insert asset: %w", err)
	}
	return nil
}

func (r *SQLiteAssetRepository) GetByID(ctx context.Context, assetID string) (*AssetRecord, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("asset repository: store not initialized")
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT asset_id, kind, status, sha256, COALESCE(mime_type,''),
		        size_bytes, storage_provider, storage_key, COALESCE(metadata_json,''),
		        created_at, COALESCE(verified_at,''), COALESCE(deleted_at,'')
		 FROM assets WHERE asset_id = ?`, assetID,
	)
	var a AssetRecord
	err := row.Scan(&a.AssetID, &a.Kind, &a.Status, &a.SHA256, &a.MimeType,
		&a.SizeBytes, &a.StorageProvider, &a.StorageKey, &a.MetadataJSON,
		&a.CreatedAt, &a.VerifiedAt, &a.DeletedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get asset by id: %w", err)
	}
	return &a, nil
}

func (r *SQLiteAssetRepository) GetBySHA256(ctx context.Context, sha256 string) (*AssetRecord, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("asset repository: store not initialized")
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT asset_id, kind, status, sha256, COALESCE(mime_type,''),
		        size_bytes, storage_provider, storage_key, COALESCE(metadata_json,''),
		        created_at, COALESCE(verified_at,''), COALESCE(deleted_at,'')
		 FROM assets WHERE sha256 = ?`, sha256,
	)
	var a AssetRecord
	err := row.Scan(&a.AssetID, &a.Kind, &a.Status, &a.SHA256, &a.MimeType,
		&a.SizeBytes, &a.StorageProvider, &a.StorageKey, &a.MetadataJSON,
		&a.CreatedAt, &a.VerifiedAt, &a.DeletedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get asset by sha256: %w", err)
	}
	return &a, nil
}

func (r *SQLiteAssetRepository) UpdateStatus(ctx context.Context, assetID, from, to string) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("asset repository: store not initialized")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	var setClauses string
	if to == "READY" {
		setClauses = fmt.Sprintf("status = ?, verified_at = '%s'", now)
	} else if to == "DELETED" {
		setClauses = fmt.Sprintf("status = ?, deleted_at = '%s'", now)
	} else {
		setClauses = "status = ?"
	}
	res, err := r.store.db.ExecContext(ctx,
		fmt.Sprintf(`UPDATE assets SET %s WHERE asset_id = ? AND status = ?`, setClauses),
		to, assetID, from,
	)
	if err != nil {
		return fmt.Errorf("update asset status: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update asset status rows: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("asset %s: %w (expected status %s)", assetID, ErrAssetConflict, from)
	}
	return nil
}

func (r *SQLiteAssetRepository) SoftDelete(ctx context.Context, assetID string) error {
	return r.UpdateStatus(ctx, assetID, "READY", "DELETED")
}

func (r *SQLiteAssetRepository) InsertSource(ctx context.Context, s AssetSourceRecord) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("asset repository: store not initialized")
	}
	if s.SourceID == "" {
		return fmt.Errorf("asset source repository: empty source_id")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if s.CreatedAt == "" {
		s.CreatedAt = now
	}
	_, err := r.store.db.ExecContext(ctx,
		`INSERT INTO asset_sources (source_id, asset_id, source_type, source_reference,
		                           source_account_id, metadata_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		s.SourceID, s.AssetID, s.SourceType, s.SourceReference,
		nullIfEmpty(s.SourceAccountID), nullIfEmpty(s.MetadataJSON), s.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert asset source: %w", err)
	}
	return nil
}

func (r *SQLiteAssetRepository) LinkToJob(ctx context.Context, jobID, assetID, role string, ordinal int, required bool) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("asset repository: store not initialized")
	}
	now := time.Now().UTC().Format(time.RFC3339)
	reqInt := 0
	if required {
		reqInt = 1
	}
	_, err := r.store.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO job_assets (job_id, asset_id, role, ordinal, required, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		jobID, assetID, role, ordinal, reqInt, now,
	)
	if err != nil {
		return fmt.Errorf("link asset to job: %w", err)
	}
	return nil
}

func (r *SQLiteAssetRepository) ListByJob(ctx context.Context, jobID string) ([]AssetRecord, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("asset repository: store not initialized")
	}
	rows, err := r.store.db.QueryContext(ctx,
		`SELECT a.asset_id, a.kind, a.status, a.sha256, COALESCE(a.mime_type,''),
		        a.size_bytes, a.storage_provider, a.storage_key, COALESCE(a.metadata_json,''),
		        a.created_at, COALESCE(a.verified_at,''), COALESCE(a.deleted_at,'')
		 FROM assets a
		 JOIN job_assets ja ON ja.asset_id = a.asset_id
		 WHERE ja.job_id = ?
		 ORDER BY ja.ordinal`, jobID,
	)
	if err != nil {
		return nil, fmt.Errorf("list assets by job: %w", err)
	}
	defer rows.Close()

	var assets []AssetRecord
	for rows.Next() {
		var a AssetRecord
		if err := rows.Scan(&a.AssetID, &a.Kind, &a.Status, &a.SHA256, &a.MimeType,
			&a.SizeBytes, &a.StorageProvider, &a.StorageKey, &a.MetadataJSON,
			&a.CreatedAt, &a.VerifiedAt, &a.DeletedAt); err != nil {
			continue
		}
		assets = append(assets, a)
	}
	return assets, rows.Err()
}

var _ AssetRepository = (*SQLiteAssetRepository)(nil)

// Compile-time guard: every store.BlobStore implementation satisfies assets.BlobStore.
// This ensures the subset interface in assets doesn't drift from the canonical definition in store.
var _ assets.BlobStore = (*LocalBlobStore)(nil)
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
