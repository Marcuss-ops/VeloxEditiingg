package store

import (
	"context"
	"fmt"
	"strings"
)

// store_assets_write.go owns every write path for assets and their dependent
// tables (asset_media_metadata, asset_sources, job_assets): the asset INSERTs,
// the verified composite inserts, the source insert, the media-metadata
// upsert, and the status transitions. Read paths live in store_assets_read.go.

func (r *SQLiteAssetRepository) Insert(ctx context.Context, a AssetRecord) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("asset repository: store not initialized")
	}
	if a.AssetID == "" {
		return fmt.Errorf("asset repository: empty asset_id")
	}
	now := nowRFC3339()
	if a.CreatedAt == "" {
		a.CreatedAt = now
	}
	_, err := r.store.db.ExecContext(ctx,
		`INSERT INTO assets (asset_id, kind, status, sha256, mime_type, size_bytes,
		                     storage_provider, storage_key, metadata_json, created_at, verified_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.AssetID, a.Kind, a.Status, a.SHA256, a.MimeType, a.SizeBytes,
		a.StorageProvider, a.StorageKey, nullIfEmpty(a.MetadataJSON), a.CreatedAt, nullIfEmpty(a.VerifiedAt),
	)
	if err != nil {
		if isUniqueConstraintError(err) {
			return fmt.Errorf("asset %s: %w", a.AssetID, ErrAssetAlreadyExists)
		}
		return fmt.Errorf("insert asset: %w", err)
	}
	return nil
}

// InsertWithMediaMetadata atomically writes a verified asset row and its
// registry-authoritative media metadata. It is the fail-closed insertion
// surface for final_audio; a metadata constraint/persistence failure rolls
// back the asset INSERT so no READY row can exist without its proof.
func (r *SQLiteAssetRepository) InsertWithMediaMetadata(ctx context.Context, a AssetRecord, m MediaMetadataRecord) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("asset repository: store not initialized")
	}
	if strings.TrimSpace(a.AssetID) == "" {
		return fmt.Errorf("asset repository: empty asset_id")
	}
	if strings.TrimSpace(m.AssetID) == "" {
		m.AssetID = a.AssetID
	}
	if m.AssetID != a.AssetID {
		return fmt.Errorf("asset repository: media metadata asset_id mismatch")
	}
	if !m.Verified() {
		return fmt.Errorf("asset repository: media metadata is not verified")
	}

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin verified asset insert: %w", err)
	}
	defer tx.Rollback()

	now := nowRFC3339()
	if a.CreatedAt == "" {
		a.CreatedAt = now
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO assets (asset_id, kind, status, sha256, mime_type, size_bytes,
		                     storage_provider, storage_key, metadata_json, created_at, verified_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.AssetID, a.Kind, a.Status, a.SHA256, a.MimeType, a.SizeBytes,
		a.StorageProvider, a.StorageKey, nullIfEmpty(a.MetadataJSON), a.CreatedAt, nullIfEmpty(a.VerifiedAt),
	); err != nil {
		if isUniqueConstraintError(err) {
			return fmt.Errorf("asset %s: %w", a.AssetID, ErrAssetAlreadyExists)
		}
		return fmt.Errorf("insert verified asset: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO asset_media_metadata
			(asset_id, container, duration_ms, video_codec, pix_fmt, width, height,
		 fps_num, fps_den, time_base_num, time_base_den, audio_codec,
		 audio_sample_rate, audio_channels, metadata_verified_at, metadata_schema_version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.AssetID, m.Container, m.DurationMs, m.VideoCodec, m.PixelFormat, m.Width, m.Height,
		m.FPSNum, m.FPSDen, m.TimeBaseNum, m.TimeBaseDen, m.AudioCodec,
		m.AudioSampleRate, m.AudioChannels, nullIfEmpty(m.MetadataVerifiedAt), m.MetadataSchemaVersion,
	); err != nil {
		return fmt.Errorf("insert verified asset metadata: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit verified asset insert: %w", err)
	}
	return nil
}

// InsertWithMediaMetadataAndSource atomically writes a verified asset, its
// registry-authoritative media metadata, and its source provenance. This is
// the complete final_audio commit boundary: any failure rolls back all rows.
func (r *SQLiteAssetRepository) InsertWithMediaMetadataAndSource(ctx context.Context, a AssetRecord, m MediaMetadataRecord, source AssetSourceRecord) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("asset repository: store not initialized")
	}
	if strings.TrimSpace(a.AssetID) == "" || strings.TrimSpace(source.AssetID) != a.AssetID {
		return fmt.Errorf("asset repository: source must identify inserted asset")
	}
	if strings.TrimSpace(m.AssetID) == "" {
		m.AssetID = a.AssetID
	}
	if m.AssetID != a.AssetID {
		return fmt.Errorf("asset repository: media metadata asset_id mismatch")
	}
	if !m.Verified() {
		return fmt.Errorf("asset repository: media metadata is not verified")
	}
	if strings.TrimSpace(source.SourceID) == "" {
		return fmt.Errorf("asset repository: source_id is required")
	}

	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin verified asset/source insert: %w", err)
	}
	defer tx.Rollback()

	now := nowRFC3339()
	if a.CreatedAt == "" {
		a.CreatedAt = now
	}
	if source.CreatedAt == "" {
		source.CreatedAt = a.CreatedAt
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO assets (asset_id, kind, status, sha256, mime_type, size_bytes,
		                     storage_provider, storage_key, metadata_json, created_at, verified_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		a.AssetID, a.Kind, a.Status, a.SHA256, a.MimeType, a.SizeBytes,
		a.StorageProvider, a.StorageKey, nullIfEmpty(a.MetadataJSON), a.CreatedAt, nullIfEmpty(a.VerifiedAt),
	); err != nil {
		if isUniqueConstraintError(err) {
			return fmt.Errorf("asset %s: %w", a.AssetID, ErrAssetAlreadyExists)
		}
		return fmt.Errorf("insert verified asset/source: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO asset_media_metadata
			(asset_id, container, duration_ms, video_codec, pix_fmt, width, height,
		 fps_num, fps_den, time_base_num, time_base_den, audio_codec,
		 audio_sample_rate, audio_channels, metadata_verified_at, metadata_schema_version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.AssetID, m.Container, m.DurationMs, m.VideoCodec, m.PixelFormat, m.Width, m.Height,
		m.FPSNum, m.FPSDen, m.TimeBaseNum, m.TimeBaseDen, m.AudioCodec,
		m.AudioSampleRate, m.AudioChannels, nullIfEmpty(m.MetadataVerifiedAt), m.MetadataSchemaVersion,
	); err != nil {
		return fmt.Errorf("insert verified asset metadata/source: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO asset_sources (source_id, asset_id, source_type, source_reference,
		                           source_account_id, metadata_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		source.SourceID, source.AssetID, source.SourceType, source.SourceReference,
		nullIfEmpty(source.SourceAccountID), nullIfEmpty(source.MetadataJSON), source.CreatedAt,
	); err != nil {
		return fmt.Errorf("insert verified asset source: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit verified asset/source insert: %w", err)
	}
	return nil
}

func (r *SQLiteAssetRepository) InsertSource(ctx context.Context, s AssetSourceRecord) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("asset repository: store not initialized")
	}
	if s.SourceID == "" {
		return fmt.Errorf("asset source repository: empty source_id")
	}
	now := nowRFC3339()
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

// UpsertMediaMetadata persists (insert-or-replace) the canonical one-time
// media probe row for an asset (Fase C1: registry-authoritative metadata;
// consumers read it instead of spawning ffprobe).
func (r *SQLiteAssetRepository) UpsertMediaMetadata(ctx context.Context, assetID string, m MediaMetadataRecord) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("asset repository: store not initialized")
	}
	if strings.TrimSpace(assetID) == "" {
		return fmt.Errorf("asset repository: empty asset_id")
	}
	_, err := r.store.db.ExecContext(ctx, `
		INSERT INTO asset_media_metadata
			(asset_id, container, duration_ms, video_codec, pix_fmt, width, height,
		 fps_num, fps_den, time_base_num, time_base_den, audio_codec,
		 audio_sample_rate, audio_channels, metadata_verified_at, metadata_schema_version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(asset_id) DO UPDATE SET
			container = excluded.container,
			duration_ms = excluded.duration_ms,
			video_codec = excluded.video_codec,
			pix_fmt = excluded.pix_fmt,
			width = excluded.width,
			height = excluded.height,
			fps_num = excluded.fps_num,
			fps_den = excluded.fps_den,
			time_base_num = excluded.time_base_num,
			time_base_den = excluded.time_base_den,
			audio_codec = excluded.audio_codec,
			audio_sample_rate = excluded.audio_sample_rate,
			audio_channels = excluded.audio_channels,
			metadata_verified_at = excluded.metadata_verified_at,
			metadata_schema_version = excluded.metadata_schema_version`,
		assetID, m.Container, m.DurationMs, m.VideoCodec, m.PixelFormat, m.Width, m.Height,
		m.FPSNum, m.FPSDen, m.TimeBaseNum, m.TimeBaseDen, m.AudioCodec,
		m.AudioSampleRate, m.AudioChannels, nullIfEmpty(m.MetadataVerifiedAt), m.MetadataSchemaVersion,
	)
	if err != nil {
		return fmt.Errorf("upsert asset media metadata: %w", err)
	}
	return nil
}

func (r *SQLiteAssetRepository) UpdateStatus(ctx context.Context, assetID, from, to string) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("asset repository: store not initialized")
	}
	now := nowRFC3339()
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

func (r *SQLiteAssetRepository) LinkToJob(ctx context.Context, jobID, assetID, role string, ordinal int, required bool) error {
	if r.store == nil || r.store.db == nil {
		return fmt.Errorf("asset repository: store not initialized")
	}
	now := nowRFC3339()
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
