package store

import (
	"context"
	"database/sql"
	"fmt"
)

// store_assets_read.go owns the read paths for assets and their dependent
// tables (asset_media_metadata, asset_sources, job_assets). Write paths live
// in store_assets_write.go.

// GetByIDAndWorkspace returns an asset only if it belongs to the given
// workspace. Rows with NULL workspace_id are not returned.
func (r *SQLiteAssetRepository) GetByIDAndWorkspace(ctx context.Context, assetID string, workspaceID int64) (*AssetRecord, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("asset repository: store not initialized")
	}
	row := r.store.db.QueryRowContext(ctx,
		`SELECT asset_id, kind, status, sha256, COALESCE(mime_type,''),
		        size_bytes, storage_provider, storage_key, COALESCE(metadata_json,''),
		        created_at, COALESCE(verified_at,''), COALESCE(deleted_at,'')
		 FROM assets WHERE asset_id = ? AND workspace_id = ?`, assetID, workspaceID,
	)
	var a AssetRecord
	err := row.Scan(&a.AssetID, &a.Kind, &a.Status, &a.SHA256, &a.MimeType,
		&a.SizeBytes, &a.StorageProvider, &a.StorageKey, &a.MetadataJSON,
		&a.CreatedAt, &a.VerifiedAt, &a.DeletedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get asset by id and workspace: %w", err)
	}
	return &a, nil
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

// GetBySourceReference returns the newest READY asset registered for an exact
// provider reference. This is the durable reverse index for deferred sources
// materialized by the remote worker bridge.
func (r *SQLiteAssetRepository) GetBySourceReference(ctx context.Context, reference string) (*AssetRecord, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("asset repository: store not initialized")
	}
	row := r.store.db.QueryRowContext(ctx, `
		SELECT a.asset_id, a.kind, a.status, a.sha256, COALESCE(a.mime_type,''),
		       a.size_bytes, a.storage_provider, a.storage_key, CASE WHEN s.source_reference LIKE 'prepared-window:%' THEN COALESCE(s.metadata_json,'') ELSE COALESCE(a.metadata_json,'') END,
		       a.created_at, COALESCE(a.verified_at,''), COALESCE(a.deleted_at,'')
		  FROM assets a
		  JOIN asset_sources s ON s.asset_id = a.asset_id
		 WHERE s.source_reference = ? AND a.status = 'READY'
		 ORDER BY s.created_at DESC, s.source_id DESC
		 LIMIT 1`, reference)
	var a AssetRecord
	err := row.Scan(&a.AssetID, &a.Kind, &a.Status, &a.SHA256, &a.MimeType,
		&a.SizeBytes, &a.StorageProvider, &a.StorageKey, &a.MetadataJSON,
		&a.CreatedAt, &a.VerifiedAt, &a.DeletedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get asset by source reference: %w", err)
	}
	return &a, nil
}

// GetBySourceReferences returns the newest READY asset for each exact source
// reference in one indexed read. It avoids one SQLite round trip per cache key
// when FINALIZE checks a large prepared-window set.
func (r *SQLiteAssetRepository) GetBySourceReferences(ctx context.Context, references []string) (map[string]*AssetRecord, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("asset repository: store not initialized")
	}
	out := make(map[string]*AssetRecord, len(references))
	const maxParameters = 900
	for offset := 0; offset < len(references); offset += maxParameters {
		end := offset + maxParameters
		if end > len(references) {
			end = len(references)
		}
		chunk := references[offset:end]
		placeholders := make([]byte, 0, len(chunk)*2)
		args := make([]interface{}, len(chunk))
		for i, reference := range chunk {
			if i > 0 {
				placeholders = append(placeholders, ',')
			}
			placeholders = append(placeholders, '?')
			args[i] = reference
		}
		query := `SELECT s.source_reference, a.asset_id, a.kind, a.status,
			COALESCE(a.sha256,''), COALESCE(a.mime_type,''), a.size_bytes,
			COALESCE(a.storage_provider,''), COALESCE(a.storage_key,''),
			CASE WHEN s.source_reference LIKE 'prepared-window:%' THEN COALESCE(s.metadata_json,'') ELSE COALESCE(a.metadata_json,'') END, a.created_at, COALESCE(a.verified_at,''),
			COALESCE(a.deleted_at,'')
			FROM asset_sources s JOIN assets a ON a.asset_id=s.asset_id
			WHERE s.source_reference IN (` + string(placeholders) + `) AND a.status='READY'
			ORDER BY s.source_reference, s.created_at DESC, s.source_id DESC`
		rows, err := r.store.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("get assets by source references: %w", err)
		}
		for rows.Next() {
			var reference string
			a := new(AssetRecord)
			if err := rows.Scan(&reference, &a.AssetID, &a.Kind, &a.Status,
				&a.SHA256, &a.MimeType, &a.SizeBytes, &a.StorageProvider,
				&a.StorageKey, &a.MetadataJSON, &a.CreatedAt, &a.VerifiedAt,
				&a.DeletedAt); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan assets by source references: %w", err)
			}
			if _, exists := out[reference]; !exists {
				out[reference] = a
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("iterate assets by source references: %w", err)
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("close assets by source references: %w", err)
		}
	}
	return out, nil
}

// GetMediaMetadata returns the verified media metadata row for an asset, or
// (nil, nil) when no row exists (metadata_verified=false).
func (r *SQLiteAssetRepository) GetMediaMetadata(ctx context.Context, assetID string) (*MediaMetadataRecord, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("asset repository: store not initialized")
	}
	row := r.store.db.QueryRowContext(ctx, `
		SELECT asset_id, container, duration_ms, video_codec, pix_fmt, width, height,
		       fps_num, fps_den, time_base_num, time_base_den, audio_codec,
		       audio_sample_rate, audio_channels,
		       COALESCE(metadata_verified_at,''), metadata_schema_version
		 FROM asset_media_metadata WHERE asset_id = ?`, assetID,
	)
	var m MediaMetadataRecord
	err := row.Scan(&m.AssetID, &m.Container, &m.DurationMs, &m.VideoCodec, &m.PixelFormat,
		&m.Width, &m.Height, &m.FPSNum, &m.FPSDen, &m.TimeBaseNum, &m.TimeBaseDen,
		&m.AudioCodec, &m.AudioSampleRate, &m.AudioChannels,
		&m.MetadataVerifiedAt, &m.MetadataSchemaVersion)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get asset media metadata: %w", err)
	}
	return &m, nil
}

// ListSources returns the newest registered source first. Asset IDs are
// content-addressed, so callers must use SourceReference for external
// acquisition rather than treating the asset ID as a Drive file ID.
func (r *SQLiteAssetRepository) ListSources(ctx context.Context, assetID string) ([]AssetSourceRecord, error) {
	if r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("asset repository: store not initialized")
	}
	rows, err := r.store.db.QueryContext(ctx, `
		SELECT source_id, asset_id, source_type, source_reference,
		       COALESCE(source_account_id,''), COALESCE(metadata_json,''), created_at
		  FROM asset_sources
		 WHERE asset_id = ?
		 ORDER BY created_at DESC, source_id DESC`, assetID)
	if err != nil {
		return nil, fmt.Errorf("list asset sources: %w", err)
	}
	defer rows.Close()
	var sources []AssetSourceRecord
	for rows.Next() {
		var source AssetSourceRecord
		if err := rows.Scan(&source.SourceID, &source.AssetID, &source.SourceType,
			&source.SourceReference, &source.SourceAccountID, &source.MetadataJSON,
			&source.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan asset source: %w", err)
		}
		sources = append(sources, source)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate asset sources: %w", err)
	}
	return sources, nil
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
			return nil, fmt.Errorf("scan asset: %w", err)
		}
		assets = append(assets, a)
	}
	return assets, rows.Err()
}
