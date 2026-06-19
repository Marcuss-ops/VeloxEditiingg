package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

func (s *SQLiteStore) CreateAsset(ctx context.Context, asset *Asset) error {
	if asset.ID == "" {
		asset.ID = fmt.Sprintf("asset_%d", time.Now().UnixNano())
	}
	if asset.StorageType == "" {
		asset.StorageType = "local"
	}
	metadata, _ := json.Marshal(asset.Metadata)

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO dark_editor_assets (id, project_id, user_id, type, filename, original_filename, storage_path, storage_type, mime_type, size_bytes, width, height, metadata, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		asset.ID, asset.ProjectID, asset.UserID, asset.Type, asset.Filename,
		asset.OriginalFilename, asset.StoragePath, asset.StorageType,
		asset.MimeType, asset.SizeBytes, asset.Width, asset.Height,
		string(metadata), time.Now().Format(time.RFC3339),
	)
	return err
}

func (s *SQLiteStore) GetAsset(ctx context.Context, id string) (*Asset, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, project_id, user_id, type, filename, original_filename, storage_path, storage_type, mime_type, size_bytes, width, height, metadata, created_at
		 FROM dark_editor_assets WHERE id = ?`, id)

	asset := &Asset{}
	var metadata []byte
	var projectID, userID sql.NullString

	err := row.Scan(&asset.ID, &projectID, &userID, &asset.Type, &asset.Filename,
		&asset.OriginalFilename, &asset.StoragePath, &asset.StorageType,
		&asset.MimeType, &asset.SizeBytes, &asset.Width, &asset.Height,
		&metadata, &asset.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if projectID.Valid {
		asset.ProjectID = &projectID.String
	}
	if userID.Valid {
		asset.UserID = &userID.String
	}
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &asset.Metadata); err != nil {
			asset.Metadata = make(map[string]interface{})
		}
	}
	return asset, nil
}

func (s *SQLiteStore) GetAssetByFilename(ctx context.Context, filename string) (*Asset, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, project_id, user_id, type, filename, original_filename, storage_path, storage_type, mime_type, size_bytes, width, height, metadata, created_at
		 FROM dark_editor_assets WHERE filename = ?`, filename)

	asset := &Asset{}
	var metadata []byte
	var projectID, userID sql.NullString

	err := row.Scan(&asset.ID, &projectID, &userID, &asset.Type, &asset.Filename,
		&asset.OriginalFilename, &asset.StoragePath, &asset.StorageType,
		&asset.MimeType, &asset.SizeBytes, &asset.Width, &asset.Height,
		&metadata, &asset.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if projectID.Valid {
		asset.ProjectID = &projectID.String
	}
	if userID.Valid {
		asset.UserID = &userID.String
	}
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &asset.Metadata); err != nil {
			asset.Metadata = make(map[string]interface{})
		}
	}
	return asset, nil
}

func (s *SQLiteStore) DeleteAsset(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM dark_editor_assets WHERE id = ?`, id)
	return err
}

func (s *SQLiteStore) ListProjectAssets(ctx context.Context, projectID string) ([]*Asset, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, project_id, user_id, type, filename, original_filename, storage_path, storage_type, mime_type, size_bytes, width, height, metadata, created_at
		 FROM dark_editor_assets WHERE project_id = ? ORDER BY created_at DESC`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	assets := []*Asset{}
	for rows.Next() {
		asset := &Asset{}
		var metadata []byte
		var projectID, userID sql.NullString

		if err := rows.Scan(&asset.ID, &projectID, &userID, &asset.Type, &asset.Filename,
			&asset.OriginalFilename, &asset.StoragePath, &asset.StorageType,
			&asset.MimeType, &asset.SizeBytes, &asset.Width, &asset.Height,
			&metadata, &asset.CreatedAt); err != nil {
			continue
		}

		if projectID.Valid {
			asset.ProjectID = &projectID.String
		}
		if userID.Valid {
			asset.UserID = &userID.String
		}
		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &asset.Metadata); err != nil {
				asset.Metadata = make(map[string]interface{})
			}
		}
		assets = append(assets, asset)
	}
	return assets, nil
}

func (s *SQLiteStore) CreateGenerationRecord(ctx context.Context, record *GenerationRecord) error {
	if record.ID == "" {
		record.ID = fmt.Sprintf("gen_%d", time.Now().UnixNano())
	}
	if record.Model == "" {
		record.Model = "flux.1-schnell"
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO dark_editor_generations (id, user_id, project_id, prompt, negative_prompt, model, width, height, steps, seed, asset_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ID, record.UserID, record.ProjectID, record.Prompt, record.NegativePrompt,
		record.Model, record.Width, record.Height, record.Steps, record.Seed,
		record.AssetID, time.Now().Format(time.RFC3339),
	)
	return err
}

func (s *SQLiteStore) ListGenerationHistory(ctx context.Context, opts GenerationListOptions) ([]*GenerationRecord, error) {
	if opts.Limit <= 0 {
		opts.Limit = 50
	}

	query := `SELECT id, user_id, project_id, prompt, negative_prompt, model, width, height, steps, seed, asset_id, created_at FROM dark_editor_generations WHERE 1=1`
	args := []interface{}{}

	if opts.UserID != "" {
		query += " AND user_id = ?"
		args = append(args, opts.UserID)
	}
	if opts.ProjectID != "" {
		query += " AND project_id = ?"
		args = append(args, opts.ProjectID)
	}
	if opts.Model != "" {
		query += " AND model = ?"
		args = append(args, opts.Model)
	}

	query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
	args = append(args, opts.Limit, opts.Offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := []*GenerationRecord{}
	for rows.Next() {
		record := &GenerationRecord{}
		var userID, projectID, assetID sql.NullString

		if err := rows.Scan(&record.ID, &userID, &projectID, &record.Prompt, &record.NegativePrompt,
			&record.Model, &record.Width, &record.Height, &record.Steps, &record.Seed,
			&assetID, &record.CreatedAt); err != nil {
			continue
		}

		if userID.Valid {
			record.UserID = &userID.String
		}
		if projectID.Valid {
			record.ProjectID = &projectID.String
		}
		if assetID.Valid {
			record.AssetID = &assetID.String
		}
		records = append(records, record)
	}
	return records, nil
}

func (s *SQLiteStore) CreateTempFile(ctx context.Context, file *TempFile) error {
	if file.ID == "" {
		file.ID = fmt.Sprintf("tmp_%d", time.Now().UnixNano())
	}
	if file.CreatedAt.IsZero() {
		file.CreatedAt = time.Now()
	}
	if file.ExpiresAt.IsZero() {
		file.ExpiresAt = file.CreatedAt.Add(24 * time.Hour)
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO dark_editor_temp_files (id, filename, original_filename, storage_path, mime_type, size_bytes, expires_at, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		file.ID, file.Filename, file.OriginalFilename, file.StoragePath,
		file.MimeType, file.SizeBytes, file.ExpiresAt.Format(time.RFC3339), file.CreatedAt.Format(time.RFC3339),
	)
	return err
}

func (s *SQLiteStore) GetTempFile(ctx context.Context, filename string) (*TempFile, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, filename, original_filename, storage_path, mime_type, size_bytes, expires_at, created_at
		 FROM dark_editor_temp_files WHERE filename = ?`, filename)

	file := &TempFile{}
	var expiresAt, createdAt string

	err := row.Scan(&file.ID, &file.Filename, &file.OriginalFilename, &file.StoragePath,
		&file.MimeType, &file.SizeBytes, &expiresAt, &createdAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	file.ExpiresAt, _ = time.Parse(time.RFC3339, expiresAt)
	file.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	return file, nil
}

func (s *SQLiteStore) DeleteTempFile(ctx context.Context, filename string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM dark_editor_temp_files WHERE filename = ?`, filename)
	return err
}

func (s *SQLiteStore) CleanupExpiredTempFiles(ctx context.Context) (int64, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM dark_editor_temp_files WHERE expires_at < ?`, time.Now().Format(time.RFC3339))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
