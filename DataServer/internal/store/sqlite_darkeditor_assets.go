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
