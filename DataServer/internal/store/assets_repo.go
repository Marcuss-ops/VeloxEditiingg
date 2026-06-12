package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CreateAsset creates a new asset
func (s *PostgresProjectStore) CreateAsset(ctx context.Context, asset *Asset) error {
	if asset.ID == "" {
		asset.ID = uuid.New().String()
	}
	if asset.StorageType == "" {
		asset.StorageType = "local"
	}
	if asset.CreatedAt.IsZero() {
		asset.CreatedAt = time.Now()
	}

	metadata, err := json.Marshal(asset.Metadata)
	if err != nil {
		metadata = []byte("{}")
	}

	query := `
		INSERT INTO assets (id, project_id, user_id, type, filename, original_filename, storage_path, storage_type, mime_type, size_bytes, width, height, metadata, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	`

	var projectID, userID interface{}
	if asset.ProjectID != nil {
		projectID = *asset.ProjectID
	}
	if asset.UserID != nil {
		userID = *asset.UserID
	}

	_, err = s.db.ExecContext(ctx, query,
		asset.ID, projectID, userID, asset.Type, asset.Filename,
		asset.OriginalFilename, asset.StoragePath, asset.StorageType,
		asset.MimeType, asset.SizeBytes, asset.Width, asset.Height,
		metadata, asset.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create asset: %w", err)
	}

	return nil
}

// GetAsset retrieves an asset by ID
func (s *PostgresProjectStore) GetAsset(ctx context.Context, id string) (*Asset, error) {
	query := `
		SELECT id, project_id, user_id, type, filename, original_filename, storage_path, storage_type, mime_type, size_bytes, width, height, metadata, created_at
		FROM assets WHERE id = $1
	`

	asset := &Asset{}
	var metadata []byte
	var projectID, userID sql.NullString

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&asset.ID, &projectID, &userID, &asset.Type, &asset.Filename,
		&asset.OriginalFilename, &asset.StoragePath, &asset.StorageType,
		&asset.MimeType, &asset.SizeBytes, &asset.Width, &asset.Height,
		&metadata, &asset.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get asset: %w", err)
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

// GetAssetByFilename retrieves an asset by filename
func (s *PostgresProjectStore) GetAssetByFilename(ctx context.Context, filename string) (*Asset, error) {
	query := `
		SELECT id, project_id, user_id, type, filename, original_filename, storage_path, storage_type, mime_type, size_bytes, width, height, metadata, created_at
		FROM assets WHERE filename = $1
	`

	asset := &Asset{}
	var metadata []byte
	var projectID, userID sql.NullString

	err := s.db.QueryRowContext(ctx, query, filename).Scan(
		&asset.ID, &projectID, &userID, &asset.Type, &asset.Filename,
		&asset.OriginalFilename, &asset.StoragePath, &asset.StorageType,
		&asset.MimeType, &asset.SizeBytes, &asset.Width, &asset.Height,
		&metadata, &asset.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get asset by filename: %w", err)
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

// DeleteAsset deletes an asset by ID
func (s *PostgresProjectStore) DeleteAsset(ctx context.Context, id string) error {
	query := `DELETE FROM assets WHERE id = $1`
	result, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete asset: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return errors.New("asset not found")
	}

	return nil
}

// ListProjectAssets lists all assets for a project
func (s *PostgresProjectStore) ListProjectAssets(ctx context.Context, projectID string) ([]*Asset, error) {
	query := `
		SELECT id, project_id, user_id, type, filename, original_filename, storage_path, storage_type, mime_type, size_bytes, width, height, metadata, created_at
		FROM assets WHERE project_id = $1 ORDER BY created_at DESC
	`

	rows, err := s.db.QueryContext(ctx, query, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to list project assets: %w", err)
	}
	defer rows.Close()

	assets := []*Asset{}
	for rows.Next() {
		asset := &Asset{}
		var metadata []byte
		var projID, userID sql.NullString

		err := rows.Scan(
			&asset.ID, &projID, &userID, &asset.Type, &asset.Filename,
			&asset.OriginalFilename, &asset.StoragePath, &asset.StorageType,
			&asset.MimeType, &asset.SizeBytes, &asset.Width, &asset.Height,
			&metadata, &asset.CreatedAt,
		)
		if err != nil {
			continue
		}

		if projID.Valid {
			asset.ProjectID = &projID.String
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
