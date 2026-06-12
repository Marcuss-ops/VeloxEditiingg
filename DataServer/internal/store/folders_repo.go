package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

func (s *PostgresProjectStore) ListFolders(ctx context.Context) ([]*Folder, error) {
	query := `
		SELECT id, name, parent_id, created_at, updated_at
		FROM dark_editor_folders
		ORDER BY created_at ASC
	`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list folders: %w", err)
	}
	defer rows.Close()

	folders := []*Folder{}
	for rows.Next() {
		var folder Folder
		var parentID sql.NullString
		if err := rows.Scan(&folder.ID, &folder.Name, &parentID, &folder.CreatedAt, &folder.UpdatedAt); err != nil {
			continue
		}
		if parentID.Valid {
			folder.ParentID = &parentID.String
		}
		folders = append(folders, &folder)
	}
	return folders, nil
}

func (s *PostgresProjectStore) GetFolder(ctx context.Context, id string) (*Folder, error) {
	query := `
		SELECT id, name, parent_id, created_at, updated_at
		FROM dark_editor_folders
		WHERE id = $1
	`
	var folder Folder
	var parentID sql.NullString
	err := s.db.QueryRowContext(ctx, query, id).Scan(&folder.ID, &folder.Name, &parentID, &folder.CreatedAt, &folder.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get folder: %w", err)
	}
	if parentID.Valid {
		folder.ParentID = &parentID.String
	}
	return &folder, nil
}

func (s *PostgresProjectStore) CreateFolder(ctx context.Context, folder *Folder) error {
	if folder.ID == "" {
		folder.ID = uuid.New().String()
	}
	now := time.Now().UTC()
	if folder.CreatedAt.IsZero() {
		folder.CreatedAt = now
	}
	folder.UpdatedAt = now

	query := `
		INSERT INTO dark_editor_folders (id, name, parent_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5)
	`
	var parentID interface{}
	if folder.ParentID != nil && *folder.ParentID != "" {
		parentID = *folder.ParentID
	}

	_, err := s.db.ExecContext(ctx, query, folder.ID, folder.Name, parentID, folder.CreatedAt, folder.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to create folder: %w", err)
	}
	return nil
}

func (s *PostgresProjectStore) UpdateFolder(ctx context.Context, folder *Folder) error {
	folder.UpdatedAt = time.Now().UTC()
	query := `
		UPDATE dark_editor_folders
		SET name = $2, parent_id = $3, updated_at = $4
		WHERE id = $1
	`
	var parentID interface{}
	if folder.ParentID != nil && *folder.ParentID != "" {
		parentID = *folder.ParentID
	}

	result, err := s.db.ExecContext(ctx, query, folder.ID, folder.Name, parentID, folder.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to update folder: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return errors.New("folder not found")
	}
	return nil
}

func (s *PostgresProjectStore) DeleteFolder(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `UPDATE projects SET folder_id = NULL WHERE folder_id = $1`, id); err != nil {
		return fmt.Errorf("failed to clear folder references: %w", err)
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM dark_editor_folders WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("failed to delete folder: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return errors.New("folder not found")
	}
	return tx.Commit()
}
