package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (s *SQLiteStore) ListFolders(ctx context.Context) ([]*Folder, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, parent_id, drive_folder_id, created_at, updated_at FROM dark_editor_folders ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	folders := []*Folder{}
	for rows.Next() {
		var folder Folder
		var parentID, driveFolderID sql.NullString
		var createdAt, updatedAt sql.NullString
		if err := rows.Scan(&folder.ID, &folder.Name, &parentID, &driveFolderID, &createdAt, &updatedAt); err != nil {
			continue
		}
		if parentID.Valid {
			folder.ParentID = &parentID.String
		}
		if driveFolderID.Valid {
			folder.DriveFolderID = &driveFolderID.String
		}
		if createdAt.Valid && createdAt.String != "" {
			if t, err := time.Parse(time.RFC3339, createdAt.String); err == nil {
				folder.CreatedAt = t
			}
		}
		if updatedAt.Valid && updatedAt.String != "" {
			if t, err := time.Parse(time.RFC3339, updatedAt.String); err == nil {
				folder.UpdatedAt = t
			}
		}
		folders = append(folders, &folder)
	}
	return folders, nil
}

func (s *SQLiteStore) GetFolder(ctx context.Context, id string) (*Folder, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, name, parent_id, drive_folder_id, created_at, updated_at FROM dark_editor_folders WHERE id = ?`, id)
	var folder Folder
	var parentID, driveFolderID sql.NullString
	var createdAt, updatedAt sql.NullString
	err := row.Scan(&folder.ID, &folder.Name, &parentID, &driveFolderID, &createdAt, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if parentID.Valid {
		folder.ParentID = &parentID.String
	}
	if driveFolderID.Valid {
		folder.DriveFolderID = &driveFolderID.String
	}
	if createdAt.Valid && createdAt.String != "" {
		if t, err := time.Parse(time.RFC3339, createdAt.String); err == nil {
			folder.CreatedAt = t
		}
	}
	if updatedAt.Valid && updatedAt.String != "" {
		if t, err := time.Parse(time.RFC3339, updatedAt.String); err == nil {
			folder.UpdatedAt = t
		}
	}
	return &folder, nil
}

func (s *SQLiteStore) CreateFolder(ctx context.Context, folder *Folder) error {
	if folder.ID == "" {
		folder.ID = fmt.Sprintf("folder_%d", time.Now().UnixNano())
	}
	if folder.CreatedAt.IsZero() {
		folder.CreatedAt = time.Now()
	}
	folder.UpdatedAt = folder.CreatedAt

	var parentID interface{}
	if folder.ParentID != nil && *folder.ParentID != "" {
		parentID = *folder.ParentID
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO dark_editor_folders (id, name, parent_id, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)`,
		folder.ID, folder.Name, parentID,
		folder.CreatedAt.Format(time.RFC3339), folder.UpdatedAt.Format(time.RFC3339),
	)
	return err
}

func (s *SQLiteStore) UpdateFolder(ctx context.Context, folder *Folder) error {
	folder.UpdatedAt = time.Now()

	var parentID interface{}
	if folder.ParentID != nil && *folder.ParentID != "" {
		parentID = *folder.ParentID
	}

	result, err := s.db.ExecContext(ctx,
		`UPDATE dark_editor_folders SET name=?, parent_id=?, updated_at=? WHERE id = ?`,
		folder.Name, parentID, folder.UpdatedAt.Format(time.RFC3339), folder.ID,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("folder not found")
	}
	return nil
}

func (s *SQLiteStore) DeleteFolder(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `UPDATE dark_editor_projects SET folder_id = NULL WHERE folder_id = ?`, id); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM dark_editor_folders WHERE id = ?`, id)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("folder not found")
	}
	return tx.Commit()
}
