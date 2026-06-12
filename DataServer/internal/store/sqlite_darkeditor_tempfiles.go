package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

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
