package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CreateTempFile creates a new temp file record
func (s *PostgresProjectStore) CreateTempFile(ctx context.Context, file *TempFile) error {
	if file.ID == "" {
		file.ID = uuid.New().String()
	}
	if file.CreatedAt.IsZero() {
		file.CreatedAt = time.Now()
	}
	if file.ExpiresAt.IsZero() {
		file.ExpiresAt = file.CreatedAt.Add(24 * time.Hour)
	}

	query := `
		INSERT INTO temp_files (id, filename, original_filename, storage_path, mime_type, size_bytes, expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	_, err := s.db.ExecContext(ctx, query,
		file.ID, file.Filename, file.OriginalFilename, file.StoragePath,
		file.MimeType, file.SizeBytes, file.ExpiresAt, file.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	return nil
}

// GetTempFile retrieves a temp file by filename
func (s *PostgresProjectStore) GetTempFile(ctx context.Context, filename string) (*TempFile, error) {
	query := `
		SELECT id, filename, original_filename, storage_path, mime_type, size_bytes, expires_at, created_at
		FROM temp_files WHERE filename = $1
	`

	file := &TempFile{}
	err := s.db.QueryRowContext(ctx, query, filename).Scan(
		&file.ID, &file.Filename, &file.OriginalFilename, &file.StoragePath,
		&file.MimeType, &file.SizeBytes, &file.ExpiresAt, &file.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get temp file: %w", err)
	}

	return file, nil
}

// DeleteTempFile deletes a temp file by filename
func (s *PostgresProjectStore) DeleteTempFile(ctx context.Context, filename string) error {
	query := `DELETE FROM temp_files WHERE filename = $1`
	_, err := s.db.ExecContext(ctx, query, filename)
	return err
}

// CleanupExpiredTempFiles removes expired temp files
func (s *PostgresProjectStore) CleanupExpiredTempFiles(ctx context.Context) (int64, error) {
	query := `DELETE FROM temp_files WHERE expires_at < NOW()`
	result, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
