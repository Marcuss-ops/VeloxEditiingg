// Package artifactsstore / artifact_uploads_chunks.go
//
// Per-chunk CRUD over artifact_upload_chunks rows (resumable chunked
// uploads). Part of the UploadRepository contract; see
// artifact_uploads.go for the interface and wiring.
package artifactsstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// InsertChunk persists a single chunk record.
func (r *SQLiteUploadRepository) InsertChunk(ctx context.Context, c ChunkRecord) error {
	if c.UploadID == "" {
		return fmt.Errorf("artifactsstore: InsertChunk: empty uploadID")
	}
	now := c.ReceivedAt.UTC().Format(time.RFC3339)
	if c.ReceivedAt.IsZero() {
		now = nowRFC3339()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO artifact_upload_chunks
		 (upload_id, chunk_index, size_bytes, sha256, storage_key, received_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		c.UploadID, c.ChunkIndex, c.SizeBytes, nilOrString(c.SHA256),
		c.StorageKey, now,
	)
	if err != nil {
		return fmt.Errorf("artifactsstore: InsertChunk: %w", err)
	}
	return nil
}

// GetChunk returns one persisted chunk, or (nil, nil) when the index has not
// been received yet. It is used to make duplicate chunk retries compare the
// incoming bytes with the first durable record before discarding the retry.
func (r *SQLiteUploadRepository) GetChunk(ctx context.Context, uploadID string, chunkIndex int) (*ChunkRecord, error) {
	if uploadID == "" {
		return nil, fmt.Errorf("artifactsstore: GetChunk: empty uploadID")
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT upload_id, chunk_index, size_bytes,
		       COALESCE(sha256, ''), storage_key, received_at
		FROM artifact_upload_chunks
		WHERE upload_id = ? AND chunk_index = ?`, uploadID, chunkIndex)
	var c ChunkRecord
	var receivedAt string
	if err := row.Scan(&c.UploadID, &c.ChunkIndex, &c.SizeBytes,
		&c.SHA256, &c.StorageKey, &receivedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("artifactsstore: GetChunk: %w", err)
	}
	parsed, err := parsePersistedWorkerTimestamp(receivedAt, "artifact_upload_chunks.received_at")
	if err != nil {
		return nil, err
	}
	c.ReceivedAt = parsed
	return &c, nil
}

// ListChunks returns all chunks for an upload, ordered by chunk_index.
func (r *SQLiteUploadRepository) ListChunks(ctx context.Context, uploadID string) ([]ChunkRecord, error) {
	if uploadID == "" {
		return nil, fmt.Errorf("artifactsstore: ListChunks: empty uploadID")
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT upload_id, chunk_index, size_bytes,
		       COALESCE(sha256, ''), storage_key, received_at
		FROM artifact_upload_chunks
		WHERE upload_id = ?
		ORDER BY chunk_index ASC`, uploadID)
	if err != nil {
		return nil, fmt.Errorf("artifactsstore: ListChunks: %w", err)
	}
	defer rows.Close()

	var out []ChunkRecord
	for rows.Next() {
		var c ChunkRecord
		var receivedAt string
		if err := rows.Scan(&c.UploadID, &c.ChunkIndex, &c.SizeBytes,
			&c.SHA256, &c.StorageKey, &receivedAt); err != nil {
			return nil, fmt.Errorf("artifactsstore: ListChunks scan: %w", err)
		}
		parsed, err := parsePersistedWorkerTimestamp(receivedAt, "artifact_upload_chunks.received_at")
		if err != nil {
			return nil, err
		}
		c.ReceivedAt = parsed
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("artifactsstore: ListChunks rows: %w", err)
	}
	return out, nil
}

// DeleteChunks removes all chunk records for an upload (cleanup after finalize).
func (r *SQLiteUploadRepository) DeleteChunks(ctx context.Context, uploadID string) error {
	if uploadID == "" {
		return fmt.Errorf("artifactsstore: DeleteChunks: empty uploadID")
	}
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM artifact_upload_chunks WHERE upload_id = ?`, uploadID); err != nil {
		return fmt.Errorf("artifactsstore: DeleteChunks: %w", err)
	}
	return nil
}
