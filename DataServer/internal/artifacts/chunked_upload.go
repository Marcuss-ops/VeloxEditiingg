package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"velox-server/internal/store"
)

// UploadChunk persists a single chunk to blob-store staging and records it in
// artifact_upload_chunks. Identical retries are idempotent; conflicting bytes
// for an existing index are rejected.
func (s *ChunkedUploadService) UploadChunk(ctx context.Context, cmd ChunkedUploadCommand) error {
	if cmd.UploadID == "" || cmd.Reader == nil {
		return fmt.Errorf("artifacts: ChunkedUpload: uploadID and reader are required")
	}
	if cmd.ChunkIndex < 0 {
		return fmt.Errorf("artifacts: ChunkedUpload: chunk index must be non-negative")
	}

	session, err := s.repo.GetUploadSession(ctx, cmd.UploadID)
	if err != nil {
		return translateStoreErr(err)
	}
	if session == nil {
		return fmt.Errorf("%w: upload_id=%s", ErrUploadNotFound, cmd.UploadID)
	}
	if session.Status != string(store.UploadCreated) && session.Status != string(store.UploadUploading) {
		return fmt.Errorf("%w: upload=%s status=%s", ErrUploadStateInvalid, cmd.UploadID, session.Status)
	}
	if !session.ExpiresAt.IsZero() && time.Now().After(session.ExpiresAt) {
		return fmt.Errorf("%w: upload=%s expired_at=%s", ErrUploadExpired, cmd.UploadID, session.ExpiresAt.Format(time.RFC3339))
	}

	chunkKey := chunkRetryStagingKey(s.blobStore, cmd.UploadID, cmd.ChunkIndex)
	dst, err := s.blobStore.OpenStagedWrite(chunkKey)
	if err != nil {
		return fmt.Errorf("%w: create chunk file: %v", ErrBlobWriteFailed, err)
	}

	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(dst, hasher), cmd.Reader)
	if err != nil {
		_ = dst.Close()
		_ = s.blobStore.RemoveStaging(chunkKey)
		return fmt.Errorf("%w: write chunk: %v", ErrBlobWriteFailed, err)
	}
	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		_ = s.blobStore.RemoveStaging(chunkKey)
		return fmt.Errorf("%w: sync chunk: %v", ErrBlobWriteFailed, err)
	}
	if err := dst.Close(); err != nil {
		_ = s.blobStore.RemoveStaging(chunkKey)
		return fmt.Errorf("%w: close chunk: %v", ErrBlobWriteFailed, err)
	}
	if written <= 0 {
		_ = s.blobStore.RemoveStaging(chunkKey)
		return fmt.Errorf("%w: chunk %d", ErrEmptyChunk, cmd.ChunkIndex)
	}

	chunkSHA := hex.EncodeToString(hasher.Sum(nil))
	existing, err := s.repo.GetChunk(ctx, cmd.UploadID, cmd.ChunkIndex)
	if err != nil {
		_ = s.blobStore.RemoveStaging(chunkKey)
		return translateStoreErr(err)
	}
	if existing != nil {
		if existing.SizeBytes != written || !strings.EqualFold(existing.SHA256, chunkSHA) {
			_ = s.blobStore.RemoveStaging(chunkKey)
			return fmt.Errorf("%w: upload=%s index=%d", ErrChunkConflict, cmd.UploadID, cmd.ChunkIndex)
		}
		_ = s.blobStore.RemoveStaging(chunkKey)
		return nil
	}

	if err := s.repo.InsertChunk(ctx, store.ChunkRecord{
		UploadID: cmd.UploadID, ChunkIndex: cmd.ChunkIndex, SizeBytes: written,
		SHA256: chunkSHA, StorageKey: chunkKey, ReceivedAt: time.Now().UTC(),
	}); err != nil {
		_ = s.blobStore.RemoveStaging(chunkKey)
		return translateStoreErr(err)
	}
	stored, err := s.repo.GetChunk(ctx, cmd.UploadID, cmd.ChunkIndex)
	if err != nil {
		_ = s.blobStore.RemoveStaging(chunkKey)
		return translateStoreErr(err)
	}
	if stored == nil {
		_ = s.blobStore.RemoveStaging(chunkKey)
		return fmt.Errorf("artifacts: ChunkedUpload: chunk record disappeared upload=%s index=%d", cmd.UploadID, cmd.ChunkIndex)
	}
	if stored.SizeBytes != written || !strings.EqualFold(stored.SHA256, chunkSHA) {
		_ = s.blobStore.RemoveStaging(chunkKey)
		return fmt.Errorf("%w: upload=%s index=%d", ErrChunkConflict, cmd.UploadID, cmd.ChunkIndex)
	}
	if stored.StorageKey != chunkKey {
		_ = s.blobStore.RemoveStaging(chunkKey)
	}
	return nil
}

// GetChunkState returns the uploaded chunk indexes for a session.
func (s *ChunkedUploadService) GetChunkState(ctx context.Context, uploadID string) (*ChunkState, error) {
	session, err := s.repo.GetUploadSession(ctx, uploadID)
	if err != nil {
		return nil, translateStoreErr(err)
	}
	if session == nil {
		return nil, fmt.Errorf("%w: upload_id=%s", ErrUploadNotFound, uploadID)
	}
	chunks, err := s.repo.ListChunks(ctx, uploadID)
	if err != nil {
		return nil, translateStoreErr(err)
	}
	if len(chunks) == 0 {
		return &ChunkState{}, nil
	}
	maxIndex := 0
	for _, c := range chunks {
		if c.ChunkIndex >= maxIndex {
			maxIndex = c.ChunkIndex + 1
		}
	}
	uploaded := make([]bool, maxIndex)
	for _, c := range chunks {
		if c.ChunkIndex < len(uploaded) {
			uploaded[c.ChunkIndex] = true
		}
	}
	return &ChunkState{TotalChunks: maxIndex, Uploaded: uploaded}, nil
}
