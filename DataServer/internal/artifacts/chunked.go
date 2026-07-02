// Package artifacts / chunked.go — PR chunked upload persistence.
//
// ChunkedUploadService wraps artifacts.Service with persistent chunk tracking
// so that resumable chunked uploads survive master restarts.
//
// Flow:
//
//	InitChunkedSession → BeginUpload (artifact_uploads.CREATED)
//	    ↓
//	UploadChunk 0..N  → blob staging + artifact_upload_chunks row
//	    ↓
//	CompleteChunked   → assembles chunks → Receive (master hash) → Finalize (SUCCEEDED)
//
// The three handlers that used to live in handlers/remote/workers/uploads/chunked.go
// with a global in-memory map now delegate to this service for durable state.
package artifacts

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"velox-server/internal/store"
)

// ChunkedUploadCommand carries the per-chunk metadata for UploadChunk.
type ChunkedUploadCommand struct {
	UploadID   string
	ChunkIndex int
	Reader     io.Reader
}

// ChunkedCompleteCommand carries the auth fields for CompleteChunked.
// Mirrors FinalizeArtifactCommand but for the chunked assembly path.
type ChunkedCompleteCommand struct {
	UploadID         string
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int
}

// ChunkState summarises which chunks have been received for resume.
type ChunkState struct {
	TotalChunks int
	Uploaded    []bool
}

// ChunkedUploadService provides persistent chunked upload sessions.
// It wraps the canonical artifacts.Service pipeline (BeginUpload → Receive → Finalize)
// with chunk-level persistence via artifact_upload_chunks.
//
// Migration note: the per-session + per-chunk CRUD repository moved to
// internal/store as store.UploadRepository during file-1/4 of the
// canonical-SQL-gateway migration. The chunk service depends on the
// typed store interface for chunktable + resumable state; raw SQL
// stays only on the writer-finalize path inside the artifacts package
// (the chunk file IO + assembly IO).
type ChunkedUploadService struct {
	artifactSvc *Service
	repo        store.UploadRepository
	blobStore   store.BlobStore
	db          *sql.DB
}

// GetUploadByJob returns the active CREATED/UPLOADING upload session for a
// job_id. This bridges the worker protocol (which identifies uploads by job_id
// in URL paths) with the persistent artifact_uploads (keyed by upload_id).
func (s *ChunkedUploadService) GetUploadByJob(ctx context.Context, jobID string) (*store.UploadSession, error) {
	return s.repo.GetActiveUploadByJob(ctx, jobID)
}

// NewChunkedUploadService creates a ChunkedUploadService.
// The *sql.DB must be the same one used by artifactSvc so transactions
// can join when needed.
//
// Migration note: the repo parameter is now store.UploadRepository
// (typed SQLite CRUD for artifact_uploads + artifact_upload_chunks).
func NewChunkedUploadService(artifactSvc *Service, repo store.UploadRepository, blobStore store.BlobStore, db *sql.DB) *ChunkedUploadService {
	if artifactSvc == nil {
		panic("artifacts: NewChunkedUploadService requires a non-nil artifactSvc")
	}
	if repo == nil {
		panic("artifacts: NewChunkedUploadService requires a non-nil UploadRepository")
	}
	return &ChunkedUploadService{
		artifactSvc: artifactSvc,
		repo:        repo,
		blobStore:   blobStore,
		db:          db,
	}
}

// InitChunkedSession creates a chunked upload session via BeginUpload.
// It returns the upload session so the handler can respond with session metadata.
func (s *ChunkedUploadService) InitChunkedSession(ctx context.Context, cmd BeginUploadCommand) (*store.UploadSession, error) {
	return s.artifactSvc.BeginUpload(ctx, cmd)
}

// UploadChunk persists a single chunk to blob store staging and records it
// in artifact_upload_chunks. Idempotent: re-uploading the same chunk_index
// is a no-op (INSERT OR IGNORE).
func (s *ChunkedUploadService) UploadChunk(ctx context.Context, cmd ChunkedUploadCommand) error {
	if cmd.UploadID == "" || cmd.Reader == nil {
		return fmt.Errorf("artifacts: ChunkedUpload: uploadID and reader are required")
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

	// Write chunk to a unique staging path.
	chunkKey := chunkStagingKey(s.blobStore, cmd.UploadID, cmd.ChunkIndex)
	if err := os.MkdirAll(filepath.Dir(chunkKey), 0o755); err != nil {
		return fmt.Errorf("%w: mkdir chunk staging: %v", ErrBlobWriteFailed, err)
	}

	dst, err := os.OpenFile(filepath.Clean(chunkKey), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("%w: create chunk file: %v", ErrBlobWriteFailed, err)
	}

	written, err := io.Copy(dst, cmd.Reader)
	if err != nil {
		_ = dst.Close()
		_ = os.Remove(chunkKey)
		return fmt.Errorf("%w: write chunk: %v", ErrBlobWriteFailed, err)
	}
	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		_ = os.Remove(chunkKey)
		return fmt.Errorf("%w: sync chunk: %v", ErrBlobWriteFailed, err)
	}
	_ = dst.Close()

	if written <= 0 {
		_ = os.Remove(chunkKey)
		return fmt.Errorf("artifacts: ChunkedUpload: empty chunk %d", cmd.ChunkIndex)
	}

	// Master-compute SHA-256 for trust boundary.
	chunkSHA, err := hashFile(chunkKey)
	if err != nil {
		_ = os.Remove(chunkKey)
		return fmt.Errorf("artifacts: ChunkedUpload: hash: %w", err)
	}

	// Persist chunk record.
	if err := s.repo.InsertChunk(ctx, store.ChunkRecord{
		UploadID:   cmd.UploadID,
		ChunkIndex: cmd.ChunkIndex,
		SizeBytes:  written,
		SHA256:     chunkSHA,
		StorageKey: chunkKey,
		ReceivedAt: time.Now().UTC(),
	}); err != nil {
		_ = os.Remove(chunkKey)
		return translateStoreErr(err)
	}

	return nil
}

// GetChunkState returns which chunks have been uploaded for a session.
// Used by the Init handler to support resume: the worker skips already-uploaded
// chunks.
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

	return &ChunkState{
		TotalChunks: maxIndex,
		Uploaded:    uploaded,
	}, nil
}

// CompleteChunked assembles all chunks into the staging blob, then runs the
// canonical Receive → Finalize pipeline (master hash + atomic SUCCEEDED).
func (s *ChunkedUploadService) CompleteChunked(ctx context.Context, cmd ChunkedCompleteCommand) (*store.Artifact, error) {
	if cmd.UploadID == "" || cmd.JobID == "" {
		return nil, fmt.Errorf("artifacts: CompleteChunked: uploadID and jobID are required")
	}

	session, err := s.repo.GetUploadSession(ctx, cmd.UploadID)
	if err != nil {
		return nil, translateStoreErr(err)
	}
	if session == nil {
		return nil, fmt.Errorf("%w: upload_id=%s", ErrUploadNotFound, cmd.UploadID)
	}

	chunks, err := s.repo.ListChunks(ctx, cmd.UploadID)
	if err != nil {
		return nil, translateStoreErr(err)
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("artifacts: CompleteChunked: no chunks for upload=%s", cmd.UploadID)
	}

	// Verify no gaps — chunks must be contiguous starting from 0.
	for i, c := range chunks {
		if c.ChunkIndex != i {
			return nil, fmt.Errorf("artifacts: CompleteChunked: missing chunk %d for upload=%s (got idx=%d)", i, cmd.UploadID, c.ChunkIndex)
		}
	}

	// Assemble chunks into the temporary_storage_key.
	assemblyPath := session.TemporaryStorageKey
	if err := os.MkdirAll(filepath.Dir(assemblyPath), 0o755); err != nil {
		return nil, fmt.Errorf("%w: mkdir assembly: %v", ErrBlobWriteFailed, err)
	}

	out, err := os.OpenFile(filepath.Clean(assemblyPath), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("%w: create assembly file: %v", ErrBlobWriteFailed, err)
	}

	for _, c := range chunks {
		in, openErr := os.Open(filepath.Clean(c.StorageKey))
		if openErr != nil {
			_ = out.Close()
			_ = os.Remove(assemblyPath)
			return nil, fmt.Errorf("artifacts: CompleteChunked: open chunk %d: %w", c.ChunkIndex, openErr)
		}
		if _, copyErr := io.Copy(out, in); copyErr != nil {
			_ = in.Close()
			_ = out.Close()
			_ = os.Remove(assemblyPath)
			return nil, fmt.Errorf("artifacts: CompleteChunked: copy chunk %d: %w", c.ChunkIndex, copyErr)
		}
		_ = in.Close()
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(assemblyPath)
		return nil, fmt.Errorf("%w: sync assembly: %v", ErrBlobWriteFailed, err)
	}
	_ = out.Close()

	// Open the assembled file as reader for Receive.
	assembledFile, err := os.Open(filepath.Clean(assemblyPath))
	if err != nil {
		_ = os.Remove(assemblyPath)
		return nil, fmt.Errorf("artifacts: CompleteChunked: open assembled: %w", err)
	}
	defer assembledFile.Close()

	// Receive — master hashes the assembled blob, marks RECEIVED.
	if _, recvErr := s.artifactSvc.Receive(ctx, cmd.UploadID, assembledFile); recvErr != nil {
		return nil, fmt.Errorf("artifacts: CompleteChunked Receive: %w", recvErr)
	}

	// Finalize — promotes blob, atomic SUCCEEDED tx.
	art, finErr := s.artifactSvc.Finalize(ctx, FinalizeArtifactCommand{
		UploadID:         cmd.UploadID,
		JobID:            cmd.JobID,
		WorkerID:         cmd.WorkerID,
		LeaseID:          cmd.LeaseID,
		AttemptNumber:    cmd.AttemptNumber,
		ExpectedRevision: cmd.ExpectedRevision,
	})
	if finErr != nil {
		return nil, fmt.Errorf("artifacts: CompleteChunked Finalize: %w", finErr)
	}

	// Best-effort cleanup of chunk records + staging files.
	_ = s.cleanupChunks(ctx, cmd.UploadID)

	return art, nil
}

// cleanupChunks removes chunk records and their staging files.
func (s *ChunkedUploadService) cleanupChunks(ctx context.Context, uploadID string) error {
	chunks, err := s.repo.ListChunks(ctx, uploadID)
	if err != nil {
		return translateStoreErr(err)
	}
	for _, c := range chunks {
		if c.StorageKey != "" {
			_ = os.Remove(filepath.Clean(c.StorageKey))
		}
	}
	return translateStoreErr(s.repo.DeleteChunks(ctx, uploadID))
}

// chunkStagingKey returns the staging path for a single chunk.
// Format: <stagingDir>/chunks/<uploadID>/chunk_<index>
func chunkStagingKey(bl store.BlobStore, uploadID string, chunkIndex int) string {
	dir := filepath.Join(bl.StagingDir(), "chunks", uploadID)
	return filepath.Join(dir, fmt.Sprintf("chunk_%04d", chunkIndex))
}

// hashFile computes SHA-256 of a file.
func hashFile(path string) (string, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Compile-time check: *ChunkedUploadService is used as a value receiver.
var _ = (*ChunkedUploadService)(nil)
