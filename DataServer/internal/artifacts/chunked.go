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
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"strings"
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
// The per-session + per-chunk CRUD repository is
// store.UploadRepository; the chunk service depends on it for
// chunktable + resumable state. Raw SQL stays only on the
// writer-finalize path inside the artifacts package, while the chunk
// file IO + assembly IO is routed through store.BlobStore
// (OpenStagedWrite / OpenStagedRead / RemoveStaging) so the service
// never touches the filesystem driver directly.
type ChunkedUploadService struct {
	artifactSvc *Service
	repo        store.UploadRepository
	blobStore   store.BlobStore
}

// GetUploadByJob returns the active CREATED/UPLOADING upload session for a
// job_id. This bridges the worker protocol (which identifies uploads by job_id
// in URL paths) with the persistent artifact_uploads (keyed by upload_id).
func (s *ChunkedUploadService) GetUploadByJob(ctx context.Context, jobID string) (*store.UploadSession, error) {
	return s.repo.GetActiveUploadByJob(ctx, jobID)
}

// NewChunkedUploadService creates a ChunkedUploadService.
// The repository owns all upload-session and chunk persistence; the
// application service only orchestrates file assembly and lifecycle calls.
func NewChunkedUploadService(artifactSvc *Service, repo store.UploadRepository, blobStore store.BlobStore) *ChunkedUploadService {
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
	}
}

// InitChunkedSession creates a chunked upload session via BeginUpload.
// It returns the upload session so the handler can respond with session metadata.
func (s *ChunkedUploadService) InitChunkedSession(ctx context.Context, cmd BeginUploadCommand) (*store.UploadSession, error) {
	return s.artifactSvc.BeginUpload(ctx, cmd)
}

// UploadChunk persists a single chunk to blob store staging and records it
// in artifact_upload_chunks. Idempotent: re-uploading the same chunk_index
// is a no-op through the repository uniqueness contract.
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

	// Always write to a unique temporary path. A retry must never overwrite
	// the first chunk file that the database already references.
	chunkKey := chunkRetryStagingKey(s.blobStore, cmd.UploadID, cmd.ChunkIndex)
	dst, err := s.blobStore.OpenStagedWrite(chunkKey)
	if err != nil {
		return fmt.Errorf("%w: create chunk file: %v", ErrBlobWriteFailed, err)
	}

	// Master-compute SHA-256 INCREMENTALLY during the write (no-second-read
	// pattern, same as the artifact Receive path): the hasher sees exactly
	// the bytes written to dst, so chunk_sha256 is derived from the stream
	// without reopening the chunk file afterwards.
	//
	// IMPORTANT invariant: the hasher is authoritative ONLY when the write
	// fully succeeded. io.MultiWriter feeds [dst, hasher] in order, so a
	// pathological short write from dst would skip the hasher for that
	// buffer; every such failure path below (copy error, sync error, empty
	// chunk) removes chunkKey and returns, discarding the hasher value — an
	// incorrect hash is never persisted. Do not "optimize" those paths into
	// continuing with a partial file.
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

	// A duplicate retry is accepted only when its streamed size and SHA match
	// the first durable record. The temporary file is removed in either case;
	// the original storage key remains authoritative.
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

	// Persist chunk record. Concurrent writers may race here; the follow-up
	// lookup below determines whether this writer won INSERT OR IGNORE.
	if err := s.repo.InsertChunk(ctx, store.ChunkRecord{
		UploadID:   cmd.UploadID,
		ChunkIndex: cmd.ChunkIndex,
		SizeBytes:  written,
		SHA256:     chunkSHA,
		StorageKey: chunkKey,
		ReceivedAt: time.Now().UTC(),
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

// GetUpload returns a durable upload session for the typed master-stream
// protocol. Callers use it only for fenced, post-receive verification; the
// worker never supplies the session identity as authority.
func (s *ChunkedUploadService) GetUpload(ctx context.Context, uploadID string) (*store.UploadSession, error) {
	if uploadID == "" {
		return nil, fmt.Errorf("artifacts: GetUpload: uploadID required")
	}
	session, err := s.repo.GetUploadSession(ctx, uploadID)
	if err != nil {
		return nil, translateStoreErr(err)
	}
	if session == nil {
		return nil, fmt.Errorf("%w: upload_id=%s", ErrUploadNotFound, uploadID)
	}
	return session, nil
}

// ReceiveChunked assembles the durable chunks and runs the master-side
// Receive phase without invoking the legacy Finalize path. The typed commit
// coordinator performs the later artifact promotion and job commit after all
// declared outputs have passed verification.
func (s *ChunkedUploadService) ReceiveChunked(ctx context.Context, uploadID string) (*ReceiveResult, error) {
	session, err := s.GetUpload(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if session.Status == string(store.UploadReceived) {
		return receiveResultFromSession(session)
	}
	chunks, err := s.repo.ListChunks(ctx, uploadID)
	if err != nil {
		return nil, translateStoreErr(err)
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("artifacts: ReceiveChunked: no chunks for upload=%s", uploadID)
	}
	for i, c := range chunks {
		if c.ChunkIndex != i {
			return nil, fmt.Errorf("artifacts: ReceiveChunked: missing chunk %d for upload=%s", i, uploadID)
		}
	}
	// Assemble into a distinct file. Receive writes the verified stream to
	// session.TemporaryStorageKey; using that same path as the reader would
	// truncate the input before io.Copy reads it, producing a zero-byte
	// artifact and a SHA-256 of the empty string.
	assemblyPath := session.TemporaryStorageKey + ".assembled"
	defer s.blobStore.RemoveStaging(assemblyPath)
	out, err := s.blobStore.OpenStagedWrite(assemblyPath)
	if err != nil {
		return nil, fmt.Errorf("%w: create assembly: %v", ErrBlobWriteFailed, err)
	}
	if asmErr := s.assembleChunksVerified(out, chunks); asmErr != nil {
		_ = out.Close()
		return nil, fmt.Errorf("artifacts: ReceiveChunked: %w", asmErr)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return nil, fmt.Errorf("%w: sync assembly: %v", ErrBlobWriteFailed, err)
	}
	if err := out.Close(); err != nil {
		return nil, fmt.Errorf("%w: close assembly: %v", ErrBlobWriteFailed, err)
	}
	assembled, err := s.blobStore.OpenStagedRead(assemblyPath)
	if err != nil {
		return nil, fmt.Errorf("artifacts: ReceiveChunked: open assembled: %w", err)
	}
	defer assembled.Close()
	result, err := s.artifactSvc.Receive(ctx, uploadID, assembled)
	if err != nil {
		return nil, fmt.Errorf("artifacts: ReceiveChunked Receive: %w", err)
	}
	_ = s.cleanupChunks(ctx, uploadID)
	return result, nil
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

	// Assemble into a distinct file. Receive writes the verified stream to
	// session.TemporaryStorageKey; using that same path as the reader would
	// truncate the input before io.Copy reads it, producing a zero-byte
	// artifact and a SHA-256 of the empty string.
	assemblyPath := session.TemporaryStorageKey + ".assembled"
	defer s.blobStore.RemoveStaging(assemblyPath)

	out, err := s.blobStore.OpenStagedWrite(assemblyPath)
	if err != nil {
		return nil, fmt.Errorf("%w: create assembly file: %v", ErrBlobWriteFailed, err)
	}

	if asmErr := s.assembleChunksVerified(out, chunks); asmErr != nil {
		_ = out.Close()
		_ = s.blobStore.RemoveStaging(assemblyPath)
		return nil, fmt.Errorf("artifacts: CompleteChunked: %w", asmErr)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = s.blobStore.RemoveStaging(assemblyPath)
		return nil, fmt.Errorf("%w: sync assembly: %v", ErrBlobWriteFailed, err)
	}
	_ = out.Close()

	// Open the assembled file as reader for Receive.
	assembledFile, err := s.blobStore.OpenStagedRead(assemblyPath)
	if err != nil {
		_ = s.blobStore.RemoveStaging(assemblyPath)
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

// assembleChunksVerified streams the recorded chunks into dst while
// verifying each chunk's SHA-256 INCREMENTALLY during the copy (no second
// read — the same MultiWriter discipline as UploadChunk). The recorded
// chunk_sha256 (computed at upload time) is the integrity contract: if a
// staged chunk was corrupted or truncated on disk since upload, the copy
// hash diverges and the assembly fails closed with
// ErrArtifactTransferCorrupted BEFORE the final Receive pass, localizing
// the fault to a chunk index instead of a whole-artifact rejection.
//
// A chunk record with an empty SHA256 (legacy rows) is copied without
// verification: the final Receive pass against expected_sha256 remains
// authoritative for those.
func (s *ChunkedUploadService) assembleChunksVerified(dst io.Writer, chunks []store.ChunkRecord) error {
	for _, c := range chunks {
		in, openErr := s.blobStore.OpenStagedRead(c.StorageKey)
		if openErr != nil {
			return fmt.Errorf("open chunk %d: %w", c.ChunkIndex, openErr)
		}
		hasher := sha256.New()
		_, copyErr := io.Copy(io.MultiWriter(dst, hasher), in)
		_ = in.Close()
		if copyErr != nil {
			return fmt.Errorf("copy chunk %d: %w", c.ChunkIndex, copyErr)
		}
		if c.SHA256 == "" {
			continue
		}
		if got := hex.EncodeToString(hasher.Sum(nil)); got != c.SHA256 {
			return fmt.Errorf("%w: %w: chunk %d: recorded=%s computed=%s (staged chunk corrupted since upload)",
				ErrArtifactTransferCorrupted, ErrHashMismatch, c.ChunkIndex, c.SHA256, got)
		}
	}
	return nil
}

// cleanupChunks removes chunk records and their staging files.
func (s *ChunkedUploadService) cleanupChunks(ctx context.Context, uploadID string) error {
	chunks, err := s.repo.ListChunks(ctx, uploadID)
	if err != nil {
		return translateStoreErr(err)
	}
	for _, c := range chunks {
		if c.StorageKey != "" {
			_ = s.blobStore.RemoveStaging(c.StorageKey)
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

// chunkRetryStagingKey is deliberately unique per write. A retry cannot
// overwrite the staging file referenced by the first durable DB record.
func chunkRetryStagingKey(bl store.BlobStore, uploadID string, chunkIndex int) string {
	dir := filepath.Join(bl.StagingDir(), "chunks", uploadID)
	return filepath.Join(dir, fmt.Sprintf("chunk_%04d.retry_%d", chunkIndex, time.Now().UnixNano()))
}

// Compile-time check: *ChunkedUploadService is used as a value receiver.
var _ = (*ChunkedUploadService)(nil)
