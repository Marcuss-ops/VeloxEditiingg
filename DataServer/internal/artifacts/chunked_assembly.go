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

	"velox-server/internal/repository"
)

// ReceiveChunked assembles durable chunks and runs the master-side Receive phase.
func (s *ChunkedUploadService) ReceiveChunked(ctx context.Context, uploadID string) (*ReceiveResult, error) {
	release := s.receiveLocks.acquire(uploadID)
	defer release()

	session, err := s.GetUpload(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if session.Status == string(repository.UploadReceived) {
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
	assemblyPath := session.TemporaryStorageKey
	keepAssembly := false
	defer func() {
		if !keepAssembly {
			_ = s.blobStore.RemoveStaging(assemblyPath)
		}
	}()
	out, err := s.blobStore.OpenStagedWrite(assemblyPath)
	if err != nil {
		return nil, fmt.Errorf("%w: create assembly: %w", ErrBlobWriteFailed, err)
	}
	sha, size, asmErr := s.assembleChunksVerified(out, chunks)
	if asmErr != nil {
		_ = out.Close()
		return nil, fmt.Errorf("artifacts: ReceiveChunked: %w", asmErr)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return nil, fmt.Errorf("%w: sync assembly: %w", ErrBlobWriteFailed, err)
	}
	if err := out.Close(); err != nil {
		return nil, fmt.Errorf("%w: close assembly: %w", ErrBlobWriteFailed, err)
	}
	result, err := s.artifactSvc.ReceiveVerifiedStaged(ctx, uploadID, sha, size)
	if err != nil {
		return nil, fmt.Errorf("artifacts: ReceiveChunked Receive: %w", err)
	}
	keepAssembly = true
	_ = s.cleanupChunks(ctx, uploadID)
	return result, nil
}

// CompleteChunked assembles all chunks and runs Receive followed by Finalize.
func (s *ChunkedUploadService) CompleteChunked(ctx context.Context, cmd ChunkedCompleteCommand) (*repository.Artifact, error) {
	if cmd.UploadID == "" || cmd.JobID == "" {
		return nil, fmt.Errorf("artifacts: CompleteChunked: uploadID and jobID are required")
	}
	release := s.receiveLocks.acquire(cmd.UploadID)
	defer release()

	session, err := s.repo.GetUploadSession(ctx, cmd.UploadID)
	if err != nil {
		return nil, translateStoreErr(err)
	}
	if session == nil {
		return nil, fmt.Errorf("%w: upload_id=%s", ErrUploadNotFound, cmd.UploadID)
	}
	if session.Status == string(repository.UploadCompleted) {
		if session.WorkerID != cmd.WorkerID {
			return nil, fmt.Errorf("%w: completed upload=%s worker=%s->%s", ErrTransitionConflict, cmd.UploadID, session.WorkerID, cmd.WorkerID)
		}
		if session.LeaseID != cmd.LeaseID {
			return nil, fmt.Errorf("%w: completed upload=%s lease_mismatch", ErrTransitionConflict, cmd.UploadID)
		}
		if session.ExpectedRevision != 0 && session.ExpectedRevision != cmd.ExpectedRevision {
			return nil, fmt.Errorf("%w: completed upload=%s revision_mismatch", ErrTransitionConflict, cmd.UploadID)
		}
		if cmd.AttemptNumber != 0 && session.AttemptNumber != cmd.AttemptNumber {
			return nil, fmt.Errorf("%w: completed upload=%s attempt=%d->%d", ErrAttemptMismatch, cmd.UploadID, session.AttemptNumber, cmd.AttemptNumber)
		}
		art, lerr := s.artifactSvc.artifactReader.GetByID(ctx, session.ArtifactID)
		if lerr != nil {
			return nil, lerr
		}
		if art == nil {
			return nil, fmt.Errorf("%w: completed upload=%s but artifact missing", ErrTransitionConflict, cmd.UploadID)
		}
		return art, nil
	}

	chunks, err := s.repo.ListChunks(ctx, cmd.UploadID)
	if err != nil {
		return nil, translateStoreErr(err)
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("artifacts: CompleteChunked: no chunks for upload=%s", cmd.UploadID)
	}
	for i, c := range chunks {
		if c.ChunkIndex != i {
			return nil, fmt.Errorf("artifacts: CompleteChunked: missing chunk %d for upload=%s (got idx=%d)", i, cmd.UploadID, c.ChunkIndex)
		}
	}

	assemblyPath := session.TemporaryStorageKey
	keepAssembly := false
	defer func() {
		if !keepAssembly {
			_ = s.blobStore.RemoveStaging(assemblyPath)
		}
	}()
	out, err := s.blobStore.OpenStagedWrite(assemblyPath)
	if err != nil {
		return nil, fmt.Errorf("%w: create assembly file: %w", ErrBlobWriteFailed, err)
	}
	sha, size, asmErr := s.assembleChunksVerified(out, chunks)
	if asmErr != nil {
		_ = out.Close()
		_ = s.blobStore.RemoveStaging(assemblyPath)
		return nil, fmt.Errorf("artifacts: CompleteChunked: %w", asmErr)
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = s.blobStore.RemoveStaging(assemblyPath)
		return nil, fmt.Errorf("%w: sync assembly: %w", ErrBlobWriteFailed, err)
	}
	_ = out.Close()

	if _, recvErr := s.artifactSvc.ReceiveVerifiedStaged(ctx, cmd.UploadID, sha, size); recvErr != nil {
		return nil, fmt.Errorf("artifacts: CompleteChunked Receive: %w", recvErr)
	}
	keepAssembly = true
	art, finErr := s.artifactSvc.Finalize(ctx, FinalizeArtifactCommand{
		UploadID: cmd.UploadID, JobID: cmd.JobID, WorkerID: cmd.WorkerID,
		LeaseID: cmd.LeaseID, AttemptNumber: cmd.AttemptNumber,
		ExpectedRevision: cmd.ExpectedRevision,
	})
	if finErr != nil {
		return nil, fmt.Errorf("artifacts: CompleteChunked Finalize: %w", finErr)
	}
	_ = s.cleanupChunks(ctx, cmd.UploadID)
	return art, nil
}

func (s *ChunkedUploadService) assembleChunksVerified(dst io.Writer, chunks []repository.ChunkRecord) (string, int64, error) {
	whole := sha256.New()
	var size int64
	writer := io.MultiWriter(dst, whole)
	for _, c := range chunks {
		in, openErr := s.blobStore.OpenStagedRead(c.StorageKey)
		if openErr != nil {
			return "", 0, fmt.Errorf("open chunk %d: %w", c.ChunkIndex, openErr)
		}
		hasher := sha256.New()
		n, copyErr := io.Copy(io.MultiWriter(writer, hasher), in)
		size += n
		_ = in.Close()
		if copyErr != nil {
			return "", 0, fmt.Errorf("copy chunk %d: %w", c.ChunkIndex, copyErr)
		}
		if c.SHA256 == "" {
			continue
		}
		if got := hex.EncodeToString(hasher.Sum(nil)); !strings.EqualFold(got, c.SHA256) {
			return "", 0, fmt.Errorf("%w: %w: chunk %d: recorded=%s computed=%s (staged chunk corrupted since upload)", ErrArtifactTransferCorrupted, ErrHashMismatch, c.ChunkIndex, c.SHA256, got)
		}
	}
	return hex.EncodeToString(whole.Sum(nil)), size, nil
}

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
func chunkStagingKey(bl repository.BlobStore, uploadID string, chunkIndex int) string {
	dir := filepath.Join(bl.StagingDir(), "chunks", uploadID)
	return filepath.Join(dir, fmt.Sprintf("chunk_%04d", chunkIndex))
}

// chunkRetryStagingKey is unique per write so retries cannot overwrite the
// staging file referenced by an existing durable row.
func chunkRetryStagingKey(bl repository.BlobStore, uploadID string, chunkIndex int) string {
	dir := filepath.Join(bl.StagingDir(), "chunks", uploadID)
	return filepath.Join(dir, fmt.Sprintf("chunk_%04d.retry_%d", chunkIndex, time.Now().UnixNano()))
}
