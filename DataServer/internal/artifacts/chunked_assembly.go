package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"log"
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
	result, err := s.receiveChunkAssembly(ctx, uploadID, session, chunks)
	if err != nil {
		return nil, fmt.Errorf("artifacts: ReceiveChunked Receive: %w", err)
	}
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

	if _, recvErr := s.receiveChunkAssembly(ctx, cmd.UploadID, session, chunks); recvErr != nil {
		return nil, fmt.Errorf("artifacts: CompleteChunked Receive: %w", recvErr)
	}
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

func (s *ChunkedUploadService) receiveChunkAssembly(ctx context.Context, uploadID string, session *repository.UploadSession, chunks []repository.ChunkRecord) (*ReceiveResult, error) {
	if s.directAssemblyMode == DirectAssemblyEnforce {
		path := session.TemporaryStorageKey
		sha, size, err := s.assembleDirect(path, chunks)
		if err == nil {
			return s.artifactSvc.markReceivedVerified(ctx, uploadID, sha, size)
		} else {
			// An assembly or durability failure falls back to the established
			// .assembled -> Receive path. Validation failures after the direct
			// bytes have been accepted are returned by assembleDirect directly.
			_ = s.blobStore.RemoveStaging(path)
			log.Printf("[CHUNKED] direct assembly fallback upload=%s err=%v", uploadID, err)
			return s.assembleLegacy(ctx, uploadID, session, chunks, false)
		}
	}
	return s.assembleLegacy(ctx, uploadID, session, chunks, s.directAssemblyMode == DirectAssemblyShadow)
}

func (s *ChunkedUploadService) assembleDirect(path string, chunks []repository.ChunkRecord) (string, int64, error) {
	out, err := s.blobStore.OpenStagedWrite(path)
	if err != nil {
		return "", 0, fmt.Errorf("create direct assembly: %w", err)
	}
	sha, size, err := s.assembleChunksVerified(out, chunks, true)
	if err == nil {
		err = out.Sync()
	}
	closeErr := out.Close()
	if err != nil {
		return "", 0, fmt.Errorf("assemble direct chunks: %w", err)
	}
	if closeErr != nil {
		return "", 0, fmt.Errorf("close direct assembly: %w", closeErr)
	}
	return sha, size, nil
}

func (s *ChunkedUploadService) assembleLegacy(ctx context.Context, uploadID string, session *repository.UploadSession, chunks []repository.ChunkRecord, compare bool) (*ReceiveResult, error) {
	path := session.TemporaryStorageKey + ".assembled"
	defer s.blobStore.RemoveStaging(path)
	out, err := s.blobStore.OpenStagedWrite(path)
	if err != nil {
		return nil, fmt.Errorf("create assembly file: %w", err)
	}
	sha, size, err := s.assembleChunksVerified(out, chunks, compare)
	if err == nil {
		err = out.Sync()
	}
	closeErr := out.Close()
	if err != nil {
		return nil, fmt.Errorf("assemble chunks: %w", err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close assembly: %w", closeErr)
	}
	assembled, err := s.blobStore.OpenStagedRead(path)
	if err != nil {
		return nil, fmt.Errorf("open assembled chunks: %w", err)
	}
	result, receiveErr := s.artifactSvc.Receive(ctx, uploadID, assembled)
	_ = assembled.Close()
	if receiveErr != nil {
		return nil, receiveErr
	}
	if compare && (result.ReceivedSHA256 != sha || result.ReceivedSizeBytes != size) {
		log.Printf("[CHUNKED] direct assembly shadow mismatch upload=%s direct_sha=%s receive_sha=%s direct_size=%d receive_size=%d", uploadID, sha, result.ReceivedSHA256, size, result.ReceivedSizeBytes)
	} else if compare {
		log.Printf("[CHUNKED] direct assembly shadow match upload=%s sha=%s size=%d", uploadID, sha, size)
	}
	return result, nil
}

func (s *ChunkedUploadService) assembleChunksVerified(dst io.Writer, chunks []repository.ChunkRecord, computeWhole bool) (string, int64, error) {
	var whole hash.Hash
	if computeWhole {
		whole = sha256.New()
	}
	var size int64
	var writer io.Writer = dst
	if computeWhole {
		writer = io.MultiWriter(dst, whole)
	}
	for _, c := range chunks {
		in, openErr := s.blobStore.OpenStagedRead(c.StorageKey)
		if openErr != nil {
			return "", 0, fmt.Errorf("open chunk %d: %w", c.ChunkIndex, openErr)
		}
		hasher := sha256.New()
		n, copyErr := io.Copy(io.MultiWriter(writer, hasher), in)
		if computeWhole {
			size += n
		}
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
	if !computeWhole {
		return "", 0, nil
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
