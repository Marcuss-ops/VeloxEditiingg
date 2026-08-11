// Package artifacts / chunked_test.go
//
// Tests for the chunked upload persistence path (ChunkedUploadService).
// They pin the no-second-read contract: chunk_sha256 is derived from the
// io.MultiWriter stream during UploadChunk, never by re-reading the staged
// file afterwards.
package artifacts

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// setupChunkedEnv seeds a RUNNING job + RENDER_FINISHED attempt, opens a
// chunked upload session and returns the service with its env.
func setupChunkedEnv(t *testing.T) (*ChunkedUploadService, *testEnv, string) {
	t.Helper()
	e := setupTestEnv(t)
	// ChunkedUploadService validates session expiry against the REAL wall
	// clock (time.Now), while BeginUpload stamps ExpiresAt from the injected
	// manual clock. Jump the manual clock past real-now so the session is
	// never instantly expired on the chunked path.
	if delta := time.Now().UTC().Sub(e.clock.Now()); delta > 0 {
		e.clock.Advance(delta)
	}
	e.seedJob("JC", "RUNNING", testWorkerID, testLeaseID, testRevision, e.clock.Now().Add(5*time.Minute))
	e.seedAttempt("JC", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	sess, err := e.svc.BeginUpload(context.Background(), beginUploadDefaultCmd("JC"))
	require.NoError(t, err, "BeginUpload")
	chunked := NewChunkedUploadService(e.svc, e.repo, e.bs)
	return chunked, e, sess.UploadID
}

func uploadChunk(t *testing.T, chunked *ChunkedUploadService, uploadID string, index int, data []byte) error {
	t.Helper()
	return chunked.UploadChunk(context.Background(), ChunkedUploadCommand{
		UploadID:   uploadID,
		ChunkIndex: index,
		Reader:     bytes.NewReader(data),
	})
}

// TestChunkedUpload_StreamHashMatchesInput pins the no-second-read contract:
// the recorded sha256 must equal the hash of the exact uploaded bytes (as
// computed by the incremental hasher), the size must match, and the staged
// file content must be byte-identical to the input.
func TestChunkedUpload_StreamHashMatchesInput(t *testing.T) {
	chunked, e, uploadID := setupChunkedEnv(t)

	payload := bytes.Repeat([]byte("chunk-payload-"), 4096) // ~56 KB
	require.NoError(t, uploadChunk(t, chunked, uploadID, 0, payload))

	chunks, err := e.repo.ListChunks(context.Background(), uploadID)
	require.NoError(t, err)
	require.Len(t, chunks, 1)

	got := chunks[0]
	require.Equal(t, 0, got.ChunkIndex)
	require.Equal(t, int64(len(payload)), got.SizeBytes)
	require.Equal(t, sha256Hex(payload), got.SHA256, "chunk_sha256 must be computed from the stream, not a second read")

	// The staged chunk must exist and match the hashed snapshot exactly.
	info, statErr := os.Stat(got.StorageKey)
	require.NoError(t, statErr)
	require.True(t, info.Mode().IsRegular())
	require.Equal(t, int64(len(payload)), info.Size())
	staged, readErr := os.ReadFile(filepath.Clean(got.StorageKey))
	require.NoError(t, readErr)
	require.Equal(t, payload, staged)
	require.Equal(t, sha256Hex(payload), sha256Hex(staged))
}

// TestChunkedUpload_IdempotentChunkIndex pins the resume contract: re-uploading
// the same chunk_index is a no-op through the repository uniqueness contract
// (INSERT OR IGNORE on (upload_id, chunk_index)) — the FIRST record wins even
// when the second upload carries different bytes.
func TestChunkedUpload_IdempotentChunkIndex(t *testing.T) {
	chunked, e, uploadID := setupChunkedEnv(t)

	first := []byte("first-content")
	second := []byte("second-content-OVERWRITE")
	require.NoError(t, uploadChunk(t, chunked, uploadID, 0, first))
	require.NoError(t, uploadChunk(t, chunked, uploadID, 0, second))

	chunks, err := e.repo.ListChunks(context.Background(), uploadID)
	require.NoError(t, err)
	require.Len(t, chunks, 1, "duplicate chunk_index must not create a second row")

	// INSERT OR IGNORE keeps the first record's identity.
	require.Equal(t, int64(len(first)), chunks[0].SizeBytes)
	require.Equal(t, sha256Hex(first), chunks[0].SHA256)

	// Adjacent indices are independent.
	require.NoError(t, uploadChunk(t, chunked, uploadID, 1, second))
	chunks, err = e.repo.ListChunks(context.Background(), uploadID)
	require.NoError(t, err)
	require.Len(t, chunks, 2)
	require.Equal(t, 1, chunks[1].ChunkIndex)
	require.Equal(t, sha256Hex(second), chunks[1].SHA256)
}

// TestChunkedUpload_EmptyChunkRejected: a zero-byte chunk must fail and leave
// no row and no staged file behind.
func TestChunkedUpload_EmptyChunkRejected(t *testing.T) {
	chunked, e, uploadID := setupChunkedEnv(t)

	err := uploadChunk(t, chunked, uploadID, 0, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrEmptyChunk), "got %v want ErrEmptyChunk", err)

	chunks, listErr := e.repo.ListChunks(context.Background(), uploadID)
	require.NoError(t, listErr)
	require.Empty(t, chunks)

	staged := chunkStagingKey(e.bs, uploadID, 0)
	_, statErr := os.Stat(staged)
	require.True(t, os.IsNotExist(statErr), "staged file must be removed on empty chunk")
}

// TestChunkedUpload_RequiresUploadIDAndReader: the guard fails fast on an
// empty uploadID and on a nil reader.
func TestChunkedUpload_RequiresUploadIDAndReader(t *testing.T) {
	chunked, _, _ := setupChunkedEnv(t)

	err := chunked.UploadChunk(context.Background(), ChunkedUploadCommand{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "uploadID and reader are required")

	chunked2, _, uploadID := setupChunkedEnv(t)
	err = chunked2.UploadChunk(context.Background(), ChunkedUploadCommand{UploadID: uploadID, ChunkIndex: 0})
	require.Error(t, err)
	require.Contains(t, err.Error(), "uploadID and reader are required")
}

// TestChunkedUpload_UnknownUploadFails: an uploadID that resolves to no
// session must surface ErrUploadNotFound, never a stray chunk file.
func TestChunkedUpload_UnknownUploadFails(t *testing.T) {
	chunked, _, _ := setupChunkedEnv(t)

	err := chunked.UploadChunk(context.Background(), ChunkedUploadCommand{
		UploadID:   "no-such-upload",
		ChunkIndex: 0,
		Reader:     bytes.NewReader([]byte("x")),
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrUploadNotFound), "got %v want ErrUploadNotFound", err)
}

// TestChunkedUpload_StreamErrorCleansUp: a reader error mid-stream must
// remove the partial staged file and leave no chunk row.
func TestChunkedUpload_StreamErrorCleansUp(t *testing.T) {
	chunked, e, uploadID := setupChunkedEnv(t)

	failing := &errReader{after: 8, err: errors.New("simulated stream drop")}
	err := chunked.UploadChunk(context.Background(), ChunkedUploadCommand{
		UploadID:   uploadID,
		ChunkIndex: 0,
		Reader:     failing,
	})
	require.Error(t, err)

	chunks, listErr := e.repo.ListChunks(context.Background(), uploadID)
	require.NoError(t, listErr)
	require.Empty(t, chunks)

	staged := chunkStagingKey(e.bs, uploadID, 0)
	_, statErr := os.Stat(staged)
	require.True(t, os.IsNotExist(statErr), "partial staged file must be removed on stream error")
}
