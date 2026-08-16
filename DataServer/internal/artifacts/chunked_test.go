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
	"fmt"
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

// TestChunkedUpload_IdempotentChunkIndex pins the resume contract: an identical
// retry is a no-op, while different bytes for the same index are rejected and
// cannot overwrite the first staged file.
func TestChunkedUpload_IdempotentChunkIndex(t *testing.T) {
	chunked, e, uploadID := setupChunkedEnv(t)

	first := []byte("first-content")
	second := []byte("second-content-OVERWRITE")
	require.NoError(t, uploadChunk(t, chunked, uploadID, 0, first))
	require.NoError(t, uploadChunk(t, chunked, uploadID, 0, first))
	err := uploadChunk(t, chunked, uploadID, 0, second)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrChunkConflict)

	chunks, err := e.repo.ListChunks(context.Background(), uploadID)
	require.NoError(t, err)
	require.Len(t, chunks, 1, "duplicate chunk_index must not create a second row")

	// The first record and its physical bytes remain authoritative.
	require.Equal(t, int64(len(first)), chunks[0].SizeBytes)
	require.Equal(t, sha256Hex(first), chunks[0].SHA256)
	stored, readErr := os.ReadFile(chunks[0].StorageKey)
	require.NoError(t, readErr)
	require.Equal(t, first, stored)

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

// TestChunkedAssembly_VerifiesAllChunkSHAs pins the assembly contract: the
// concatenated assembly must equal the uploaded bytes and ReceiveChunked must
// return the master-computed SHA of exactly those bytes (chunk SHAs verified
// incrementally during the copy — no second read).
func TestChunkedAssembly_VerifiesAllChunkSHAs(t *testing.T) {
	chunked, _, uploadID := setupChunkedEnv(t)

	chunk0 := bytes.Repeat([]byte("first-chunk-data-"), 2048) // ~34 KB
	chunk1 := bytes.Repeat([]byte("second-chunk-data-"), 2048)
	require.NoError(t, uploadChunk(t, chunked, uploadID, 0, chunk0))
	require.NoError(t, uploadChunk(t, chunked, uploadID, 1, chunk1))

	result, err := chunked.ReceiveChunked(context.Background(), uploadID)
	require.NoError(t, err)

	concat := append(append([]byte{}, chunk0...), chunk1...)
	require.Equal(t, int64(len(concat)), result.ReceivedSizeBytes)
	require.Equal(t, sha256Hex(concat), result.ReceivedSHA256,
		"assembly SHA must be the master hash of the concatenated chunk bytes")

	// A lost response must be safe to retry after chunk cleanup. The second
	// call returns the persisted result without requiring the chunks again.
	retry, err := chunked.ReceiveChunked(context.Background(), uploadID)
	require.NoError(t, err)
	require.Equal(t, result, retry)
}

// TestChunkedAssembly_CorruptedChunkFailsClosed: a staged chunk that was
// corrupted on disk since upload must be caught at ASSEMBLY time (incremental
// per-chunk SHA check) with ErrArtifactTransferCorrupted — before the final
// Receive pass — on both the ReceiveChunked and CompleteChunked paths, and
// for both the first chunk and a later chunk (proving preceding chunks are
// still verified before the failure localizes).
func TestChunkedAssembly_CorruptedChunkFailsClosed(t *testing.T) {
	cases := []struct {
		name         string
		complete     bool
		corruptIndex int
	}{
		{"receive_chunked_first_chunk", false, 0},
		{"receive_chunked_later_chunk", false, 1},
		{"complete_chunked_first_chunk", true, 0},
		{"complete_chunked_later_chunk", true, 1},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			chunked, e, uploadID := setupChunkedEnv(t)

			chunk0 := bytes.Repeat([]byte("chunk-a-"), 1024)
			chunk1 := bytes.Repeat([]byte("chunk-b-"), 1024)
			require.NoError(t, uploadChunk(t, chunked, uploadID, 0, chunk0))
			require.NoError(t, uploadChunk(t, chunked, uploadID, 1, chunk1))

			// Corrupt the selected chunk's staged file on disk (truncate
			// + write different bytes). The chunk record still carries the
			// original incremental hash.
			chunks, listErr := e.repo.ListChunks(context.Background(), uploadID)
			require.NoError(t, listErr)
			require.Len(t, chunks, 2)
			require.NoError(t, os.WriteFile(filepath.Clean(chunks[tc.corruptIndex].StorageKey), []byte("corrupted!"), 0o644))

			var err error
			if tc.complete {
				_, err = chunked.CompleteChunked(context.Background(), ChunkedCompleteCommand{
					UploadID: uploadID, JobID: "JC", WorkerID: testWorkerID, LeaseID: testLeaseID,
					AttemptNumber: 1, ExpectedRevision: testRevision,
				})
			} else {
				_, err = chunked.ReceiveChunked(context.Background(), uploadID)
			}
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrArtifactTransferCorrupted),
				"got %v want ErrArtifactTransferCorrupted", err)
			require.Contains(t, err.Error(), fmt.Sprintf("chunk %d", tc.corruptIndex),
				"assembly error must localize the fault to the corrupted chunk index")
		})
	}
}

// TestChunkedAssembly_LegacyEmptySHAChunkStillAssembles pins the legacy
// compatibility branch of assembleChunksVerified: a chunk record whose sha256
// is empty (pre-incremental-hash rows) is copied WITHOUT per-chunk
// verification and the assembly still completes — the final Receive pass
// against expected_sha256 remains authoritative for those rows.
func TestChunkedAssembly_LegacyEmptySHAChunkStillAssembles(t *testing.T) {
	chunked, e, uploadID := setupChunkedEnv(t)

	chunk0 := []byte("legacy-chunk-0")
	chunk1 := []byte("legacy-chunk-1")
	require.NoError(t, uploadChunk(t, chunked, uploadID, 0, chunk0))
	require.NoError(t, uploadChunk(t, chunked, uploadID, 1, chunk1))

	// Wipe the recorded incremental hash to simulate a legacy row.
	_, err := e.db.Exec(`UPDATE artifact_upload_chunks SET sha256 = '' WHERE upload_id = ?`, uploadID)
	require.NoError(t, err)

	result, err := chunked.ReceiveChunked(context.Background(), uploadID)
	require.NoError(t, err)
	concat := append(append([]byte{}, chunk0...), chunk1...)
	require.Equal(t, sha256Hex(concat), result.ReceivedSHA256,
		"empty-SHA legacy chunks must still assemble to the master hash of the concatenated bytes")
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
