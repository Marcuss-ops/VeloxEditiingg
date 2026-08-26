// Package artifacts / chunked_recovery_test.go
//
// Recovery-artifact certification (FASE 8): duplicate chunks, duplicate
// /complete, and master restart during upload. These pin the fail-closed +
// idempotent behaviour of the chunked upload persistence path so a lost
// response or a master crash can never produce a duplicate artifact or a
// bogus 400/conflict on retry.

package artifacts

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"velox-server/internal/repository"
)

// advanceClockPastRealNow mirrors the clock jump in setupChunkedEnv: the
// chunked service validates session expiry against the REAL wall clock in
// UploadChunk, while BeginUpload stamps ExpiresAt from the injected manual
// clock. Restart tests must re-apply the jump after re-opening the DB so the
// freshly-injected clock is never left in the past.
func advanceClockPastRealNow(e *testEnv) {
	e.t.Helper()
	if delta := time.Now().UTC().Sub(e.clock.Now()); delta > 0 {
		e.clock.Advance(delta)
	}
}

// setupChunkedEnvWithExpected seeds a RUNNING job + RENDER_FINISHED attempt,
// then opens a chunked upload session whose worker-declared ExpectedSHA256 /
// ExpectedSizeBytes are the hash/size of the FULL assembled payload (the
// concatenation of every chunk the test will upload). The chunked receiver
// re-assembles the chunks into exactly that payload, so FinalizeVerified's
// expected == received gate passes — mirroring production, where the worker
// declares the spool manifest's SHA/size at BeginUpload.
func setupChunkedEnvWithExpected(t *testing.T, fullPayload []byte) (*ChunkedUploadService, *testEnv, string) {
	t.Helper()
	e := setupTestEnv(t)
	if delta := time.Now().UTC().Sub(e.clock.Now()); delta > 0 {
		e.clock.Advance(delta)
	}
	// AWAITING_ARTIFACT is the canonical post-ingest state FinalizeVerified's
	// job transition expects (jobs status AWAITING_ARTIFACT → SUCCEEDED).
	e.seedJob("JC", "AWAITING_ARTIFACT", testWorkerID, testLeaseID, testRevision, e.clock.Now().Add(5*time.Minute))
	e.seedAttempt("JC", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	cmd := beginUploadDefaultCmd("JC")
	cmd.ExpectedSHA256 = sha256Hex(fullPayload)
	cmd.ExpectedSizeBytes = int64(len(fullPayload))
	sess, err := e.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err, "BeginUpload")
	return NewChunkedUploadService(e.svc, e.repo, e.bs), e, sess.UploadID
}

func completeCmd(uploadID string) ChunkedCompleteCommand {
	return ChunkedCompleteCommand{
		UploadID: uploadID, JobID: "JC", WorkerID: testWorkerID, LeaseID: testLeaseID,
		AttemptNumber: 1, ExpectedRevision: testRevision,
	}
}

// TestChunkedRecovery_DuplicateChunkSucceedsWithoutDuplication certifies the
// duplicate-chunk sequence: chunk 0, chunk 1, chunk 1 again (lost-response
// retry), chunk 2 → complete. The retried chunk must not create a second row
// or overwrite the first bytes, and the final artifact must be READY with the
// SHA/size of the exact concatenated bytes.
func TestChunkedRecovery_DuplicateChunkSucceedsWithoutDuplication(t *testing.T) {
	chunk0 := []byte("recovery-chunk-zero")
	chunk1 := []byte("recovery-chunk-one")
	chunk2 := []byte("recovery-chunk-two")
	full := append(append(append([]byte{}, chunk0...), chunk1...), chunk2...)

	chunked, e, uploadID := setupChunkedEnvWithExpected(t, full)

	require.NoError(t, uploadChunk(t, chunked, uploadID, 0, chunk0))
	require.NoError(t, uploadChunk(t, chunked, uploadID, 1, chunk1))
	require.NoError(t, uploadChunk(t, chunked, uploadID, 1, chunk1)) // duplicate retry
	require.NoError(t, uploadChunk(t, chunked, uploadID, 2, chunk2))

	// No duplication: exactly one row per logical chunk index.
	chunks, err := e.repo.ListChunks(context.Background(), uploadID)
	require.NoError(t, err)
	require.Len(t, chunks, 3, "duplicate chunk must not create a second row")
	require.Equal(t, []int{0, 1, 2}, []int{chunks[0].ChunkIndex, chunks[1].ChunkIndex, chunks[2].ChunkIndex})

	art, err := chunked.CompleteChunked(context.Background(), completeCmd(uploadID))
	require.NoError(t, err)
	require.Equal(t, "READY", art.Status)
	require.Equal(t, sha256Hex(full), art.SHA256, "artifact SHA must be the hash of the exact concatenated chunks")
	require.Equal(t, int64(len(full)), art.SizeBytes)
}

// TestChunkedRecovery_DuplicateCompleteReturnsSameArtifact certifies the
// duplicate /complete contract: the first CompleteChunked runs Receive →
// Finalize → cleanup (chunk rows removed), so a lost-response retry must
// return the SAME persisted artifact via the COMPLETED short-circuit — not a
// 400/"no chunks" failure. A retry from a different worker is still fenced.
func TestChunkedRecovery_DuplicateCompleteReturnsSameArtifact(t *testing.T) {
	chunk0 := []byte("complete-recovery-0")
	chunk1 := []byte("complete-recovery-1")
	full := append(append([]byte{}, chunk0...), chunk1...)

	chunked, _, uploadID := setupChunkedEnvWithExpected(t, full)
	require.NoError(t, uploadChunk(t, chunked, uploadID, 0, chunk0))
	require.NoError(t, uploadChunk(t, chunked, uploadID, 1, chunk1))

	cmd := completeCmd(uploadID)
	first, err := chunked.CompleteChunked(context.Background(), cmd)
	require.NoError(t, err)
	require.Equal(t, "READY", first.Status)

	// Duplicate /complete (lost-response retry) → same artifact, no error.
	second, err := chunked.CompleteChunked(context.Background(), cmd)
	require.NoError(t, err)
	require.Equal(t, first.ID, second.ID)
	require.Equal(t, first.SHA256, second.SHA256)
	require.Equal(t, first.SizeBytes, second.SizeBytes)
	require.Equal(t, first.Status, second.Status)

	// A duplicate /complete from a DIFFERENT worker must be fenced.
	_, err = chunked.CompleteChunked(context.Background(), ChunkedCompleteCommand{
		UploadID: uploadID, JobID: "JC", WorkerID: "other-worker", LeaseID: testLeaseID,
		AttemptNumber: 1, ExpectedRevision: testRevision,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrTransitionConflict)
}

// TestChunkedRecovery_MasterRestartDuringUploadResumesAndSucceeds certifies
// the master-restart contract: upload chunk 0, "crash" (close the DB),
// re-open the SAME database file with fresh service instances, then resume
// chunk 1 and complete. The session + chunk 0 must survive the restart and
// the final artifact must be READY with the correct SHA/size.
func TestChunkedRecovery_MasterRestartDuringUploadResumesAndSucceeds(t *testing.T) {
	tmp := t.TempDir()

	chunk0 := []byte("restart-chunk-zero")
	chunk1 := []byte("restart-chunk-one")
	full := append(append([]byte{}, chunk0...), chunk1...)

	// Phase 1: open, begin upload (with expected SHA/size), upload chunk 0.
	e1 := openTestEnvAt(t, tmp)
	advanceClockPastRealNow(e1)
	e1.seedJob("JC", "AWAITING_ARTIFACT", testWorkerID, testLeaseID, testRevision, e1.clock.Now().Add(5*time.Minute))
	e1.seedAttempt("JC", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)
	begin := beginUploadDefaultCmd("JC")
	begin.ExpectedSHA256 = sha256Hex(full)
	begin.ExpectedSizeBytes = int64(len(full))
	sess, err := e1.svc.BeginUpload(context.Background(), begin)
	require.NoError(t, err)
	uploadID := sess.UploadID

	require.NoError(t, uploadChunk(t, NewChunkedUploadService(e1.svc, e1.repo, e1.bs), uploadID, 0, chunk0))
	require.NoError(t, e1.db.Close(), "simulated master crash")

	// Phase 2: re-open the SAME DB file (new service instances, same data).
	e2 := openTestEnvAt(t, tmp)
	advanceClockPastRealNow(e2)
	chunked2 := NewChunkedUploadService(e2.svc, e2.repo, e2.bs)

	// The session and chunk 0 survived the restart.
	state, err := chunked2.GetChunkState(context.Background(), uploadID)
	require.NoError(t, err)
	require.Len(t, state.Uploaded, 1, "chunk 0 must persist across master restart")
	require.True(t, state.Uploaded[0], "chunk 0 must be marked uploaded after restart")

	require.NoError(t, uploadChunk(t, chunked2, uploadID, 1, chunk1))

	art, err := chunked2.CompleteChunked(context.Background(), completeCmd(uploadID))
	require.NoError(t, err)
	require.Equal(t, "READY", art.Status)
	require.Equal(t, sha256Hex(full), art.SHA256)
	require.Equal(t, int64(len(full)), art.SizeBytes)

	// The durable session must be COMPLETED, never left RECEIVED/UPLOADING.
	session, err := chunked2.GetUpload(context.Background(), uploadID)
	require.NoError(t, err)
	require.Equal(t, string(repository.UploadCompleted), session.Status)
}
