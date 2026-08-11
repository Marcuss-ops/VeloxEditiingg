package artifacts

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	"velox-server/internal/store"
)

// =====================================================================
// #region 5 — hash differente (Receive)
// =====================================================================

func TestReceive_HashMismatch(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J5", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J5", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	payload := []byte("hello-world video bytes")
	realHash := sha256Hex(payload)
	wrongHash := strings.Repeat("a", len(realHash))

	cmd := beginUploadDefaultCmd("J5")
	cmd.ExpectedSHA256 = wrongHash
	cmd.ExpectedSizeBytes = int64(len(payload))
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)

	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrHashMismatch), "got %v want ErrHashMismatch", err)
	require.True(t, errors.Is(err, ErrArtifactTransferCorrupted), "got %v want ErrArtifactTransferCorrupted (worker-declared != master-computed)", err)

	// Staging file must be cleaned up.
	_, statErr := os.Stat(sess.TemporaryStorageKey)
	require.True(t, os.IsNotExist(statErr), "staging file should be removed on hash mismatch")

	// Upload must be marked FAILED.
	fresh, err := env.repo.GetUploadSession(context.Background(), sess.UploadID)
	require.NoError(t, err)
	require.Equal(t, "FAILED", fresh.Status)
}

// =====================================================================
// #region 6 — size differente (Receive)
// =====================================================================

func TestReceive_SizeMismatch(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J6", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J6", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	payload := []byte("short-payload-for-size-mismatch")
	cmd := beginUploadDefaultCmd("J6")
	cmd.ExpectedSHA256 = sha256Hex(payload)
	cmd.ExpectedSizeBytes = int64(len(payload) + 999)
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)

	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrSizeMismatch), "got %v want ErrSizeMismatch", err)
}

// =====================================================================
// #region 7 — upload interrotto (Receive abort := FAILED + cleanup)
// =====================================================================

func TestReceive_Interrupted(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J7", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J7", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	cmd := beginUploadDefaultCmd("J7")
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)

	interrupted := &errReader{after: 4, err: errors.New("simulated network drop")}
	_, err = env.svc.Receive(context.Background(), sess.UploadID, interrupted)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrBlobWriteFailed), "got %v", err)

	_, statErr := os.Stat(sess.TemporaryStorageKey)
	require.True(t, os.IsNotExist(statErr))

	fresh, err := env.repo.GetUploadSession(context.Background(), sess.UploadID)
	require.NoError(t, err)
	require.Equal(t, "FAILED", fresh.Status)
}

type errReader struct {
	after int
	err   error
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.after <= 0 {
		return 0, r.err
	}
	n := r.after
	if n > len(p) {
		n = len(p)
	}
	r.after -= n
	return n, nil
}

// =====================================================================
// B2 — no second read: staged blob must match the hashed snapshot
// =====================================================================

// TestReceive_StagedBlobMatchesHashedBytes pins the B2 invariant that
// replaced the post-write full-file re-hash: after a successful Receive
// the staged blob on disk is byte-identical to what the incremental
// io.MultiWriter hasher observed (same size + same SHA-256). The test
// runs WITHOUT any worker-declared expected hash/size, so the
// disk==hashed invariant is the only integrity check available — exactly
// the trust boundary the O(1) stat-size check now guarantees.
func TestReceive_StagedBlobMatchesHashedBytes(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("JB2", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("JB2", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	payload := []byte("b2 no-second-read payload bytes")
	cmd := beginUploadDefaultCmd("JB2") // no expected sha/size declared
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)

	received, err := env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.NoError(t, err)
	require.Equal(t, sha256Hex(payload), received.ReceivedSHA256)
	require.Equal(t, int64(len(payload)), received.ReceivedSizeBytes)

	// The staged blob must exist and match the hashed snapshot exactly.
	info, statErr := os.Stat(sess.TemporaryStorageKey)
	require.NoError(t, statErr)
	require.True(t, info.Mode().IsRegular())
	require.Equal(t, int64(len(payload)), info.Size(), "staged blob size must match the counted bytes")
	staged, readErr := os.ReadFile(sess.TemporaryStorageKey)
	require.NoError(t, readErr)
	require.Equal(t, payload, staged)
	require.Equal(t, sha256Hex(payload), sha256Hex(staged))
}

// =====================================================================
// EXTRA — Receive out-of-order state (no resurrection)
// =====================================================================

func TestReceive_StateMachineGuards(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("JSM", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("JSM", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	sess, err := env.svc.BeginUpload(context.Background(), beginUploadDefaultCmd("JSM"))
	require.NoError(t, err)

	// Force upload to FAILED.
	failed := "FAILED"
	require.NoError(t, env.repo.UpdateUploadStatus(context.Background(), sess.UploadID, store.UploadFields{Status: &failed}))

	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes([]byte("resurrect")))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrUploadStateInvalid))
}
