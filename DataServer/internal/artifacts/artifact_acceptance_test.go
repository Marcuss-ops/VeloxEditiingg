package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"velox-server/internal/store"
)

func TestArtifactAcceptance_HashMismatchNeverFinalizesAndCleansTemporaryFile(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J-accept-hash", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J-accept-hash", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	payload := []byte("artifact bytes whose digest is intentionally wrong")
	cmd := beginUploadDefaultCmd("J-accept-hash")
	cmd.ExpectedSHA256 = strings.Repeat("0", sha256.Size*2)
	cmd.ExpectedSizeBytes = int64(len(payload))
	session, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)

	_, err = env.svc.Receive(context.Background(), session.UploadID, bytes.NewReader(payload))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrHashMismatch), "error=%v", err)

	_, statErr := os.Stat(session.TemporaryStorageKey)
	require.ErrorIs(t, statErr, os.ErrNotExist, "hash mismatch must remove the staging file")
	fresh, err := env.repo.GetUploadSession(context.Background(), session.UploadID)
	require.NoError(t, err)
	require.Equal(t, string(store.UploadFailed), fresh.Status)

	var artifactStatus string
	require.NoError(t, env.db.QueryRow(`SELECT status FROM artifacts WHERE id = ?`, session.ArtifactID).Scan(&artifactStatus))
	require.Equal(t, "STAGING", artifactStatus, "a failed receive must not promote the artifact")
}

func TestArtifactAcceptance_ReceiveRecordsMasterSHAAndFinalizeReachesReadyWithDurableBlob(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J-accept-ready", "AWAITING_ARTIFACT", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J-accept-ready", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	payload := []byte("small deterministic render payload")
	expected := sha256.Sum256(payload)
	cmd := beginUploadDefaultCmd("J-accept-ready")
	cmd.ExpectedSHA256 = hex.EncodeToString(expected[:])
	cmd.ExpectedSizeBytes = int64(len(payload))
	session, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)

	received, err := env.svc.Receive(context.Background(), session.UploadID, bytes.NewReader(payload))
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(expected[:]), received.ReceivedSHA256)
	require.Equal(t, int64(len(payload)), received.ReceivedSizeBytes)

	artifact, err := env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
		UploadID: session.UploadID, JobID: "J-accept-ready", WorkerID: testWorkerID,
		LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
	})
	require.NoError(t, err)
	require.Equal(t, string(ArtifactReady), artifact.Status)
	require.Equal(t, hex.EncodeToString(expected[:]), artifact.SHA256)
	require.Equal(t, int64(len(payload)), artifact.SizeBytes)

	finalPath := filepath.Join(env.bs.FinalDir(), filepath.FromSlash(artifact.StorageKey))
	got, err := os.ReadFile(finalPath)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	actual := sha256.Sum256(got)
	require.Equal(t, expected, actual, "persisted blob must match the recorded SHA-256")

	// Promotion must leave no staging temp for the completed upload.
	_, err = os.Stat(session.TemporaryStorageKey)
	require.ErrorIs(t, err, os.ErrNotExist)

	err = filepath.Walk(env.bs.FinalDir(), func(path string, info os.FileInfo, walkErr error) error {
		require.NoError(t, walkErr)
		if info != nil && !info.IsDir() {
			require.NotContains(t, info.Name(), ".tmp", "final storage must not retain promotion temp files")
		}
		return nil
	})
	require.NoError(t, err)
}

func TestArtifactAcceptance_FFProbeValidatesRenderedVideoAndRejectsCorruptBytes(t *testing.T) {
	requireMediaTools(t)

	env := setupTestEnv(t)
	validPath := filepath.Join(env.tmpDir, "acceptance.mp4")
	synthesizeAcceptanceVideo(t, validPath)
	valid, err := os.ReadFile(validPath)
	require.NoError(t, err)

	ctx := context.Background()
	env.seedJob("J-accept-media", "AWAITING_ARTIFACT", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J-accept-media", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)
	mediaCmd := beginUploadDefaultCmd("J-accept-media")
	mediaCmd.ExpectedSHA256 = sha256Hex(valid)
	mediaCmd.ExpectedSizeBytes = int64(len(valid))
	session, err := env.svc.BeginUpload(ctx, mediaCmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(ctx, session.UploadID, bytes.NewReader(valid))
	require.NoError(t, err)

	// Independent media acceptance check: ffprobe must parse the exact
	// bytes that will be promoted, rather than trusting SUCCEEDED alone.
	probe := exec.Command("ffprobe", "-v", "error", "-show_entries", "format=duration,size:stream=codec_name,width,height,r_frame_rate", "-of", "json", session.TemporaryStorageKey)
	probeOut, err := probe.CombinedOutput()
	require.NoError(t, err, "ffprobe output=%s", probeOut)
	require.NotEmpty(t, probeOut)

	env.svc.WithFFProbeMode("enforce")
	artifact, err := env.svc.Finalize(ctx, FinalizeArtifactCommand{
		UploadID: session.UploadID, JobID: "J-accept-media", WorkerID: testWorkerID,
		LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
	})
	require.NoError(t, err)
	require.Equal(t, string(ArtifactReady), artifact.Status)

	// A corrupt/non-media payload is rejected by ffprobe before it can be
	// accepted as a rendered video. The upload itself remains unfinalized.
	env.seedJob("J-accept-corrupt", "AWAITING_ARTIFACT", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J-accept-corrupt", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)
	corrupt := []byte("not a video")
	corruptCmd := beginUploadDefaultCmd("J-accept-corrupt")
	corruptCmd.ExpectedSHA256 = sha256Hex(corrupt)
	corruptCmd.ExpectedSizeBytes = int64(len(corrupt))
	corruptSession, err := env.svc.BeginUpload(ctx, corruptCmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(ctx, corruptSession.UploadID, bytes.NewReader(corrupt))
	require.NoError(t, err)
	corruptProbe := exec.Command("ffprobe", "-v", "error", corruptSession.TemporaryStorageKey)
	require.Error(t, corruptProbe.Run(), "corrupt render must fail ffprobe")
	_, err = env.svc.Finalize(ctx, FinalizeArtifactCommand{
		UploadID: corruptSession.UploadID, JobID: "J-accept-corrupt", WorkerID: testWorkerID,
		LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
	})
	require.ErrorIs(t, err, ErrFFProbeAudioCountMismatch)
	fresh, err := env.repo.GetUploadSession(ctx, corruptSession.UploadID)
	require.NoError(t, err)
	require.Equal(t, string(store.UploadReceived), fresh.Status)
	var corruptStatus string
	require.NoError(t, env.db.QueryRow(`SELECT status FROM artifacts WHERE id = ?`, corruptSession.ArtifactID).Scan(&corruptStatus))
	require.Equal(t, string(ArtifactStaging), corruptStatus, "ffprobe failure must not make a corrupt artifact READY")
}

func requireMediaTools(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skipf("ffmpeg unavailable: %v", err)
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skipf("ffprobe unavailable: %v", err)
	}
}

func synthesizeAcceptanceVideo(t *testing.T, path string) {
	t.Helper()
	cmd := exec.Command("ffmpeg", "-y",
		"-f", "lavfi", "-i", "color=c=black:s=320x180:r=24",
		"-f", "lavfi", "-i", "anullsrc=r=8000:cl=mono",
		"-t", "1", "-c:v", "libx264", "-c:a", "aac", "-shortest", "-pix_fmt", "yuv420p", path)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "ffmpeg output=%s", output)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(0))
}
