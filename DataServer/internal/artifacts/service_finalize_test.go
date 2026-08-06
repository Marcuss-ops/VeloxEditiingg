package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	"velox-server/internal/store"
)

// =====================================================================
// #region 8 — doppia finalizzazione (idempotent path)
// =====================================================================

func TestFinalize_DoubleFinalizeIdempotent(t *testing.T) {
	env := setupTestEnv(t)
	// AWAITING_ARTIFACT: the canonical pre-upload state (ingest rolled the
	// job RUNNING→AWAITING_ARTIFACT when the worker reported render-finished).
	env.seedJob("J8", "AWAITING_ARTIFACT", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J8", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	cmd := beginUploadDefaultCmd("J8")
	payload := []byte("dup-finalize-payload")
	cmd.ExpectedSizeBytes = int64(len(payload))
	cmd.ExpectedSHA256 = sha256Hex(payload)
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.NoError(t, err)

	first, err := env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
		UploadID: sess.UploadID, JobID: "J8", WorkerID: testWorkerID,
		LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
	})
	require.NoError(t, err)
	require.Equal(t, "READY", first.Status)
	require.NotEmpty(t, first.SHA256)

	// Second finalize on same auth: idempotent.
	second, err := env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
		UploadID: sess.UploadID, JobID: "J8", WorkerID: testWorkerID,
		LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
	})
	require.NoError(t, err, "second finalize must be idempotent success per spec")
	require.Equal(t, first.ID, second.ID)
	require.Equal(t, "READY", second.Status)

	// Legacy outbox ARTIFACT_READY emissions: none expected.
	var n int
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM outbox_events WHERE event_type='ARTIFACT_READY'`).Scan(&n))
	require.Equal(t, 0, n, "LEGACY ARTIFACT_READY outbox emission must be gone")

	// Delivery: exactly one row for (artifact, primary).
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id=? AND destination_id='primary'`, first.ID).Scan(&n))
	require.Equal(t, 1, n)
}

// =====================================================================
// #region 9 — due finalizzazioni concorrenti (channel-coordinated race)
// =====================================================================

func TestFinalize_Concurrent(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J9", "AWAITING_ARTIFACT", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J9", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	cmd := beginUploadDefaultCmd("J9")
	payload := []byte("concurrent-finalize")
	cmd.ExpectedSizeBytes = int64(len(payload))
	cmd.ExpectedSHA256 = sha256Hex(payload)
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.NoError(t, err)

	// Channel-coordinated start: both goroutines signal ready, the main
	// goroutine waits for both, then closes `start`. No time.Sleep — the
	// race window is closed deterministically.
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	type result struct {
		art *store.Artifact
		err error
	}
	results := make([]result, 2)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			results[i].art, results[i].err = env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
				UploadID: sess.UploadID, JobID: "J9", WorkerID: testWorkerID,
				LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
			})
		}()
	}
	<-ready
	<-ready
	close(start)
	wg.Wait()

	// Either both succeed (idempotent path on slow commit / fast retry),
	// or exactly one succeeds and the other is rejected by CAS.
	// What MUST hold: the job is DELIVERING until its explicit delivery
	// plan completes; no legacy ARTIFACT_READY outbox is emitted; the
	// delivery row is unique; no artifact is double-finalized.
	var successCount int
	for i, r := range results {
		if r.err == nil {
			successCount++
			t.Logf("goro %d: success art=%s", i, r.art.ID)
		} else {
			t.Logf("goro %d: err=%v", i, r.err)
			require.True(t, errors.Is(r.err, ErrTransitionConflict),
				"concurrent finalizer's only valid error is ErrTransitionConflict; got %v", r.err)
		}
	}
	require.GreaterOrEqual(t, successCount, 1, "at least one finalize must succeed")

	var jobStatus string
	require.NoError(t, env.db.QueryRow(`SELECT status FROM jobs WHERE job_id='J9'`).Scan(&jobStatus))
	require.Equal(t, "DELIVERING", jobStatus, "explicit delivery plans keep the job in DELIVERING")

	var n int
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM outbox_events WHERE event_type='ARTIFACT_READY'`).Scan(&n))
	require.Equal(t, 0, n, "LEGACY ARTIFACT_READY outbox emission must be gone")

	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id=? AND destination_id='primary'`, sess.ArtifactID).Scan(&n))
	require.Equal(t, 1, n, "exactly one delivery row")
}

// =====================================================================
// #region 10 — blob promosso + DB rollback
// =====================================================================

func TestFinalize_BlobPromotedButDBCASMissed(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J10", "AWAITING_ARTIFACT", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J10", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	cmd := beginUploadDefaultCmd("J10")
	payload := []byte("rollback-payload")
	cmd.ExpectedSizeBytes = int64(len(payload))
	cmd.ExpectedSHA256 = sha256Hex(payload)
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.NoError(t, err)

	// Capture the detected extension before Finalize removes the staging
	// source after the atomic promotion. The final path must remain
	// addressable even when the SQL transaction later rolls back.
	detectedExt := mimeToExt(detectMIME(sess.TemporaryStorageKey))

	// White-box mutation: flip job status to CANCELED so the CAS in
	// FinalizeArtifactAndCompleteJob fails. Finalize itself does NOT
	// pre-check jobs.status — promote happens BEFORE the SQL tx — so
	// the blob WILL exist on disk and the tx will roll back.
	_, err = env.db.Exec(`UPDATE jobs SET status='CANCELED' WHERE job_id='J10'`)
	require.NoError(t, err)

	_, err = env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
		UploadID: sess.UploadID, JobID: "J10", WorkerID: testWorkerID,
		LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
	})
	require.Error(t, err, "Finalize must fail because CAS missed")
	require.True(t, errors.Is(err, ErrTransitionConflict), "got %v", err)

	var jobStatus string
	require.NoError(t, env.db.QueryRow(`SELECT status FROM jobs WHERE job_id='J10'`).Scan(&jobStatus))
	require.Equal(t, "CANCELED", jobStatus)

	var artStatus string
	require.NoError(t, env.db.QueryRow(`SELECT status FROM artifacts WHERE id=?`, sess.ArtifactID).Scan(&artStatus))
	require.NotEqual(t, "READY", artStatus)
	require.Equal(t, "STAGING", artStatus)

	// Orphan blob must exist on disk (reconciler will reclaim).
	// The finalized extension comes from the detected MIME type, not
	// the requested upload MIME, so compute it the same way the service
	// does before asserting the promoted blob path.
	_, absFinal, err := FinalStorageKey(env.bs, sha256Hex(payload), detectedExt)
	require.NoError(t, err)
	_, err = os.Stat(absFinal)
	require.NoError(t, err, "orphan blob at %s must exist", absFinal)
}

// =====================================================================
// #region 13 — delivery creata una volta
// =====================================================================

func TestFinalize_DeliveryIsIdempotent(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J13", "AWAITING_ARTIFACT", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J13", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)
	cmd := beginUploadDefaultCmd("J13")
	payload := []byte("delivery-once")
	cmd.ExpectedSizeBytes = int64(len(payload))
	cmd.ExpectedSHA256 = sha256Hex(payload)
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.NoError(t, err)
	_, err = env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
		UploadID: sess.UploadID, JobID: "J13", WorkerID: testWorkerID,
		LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
	})
	require.NoError(t, err)

	var n int
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id=? AND destination_id='primary'`, sess.ArtifactID).Scan(&n))
	require.Equal(t, 1, n)

	// Manually re-run the delivery INSERT to verify the unique
	// (artifact_id, destination_id) constraint makes it a no-op.
	now := env.clock.Now().UTC().Format(time.RFC3339)
	_, err = env.db.Exec(`INSERT INTO job_deliveries
		(delivery_id, artifact_id, destination_id, status, max_attempts, idempotency_key, created_at, updated_at)
		VALUES (?, ?, 'primary', 'PENDING', 5, ?, ?, ?)
		ON CONFLICT(artifact_id, destination_id) DO NOTHING`,
		"duplicate-delivery", sess.ArtifactID, sess.ArtifactID+"_primary", now, now)
	require.NoError(t, err)
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id=? AND destination_id='primary'`, sess.ArtifactID).Scan(&n))
	require.Equal(t, 1, n)
}

// =====================================================================
// #region 14 — outbox inserita una volta
// Covered in TestFinalize_DoubleFinalizeIdempotent above.
// =====================================================================

// =====================================================================
// #region 15 — artifact READY implica blob esistente
// =====================================================================

func TestFinalize_ArtifactREADYMeansBlobExists(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J15", "AWAITING_ARTIFACT", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J15", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)
	cmd := beginUploadDefaultCmd("J15")
	payload := []byte("artifact-ready-test-blob-payload")
	cmd.ExpectedSizeBytes = int64(len(payload))
	cmd.MimeType = "video/mp4"
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.NoError(t, err)

	art, err := env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
		UploadID: sess.UploadID, JobID: "J15", WorkerID: testWorkerID,
		LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
	})
	require.NoError(t, err)
	require.Equal(t, "READY", art.Status)

	require.NotEmpty(t, art.StorageKey)
	absFinal := filepath.Join(env.bs.FinalDir(), filepath.FromSlash(art.StorageKey))
	info, err := os.Stat(absFinal)
	require.NoError(t, err, "blob at %s must exist", absFinal)
	require.Equal(t, int64(len(payload)), info.Size())

	f, err := os.Open(absFinal)
	require.NoError(t, err)
	defer f.Close()
	h := sha256.New()
	_, err = io.Copy(h, f)
	require.NoError(t, err)
	require.Equal(t, art.SHA256, hex.EncodeToString(h.Sum(nil)), "disk SHA must match artifact SHA256")
}

// =====================================================================
// #region 16 — doppio worker sullo stesso task: il secondo worker non
// può finalizzare l'upload creato dal primo
// =====================================================================

func TestFinalize_DoubleWorkerSameTask_SecondRejected(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J16", "AWAITING_ARTIFACT", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J16", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	cmd := beginUploadDefaultCmd("J16")
	payload := []byte("double-worker-payload")
	cmd.ExpectedSizeBytes = int64(len(payload))
	cmd.ExpectedSHA256 = sha256Hex(payload)
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.NoError(t, err)

	// A different worker tries to finalize the same upload session.
	_, err = env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
		UploadID: sess.UploadID, JobID: "J16", WorkerID: "worker-test-2",
		LeaseID: "lease-test-2", AttemptNumber: 1, ExpectedRevision: testRevision,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrWrongJobOwner), "got %v want ErrWrongJobOwner", err)

	// The rejected worker must not cross any finalization boundary:
	// the upload remains RECEIVED, the artifact remains STAGING, the
	// job remains AWAITING_ARTIFACT, and the canonical attempt keeps
	// its original worker/lease identity.
	var uploadStatus, artifactStatus, jobStatus, attemptWorker, attemptLease, attemptStatus, taskStatus string
	var taskRevision int
	require.NoError(t, env.db.QueryRow(`SELECT status FROM artifact_uploads WHERE upload_id = ?`, sess.UploadID).Scan(&uploadStatus))
	require.NoError(t, env.db.QueryRow(`SELECT status FROM artifacts WHERE id = ?`, sess.ArtifactID).Scan(&artifactStatus))
	require.NoError(t, env.db.QueryRow(`SELECT status FROM jobs WHERE job_id = 'J16'`).Scan(&jobStatus))
	require.NoError(t, env.db.QueryRow(`SELECT worker_id, lease_id, status FROM task_attempts WHERE id = 'J16-attempt'`).Scan(&attemptWorker, &attemptLease, &attemptStatus))
	require.NoError(t, env.db.QueryRow(`SELECT status, revision FROM tasks WHERE task_id = 'J16-task'`).Scan(&taskStatus, &taskRevision))
	require.Equal(t, "RECEIVED", uploadStatus)
	require.Equal(t, "STAGING", artifactStatus)
	require.Equal(t, "AWAITING_ARTIFACT", jobStatus)
	require.Equal(t, testWorkerID, attemptWorker)
	require.Equal(t, testLeaseID, attemptLease)
	require.Equal(t, "RENDER_FINISHED", attemptStatus)
	require.Equal(t, "RUNNING", taskStatus)
	require.Equal(t, 0, taskRevision)

	// The rejected path must not promote the received bytes to the
	// durable final directory. The legitimate worker performs the
	// first promotion below, so this check is intentionally before it.
	var durableFiles []string
	walkErr := filepath.WalkDir(env.bs.FinalDir(), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			durableFiles = append(durableFiles, path)
		}
		return nil
	})
	require.NoError(t, walkErr)
	require.Empty(t, durableFiles, "rejected finalization must not create a durable blob")

	// The legitimate worker can still finalize.
	art, err := env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
		UploadID: sess.UploadID, JobID: "J16", WorkerID: testWorkerID,
		LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
	})
	require.NoError(t, err)
	require.Equal(t, "READY", art.Status)
}
