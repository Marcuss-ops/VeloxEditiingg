// Package artifacts / service_test.go
//
// PR 2 chunk 6: integration tests for the artifacts.Service trust boundary.
//
// Each test owns a file-backed SQLite DB in t.TempDir() with all 029
// migrations applied (via migrations.RunMigrations against the embedded
// store/migrations/*.sql), plus a LocalBlobStore rooted in temp
// subdirs. Per-test isolation; no shared state.

package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	"velox-server/internal/store"
	"velox-server/internal/store/migrations"
)

// =====================================================================
// FIXTURES
// =====================================================================

const (
	testWorkerID = "worker-test-1"
	testLeaseID   = "lease-test-1"
	testRevision  = 5
)

type testEnv struct {
	t      *testing.T
	db     *sql.DB
	bs     store.BlobStore
	svc    *Service
	repo   Repository
	fin    FinalizationRepository
	clock  *manualClock
	tmpDir string
}

// manualClock satisfies Clock for tests; advance via Advance().
type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func newManualClock() *manualClock {
	return &manualClock{t: time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)}
}
func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()
	tmp := t.TempDir()

	dbPath := filepath.Join(tmp, "test.db")
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	require.NoError(t, err, "open sqlite")

	for _, p := range []string{
		"PRAGMA synchronous = NORMAL",
		"PRAGMA foreign_keys = ON",
	} {
		_, err := db.Exec(p)
		require.NoError(t, err, "pragma "+p)
	}
	db.SetMaxOpenConns(4)

	err = migrations.RunMigrations(db, migrations.MigrationsFS, ".")
	require.NoError(t, err, "migrations.RunMigrations")

	// Seed a delivery destination for FinalizeVerified tests.
	_, _ = db.Exec(`INSERT OR IGNORE INTO delivery_destinations (destination_id, provider, name, enabled, created_at, updated_at) VALUES ('primary', 'test', 'Test', 1, '', '')`)

	staging := filepath.Join(tmp, "staging")
	final := filepath.Join(tmp, "final")
	bs, err := store.NewLocalBlobStore(staging, final)
	require.NoError(t, err, "NewLocalBlobStore")

	clk := newManualClock()
	repo := NewSQLiteRepository(db)
	// PR 3.5-a: FinalizationRepository is REQUIRED by NewService. Sharing
	// the same *sql.DB ensures the finalization tx can join with the
	// concurrent update on artifact_uploads (FinalizeVerified step 7).
	fin := NewSQLiteFinalizationRepository(db)
	svc := NewService(repo, fin, bs, db, clk)

	t.Cleanup(func() {
		_ = db.Close()
	})
	return &testEnv{
		t: t, db: db, bs: bs, svc: svc, repo: repo, fin: fin, clock: clk, tmpDir: tmp,
	}
}

// seedJob inserts a jobs row with the columns Service.loadJob queries.
// Other job columns get their DEFAULT values from the migration. We use
// a minimal schema-defensive INSERT so future migrations that add
// additional columns don't break this test.
func (e *testEnv) seedJob(jobID, status, assignedTo, leaseID string, revision int, leaseExpiry time.Time) {
	e.t.Helper()
	now := e.clock.Now().UTC().Format(time.RFC3339)
	_, err := e.db.Exec(`
		INSERT INTO jobs (
			job_id, status, assigned_to, lease_id, lease_expiry, revision,
			created_at, updated_at, raw_json, migrated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}', ?)`,
		jobID, status, assignedTo, leaseID,
		nullableTime(leaseExpiry), revision, now, now, now,
	)
	require.NoError(e.t, err, "seedJob "+jobID)
}

func (e *testEnv) seedAttempt(jobID string, attemptNumber int, status, workerID, leaseID string) {
	e.t.Helper()
	now := e.clock.Now().UTC().Format(time.RFC3339)
	_, err := e.db.Exec(`
		INSERT INTO job_attempts (
			job_id, attempt_number, worker_id, lease_id, status,
			started_at, error_code, engine_version, bundle_hash, created_at
		) VALUES (?, ?, ?, ?, ?, ?, '', '', '', ?)`,
		jobID, attemptNumber, workerID, leaseID, status, now, now,
	)
	require.NoError(e.t, err, "seedAttempt "+jobID)
}

// markPreExistingReady inserts a READY artifact row of the given kind
// for the given job. Used to test the "no existing READY of same kind"
// gate in BeginUpload.
func (e *testEnv) markPreExistingReady(jobID, kind, artifactID string) {
	e.t.Helper()
	now := e.clock.Now().UTC().Format(time.RFC3339)
	_, err := e.db.Exec(`
		INSERT INTO artifacts (
			id, job_id, attempt_id, type, storage_provider, storage_key,
			sha256, size_bytes, status, created_at
		) VALUES (?, ?, 1, ?, 'local', ?, ?, 0, 'READY', ?)`,
		artifactID, jobID, kind, "artifacts/sha256/aa/"+artifactID+".mp4",
		strings.Repeat("a", 64), now,
	)
	require.NoError(e.t, err)
}

func nullableTime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}

func beginUploadDefaultCmd(jobID string) BeginUploadCommand {
	return BeginUploadCommand{
		JobID:            jobID,
		WorkerID:         testWorkerID,
		LeaseID:          testLeaseID,
		AttemptNumber:    1,
		ExpectedRevision: testRevision,
		Kind:             "video",
		MimeType:         "video/mp4",
	}
}

// uploadBytes returns a reader over data; Receive uses io.Copy internally.
// uploadBytes takes only data (the uploadID is no longer needed at this
// layer: the service resolves the upload session by uploadID from the
// command argument, not from the byte stream).
func uploadBytes(data []byte) io.Reader {
	return bytes.NewReader(data)
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// =====================================================================
// #region 1 — wrong worker (BeginUpload)
// =====================================================================

func TestBeginUpload_WrongWorker(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J1", "RUNNING", "worker-other", testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J1", 1, "RENDER_FINISHED", "worker-other", testLeaseID)

	cmd := beginUploadDefaultCmd("J1")
	_, err := env.svc.BeginUpload(context.Background(), cmd)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrWrongJobOwner), "got %v want ErrWrongJobOwner", err)
}

// =====================================================================
// #region 2 — wrong lease (BeginUpload)
// =====================================================================

func TestBeginUpload_WrongLease(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J2", "RUNNING", testWorkerID, "lease-other", testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J2", 1, "RENDER_FINISHED", testWorkerID, "lease-other")

	cmd := beginUploadDefaultCmd("J2")
	_, err := env.svc.BeginUpload(context.Background(), cmd)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrLeaseInvalid), "got %v want ErrLeaseInvalid", err)
}

// =====================================================================
// #region 3 — wrong revision (BeginUpload)
// =====================================================================

func TestBeginUpload_WrongRevision(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J3", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J3", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	cmd := beginUploadDefaultCmd("J3")
	cmd.ExpectedRevision = testRevision + 99
	_, err := env.svc.BeginUpload(context.Background(), cmd)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrRevisionMismatch), "got %v want ErrRevisionMismatch", err)
}

// =====================================================================
// #region 4 — wrong attempt (BeginUpload)
// =====================================================================

func TestBeginUpload_WrongAttemptStatus(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J4", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J4", 1, "CREATING", testWorkerID, testLeaseID)

	cmd := beginUploadDefaultCmd("J4")
	_, err := env.svc.BeginUpload(context.Background(), cmd)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrAttemptNotRenderFinished), "got %v", err)
}

func TestBeginUpload_WrongAttemptWorker(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J4b", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J4b", 1, "RENDER_FINISHED", "worker-other", "lease-other")

	cmd := beginUploadDefaultCmd("J4b")
	_, err := env.svc.BeginUpload(context.Background(), cmd)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrWrongJobOwner), "got %v", err)
}

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
// #region 8 — doppia finalizzazione (idempotent path)
// =====================================================================

func TestFinalize_DoubleFinalizeIdempotent(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J8", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
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

	// Outbox: exactly one ARTIFACT_READY for this artifact.
	var n int
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM outbox_events WHERE event_type='ARTIFACT_READY' AND aggregate_id=?`, first.ID).Scan(&n))
	require.Equal(t, 1, n)

	// Delivery: exactly one row for (artifact, primary).
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id=? AND destination_id='primary'`, first.ID).Scan(&n))
	require.Equal(t, 1, n)
}

// =====================================================================
// #region 9 — due finalizzazioni concorrenti (channel-coordinated race)
// =====================================================================

func TestFinalize_Concurrent(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J9", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
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
	// What MUST hold: job is SUCCEEDED exactly once; outbox has exactly
	// one ARTIFACT_READY; delivery has exactly one row; no artifact is
	// double-finalized.
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
	require.Equal(t, "SUCCEEDED", jobStatus)

	var n int
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM outbox_events WHERE event_type='ARTIFACT_READY'`).Scan(&n))
	require.Equal(t, 1, n, "exactly one ARTIFACT_READY")

	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id=? AND destination_id='primary'`, sess.ArtifactID).Scan(&n))
	require.Equal(t, 1, n, "exactly one delivery row")
}

// =====================================================================
// #region 10 — blob promosso + DB rollback
// =====================================================================

func TestFinalize_BlobPromotedButDBCASMissed(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J10", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J10", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	cmd := beginUploadDefaultCmd("J10")
	payload := []byte("rollback-payload")
	cmd.ExpectedSizeBytes = int64(len(payload))
	cmd.ExpectedSHA256 = sha256Hex(payload)
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.NoError(t, err)

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
	_, absFinal, err := FinalStorageKey(env.bs, sha256Hex(payload), ".bin")
	require.NoError(t, err)
	_, err = os.Stat(absFinal)
	require.NoError(t, err, "orphan blob at %s must exist", absFinal)
}

// =====================================================================
// #region 11 — job non RUNNING
// =====================================================================

func TestBeginUpload_JobNotRunning(t *testing.T) {
	for _, status := range []string{"QUEUED", "PENDING", "SUCCEEDED", "FAILED", "CANCELED"} {
		t.Run(status, func(t *testing.T) {
			env := setupTestEnv(t)
			env.seedJob("J"+status, status, testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
			env.seedAttempt("J"+status, 1, "RENDER_FINISHED", testWorkerID, testLeaseID)
			_, err := env.svc.BeginUpload(context.Background(), beginUploadDefaultCmd("J"+status))
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrJobNotRunning), "status=%s got %v", status, err)
		})
	}
}

// =====================================================================
// #region 12 — attempt not RENDER_FINISHED (BeginUpload)
// Covered by TestBeginUpload_WrongAttemptStatus.
// =====================================================================

// =====================================================================
// #region 13 — delivery creata una volta
// =====================================================================

func TestFinalize_DeliveryIsIdempotent(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J13", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
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

	// Manually re-run the delivery INSERT to verify WHERE NOT EXISTS
	// makes it a no-op.
	now := env.clock.Now().UTC().Format(time.RFC3339)
	_, err = env.db.Exec(`INSERT INTO job_deliveries (artifact_id, destination_id, payload, status, created_at)
		SELECT ?, 'primary', '{}', 'PENDING', ?
		WHERE NOT EXISTS (SELECT 1 FROM job_deliveries WHERE artifact_id=? AND destination_id='primary')`,
		sess.ArtifactID, now, sess.ArtifactID)
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
	env.seedJob("J15", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
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
// #region 16 — no existing READY artifact of same kind (BeginUpload gate)
// =====================================================================

func TestBeginUpload_NoExistingReadyArtifactOfSameKind(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J16", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J16", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)
	env.markPreExistingReady("J16", "video", "ART-EXISTING")

	_, err := env.svc.BeginUpload(context.Background(), beginUploadDefaultCmd("J16"))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrDuplicateReadyArtifact), "got %v", err)
}

// =====================================================================
// EXTRA — empty input validation
// =====================================================================

func TestBeginUpload_EmptyInputs(t *testing.T) {
	env := setupTestEnv(t)
	cases := []struct {
		name string
		cmd  BeginUploadCommand
	}{
		{"missing_job", BeginUploadCommand{WorkerID: testWorkerID, LeaseID: testLeaseID, AttemptNumber: 1}},
		{"missing_worker", BeginUploadCommand{JobID: "JX", LeaseID: testLeaseID, AttemptNumber: 1}},
		{"missing_lease", BeginUploadCommand{JobID: "JX", WorkerID: testWorkerID, AttemptNumber: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := env.svc.BeginUpload(context.Background(), tc.cmd)
			require.Error(t, err)
			require.False(t, errors.Is(err, ErrTransitionConflict))
		})
	}
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
	require.NoError(t, env.repo.UpdateUploadStatus(context.Background(), sess.UploadID, UploadFields{Status: ptrString("FAILED")}))

	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes([]byte("resurrect")))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrUploadStateInvalid))
}
