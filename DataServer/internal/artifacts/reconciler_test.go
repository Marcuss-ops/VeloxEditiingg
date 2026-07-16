package artifacts

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	"velox-server/internal/store"
)

// =====================================================================
// Failure-test suite for the artifacts Reconciler.
//
// These tests exercise the canonical failure modes the spec calls out
// as P0 before production:
//   - worker dies during upload (upload session never completes)
//   - artifact marked READY but its blob is missing
//   - artifact stuck in STAGING for too long
// =====================================================================

func setupReconcilerEnv(t *testing.T, env *testEnv) *Reconciler {
	t.Helper()
	cfg := DefaultReconcilerConfig()
	cfg.OrphanBlobAge = 24 * time.Hour
	cfg.StuckArtifactAge = 24 * time.Hour
	cfg.QuarantineMinAge = 60 * time.Second
	cfg.BatchLimit = 200

	rec, err := NewReconciler(env.db, env.bs, env.repo, env.clock, cfg)
	require.NoError(t, err)
	return rec
}

// =====================================================================
// #region 1 — worker morto durante upload: sessione UPLOADING diventa EXPIRED
// =====================================================================

func TestReconciler_WorkerDiedDuringUpload_MarksExpired(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("JR1", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("JR1", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	cmd := beginUploadDefaultCmd("JR1")
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)

	// Simulate that the worker started streaming and then died.
	uploading := string(store.UploadUploading)
	require.NoError(t, env.repo.UpdateUploadStatus(context.Background(), sess.UploadID, store.UploadFields{Status: &uploading}))

	// Advance time past the 24h upload TTL.
	env.clock.Advance(25 * time.Hour)

	rec := setupReconcilerEnv(t, env)
	stats, err := rec.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, stats.ExpiredUploads, "one expired upload session must be swept")

	fresh, err := env.repo.GetUploadSession(context.Background(), sess.UploadID)
	require.NoError(t, err)
	require.Equal(t, "EXPIRED", fresh.Status)

	// Staging temp file must be removed.
	_, statErr := os.Stat(sess.TemporaryStorageKey)
	require.True(t, os.IsNotExist(statErr), "staging temp file must be removed after expiry")
}

// =====================================================================
// #region 2 — artefatto READY senza blob: viene messo in QUARANTINED
// =====================================================================

func TestReconciler_ReadyArtifactMissingBlob_Quarantines(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("JQR1", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("JQR1", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	artifactID := "art-missing-blob-001"
	storageKey := "artifacts/sha256/ab/abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890.mp4"

	now := env.clock.Now().UTC().Format(time.RFC3339)
	// verified_at old enough to pass the QuarantineMinAge guard.
	verifiedAt := env.clock.Now().UTC().Add(-2 * time.Minute).Format(time.RFC3339)
	_, err := env.db.Exec(`
		INSERT INTO artifacts (
			id, job_id, attempt_id, type, storage_provider, storage_key,
			sha256, size_bytes, status, created_at, verified_at
		) VALUES (?, 'JQR1', 1, 'video', 'local', ?, ?, 0, 'READY', ?, ?)`,
		artifactID, storageKey, strings.Repeat("a", 64), now, verifiedAt,
	)
	require.NoError(t, err)

	// The failure condition must be explicit: the referenced blob is absent.
	require.NoFileExists(t, filepath.Join(env.bs.FinalDir(), filepath.FromSlash(storageKey)))

	rec := setupReconcilerEnv(t, env)
	stats, err := rec.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, stats.QuarantinedWithEvent, "one READY-without-blob artifact must be quarantined")

	var status string
	require.NoError(t, env.db.QueryRow(`SELECT status FROM artifacts WHERE id = ?`, artifactID).Scan(&status))
	require.Equal(t, "QUARANTINED", status)

	var outboxCount int
	require.NoError(t, env.db.QueryRow(`
		SELECT COUNT(*) FROM outbox_events
		WHERE aggregate_type = 'artifact' AND aggregate_id = ? AND event_type = 'ARTIFACT_QUARANTINED'`,
		artifactID).Scan(&outboxCount))
	require.Equal(t, 1, outboxCount)

	// The outbox event payload should reference the missing blob path.
	var payload string
	require.NoError(t, env.db.QueryRow(`
		SELECT payload_json FROM outbox_events
		WHERE aggregate_type = 'artifact' AND aggregate_id = ? AND event_type = 'ARTIFACT_QUARANTINED'`,
		artifactID).Scan(&payload))
	require.Contains(t, payload, artifactID)
	require.Contains(t, payload, "blob_missing_on_disk")
}

// =====================================================================
// #region 3 — artifact STAGING troppo vecchio: viene marcato FAILED
// =====================================================================

func TestReconciler_StagingArtifactTooOld_MarksFailed(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("JST1", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("JST1", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	artifactID := "art-staging-old-001"
	createdAt := env.clock.Now().UTC().Add(-25 * time.Hour).Format(time.RFC3339)
	_, err := env.db.Exec(`
		INSERT INTO artifacts (
			id, job_id, attempt_id, type, storage_provider, storage_key,
			sha256, size_bytes, status, created_at
		) VALUES (?, 'JST1', 1, 'video', 'local', '', ?, 0, 'STAGING', ?)`,
		artifactID, strings.Repeat("b", 64), createdAt,
	)
	require.NoError(t, err)

	rec := setupReconcilerEnv(t, env)
	stats, err := rec.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, stats.StuckArtifacts, "one stuck STAGING artifact must be marked FAILED")

	var status string
	require.NoError(t, env.db.QueryRow(`SELECT status FROM artifacts WHERE id = ?`, artifactID).Scan(&status))
	require.Equal(t, "FAILED", status)
}

// =====================================================================
// #region 4 — blob finale orfano senza riga DB: viene rimosso
// =====================================================================

func TestReconciler_OrphanFinalBlob_Removed(t *testing.T) {
	env := setupTestEnv(t)

	// Create an old orphan blob in final dir with no matching artifacts row.
	orphanPath := filepath.Join(env.bs.FinalDir(), "artifacts", "sha256", "aa", "orphan.bin")
	require.NoError(t, os.MkdirAll(filepath.Dir(orphanPath), 0o755))
	require.NoError(t, os.WriteFile(orphanPath, []byte("orphan"), 0o644))

	// Back-date the file modification time so it passes the OrphanBlobAge guard.
	oldTime := env.clock.Now().Add(-25 * time.Hour)
	require.NoError(t, os.Chtimes(orphanPath, oldTime, oldTime))

	rec := setupReconcilerEnv(t, env)
	stats, err := rec.Reconcile(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, stats.OrphanFinalBlobs, "one orphan final blob must be removed")

	_, statErr := os.Stat(orphanPath)
	require.True(t, os.IsNotExist(statErr), "orphan final blob must be deleted")
}
