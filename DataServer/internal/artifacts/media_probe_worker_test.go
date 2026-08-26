package artifacts

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"velox-server/internal/artifactsstore"
)

func TestFinalizeWithMediaProbeQueueDoesNotWaitForSlowProbe(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J-probe-slow", "AWAITING_ARTIFACT", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J-probe-slow", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	payload := []byte("probe queue payload")
	cmd := beginUploadDefaultCmd("J-probe-slow")
	cmd.ExpectedSizeBytes = int64(len(payload))
	cmd.ExpectedSHA256 = sha256Hex(payload)
	session, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(context.Background(), session.UploadID, uploadBytes(payload))
	require.NoError(t, err)

	probeRepo := artifactsstore.NewSQLiteMediaProbeRepository(env.db)
	env.svc.WithMediaProbeQueue(probeRepo)

	started := time.Now()
	artifact, err := env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
		UploadID: session.UploadID, JobID: "J-probe-slow", WorkerID: testWorkerID,
		LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
	})
	require.NoError(t, err)
	require.Less(t, time.Since(started), 500*time.Millisecond, "Finalize must not wait for ffprobe")
	require.Equal(t, string(ArtifactVerifying), artifact.Status)

	var status string
	require.NoError(t, env.db.QueryRow(`SELECT status FROM artifacts WHERE id=?`, session.ArtifactID).Scan(&status))
	require.Equal(t, string(ArtifactVerifying), status)
	var probes, deliveries int
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM media_probe_jobs WHERE artifact_id=?`, session.ArtifactID).Scan(&probes))
	require.Equal(t, 1, probes)
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id=?`, session.ArtifactID).Scan(&deliveries))
	require.Equal(t, 0, deliveries, "publishing is gated until the asynchronous probe succeeds")
}

func TestMediaProbeEnqueueDeduplicatesByArtifactAndSHA(t *testing.T) {
	env := setupTestEnv(t)
	repo := artifactsstore.NewSQLiteMediaProbeRepository(env.db)
	params := artifactsstore.MediaProbeEnqueueParams{
		ArtifactID: "artifact-dedupe", SHA256: "sha-dedupe", StorageKey: "artifacts/sha-dedupe.mp4",
		ExpectedAudioStreams: 1, Now: env.clock.Now(),
	}
	require.NoError(t, repo.EnqueueMediaProbe(context.Background(), params))
	require.NoError(t, repo.EnqueueMediaProbe(context.Background(), params))

	var count int
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM media_probe_jobs WHERE artifact_id=? AND sha256=?`, params.ArtifactID, params.SHA256).Scan(&count))
	require.Equal(t, 1, count)
}

func TestMediaProbeWorkerUsesBoundedConcurrencyAndPublishesAfterProbe(t *testing.T) {
	env := setupTestEnv(t)
	repo := artifactsstore.NewSQLiteMediaProbeRepository(env.db)
	for i := 0; i < 4; i++ {
		jobID := "J-probe-pool-" + string(rune('0'+i))
		env.seedJob(jobID, "AWAITING_ARTIFACT", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
		env.seedAttempt(jobID, 1, "RENDER_FINISHED", testWorkerID, testLeaseID)
		artifactID := "artifact-probe-pool-" + string(rune('0'+i))
		_, err := env.db.Exec(`INSERT INTO artifacts (id,job_id,type,storage_provider,storage_key,sha256,size_bytes,status,created_at) VALUES (?,?, 'video','local',?,?,1,'VERIFYING',?)`, artifactID, jobID, "artifacts/"+artifactID, "sha-"+artifactID, env.clock.Now().UTC().Format(time.RFC3339))
		require.NoError(t, err)
		require.NoError(t, repo.EnqueueMediaProbe(context.Background(), artifactsstore.MediaProbeEnqueueParams{ArtifactID: artifactID, SHA256: "sha-" + artifactID, StorageKey: "artifacts/" + artifactID, ExpectedAudioStreams: 1, Now: env.clock.Now()}))
	}

	var active, maxActive atomic.Int32
	probe := func(ctx context.Context, _ string) (int, int64, error) {
		current := active.Add(1)
		for {
			old := maxActive.Load()
			if current <= old || maxActive.CompareAndSwap(old, current) {
				break
			}
		}
		select {
		case <-time.After(40 * time.Millisecond):
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		}
		active.Add(-1)
		return 1, 1250, nil
	}
	worker := NewMediaProbeWorker(repo, env.bs.FinalDir(), 2, probe).WithPollInterval(time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	for {
		var ready int
		require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM artifacts WHERE status='READY' AND id LIKE 'artifact-probe-pool-%'`).Scan(&ready))
		if ready == 4 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("probe pool did not complete all jobs")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.LessOrEqual(t, maxActive.Load(), int32(2))
}

func TestMediaProbeRetryIsIsolatedPerJob(t *testing.T) {
	env := setupTestEnv(t)
	repo := artifactsstore.NewSQLiteMediaProbeRepository(env.db)
	for _, id := range []string{"retry-a", "retry-b"} {
		require.NoError(t, repo.EnqueueMediaProbe(context.Background(), artifactsstore.MediaProbeEnqueueParams{ArtifactID: id, SHA256: id + "-sha", StorageKey: "artifacts/" + id, Now: env.clock.Now(), MaxAttempts: 2}))
	}
	first, err := repo.ClaimMediaProbe(context.Background(), "test-owner", time.Minute, env.clock.Now())
	require.NoError(t, err)
	require.NotNil(t, first)
	require.NoError(t, repo.FailMediaProbe(context.Background(), *first, errors.New("temporary ffprobe failure"), env.clock.Now()))

	second, err := repo.ClaimMediaProbe(context.Background(), "test-owner", time.Minute, env.clock.Now())
	require.NoError(t, err)
	require.NotNil(t, second)
	require.NotEqual(t, first.ArtifactID, second.ArtifactID, "one failed probe must not block another job")

	var failedStatus, otherStatus string
	require.NoError(t, env.db.QueryRow(`SELECT status FROM media_probe_jobs WHERE artifact_id=?`, first.ArtifactID).Scan(&failedStatus))
	require.NoError(t, env.db.QueryRow(`SELECT status FROM media_probe_jobs WHERE artifact_id=?`, second.ArtifactID).Scan(&otherStatus))
	require.Equal(t, "PENDING", failedStatus)
	require.Equal(t, "RUNNING", otherStatus)
}

var _ = sync.Once{}
