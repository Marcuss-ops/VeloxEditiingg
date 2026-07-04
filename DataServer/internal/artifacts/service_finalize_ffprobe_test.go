// Package artifacts / service_finalize_ffprobe_test.go
//
// Minimal scaffold test for the post-finalize ffprobe invariant
// (see service_finalize_ffprobe.go for the gate semantics). Gated
// tests skip cleanly when ffmpeg is absent from PATH (ffmpeg is
// required to synthesize the audio fixture; ffprobe is what the
// gate itself shells out to).
//
// Happy path: synthesize a 1-audio-stream mp4 with ffmpeg, end-to-
// end through Service.Finalize with the env gate enabled, and
// assert the gate passes (audio_count == delivered_deliveries
// count == 1).
//
// Skipping behavior follows the bootstrap_test.go pattern at
// RemoteCodex/.../pkg/bootstrap/bootstrap_test.go so minimal CI
// hosts without the media toolchain stay green.
//
// Mismatch path (3 deliveries + 1 audio stream → sentinel): needs
// a hand-coded multi-stream mp4 fixture, deferred to a follow-up.

package artifacts

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// requireFFMPEGTools skips the test if either ffmpeg or ffprobe is
// absent. ffmpeg synthesizes the fixture; ffprobe is what the gate
// itself shells out to.
func requireFFMPEGTools(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skipf("ffmpeg not available on PATH: %v", err)
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skipf("ffprobe not available on PATH: %v", err)
	}
}

// synthesizeSoloAudioMP4 shells out to ffmpeg to produce a minimal
// 1-second, 1-audio-stream mp4 at `out`. anullsrc (silent audio
// source) requires no external input so the test is hermetic. The
// `-c:a aac -f mp4` flags force .mp4 output, so any caller computing
// the canonical key via FinalStorageKey(sha256Hex(payload),
// mimeToExt(detectMIME(path))) is stable across ffmpeg version
// skew — no extension-sniff logic is needed.
func synthesizeSoloAudioMP4(t *testing.T, out string) {
	t.Helper()
	cmd := exec.Command("ffmpeg",
		"-y",
		"-f", "lavfi",
		"-i", "anullsrc=r=8000:cl=mono",
		"-t", "1",
		"-c:a", "aac",
		"-f", "mp4",
		out,
	)
	var errBuf bytes.Buffer
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		t.Fatalf("ffmpeg synth failed for %s: %v\nstderr=%s", out, err, errBuf.String())
	}
	st, statErr := os.Stat(out)
	require.NoError(t, statErr, "synthesized mp4 missing")
	require.Greater(t, st.Size(), int64(0), "synthesized mp4 empty")
}

// TestFinalize_FFProbeInvariant_HappyPath verifies the gate runs
// the ffprobe shell-out and accepts a 1-stream mp4 / 1-delivery pair
// when VELOX_FFPROBE_VERIFY_ON_FINALIZE=true is set.
func TestFinalize_FFProbeInvariant_HappyPath(t *testing.T) {
	requireFFMPEGTools(t)

	env := setupTestEnv(t)
	env.seedJob("JFP", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("JFP", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	// Synthesize a 1-second, 1-audio-stream mp4 and write it through
	// the BeginUpload/Receive path so the artifact row's storage_key
	// derives from the actual bytes (not a hard-coded hash).
	mp4Path := filepath.Join(env.tmpDir, "fp_fixture.mp4")
	synthesizeSoloAudioMP4(t, mp4Path)
	payload, err := os.ReadFile(mp4Path)
	require.NoError(t, err, "read synthesized mp4")

	cmd := beginUploadDefaultCmd("JFP")
	cmd.ExpectedSizeBytes = int64(len(payload))
	cmd.ExpectedSHA256 = sha256Hex(payload)
	cmd.MimeType = "video/mp4"
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.NoError(t, err)

	// Enable the env gate scoped to this test via t.Setenv
	// (auto-restored on Cleanup; matches bootstrap_test.go convention).
	t.Setenv("VELOX_FFPROBE_VERIFY_ON_FINALIZE", "true")

	art, err := env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
		UploadID: sess.UploadID, JobID: "JFP", WorkerID: testWorkerID,
		LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
	})
	require.NoError(t, err, "ffprobe gate should pass on 1-stream mp4 / 1-delivery pair")
	require.Equal(t, "READY", art.Status)
}

// TestFinalize_FFProbeInvariant_Mismatch verifies the pre-commit gate
// surfaces ErrFFProbeAudioCountMismatch AND keeps artifacts.status out
// of READY when the audio stream count ≠ the per-job plan's enabled
// destinations count.
//
// Setup is the canonical Jackie-Chan-style regression reproduction:
//   - 3 enabled rows in job_delivery_plans for the test job (the
//     resolveDeliveryDestinationsTx branch 1 would stamp 3
//     job_deliveries).
//   - mp4 fixture with exactly 1 audio stream (the C++ engine's
//     amix-collapse regression).
//
// Pre-commit semantics: the gate fires AFTER blob promote and BEFORE
// the CAS RECEIVED→FINALIZING. On mismatch the orchestrator returns
// the sentinel and the finalize tx never runs, so every invariant
// the writer would have stamped also stays at its pre-finalize state:
//   - artifacts.status remains STAGING (not READY).
//   - jobs.status remains RUNNING (not SUCCEEDED).
//   - 0 job_deliveries rows inserted for this artifact.
//   - The artifact blob exists on disk but is an orphan — the 24h
//     Reconciler sweep ("blob finale senza riga DB dopo 24h →
//     elimina") is the existing recovery path for that.
func TestFinalize_FFProbeInvariant_Mismatch(t *testing.T) {
	requireFFMPEGTools(t)

	env := setupTestEnv(t)

	// Distinct job ID from the happy-path test so we don't leak
	// fixture rows / artifacts. Job states seeded for the canonical
	// RUNNING → RENDER_FINISHED pipeline; the writer's CAS gates
	// require them.
	const jobID = "JMIS"
	env.seedJob(jobID, "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt(jobID, 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	// Seed 3 enabled rows in job_delivery_plans for this job —
	// mirrors what a per-job plan with 3 destinations would look
	// like in production. The gate's CountExpectedDeliveries
	// reads this table first; fallback to delivery_destinations
	// only kicks in when this is empty.
	now := env.clock.Now().UTC().Format(time.RFC3339)
	dests := []string{"mismatch-yt", "mismatch-drive", "mismatch-s3"}
	for i, did := range dests {
		// Seed destination FK target (some schemas enforce FK
		// between job_delivery_plans and delivery_destinations;
		// harmless if the constraint is permissive).
		_, err := env.db.Exec(`
			INSERT OR IGNORE INTO delivery_destinations
				(destination_id, provider, name, enabled, created_at, updated_at)
			VALUES (?, 'test', ?, 1, ?, ?)`,
			did, did, now, now,
		)
		require.NoError(t, err, "seed delivery_destinations %s", did)
		_, err = env.db.Exec(`
			INSERT INTO job_delivery_plans
				(job_id, destination_id, priority, retry_budget, enabled, created_at, updated_at)
			VALUES (?, ?, ?, 5, 1, ?, ?)`,
			jobID, did, i+1, now, now,
		)
		require.NoError(t, err, "seed job_delivery_plans %s", did)
	}

	// Synthesize a 1-audio-stream mp4 fixture via ffmpeg anullsrc
	// (silent mono aac — the canonical Jackie-style regression
	// produces a single track from a multi-voice amix collapse).
	mp4Path := filepath.Join(env.tmpDir, "fp_fixture_mismatch.mp4")
	synthesizeSoloAudioMP4(t, mp4Path)
	payload, err := os.ReadFile(mp4Path)
	require.NoError(t, err, "read synthesized mp4")

	cmd := beginUploadDefaultCmd(jobID)
	cmd.ExpectedSizeBytes = int64(len(payload))
	cmd.ExpectedSHA256 = sha256Hex(payload)
	cmd.MimeType = "video/mp4"
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.NoError(t, err)

	// Enable the env gate scoped to this test via t.Setenv
	// (auto-restored on Cleanup; matches bootstrap_test.go convention).
	t.Setenv("VELOX_FFPROBE_VERIFY_ON_FINALIZE", "true")

	// Finalize must surface ErrFFProbeAudioCountMismatch — the gate
	// sees expected=3 (per-job plan rows) and actual=1 (audio
	// streams in the mp4 container) and trips.
	_, err = env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
		UploadID: sess.UploadID, JobID: jobID, WorkerID: testWorkerID,
		LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
		// DestinationID: "" — use per-job plan path. Setting a
		// DestinationID would route through the writer's branch 1
		// (single destination) and bypass our 3-plan fixture.
	})
	require.Error(t, err, "ffprobe gate must fail on 3-plan / 1-stream mismatch")
	require.True(t, errors.Is(err, ErrFFProbeAudioCountMismatch),
		"want ErrFFProbeAudioCountMismatch, got %v", err)

	// Pre-commit invariants: the tx never committed, so every writer
	// step's durable side-effect must remain at its prior state.
	//
	// (a) artifacts.status must NOT be READY (the tripwire the
	//     user requested). Pre-finalize the row sits in STAGING.
	var artStatus string
	require.NoError(t, env.db.QueryRow(
		`SELECT status FROM artifacts WHERE id = ?`, sess.ArtifactID).Scan(&artStatus))
	require.NotEqual(t, "READY", artStatus, "artifact must NOT be READY on gate failure")
	require.Equal(t, "STAGING", artStatus, "artifact stays in pre-finalize state")

	// (b) jobs.status must NOT be SUCCEEDED. Pre-finalize RUNNING.
	var jobStatus string
	require.NoError(t, env.db.QueryRow(
		`SELECT status FROM jobs WHERE job_id = ?`, jobID).Scan(&jobStatus))
	require.NotEqual(t, "SUCCEEDED", jobStatus, "jobs must NOT be SUCCEEDED on gate failure")
	require.Equal(t, "RUNNING", jobStatus, "jobs stays in pre-finalize state")

	// (c) 0 job_deliveries stamped for this artifact (the writer's
	//     Step 5 INSERT would have produced 3 rows here; the pre-
	//     commit gate ensures none of them reached the disk).
	var delCount int
	require.NoError(t, env.db.QueryRow(
		`SELECT COUNT(*) FROM job_deliveries WHERE artifact_id = ?`, sess.ArtifactID).Scan(&delCount))
	require.Equal(t, 0, delCount, "no delivery rows stamped on gate failure")

	// (d) The artifact blob DOES exist on disk (promoted before the
	//     gate ran) — this is the existing orphan-blob pattern the
	//     24h Reconciler sweep is responsible for. Compute the
	//     canonical key the same way PromoteToCanonical + the master
	//     does (per TestFinalize_BlobPromotedButDBCASMissed in
	//     service_test.go) so the assertion doesn't depend on
	//     arbitrary-dirspec tree walks that break when the
	//     storage-key schema changes.
	detectedExt := mimeToExt(detectMIME(sess.TemporaryStorageKey))
	_, absFinal, err := FinalStorageKey(env.bs, sha256Hex(payload), detectedExt)
	require.NoError(t, err, "FinalStorageKey derivation")
	_, err = os.Stat(absFinal)
	require.NoError(t, err,
		"orphan blob at %s must exist after pre-commit gate mismatch (Reconciler 24h sweep will reclaim it)",
		absFinal)
}
