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
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

// stripFFProbeFromPath mutates PATH so exec.LookPath("ffprobe")
// fails no matter which directory the binary is on. Used by tests
// that exercise the binary-missing branch.
//
// We walk every PATH directory and strip any that file-stats to
// contain an `ffprobe` executable — relying on exec.LookPath would
// only return the FIRST match, so on hosts with ffmpeg installed
// in multiple directories (e.g. both /usr/bin and /usr/local/bin
// symlinked to ffmpeg's bin) just removing one directory leaves
// the gate's exec.LookPath lookup a valid target.
//
// ffmpeg is preserved (not stripped) so the synthesizeSoloAudioMP4
// helper can run before this is called; the fixture mp4 stays on
// disk afterward so the gate has something to stat(). Auto-
// restored on test end via t.Setenv.
func stripFFProbeFromPath(t *testing.T) {
	t.Helper()
	cur := os.Getenv("PATH")
	parts := strings.Split(cur, string(os.PathListSeparator))
	filtered := make([]string, 0, len(parts))
	stripped := 0
	for _, dir := range parts {
		if dir == "" {
			continue
		}
		candidate := filepath.Join(dir, "ffprobe")
		if _, err := os.Stat(candidate); err == nil {
			// dir contains an ffprobe binary — strip it.
			stripped++
			continue
		}
		filtered = append(filtered, dir)
	}
	require.GreaterOrEqual(t, stripped, 1, "no ffprobe on PATH at test start; the missing-binary scenario cannot be exercised")
	t.Setenv("PATH", strings.Join(filtered, string(os.PathListSeparator)))
}

// captureTripwireLogs redirects the stdlib logger to a buffer
// while the closure runs, then restores os.Stderr. The closure
// gets the captured buffer plus a require-failure-fast exit on
// tripwire-log assertion failure. Pinned at T.Cleanup.
func captureTripwireLogs(t *testing.T, fn func(buf *bytes.Buffer)) {
	t.Helper()
	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(orig) })
	fn(&buf)
}

// =====================================================================
// Shadow-mode tests (Stage 1 production rollout — RW-PROD-008 A4).
// Pinned: shadow mode MUST log + return nil (no abort). Operators
// ftw + flip Stage 1->Stage 2 (shadow->enforce) after 24h of clean
// signal.
// =====================================================================

// TestFinalize_FFProbeInvariant_ShadowMatch verifies the shadow
// mode happy path: env="shadow", 1-stream mp4 / 1-delivery pair
// is a match. The gate logs a match event for Stage 1
// observability and returns nil (no abort, status=READY).
func TestFinalize_FFProbeInvariant_ShadowMatch(t *testing.T) {
	requireFFMPEGTools(t)

	env := setupTestEnv(t)
	env.seedJob("JSMT", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("JSMT", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	mp4Path := filepath.Join(env.tmpDir, "shadow_match.mp4")
	synthesizeSoloAudioMP4(t, mp4Path)
	payload, err := os.ReadFile(mp4Path)
	require.NoError(t, err)

	cmd := beginUploadDefaultCmd("JSMT")
	cmd.ExpectedSizeBytes = int64(len(payload))
	cmd.ExpectedSHA256 = sha256Hex(payload)
	cmd.MimeType = "video/mp4"
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.NoError(t, err)

	t.Setenv("VELOX_FFPROBE_VERIFY_ON_FINALIZE", "shadow")

	var logged string
	captureTripwireLogs(t, func(buf *bytes.Buffer) {
		art, lerr := env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
			UploadID: sess.UploadID, JobID: "JSMT", WorkerID: testWorkerID,
			LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
		})
		require.NoError(t, lerr, "shadow mode MUST NOT abort on match")
		require.Equal(t, "READY", art.Status, "shadow mode lets the writer commit on match")
		logged = buf.String()
	})
	require.Contains(t, logged, "event=ffprobe_invariant_match", "shadow mode logs every match for Stage 1 visibility")
	require.Contains(t, logged, "mode=shadow", "log line must carry the mode for the runbook grep")
	require.Contains(t, logged, "job_id=JSMT", "log line must carry the job_id for triage correlation")
}

// TestFinalize_FFProbeInvariant_ShadowMismatch verifies the
// shadow mode on the canonical Jackie-chan regression fixture:
// 3 enabled delivery destinations but 1 audio stream in the mp4
// (amix-collapse). The gate logs a mismatch event and returns
// nil (no abort) — the writer commits, status=READY, the
// regression is loud in logs (visible to operators) but does NOT
// take down production traffic during Stage 1.
func TestFinalize_FFProbeInvariant_ShadowMismatch(t *testing.T) {
	requireFFMPEGTools(t)

	env := setupTestEnv(t)
	const jobID = "JSMM"
	env.seedJob(jobID, "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt(jobID, 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	// 3 enabled delivery destinations (the canonical regression
	// fixture: a 6-voice payload collapsed by the C++ engine).
	now := env.clock.Now().UTC().Format(time.RFC3339)
	dests := []string{"shadow-mismatch-yt", "shadow-mismatch-drive", "shadow-mismatch-s3"}
	for i, did := range dests {
		_, err := env.db.Exec(`
			INSERT OR IGNORE INTO delivery_destinations
				(destination_id, provider, name, enabled, created_at, updated_at)
			VALUES (?, 'test', ?, 1, ?, ?)`, did, did, now, now)
		require.NoError(t, err)
		_, err = env.db.Exec(`
			INSERT INTO job_delivery_plans
				(job_id, destination_id, priority, retry_budget, enabled, created_at, updated_at)
			VALUES (?, ?, ?, 5, 1, ?, ?)`,
			jobID, did, i+1, now, now)
		require.NoError(t, err)
	}

	mp4Path := filepath.Join(env.tmpDir, "shadow_mismatch.mp4")
	synthesizeSoloAudioMP4(t, mp4Path)
	payload, err := os.ReadFile(mp4Path)
	require.NoError(t, err)

	cmd := beginUploadDefaultCmd(jobID)
	cmd.ExpectedSizeBytes = int64(len(payload))
	cmd.ExpectedSHA256 = sha256Hex(payload)
	cmd.MimeType = "video/mp4"
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.NoError(t, err)

	t.Setenv("VELOX_FFPROBE_VERIFY_ON_FINALIZE", "shadow")

	var logged string
	captureTripwireLogs(t, func(buf *bytes.Buffer) {
		art, lerr := env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
			UploadID: sess.UploadID, JobID: jobID, WorkerID: testWorkerID,
			LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
		})
		require.NoError(t, lerr, "shadow mode MUST NOT abort on mismatch (Stage 1 = log-only)")
		require.Equal(t, "READY", art.Status, "shadow mode lets the writer commit on mismatch (regression is loud in logs, not on the wire)")
		logged = buf.String()
	})
	require.Contains(t, logged, "[TRIPWIRE]", "log line carries the tripwire tag for grep routing")
	require.Contains(t, logged, "event=ffprobe_invariant_mismatch", "shadow mode logs every mismatch for Stage 1 visibility")
	require.Contains(t, logged, "mode=shadow", "log line must carry the mode so the runbook's Stage 2 grep excludes it")
	require.Contains(t, logged, "job_id="+jobID)
	require.Contains(t, logged, "expected_streams=3", "log line carries expected (per-job plan) count for triage")
	require.Contains(t, logged, "actual_streams=1", "log line carries actual (ffprobe) count for triage")
}

// TestFinalize_FFProbeInvariant_ShadowMissingBinary verifies the
// shadow mode's infra-miss path: ffprobe absent from PATH. The
// gate logs a distinct ffprobe_invariant_missing_binary event
// (separate from the count-mismatch class so the runbook triage
// paths stay clean) and returns nil (no abort).
func TestFinalize_FFProbeInvariant_ShadowMissingBinary(t *testing.T) {
	requireFFMPEGTools(t)

	env := setupTestEnv(t)
	env.seedJob("JSMX", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("JSMX", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	mp4Path := filepath.Join(env.tmpDir, "shadow_missing.mp4")
	synthesizeSoloAudioMP4(t, mp4Path)
	payload, err := os.ReadFile(mp4Path)
	require.NoError(t, err)

	cmd := beginUploadDefaultCmd("JSMX")
	cmd.ExpectedSizeBytes = int64(len(payload))
	cmd.ExpectedSHA256 = sha256Hex(payload)
	cmd.MimeType = "video/mp4"
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.NoError(t, err)

	// Strip ffprobe out of PATH while ffmpeg (used by the synth
	// helper earlier so the fixture is on disk) remains available.
	stripFFProbeFromPath(t)
	t.Setenv("VELOX_FFPROBE_VERIFY_ON_FINALIZE", "shadow")

	var logged string
	captureTripwireLogs(t, func(buf *bytes.Buffer) {
		art, lerr := env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
			UploadID: sess.UploadID, JobID: "JSMX", WorkerID: testWorkerID,
			LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
		})
		require.NoError(t, lerr, "shadow mode MUST NOT abort on binary-missing infra path")
		require.Equal(t, "READY", art.Status, "shadow mode lets the writer commit on binary-missing infra")
		logged = buf.String()
	})
	require.Contains(t, logged, "event=ffprobe_invariant_missing_binary", "infra miss is a distinct event from count mismatch so the runbook grep routes them separately")
	require.Contains(t, logged, "mode=shadow")
	require.Contains(t, logged, "job_id=JSMX")
}

// TestFinalize_FFProbeInvariant_EnforceMissingBinary verifies
// the enforce mode's infra-miss path (env="enforce"): the gate
// MUST abort the tx with ErrFFProbeInvariantMissingBinary so a
// misconfigured master cannot silently rubber-stamp artifacts
// when ffprobe is absent. This is the "deploy-gate miss" sentinel
// distinct from the count-mismatch class.
func TestFinalize_FFProbeInvariant_EnforceMissingBinary(t *testing.T) {
	requireFFMPEGTools(t)

	env := setupTestEnv(t)
	env.seedJob("JENX", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("JENX", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	mp4Path := filepath.Join(env.tmpDir, "enforce_missing.mp4")
	synthesizeSoloAudioMP4(t, mp4Path)
	payload, err := os.ReadFile(mp4Path)
	require.NoError(t, err)

	cmd := beginUploadDefaultCmd("JENX")
	cmd.ExpectedSizeBytes = int64(len(payload))
	cmd.ExpectedSHA256 = sha256Hex(payload)
	cmd.MimeType = "video/mp4"
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.NoError(t, err)

	stripFFProbeFromPath(t)
	t.Setenv("VELOX_FFPROBE_VERIFY_ON_FINALIZE", "enforce")

	var logged string
	captureTripwireLogs(t, func(buf *bytes.Buffer) {
		_, lerr := env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
			UploadID: sess.UploadID, JobID: "JENX", WorkerID: testWorkerID,
			LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
		})
		require.Error(t, lerr, "enforce mode MUST abort on binary-missing infra path")
		require.True(t, errors.Is(lerr, ErrFFProbeInvariantMissingBinary),
			"want ErrFFProbeInvariantMissingBinary (deploy-gate binary miss), got %v", lerr)
		logged = buf.String()
	})
	require.Contains(t, logged, "event=ffprobe_invariant_missing_binary")
	require.Contains(t, logged, "mode=enforce")
}

// TestFinalize_FFProbeInvariant_OffNoOp verifies the off / unset
// / typo path: the gate MUST NOOP regardless of the artifact's
// audio stream count or environment PATH. Without this assertion
// a future refactor could subtly enable the gate on a typo'd
// master (the exact failure mode the strict-literal policy
// blocks).
func TestFinalize_FFProbeInvariant_OffNoOp(t *testing.T) {
	requireFFMPEGTools(t)

	env := setupTestEnv(t)
	env.seedJob("JOFF", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("JOFF", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	mp4Path := filepath.Join(env.tmpDir, "off_mode.mp4")
	synthesizeSoloAudioMP4(t, mp4Path)
	payload, err := os.ReadFile(mp4Path)
	require.NoError(t, err)

	cmd := beginUploadDefaultCmd("JOFF")
	cmd.ExpectedSizeBytes = int64(len(payload))
	cmd.ExpectedSHA256 = sha256Hex(payload)
	cmd.MimeType = "video/mp4"
	sess, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	_, err = env.svc.Receive(context.Background(), sess.UploadID, uploadBytes(payload))
	require.NoError(t, err)

	// Cover the typo-fence: "Shadow", "SHADOW", "1", "yes" must
	// all NOT enable the gate. Run the finalization once with the
	// typo'd value; assert no error and no tripwire log line.
	t.Setenv("VELOX_FFPROBE_VERIFY_ON_FINALIZE", "1")
	captureTripwireLogs(t, func(buf *bytes.Buffer) {
		art, lerr := env.svc.Finalize(context.Background(), FinalizeArtifactCommand{
			UploadID: sess.UploadID, JobID: "JOFF", WorkerID: testWorkerID,
			LeaseID: testLeaseID, AttemptNumber: 1, ExpectedRevision: testRevision,
		})
		require.NoError(t, lerr)
		require.Equal(t, "READY", art.Status)
		require.NotContains(t, buf.String(), "[TRIPWIRE]",
			"a typo'd value (\"1\") must NOT enable the gate")
	})
}

// TestParseFFProbeModeMatrix verifies the strict-literal mapping
// at the parseFFProbeMode() layer so a future refactor that
// expands the value-space cannot drift from the runbook
// contract. ALSO pins the no-whitespace-trim invariant: the bash
// preflight parses the env file the same way (no trim either) so
// a value like `= shadow` is Off in both places — operator
// installs with the strict-literal expectation intact.
func TestParseFFProbeModeMatrix(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want ffprobeInvariantMode
	}{
		// Valid trip modes.
		{"shadow", ffprobeModeShadow},
		{"enforce", ffprobeModeEnforce},
		{"true", ffprobeModeEnforce},     // legacy alias preserved for env files shipped pre-tri-state
		// Off class — empty value (line `VELOX_FFPROBE_VERIFY_ON_FINALIZE=` with nothing after).
		{"", ffprobeModeOff},
		// Off class — explicit OFF values.
		{"off", ffprobeModeOff},
		{"OFF", ffprobeModeOff},
		{"Off", ffprobeModeOff},
		// Typo fence — different cases must NOT enable.
		{"Shadow", ffprobeModeOff},
		{"SHADOW", ffprobeModeOff},
		{"ENFORCE", ffprobeModeOff},
		{"True", ffprobeModeOff},
		{"TRUE", ffprobeModeOff},
		{"1", ffprobeModeOff},
		{"yes", ffprobeModeOff},
		{"No", ffprobeModeOff},
		{"enabled", ffprobeModeOff},
		{"log", ffprobeModeOff},
		// No-whitespace-trim invariant — the bash preflight parses
		// identically, so adding trimming to either side would let
		// `= shadow` (leading space) silently install an enforced
		// gate that the runtime treats as Off. Locked both sides via
		// this matrix.
		{" shadow", ffprobeModeOff}, // leading space
		{"shadow ", ffprobeModeOff}, // trailing space
		{"\tshadow", ffprobeModeOff}, // leading tab
		{" true ", ffprobeModeOff},  // leading + trailing space around legacy value
		{"shadow\t", ffprobeModeOff}, // trailing tab
	}
	for _, c := range cases {
		c := c
		t.Run(c.in, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, c.want, parseFFProbeMode(c.in))
		})
	}
}
