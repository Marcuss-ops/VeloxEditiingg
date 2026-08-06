package artifacts

import (
	"context"
	"errors"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

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
	assertNoArtifactUploadRows(t, env, "J1")
	assertAttemptAndTaskUnchanged(t, env, "J1", "worker-other", testLeaseID)
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
	assertNoArtifactUploadRows(t, env, "J2")
	assertAttemptAndTaskUnchanged(t, env, "J2", testWorkerID, "lease-other")
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
	// task_attempts uses a terminal/non-terminal contract:
	// non-terminal = OK to upload, terminal =
	// ErrAttemptNotRenderFinished. Seed a SUCCEEDED task_attempt
	// to exercise the terminal branch.
	env.seedAttempt("J4", 1, "SUCCEEDED", testWorkerID, testLeaseID)

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
// #region 17 — doppio worker sullo stesso task (second BeginUpload rejected)
// =====================================================================

func TestBeginUpload_DoubleWorkerSameTask_SecondRejected(t *testing.T) {
	env := setupTestEnv(t)
	env.seedJob("J17", "RUNNING", testWorkerID, testLeaseID, testRevision, env.clock.Now().Add(5*time.Minute))
	env.seedAttempt("J17", 1, "RENDER_FINISHED", testWorkerID, testLeaseID)

	// First worker succeeds. Capture the baseline so the second,
	// unauthorized attempt must not add another session or artifact.
	cmd := beginUploadDefaultCmd("J17")
	_, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)
	var uploadsBefore, artifactsBefore int
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM artifact_uploads WHERE job_id = 'J17'`).Scan(&uploadsBefore))
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM artifacts WHERE job_id = 'J17'`).Scan(&artifactsBefore))

	// Second worker tries to start an upload for the same attempt.
	cmd2 := beginUploadDefaultCmd("J17")
	cmd2.WorkerID = "worker-test-2"
	cmd2.LeaseID = "lease-test-2"
	_, err = env.svc.BeginUpload(context.Background(), cmd2)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrWrongJobOwner), "got %v want ErrWrongJobOwner", err)
	var uploadsAfter, artifactsAfter int
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM artifact_uploads WHERE job_id = 'J17'`).Scan(&uploadsAfter))
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM artifacts WHERE job_id = 'J17'`).Scan(&artifactsAfter))
	require.Equal(t, uploadsBefore, uploadsAfter, "rejected second BeginUpload must not create an upload session")
	require.Equal(t, artifactsBefore, artifactsAfter, "rejected second BeginUpload must not create an artifact row")
}

// assertNoArtifactUploadRows proves an authorization rejection occurs
// before BeginUpload's atomic artifact + upload-session insert. It is
// deliberately scoped to the test job so the assertion remains valid
// even when the fixture evolves with unrelated rows.
func assertNoArtifactUploadRows(t *testing.T, env *testEnv, jobID string) {
	t.Helper()
	var uploads, artifacts int
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM artifact_uploads WHERE job_id = ?`, jobID).Scan(&uploads))
	require.NoError(t, env.db.QueryRow(`SELECT COUNT(*) FROM artifacts WHERE job_id = ?`, jobID).Scan(&artifacts))
	require.Equal(t, 0, uploads, "rejected BeginUpload must not create an upload session")
	require.Equal(t, 0, artifacts, "rejected BeginUpload must not create an artifact row")
}

// assertAttemptAndTaskUnchanged proves an authorization rejection did
// not mutate the canonical lease/attempt or task CAS state. The seed
// fixture represents an active render attempt, so any change here would
// indicate that the rejected request crossed the ownership boundary.
func assertAttemptAndTaskUnchanged(t *testing.T, env *testEnv, jobID, wantWorker, wantLease string) {
	t.Helper()
	var attemptWorker, attemptLease, attemptStatus, taskStatus string
	var taskRevision int
	require.NoError(t, env.db.QueryRow(`
		SELECT worker_id, lease_id, status
		FROM task_attempts WHERE id = ?`, jobID+"-attempt").
		Scan(&attemptWorker, &attemptLease, &attemptStatus))
	require.NoError(t, env.db.QueryRow(`
		SELECT status, revision
		FROM tasks WHERE task_id = ?`, jobID+"-task").
		Scan(&taskStatus, &taskRevision))
	require.Equal(t, wantWorker, attemptWorker)
	require.Equal(t, wantLease, attemptLease)
	require.Equal(t, "RENDER_FINISHED", attemptStatus)
	require.Equal(t, "RUNNING", taskStatus)
	require.Equal(t, 0, taskRevision)
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
