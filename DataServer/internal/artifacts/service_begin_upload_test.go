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

	// First worker succeeds.
	cmd := beginUploadDefaultCmd("J17")
	_, err := env.svc.BeginUpload(context.Background(), cmd)
	require.NoError(t, err)

	// Second worker tries to start an upload for the same attempt.
	cmd2 := beginUploadDefaultCmd("J17")
	cmd2.WorkerID = "worker-test-2"
	cmd2.LeaseID = "lease-test-2"
	_, err = env.svc.BeginUpload(context.Background(), cmd2)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrWrongJobOwner), "got %v want ErrWrongJobOwner", err)
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
