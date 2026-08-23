package store

import (
	"context"
	"errors"
	_ "github.com/mattn/go-sqlite3"
	"path/filepath"
	"testing"
	"time"
)

func TestDeploymentRecovery_RestartDuringDeployingNeverAssumesSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery-deploying.db")
	base := time.Now().UTC().Truncate(time.Second)
	digestA := deploymentTestDigest('a')
	digestB := deploymentTestDigest('b')
	digestC := deploymentTestDigest('c')
	ctx := context.Background()

	// ── Phase 1: pre-crash Master ──────────────────────────────────
	s1 := newDeploymentTestStoreAt(t, path)
	if err := s1.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "deploy-recovery-dep-base",
		WorkerID:     "wicket",
		TargetDigest: digestA,
		StartedAt:    base,
		FinishedAt:   deploymentTimePtr(base),
		Status:       DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("baseline: %v", err)
	}
	// Rollout to B interrupted mid-DEPLOYING.
	if err := s1.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-recovery-dep",
		WorkerID:       "wicket",
		PreviousDigest: digestA,
		TargetDigest:   digestB,
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("insert PENDING: %v", err)
	}
	if err := s1.RecordDeploymentPhase(ctx, "wicket", "DEPLOYING"); err != nil {
		t.Fatalf("record phase: %v", err)
	}
	// The worker comes back advertising digest C — neither the target B
	// nor the last-known-good A.
	if err := upsertWorkerRunningDigest(ctx, s1.db, "wicket", digestC, base.Add(90*time.Second)); err != nil {
		t.Fatalf("post-crash heartbeat: %v", err)
	}
	_ = s1.Close()

	// ── Phase 2: Master restart ────────────────────────────────────
	s2 := reopenDeploymentTestStore(t, path)
	state, err := s2.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState after restart: %v", err)
	}
	if state.LastPhase != "DEPLOYING" {
		t.Errorf("LastPhase after restart = %q, want DEPLOYING (interrupted phase survives)", state.LastPhase)
	}
	if state.DesiredDigest != digestB || state.RunningDigest != digestC || state.LastSuccessfulDigest != digestA {
		t.Errorf("read model after restart = desired=%q running=%q last_successful=%q, want B/C/A (drift visible)",
			state.DesiredDigest, state.RunningDigest, state.LastSuccessfulDigest)
	}

	// The reconciler compares the digests: C != B → the interrupted rollout
	// is NOT assumed successful. The verified transition is refused.
	err = s2.MarkVerifiedSucceeded(ctx, "deploy-recovery-dep", digestC, base.Add(2*time.Minute))
	if !errors.Is(err, ErrDeploymentDigestMismatch) {
		t.Fatalf("assumed success on digest C = %v, want ErrDeploymentDigestMismatch", err)
	}

	// Nothing moved: row PENDING, last-known-good A, running C untouched.
	row, err := s2.GetLatestDeploymentForWorker(ctx, "wicket")
	if err != nil {
		t.Fatalf("journal after refused success: %v", err)
	}
	if row.Status != DeployStatusPending {
		t.Errorf("row status = %q, want PENDING (crash must not be assumed successful)", row.Status)
	}
	state, err = s2.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("read model after refused success: %v", err)
	}
	if state.LastOperationStatus != DeployStatusPending {
		t.Errorf("LastOperationStatus = %q, want PENDING", state.LastOperationStatus)
	}
	if state.LastSuccessfulDigest != digestA {
		t.Errorf("LastSuccessfulDigest = %q, want %q (never advanced without verification)", state.LastSuccessfulDigest, digestA)
	}
	if state.RunningDigest != digestC {
		t.Errorf("RunningDigest = %q, want %q (the observed drift stays visible)", state.RunningDigest, digestC)
	}
}
func TestDeploymentRecovery_RestartDuringWaitingReadyResumesOnNewSessionHeartbeat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery-waiting-ready.db")
	base := time.Now().UTC().Truncate(time.Second)
	digestA := deploymentTestDigest('a')
	digestB := deploymentTestDigest('b')
	ctx := context.Background()

	// ── Phase 1: pre-crash Master writes the durable state ──────────
	s1 := newDeploymentTestStoreAt(t, path)
	if err := s1.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "deploy-recovery-base",
		WorkerID:     "wicket",
		TargetDigest: digestA,
		StartedAt:    base,
		FinishedAt:   deploymentTimePtr(base),
		Status:       DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("baseline: %v", err)
	}
	// Rollout to B parked in WAITING_READY (drain done, restart issued,
	// new-session gate open).
	if err := s1.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-recovery-wr",
		WorkerID:       "wicket",
		PreviousDigest: digestA,
		TargetDigest:   digestB,
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("insert PENDING: %v", err)
	}
	if err := s1.RecordDeploymentPhase(ctx, "wicket", "WAITING_READY"); err != nil {
		t.Fatalf("record phase: %v", err)
	}
	// The pre-restart session heartbeats the OLD digest A (stale
	// observation that must never complete the rollout to B).
	if err := upsertWorkerRunningDigest(ctx, s1.db, "wicket", digestA, base.Add(90*time.Second)); err != nil {
		t.Fatalf("pre-restart heartbeat: %v", err)
	}
	_ = s1.Close() // crash: the file is the only survivor

	// ── Phase 2: Master restart re-opens the SAME file ──────────────
	s2 := reopenDeploymentTestStore(t, path)

	// The durable state survived: PENDING row + phase + digest intent.
	state, err := s2.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState after restart: %v", err)
	}
	if state.LastPhase != "WAITING_READY" {
		t.Errorf("LastPhase after restart = %q, want WAITING_READY (phase survives)", state.LastPhase)
	}
	if state.DesiredDigest != digestB || state.RunningDigest != digestA || state.LastSuccessfulDigest != digestA {
		t.Errorf("read model after restart = desired=%q running=%q last_successful=%q, want B/A/A",
			state.DesiredDigest, state.RunningDigest, state.LastSuccessfulDigest)
	}
	row, err := s2.GetLatestDeploymentForWorker(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetLatestDeploymentForWorker after restart: %v", err)
	}
	if row.Status != DeployStatusPending || row.DeploymentID != "deploy-recovery-wr" {
		t.Fatalf("journal row after restart = %s/%s, want PENDING deploy-recovery-wr (crash must not fabricate a terminal state)", row.Status, row.DeploymentID)
	}

	// The reconciler must NOT resume on the stale pre-restart observation:
	// observed A != target B → the verified transition is refused and the
	// row stays PENDING.
	err = s2.MarkVerifiedSucceeded(ctx, "deploy-recovery-wr", digestA, base.Add(2*time.Minute))
	if !errors.Is(err, ErrDeploymentDigestMismatch) {
		t.Fatalf("resume on stale pre-restart digest = %v, want ErrDeploymentDigestMismatch", err)
	}
	row, err = s2.GetLatestDeploymentForWorker(ctx, "wicket")
	if err != nil {
		t.Fatalf("re-read journal: %v", err)
	}
	if row.Status != DeployStatusPending {
		t.Fatalf("row status after refused resume = %q, want PENDING", row.Status)
	}

	// The NEW session's authenticated heartbeat arrives advertising B.
	if err := upsertWorkerRunningDigest(ctx, s2.db, "wicket", digestB, base.Add(3*time.Minute)); err != nil {
		t.Fatalf("new-session heartbeat: %v", err)
	}

	// NOW the rollout resumes: observed B == target B → verified SUCCEEDED.
	if err := s2.MarkVerifiedSucceeded(ctx, "deploy-recovery-wr", digestB, base.Add(4*time.Minute)); err != nil {
		t.Fatalf("resume after new-session heartbeat: %v", err)
	}
	row, err = s2.GetLatestDeploymentForWorker(ctx, "wicket")
	if err != nil {
		t.Fatalf("final journal: %v", err)
	}
	if row.Status != DeployStatusSucceeded {
		t.Errorf("row status after resume = %q, want SUCCEEDED", row.Status)
	}
	state, err = s2.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("final read model: %v", err)
	}
	if state.LastOperationStatus != DeployStatusSucceeded || state.LastSuccessfulDigest != digestB {
		t.Errorf("read model after resume = status=%q last_successful=%q, want SUCCEEDED/%q",
			state.LastOperationStatus, state.LastSuccessfulDigest, digestB)
	}
	// The phase survives the terminal write (operator sees WHERE it was).
	if state.LastPhase != "WAITING_READY" {
		t.Errorf("LastPhase after resume = %q, want preserved WAITING_READY", state.LastPhase)
	}
	// running_digest stays exactly what the heartbeat wrote — the success
	// transition never fabricates observed state.
	if state.RunningDigest != digestB {
		t.Errorf("RunningDigest after resume = %q, want %q (heartbeat observation preserved)", state.RunningDigest, digestB)
	}
}
