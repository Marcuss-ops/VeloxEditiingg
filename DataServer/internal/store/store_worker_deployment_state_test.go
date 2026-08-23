package store

import (
	"context"
	"errors"
	_ "github.com/mattn/go-sqlite3"
	"strings"
	"testing"
	"time"
)

func TestWorkerDeploymentState_ErrorCodeAndMessagePersisted(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-code-fail",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}
	repo := NewDeploymentRecordRepository(s)
	if err := repo.MarkFailed(ctx, "deploy-code-fail", base.Add(time.Minute), "DIGEST_MISMATCH", "digest_mismatch: expected=sha256:b observed=sha256:c"); err != nil {
		t.Fatalf("repo.MarkFailed: %v", err)
	}

	// Journal row carries both, in separate columns.
	rec, err := s.GetLatestDeploymentForWorker(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetLatestDeploymentForWorker: %v", err)
	}
	if rec.ErrorCode != "DIGEST_MISMATCH" {
		t.Errorf("journal ErrorCode = %q, want DIGEST_MISMATCH", rec.ErrorCode)
	}
	if rec.ErrorMessage != "digest_mismatch: expected=sha256:b observed=sha256:c" {
		t.Errorf("journal ErrorMessage = %q, want the full message", rec.ErrorMessage)
	}

	// Read model projects both, still separate.
	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationErrorCode != "DIGEST_MISMATCH" {
		t.Errorf("read model LastOperationErrorCode = %q, want DIGEST_MISMATCH", state.LastOperationErrorCode)
	}
	if state.LastOperationError != "digest_mismatch: expected=sha256:b observed=sha256:c" {
		t.Errorf("read model LastOperationError = %q, want the full message", state.LastOperationError)
	}
}
func TestWorkerDeploymentState_FailedRolloutPreservesLastSuccessfulDigest(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	digestA := deploymentTestDigest('a')
	digestB := deploymentTestDigest('b')

	if err := s.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "deploy-state-success",
		WorkerID:     "wicket",
		TargetDigest: digestA,
		StartedAt:    base,
		FinishedAt:   deploymentTimePtr(base),
		Status:       DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("InsertBaselineDeploymentRecord: %v", err)
	}
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-state-failed",
		WorkerID:       "wicket",
		PreviousDigest: digestA,
		TargetDigest:   digestB,
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}
	if err := s.updateDeploymentTerminal(ctx, "deploy-state-failed", DeployStatusFailed, base.Add(2*time.Minute), "COSIGN_FAILED", "cosign verify failed", false); err != nil {
		t.Fatalf("updateDeploymentTerminal(FAILED): %v", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.DesiredDigest != digestB {
		t.Errorf("DesiredDigest = %q, want %q (failed target remains the intent)", state.DesiredDigest, digestB)
	}
	if state.LastSuccessfulDigest != digestA {
		t.Errorf("LastSuccessfulDigest = %q, want %q (failed rollout must not erase last-known-good)", state.LastSuccessfulDigest, digestA)
	}
	if state.RunningDigest != "" {
		t.Errorf("RunningDigest = %q, want empty (no heartbeat yet)", state.RunningDigest)
	}
	if state.LastOperationID != "deploy-state-failed" || state.LastOperationStatus != DeployStatusFailed {
		t.Errorf("last operation = %s/%s, want deploy-state-failed/FAILED", state.LastOperationID, state.LastOperationStatus)
	}
	if state.LastOperationError != "cosign verify failed" {
		t.Errorf("LastOperationError = %q, want the FAILED transition error", state.LastOperationError)
	}
}
func TestWorkerDeploymentState_GenericSucceededDoesNotAdvance(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	digestA := deploymentTestDigest('a')

	// Baseline verified success to A establishes last-known-good.
	if err := s.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "deploy-generic-base",
		WorkerID:     "wicket",
		TargetDigest: digestA,
		StartedAt:    base,
		FinishedAt:   deploymentTimePtr(base),
		Status:       DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("InsertBaselineDeploymentRecord: %v", err)
	}

	// Unverified generic SUCCEEDED to B must leave last-known-good at A.
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-generic-b",
		WorkerID:       "wicket",
		PreviousDigest: digestA,
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}
	if err := s.UpdateDeploymentStatus(ctx, "deploy-generic-b", DeployStatusSucceeded, base.Add(2*time.Minute)); err != nil {
		t.Fatalf("UpdateDeploymentStatus(SUCCEEDED): %v", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastSuccessfulDigest != digestA {
		t.Errorf("LastSuccessfulDigest = %q, want %q (unverified SUCCEEDED must not advance last-known-good)", state.LastSuccessfulDigest, digestA)
	}
	if state.LastOperationStatus != DeployStatusSucceeded {
		t.Errorf("LastOperationStatus = %q, want SUCCEEDED (the row IS succeeded, only last-known-good is gated)", state.LastOperationStatus)
	}
}
func TestWorkerDeploymentState_NewOperationClearsErrorPreservesHistory(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	digestA := deploymentTestDigest('a')
	digestB := deploymentTestDigest('b')
	repo := NewDeploymentRecordRepository(s)

	if err := s.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "deploy-clear-base",
		WorkerID:     "wicket",
		TargetDigest: digestA,
		StartedAt:    base,
		FinishedAt:   deploymentTimePtr(base),
		Status:       DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("InsertBaselineDeploymentRecord: %v", err)
	}
	// op#1: FAILED with DIGEST_MISMATCH.
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-clear-op1",
		WorkerID:       "wicket",
		PreviousDigest: digestA,
		TargetDigest:   digestB,
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("insert op1: %v", err)
	}
	if err := repo.MarkFailed(ctx, "deploy-clear-op1", base.Add(2*time.Minute), "DIGEST_MISMATCH", "digest_mismatch: expected=sha256:b observed=sha256:c"); err != nil {
		t.Fatalf("MarkFailed op1: %v", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationErrorCode != "DIGEST_MISMATCH" || state.LastOperationError == "" {
		t.Fatalf("pre-condition: read model error = %q/%q, want DIGEST_MISMATCH/msg", state.LastOperationErrorCode, state.LastOperationError)
	}

	// op#2: a NEW operation starts (PENDING). The read model's current error
	// must be cleared — the failure of op#1 is no longer the CURRENT error.
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-clear-op2",
		WorkerID:       "wicket",
		PreviousDigest: digestB,
		TargetDigest:   deploymentTestDigest('c'),
		StartedAt:      base.Add(3 * time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("insert op2: %v", err)
	}

	state, err = s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationErrorCode != "" {
		t.Errorf("LastOperationErrorCode after new op = %q, want empty (current error cleared)", state.LastOperationErrorCode)
	}
	if state.LastOperationError != "" {
		t.Errorf("LastOperationError after new op = %q, want empty (current error cleared)", state.LastOperationError)
	}
	if state.LastOperationID != "deploy-clear-op2" {
		t.Errorf("LastOperationID = %q, want deploy-clear-op2", state.LastOperationID)
	}

	// History is preserved: op#1's journal row still carries code+message.
	op1, err := s.getDeploymentRecord(ctx, "deploy-clear-op1")
	if err != nil {
		t.Fatalf("get op1 journal row: %v", err)
	}
	if op1.ErrorCode != "DIGEST_MISMATCH" || op1.ErrorMessage != "digest_mismatch: expected=sha256:b observed=sha256:c" {
		t.Errorf("op1 history = code=%q msg=%q, want preserved DIGEST_MISMATCH/msg", op1.ErrorCode, op1.ErrorMessage)
	}
	if op1.Status != DeployStatusFailed {
		t.Errorf("op1 history status = %q, want FAILED (history is immutable)", op1.Status)
	}
}
func TestWorkerDeploymentState_NotFound(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	_, err := s.GetWorkerDeploymentState(ctx, "no-such-worker")
	if !errors.Is(err, ErrWorkerDeploymentStateNotFound) {
		t.Errorf("err = %v, want ErrWorkerDeploymentStateNotFound", err)
	}
}
func TestWorkerDeploymentState_PhaseRecordedAndPreserved(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	if err := s.RecordDeploymentPhase(ctx, "wicket", "DRAINING"); err != nil {
		t.Fatalf("RecordDeploymentPhase(DRAINING): %v", err)
	}
	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastPhase != "DRAINING" {
		t.Fatalf("LastPhase = %q, want DRAINING", state.LastPhase)
	}

	// A PENDING insert (control-plane intent) must preserve the recorded
	// phase and fill desired without touching it.
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-phase-1",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}
	if err := s.RecordDeploymentPhase(ctx, "wicket", "VERIFYING_DIGEST"); err != nil {
		t.Fatalf("RecordDeploymentPhase(VERIFYING_DIGEST): %v", err)
	}
	// Terminal transition (FAILED) preserves the last phase: the operator can
	// see WHERE the rollout stopped.
	if err := s.updateDeploymentTerminal(ctx, "deploy-phase-1", DeployStatusFailed, base.Add(time.Minute), "DIGEST_MISMATCH", "digest_mismatch: expected=B observed=C", false); err != nil {
		t.Fatalf("updateDeploymentTerminal(FAILED): %v", err)
	}

	state, err = s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastPhase != "VERIFYING_DIGEST" {
		t.Errorf("LastPhase after FAILED transition = %q, want VERIFYING_DIGEST (where the rollout stopped)", state.LastPhase)
	}
	if state.LastOperationStatus != DeployStatusFailed || state.LastOperationError != "digest_mismatch: expected=B observed=C" {
		t.Errorf("last operation = %s/%q, want FAILED/digest_mismatch...", state.LastOperationStatus, state.LastOperationError)
	}
	if state.DesiredDigest != deploymentTestDigest('b') {
		t.Errorf("DesiredDigest = %q, want %q (phase recording must not touch intent)", state.DesiredDigest, deploymentTestDigest('b'))
	}
}
func TestWorkerDeploymentState_RunningDigestNullUntilHeartbeat(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()

	rec := DeploymentRecord{
		DeploymentID:   "deploy-state-1",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      time.Now().UTC().Truncate(time.Second),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.RunningDigest != "" {
		t.Errorf("RunningDigest = %q, want empty (NULL) until first authenticated heartbeat", state.RunningDigest)
	}
	if state.DesiredDigest != rec.TargetDigest {
		t.Errorf("DesiredDigest = %q, want %q (control-plane intent from record)", state.DesiredDigest, rec.TargetDigest)
	}
	if state.LastOperationID != rec.DeploymentID || state.LastOperationStatus != DeployStatusPending {
		t.Errorf("last operation = %s/%s, want %s/PENDING", state.LastOperationID, state.LastOperationStatus, rec.DeploymentID)
	}

	// Authenticated heartbeat observes digest 'c' (drift from desired 'b').
	heartbeatDigest := deploymentTestDigest('c')
	if err := upsertWorkerRunningDigest(ctx, s.db, "wicket", heartbeatDigest, time.Now().UTC()); err != nil {
		t.Fatalf("upsertWorkerRunningDigest: %v", err)
	}
	state, err = s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState after heartbeat: %v", err)
	}
	if state.RunningDigest != heartbeatDigest {
		t.Errorf("RunningDigest after heartbeat = %q, want %q", state.RunningDigest, heartbeatDigest)
	}
	// The heartbeat must not touch intent.
	if state.DesiredDigest != rec.TargetDigest {
		t.Errorf("DesiredDigest after heartbeat = %q, want %q (heartbeat never writes intent)", state.DesiredDigest, rec.TargetDigest)
	}
}
func TestWorkerDeploymentState_StaleHeartbeatCannotEraseObservedDigest(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()

	observed := deploymentTestDigest('b')
	if err := upsertWorkerRunningDigest(ctx, s.db, "wicket", observed, time.Now().UTC()); err != nil {
		t.Fatalf("upsertWorkerRunningDigest: %v", err)
	}
	// A heartbeat without a digest is a no-op — it must not erase 'b'.
	if err := upsertWorkerRunningDigest(ctx, s.db, "wicket", "", time.Now().UTC()); err != nil {
		t.Fatalf("upsertWorkerRunningDigest(empty): %v", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.RunningDigest != observed {
		t.Errorf("RunningDigest = %q, want %q (empty heartbeat must not erase observation)", state.RunningDigest, observed)
	}
}
func TestWorkerDeploymentState_TerminalTransitionPreservesRunningDigest(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	digestB := deploymentTestDigest('b')

	// Heartbeat observes the worker actually running digest 'b'.
	if err := upsertWorkerRunningDigest(ctx, s.db, "wicket", digestB, base); err != nil {
		t.Fatalf("upsertWorkerRunningDigest: %v", err)
	}

	// A rollout to digest 'c' verified through MarkVerifiedSucceeded — the
	// VERIFYING_DIGEST path: it must NOT rewrite running_digest (still 'b'
	// until the next heartbeat) but MUST advance last_successful_digest.
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-state-c",
		WorkerID:       "wicket",
		PreviousDigest: digestB,
		TargetDigest:   deploymentTestDigest('c'),
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}
	if err := s.MarkVerifiedSucceeded(ctx, "deploy-state-c", deploymentTestDigest('c'), base.Add(2*time.Minute)); err != nil {
		t.Fatalf("MarkVerifiedSucceeded: %v", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.RunningDigest != digestB {
		t.Errorf("RunningDigest after SUCCEEDED transition = %q, want %q (transition must not clobber observation)", state.RunningDigest, digestB)
	}
	if state.LastSuccessfulDigest != deploymentTestDigest('c') {
		t.Errorf("LastSuccessfulDigest = %q, want %q (verification advances last-known-good)", state.LastSuccessfulDigest, deploymentTestDigest('c'))
	}
	if state.LastOperationStatus != DeployStatusSucceeded {
		t.Errorf("LastOperationStatus = %q, want SUCCEEDED", state.LastOperationStatus)
	}
}
func TestWorkerDeploymentState_VerifiedMismatchRejected(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	digestA := deploymentTestDigest('a')
	digestB := deploymentTestDigest('b')

	if err := s.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "deploy-mismatch-base",
		WorkerID:     "wicket",
		TargetDigest: digestA,
		StartedAt:    base,
		FinishedAt:   deploymentTimePtr(base),
		Status:       DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("InsertBaselineDeploymentRecord: %v", err)
	}
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-mismatch-b",
		WorkerID:       "wicket",
		PreviousDigest: digestA,
		TargetDigest:   digestB,
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}
	// Heartbeat observed the worker actually running C (drift).
	if err := upsertWorkerRunningDigest(ctx, s.db, "wicket", deploymentTestDigest('c'), base.Add(90*time.Second)); err != nil {
		t.Fatalf("upsertWorkerRunningDigest: %v", err)
	}

	err := s.MarkVerifiedSucceeded(ctx, "deploy-mismatch-b", deploymentTestDigest('c'), base.Add(2*time.Minute))
	if !errors.Is(err, ErrDeploymentDigestMismatch) {
		t.Fatalf("MarkVerifiedSucceeded(C) err = %v, want ErrDeploymentDigestMismatch", err)
	}
	if !strings.Contains(err.Error(), "expected=") || !strings.Contains(err.Error(), "observed=") {
		t.Errorf("mismatch error must carry expected/observed; got %v", err)
	}

	rec, err := s.GetLatestDeploymentForWorker(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetLatestDeploymentForWorker: %v", err)
	}
	if rec.Status != DeployStatusPending {
		t.Fatalf("row status = %q, want PENDING (mismatch applies no transition)", rec.Status)
	}
	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastSuccessfulDigest != digestA {
		t.Errorf("LastSuccessfulDigest = %q, want %q (mismatch must not advance last-known-good)", state.LastSuccessfulDigest, digestA)
	}
	if state.RunningDigest != deploymentTestDigest('c') {
		t.Errorf("RunningDigest = %q, want %q (the observation stays exactly as the heartbeat wrote it)", state.RunningDigest, deploymentTestDigest('c'))
	}
}
func TestWorkerDeploymentState_VerifiedSuccessClearsErrorCode(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	digestA := deploymentTestDigest('a')
	digestB := deploymentTestDigest('b')
	repo := NewDeploymentRecordRepository(s)

	if err := s.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "deploy-clear-ok-base",
		WorkerID:     "wicket",
		TargetDigest: digestA,
		StartedAt:    base,
		FinishedAt:   deploymentTimePtr(base),
		Status:       DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("InsertBaselineDeploymentRecord: %v", err)
	}
	// Failed op#1 leaves DIGEST_MISMATCH on the read model.
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-clear-ok-fail",
		WorkerID:       "wicket",
		PreviousDigest: digestA,
		TargetDigest:   digestB,
		StartedAt:      base.Add(time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("insert failed op: %v", err)
	}
	if err := repo.MarkFailed(ctx, "deploy-clear-ok-fail", base.Add(2*time.Minute), "DIGEST_MISMATCH", "digest_mismatch: expected=sha256:b observed=sha256:c"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	// Retry op#2 now verifies successfully.
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-clear-ok-success",
		WorkerID:       "wicket",
		PreviousDigest: digestB,
		TargetDigest:   digestB,
		StartedAt:      base.Add(3 * time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("insert success op: %v", err)
	}
	if err := s.MarkVerifiedSucceeded(ctx, "deploy-clear-ok-success", digestB, base.Add(4*time.Minute)); err != nil {
		t.Fatalf("MarkVerifiedSucceeded: %v", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationErrorCode != "" || state.LastOperationError != "" {
		t.Errorf("read model error after verified success = %q/%q, want cleared", state.LastOperationErrorCode, state.LastOperationError)
	}
	if state.LastOperationStatus != DeployStatusSucceeded {
		t.Errorf("LastOperationStatus = %q, want SUCCEEDED", state.LastOperationStatus)
	}
	// The successful journal row carries no error; the FAILED op#1 row keeps
	// its DIGEST_MISMATCH history.
	succ, err := s.getDeploymentRecord(ctx, "deploy-clear-ok-success")
	if err != nil {
		t.Fatalf("get success row: %v", err)
	}
	if succ.ErrorCode != "" || succ.ErrorMessage != "" {
		t.Errorf("success journal row error = %q/%q, want cleared", succ.ErrorCode, succ.ErrorMessage)
	}
	fail, err := s.getDeploymentRecord(ctx, "deploy-clear-ok-fail")
	if err != nil {
		t.Fatalf("get failed row: %v", err)
	}
	if fail.ErrorCode != "DIGEST_MISMATCH" {
		t.Errorf("failed journal row ErrorCode = %q, want DIGEST_MISMATCH preserved in history", fail.ErrorCode)
	}
}
