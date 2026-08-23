package store

import (
	"context"
	"errors"
	_ "github.com/mattn/go-sqlite3"
	"testing"
	"time"
)

func TestDeploymentStore_TerminalStatusIsImmutable(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	rec := DeploymentRecord{
		DeploymentID:   "deploy-immutable",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusSucceeded, base.Add(time.Minute)); err != nil {
		t.Fatalf("PENDING -> SUCCEEDED: %v", err)
	}

	// Resurrection attempt: SUCCEEDED -> FAILED must be rejected.
	err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusFailed, base.Add(2*time.Minute))
	if !errors.Is(err, ErrIllegalDeploymentTransition) {
		t.Fatalf("SUCCEEDED -> FAILED error = %v, want ErrIllegalDeploymentTransition", err)
	}

	got, err := s.GetLatestDeploymentForWorker(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != DeployStatusSucceeded {
		t.Errorf("Status after rejected transition = %q, want SUCCEEDED (row must stay terminal)", got.Status)
	}
	// "Writes nothing": the rejected FAILED attempt must not restamp
	// finished_at, must not leak an error onto the SUCCEEDED row, and the
	// read-model projection must stay byte-identical to the pre-attempt
	// state. The transition API validates BEFORE it writes, so a rejected
	// transition is a complete no-op.
	if got.FinishedAt == nil || !got.FinishedAt.Equal(base.Add(time.Minute)) {
		t.Errorf("FinishedAt after rejected transition = %v, want %v (rejected attempt must not restamp)", got.FinishedAt, base.Add(time.Minute))
	}
	if got.ErrorCode != "" || got.ErrorMessage != "" {
		t.Errorf("journal error after rejected transition = code=%q msg=%q, want empty (rejected attempt must not write error)", got.ErrorCode, got.ErrorMessage)
	}

	state, err := s.GetWorkerDeploymentState(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationStatus != DeployStatusSucceeded {
		t.Errorf("LastOperationStatus after rejected transition = %q, want SUCCEEDED (projection must stay in sync)", state.LastOperationStatus)
	}
	if state.LastOperationErrorCode != "" || state.LastOperationError != "" {
		t.Errorf("projection error after rejected transition = code=%q msg=%q, want empty (rejected attempt must not write error)", state.LastOperationErrorCode, state.LastOperationError)
	}
}
func TestDeploymentStore_TransitionUpdatesRecordAndProjectionAtomically(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	rec := DeploymentRecord{
		DeploymentID:   "deploy-atomic-pos",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert: %v", err)
	}

	finished := base.Add(time.Minute)
	if err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusSucceeded, finished); err != nil {
		t.Fatalf("UpdateDeploymentStatus(SUCCEEDED): %v", err)
	}

	// Journal: the single call updated the record.
	got, err := s.GetLatestDeploymentForWorker(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("GetLatestDeploymentForWorker: %v", err)
	}
	if got.Status != DeployStatusSucceeded {
		t.Errorf("journal Status = %q, want SUCCEEDED", got.Status)
	}
	if got.FinishedAt == nil || !got.FinishedAt.Equal(finished) {
		t.Errorf("journal FinishedAt = %v, want %v", got.FinishedAt, finished)
	}

	// Read model: the SAME call projected it, with no reconstruction needed.
	state, err := s.GetWorkerDeploymentState(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationID != rec.DeploymentID {
		t.Errorf("projection LastOperationID = %q, want %q", state.LastOperationID, rec.DeploymentID)
	}
	if state.LastOperationStatus != DeployStatusSucceeded {
		t.Errorf("projection LastOperationStatus = %q, want SUCCEEDED", state.LastOperationStatus)
	}
	if state.LastOperationErrorCode != "" || state.LastOperationError != "" {
		t.Errorf("projection error = code=%q msg=%q, want empty on a clean success", state.LastOperationErrorCode, state.LastOperationError)
	}
}
func TestDeploymentStore_FailedCannotResurrectToSucceeded(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	rec := DeploymentRecord{
		DeploymentID:   "deploy-failed-term",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusFailed, base.Add(time.Minute)); err != nil {
		t.Fatalf("PENDING -> FAILED: %v", err)
	}

	err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusSucceeded, base.Add(2*time.Minute))
	if !errors.Is(err, ErrIllegalDeploymentTransition) {
		t.Fatalf("FAILED -> SUCCEEDED error = %v, want ErrIllegalDeploymentTransition", err)
	}
	// The rollback marker path is equally barred: a FAILED forward row must
	// not be re-labelled ROLLED_BACK.
	if err := s.MarkDeploymentRolledBack(ctx, rec.DeploymentID, base.Add(2*time.Minute), true, ""); !errors.Is(err, ErrIllegalDeploymentTransition) {
		t.Fatalf("FAILED -> ROLLED_BACK error = %v, want ErrIllegalDeploymentTransition", err)
	}

	got, err := s.GetLatestDeploymentForWorker(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != DeployStatusFailed {
		t.Errorf("Status after rejected resurrections = %q, want FAILED", got.Status)
	}
}
func TestDeploymentStore_RolledBackIsTerminal(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	rec := DeploymentRecord{
		DeploymentID:   "deploy-rollback-term",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('a'), // rollback restores previous
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
		IsRollback:     true,
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert rollback row: %v", err)
	}
	if err := s.MarkDeploymentRolledBack(ctx, rec.DeploymentID, base.Add(time.Minute), true, ""); err != nil {
		t.Fatalf("PENDING -> ROLLED_BACK: %v", err)
	}

	err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusSucceeded, base.Add(2*time.Minute))
	if !errors.Is(err, ErrIllegalDeploymentTransition) {
		t.Fatalf("ROLLED_BACK -> SUCCEEDED error = %v, want ErrIllegalDeploymentTransition", err)
	}

	got, err := s.GetLatestDeploymentForWorker(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != DeployStatusRolledBack || !got.IsRollback {
		t.Errorf("row after rejected transition = %s/is_rollback=%v, want ROLLED_BACK/is_rollback=true", got.Status, got.IsRollback)
	}
}
func TestDeploymentStore_RollbackFailedIsTerminal(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	rec := DeploymentRecord{
		DeploymentID:   "deploy-rollback-failed",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('a'), // rollback restores previous
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
		IsRollback:     true,
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert rollback row: %v", err)
	}
	if err := s.MarkDeploymentRolledBack(ctx, rec.DeploymentID, base.Add(time.Minute), false, "ROLLBACK_FAILED"); err != nil {
		t.Fatalf("MarkDeploymentRolledBack(false): %v", err)
	}

	got, err := s.GetLatestDeploymentForWorker(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != DeployStatusFailed || !got.IsRollback {
		t.Errorf("row = %s/is_rollback=%v, want FAILED/is_rollback=true (rollback also failed)", got.Status, got.IsRollback)
	}

	// The rollback-failed row is terminal: it can be neither revived to
	// SUCCEEDED nor re-labelled ROLLED_BACK.
	err = s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusSucceeded, base.Add(2*time.Minute))
	if !errors.Is(err, ErrIllegalDeploymentTransition) {
		t.Fatalf("FAILED -> SUCCEEDED error = %v, want ErrIllegalDeploymentTransition", err)
	}
	err = s.MarkDeploymentRolledBack(ctx, rec.DeploymentID, base.Add(2*time.Minute), true, "")
	if !errors.Is(err, ErrIllegalDeploymentTransition) {
		t.Fatalf("FAILED -> ROLLED_BACK error = %v, want ErrIllegalDeploymentTransition", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationStatus != DeployStatusFailed {
		t.Errorf("LastOperationStatus = %q, want FAILED (projection stays with the terminal row)", state.LastOperationStatus)
	}
}
func TestDeploymentStore_ProjectionFailureRollsBackTransition(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)

	rec := DeploymentRecord{
		DeploymentID:   "deploy-atomic-tx",
		WorkerID:       "wicket",
		PreviousDigest: deploymentTestDigest('a'),
		TargetDigest:   deploymentTestDigest('b'),
		StartedAt:      base,
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}
	if err := s.InsertDeploymentRecord(ctx, rec); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Force the projection write to fail AFTER the journal UPDATE: the
	// BEFORE INSERT trigger fires for the upsert's candidate row (including
	// the ON CONFLICT DO UPDATE path) and aborts the whole statement.
	if _, err := s.db.ExecContext(ctx, `
CREATE TRIGGER fail_projection
BEFORE INSERT ON worker_deployment_state
BEGIN
  SELECT RAISE(ABORT, 'forced projection failure');
END;`); err != nil {
		t.Fatalf("create projection-failure trigger: %v", err)
	}

	err := s.UpdateDeploymentStatus(ctx, rec.DeploymentID, DeployStatusSucceeded, base.Add(time.Minute))
	if err == nil {
		t.Fatal("UpdateDeploymentStatus = nil, want projection failure to abort the transition")
	}

	got, err := s.GetLatestDeploymentForWorker(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != DeployStatusPending {
		t.Errorf("Status = %q, want PENDING (journal UPDATE must roll back with the failed projection)", got.Status)
	}

	// The read model must be untouched by the rolled-back transition.
	state, err := s.GetWorkerDeploymentState(ctx, rec.WorkerID)
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.LastOperationStatus != DeployStatusPending {
		t.Errorf("LastOperationStatus = %q, want PENDING (projection must stay with the pre-transition row)", state.LastOperationStatus)
	}
}
