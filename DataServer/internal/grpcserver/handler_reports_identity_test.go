package grpcserver

import (
	"context"
	"errors"
	"testing"

	"velox-server/internal/ingest"
	"velox-server/internal/taskattempts"

	pb "velox-shared/controltransport/pb"
)

// =====================================================================
// (A) HandlerLayerDrop — handleTaskResult fires on a spoofed wire and
// short-circuits the close-write + Job roll-up + artifact-register
// paths. Independent of the canonical sentinel — today the
// cheap AttemptNumber<=0 check fires first; once pb.TaskResult grows
// the AttemptNumber field (audit follow-up), this assertion will
// route through the lookup-miss or strict-compare sentinel instead.
// =====================================================================

func TestHandleTaskResult_RejectsIdentitySpoofing_HandlerLayerDrop(t *testing.T) {
	handler, taskRepo, jobsRepo, outputArts := buildSpoofHandler(t)
	fx := newSpoofFixture()

	tr := &pb.TaskResult{
		TaskId:        fx.taskID,
		AttemptId:     fx.wireAttemptID,
		AttemptNumber: 1,            // matches canonical seeded attempt.AttemptNumber
		LeaseId:       fx.wireLease, // SPOOF: canonical is fx.canonicalLease
		JobId:         fx.wireJobID,
		Status:        "succeeded",
	}

	handler.handleTaskResult(fx.workerID, tr, nil)

	// The handler dropped the spoof — close-write + roll-up +
	// artifact register MUST stay zero regardless of which validator
	// branch fired.

	// (1) close-write: validator must reject BEFORE the taskRepo
	// close-write is reached (ValidateIdentityTuple is the FIRST step
	// in IngestTaskResult post-PR-2).
	if got := taskRepo.transitionCalls; got != 0 {
		t.Errorf("taskRepo.TransitionTaskToTerminalAtomic calls = %d; want 0 (handleTaskResult must short-circuit before close-write)", got)
	}

	// (2) Job roll-up: maybeTransitionJob only runs after a
	// successful close-write. With close-write NEVER firing,
	// setStatusCalls must be 0.
	if got := jobsRepo.setStatusCalls; got != 0 {
		t.Errorf("jobsRepo.SetStatus calls = %d; want 0 (close-write never fired ⇒ roll-up never fires)", got)
	}

	// (3) artifact register: step (3) of IngestTaskResult runs AFTER
	// the close-write. A rejection at the validator means artifacts
	// are NEVER registered.
	if got := outputArts.registerCalls; got != 0 {
		t.Errorf("outputArtRepo.Register calls = %d; want 0 (validator rejection blocks step-3)", got)
	}
}

// =====================================================================
// (B) CanonicalSentinel_WireLeaseIDMismatch — directly probe
// ValidateIdentityTuple on a fully-populated IngestCommand whose
// lease_id deliberately differs from the canonical seed. Confirms
// the canonical taskattempts.ErrIdentityMismatch wrap is produced on
// the lookup-miss path the handler will route through once
// pb.TaskResult grows the AttemptNumber field.
//
// This test pins the canonical sentinel: a future refactor that
// drops the %w wrap would fail this test even if the side-effect
// counters in (A) were silently loosened.
// =====================================================================
func TestHandleTaskResult_ValidateIdentityTuple_CanonicalSentinel_WireLeaseIDMismatch(t *testing.T) {
	fx := newSpoofFixture()

	// Bind a fresh svc to a single canonical-seeded attempt for the
	// (task_id, worker_id, canonical_lease_id) tuple. The validator
	// probe below supplies a fully-populated IngestCommand whose
	// LeaseID deliberately differs from fx.canonicalLease so the
	// lookup at (fx.taskID, fx.workerID, fx.wireLease) returns nil →
	// ErrIdentityMismatch wrap.
	attempts := &spoofStubAttemptRepo{}
	attempts.seedCanonical(fx.taskID, fx.workerID, fx.canonicalLease)
	svc, err := ingest.NewTaskReportIngestionService(
		&spoofStubTaskRepo{},
		&spoofStubJobsRepo{},
		attempts,
		newSpoofStubOutputArts(),
	)
	if err != nil {
		t.Fatalf("NewTaskReportIngestionService: %v", err)
	}

	// Drive ValidateIdentityTuple with a fully populated command —
	// AttemptNumber=1 to skip the cheap-attempt-count branch.
	vErr := svc.ValidateIdentityTuple(context.Background(), ingest.IngestCommand{
		TaskID:        fx.taskID,
		AttemptID:     fx.wireAttemptID, // matches canonical
		LeaseID:       fx.wireLease,     // SPOOF: differs from canonical
		WorkerID:      fx.workerID,
		JobID:         fx.wireJobID,
		AttemptNumber: 1,
	})
	if vErr == nil {
		t.Errorf("ValidateIdentityTuple returned nil for spoofed lease_id; want wrapped ErrIdentityMismatch")
	}
	if vErr != nil && !errors.Is(vErr, taskattempts.ErrIdentityMismatch) {
		t.Errorf("ValidateIdentityTuple returned %v; want taskattempts.ErrIdentityMismatch wrapped", vErr)
	}
}
