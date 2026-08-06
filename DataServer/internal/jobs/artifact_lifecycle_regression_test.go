package jobs

import (
	"context"
	"errors"
	"testing"

	"velox-server/internal/statemachine"
)

type mutableArtifactReadiness bool

func (r *mutableArtifactReadiness) RequiredArtifactsReady(context.Context, string) (bool, error) {
	return bool(*r), nil
}

// TestArtifactContractLifecycleRequiresAwaitingArtifact proves the application
// transition boundary, rather than the compatibility state table alone, owns
// the artifact gate: RUNNING -> AWAITING_ARTIFACT is allowed, a direct
// RUNNING -> SUCCEEDED is rejected while artifacts are not ready, and only
// AWAITING_ARTIFACT -> SUCCEEDED succeeds after readiness becomes true.
func TestArtifactContractLifecycleRequiresAwaitingArtifact(t *testing.T) {
	writer := &transitionServiceWriter{}
	ready := mutableArtifactReadiness(false)
	svc, err := NewTransitionService(writer, &ready)
	if err != nil {
		t.Fatal(err)
	}

	if err := svc.Transition(context.Background(), "job-artifact-contract", StatusRunning, StatusAwaitingArtifact, statemachine.ActorSystem); err != nil {
		t.Fatalf("RUNNING -> AWAITING_ARTIFACT: %v", err)
	}

	if err := svc.Transition(context.Background(), "job-artifact-contract", StatusRunning, StatusSucceeded, statemachine.ActorArtifactFinalizer); !errors.Is(err, ErrArtifactNotReady) {
		t.Fatalf("direct RUNNING -> SUCCEEDED error=%v, want ErrArtifactNotReady", err)
	}
	if writer.calls != 1 {
		t.Fatalf("direct success changed writer calls=%d, want 1", writer.calls)
	}

	ready = true
	if err := svc.Transition(context.Background(), "job-artifact-contract", StatusAwaitingArtifact, StatusSucceeded, statemachine.ActorArtifactFinalizer); err != nil {
		t.Fatalf("AWAITING_ARTIFACT -> SUCCEEDED after readiness: %v", err)
	}
	if writer.calls != 2 || writer.from != StatusAwaitingArtifact || writer.to != StatusSucceeded {
		t.Fatalf("final transition=(calls=%d, from=%q, to=%q), want (2, AWAITING_ARTIFACT, SUCCEEDED)", writer.calls, writer.from, writer.to)
	}
}
