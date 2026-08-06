package jobs

import (
	"context"
	"errors"
	"testing"

	"velox-server/internal/statemachine"
)

type transitionServiceWriter struct {
	calls int
	from  Status
	to    Status
}

func (w *transitionServiceWriter) SetStatus(_ context.Context, _ string, from, to Status) error {
	w.calls++
	w.from, w.to = from, to
	return nil
}
func (w *transitionServiceWriter) Fail(context.Context, string, string) error        { return nil }
func (w *transitionServiceWriter) Cancel(context.Context, string, string, int) error { return nil }
func (w *transitionServiceWriter) Delete(context.Context, string) error              { return nil }

type transitionServiceReadiness struct {
	requires bool
	ready    bool
}

func (r transitionServiceReadiness) RequiresArtifact(context.Context, string) (bool, error) {
	return r.requires, nil
}

func (r transitionServiceReadiness) RequiredArtifactsReady(context.Context, string) (bool, error) {
	return r.ready, nil
}

func TestTransitionServiceRejectsSuccessBeforeArtifactReady(t *testing.T) {
	writer := &transitionServiceWriter{}
	svc, err := NewTransitionService(writer, transitionServiceReadiness{requires: true, ready: false})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Transition(context.Background(), "job-1", StatusAwaitingArtifact, StatusSucceeded, statemachine.ActorArtifactFinalizer); !errors.Is(err, ErrArtifactNotReady) {
		t.Fatalf("error=%v, want ErrArtifactNotReady", err)
	}
	if writer.calls != 0 {
		t.Fatalf("writer calls=%d, want 0", writer.calls)
	}
}

func TestTransitionServiceRejectsNilReadinessForDirectSuccess(t *testing.T) {
	writer := &transitionServiceWriter{}
	svc, err := NewTransitionService(writer, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Transition(context.Background(), "job-render-only", StatusRunning, StatusSucceeded, statemachine.ActorArtifactFinalizer); !errors.Is(err, ErrArtifactNotReady) {
		t.Fatalf("nil readiness error=%v, want ErrArtifactNotReady", err)
	}
	if writer.calls != 0 {
		t.Fatalf("writer calls=%d, want 0", writer.calls)
	}
}

func TestTransitionServiceRequiresReadinessDependencyForSuccess(t *testing.T) {
	writer := &transitionServiceWriter{}
	svc, err := NewTransitionService(writer, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Transition(context.Background(), "job-1", StatusAwaitingArtifact, StatusSucceeded, statemachine.ActorArtifactFinalizer); !errors.Is(err, ErrArtifactNotReady) {
		t.Fatalf("error=%v, want ErrArtifactNotReady", err)
	}
}

func TestTransitionServiceRejectsMissingActor(t *testing.T) {
	writer := &transitionServiceWriter{}
	svc, err := NewTransitionService(writer, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Transition(context.Background(), "job-1", StatusRunning, StatusAwaitingArtifact, ""); err == nil {
		t.Fatal("missing actor unexpectedly accepted")
	}
	if writer.calls != 0 {
		t.Fatalf("writer calls=%d, want 0", writer.calls)
	}
}

func TestTransitionServiceRejectsWrongActor(t *testing.T) {
	writer := &transitionServiceWriter{}
	svc, err := NewTransitionService(writer, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Transition(context.Background(), "job-1", StatusRunning, StatusAwaitingArtifact, statemachine.ActorWorker); err == nil {
		t.Fatal("wrong actor unexpectedly accepted")
	}
	if writer.calls != 0 {
		t.Fatalf("writer calls=%d, want 0", writer.calls)
	}
}

func TestTransitionServiceAllowsExplicitNoArtifactDirectSuccess(t *testing.T) {
	writer := &transitionServiceWriter{}
	contract := transitionServiceReadiness{requires: false, ready: false}
	svc, err := NewTransitionService(writer, contract)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Transition(context.Background(), "job-render-only", StatusRunning, StatusSucceeded, statemachine.ActorArtifactFinalizer); err != nil {
		t.Fatalf("no-artifact direct success: %v", err)
	}
	if writer.calls != 1 || writer.from != StatusRunning || writer.to != StatusSucceeded {
		t.Fatalf("transition=(%d,%q,%q), want (1,RUNNING,SUCCEEDED)", writer.calls, writer.from, writer.to)
	}
}

func TestTransitionServiceDelegatesNonTerminalTransition(t *testing.T) {
	writer := &transitionServiceWriter{}
	svc, err := NewTransitionService(writer, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Transition(context.Background(), "job-1", StatusRunning, StatusAwaitingArtifact, statemachine.ActorSystem); err != nil {
		t.Fatal(err)
	}
	if writer.calls != 1 || writer.from != StatusRunning || writer.to != StatusAwaitingArtifact {
		t.Fatalf("delegation=(%d,%q,%q), want (1,RUNNING,AWAITING_ARTIFACT)", writer.calls, writer.from, writer.to)
	}
}
