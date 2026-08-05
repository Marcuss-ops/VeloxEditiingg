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

type transitionServiceReadiness bool

func (r transitionServiceReadiness) RequiredArtifactsReady(context.Context, string) (bool, error) {
	return bool(r), nil
}

func TestTransitionServiceRejectsSuccessBeforeArtifactReady(t *testing.T) {
	writer := &transitionServiceWriter{}
	svc, err := NewTransitionService(writer, transitionServiceReadiness(false))
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
