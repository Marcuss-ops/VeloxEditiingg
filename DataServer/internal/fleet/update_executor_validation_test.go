package fleet

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestUpdate_EmptyPayload(t *testing.T) {
	backend, _ := stubBackends(t)
	e := NewUpdateExecutor(backend)
	op := mkOp("wkr-1", "", "")
	op.Payload = nil
	err := e.Execute(context.Background(), op)
	if err == nil || !strings.Contains(err.Error(), "payload empty") {
		t.Errorf("empty payload: want payload-empty error, got %v", err)
	}
}
func TestUpdate_PayloadMissingTargetDigest(t *testing.T) {
	backend, _ := stubBackends(t)
	e := NewUpdateExecutor(backend)
	op := mkOp("wkr-1", "", "") // empty target
	err := e.Execute(context.Background(), op)
	if err == nil || !strings.Contains(err.Error(), "target_digest missing") {
		t.Errorf("missing target_digest: want missing error, got %v", err)
	}
}
func TestUpdate_PayloadParseFails(t *testing.T) {
	backend, _ := stubBackends(t)
	e := NewUpdateExecutor(backend)
	op := mkOp("wkr-1", "ghcr.io/x/y@sha256:abc", "")
	op.Payload = []byte("{not-json")
	err := e.Execute(context.Background(), op)
	if err == nil || !strings.Contains(err.Error(), "payload parse") {
		t.Errorf("bad JSON: want parse error, got %v", err)
	}
}
func TestUpdate_InvalidImageRef_MobileTag(t *testing.T) {
	backend, st := stubBackends(t)
	st.registeredWorker = false // skip worker lookup by failing it later
	// Stub validator rejects (acts as the canonical deploy.ValidateImageRef).
	e := NewUpdateExecutor(backend)
	// Build a ref that LOOKS valid syntactically but uses :latest;
	// stubBackends' stub validator accepts any non-empty so we
	// inject the rejection via the shareable field.
	e.backend.Image = stubImageValidator{reject: errors.New("mobile tag :latest")}
	op := mkOp("wkr-1", "ghcr.io/x/y:latest", "")
	err := e.Execute(context.Background(), op)
	if err == nil || !strings.Contains(err.Error(), "target_digest validation") {
		t.Errorf("mobile-tag ref: want validator error, got %v", err)
	}
}
func TestUpdate_NilBackend(t *testing.T) {
	e := NewUpdateExecutor(UpdateBackend{})
	// Supply a parseable payload so the executor progresses
	// past Phase 1 (parsePayload) — otherwise the
	// "payload empty" short-circuit would mask the
	// "image validator not wired" failure we want to assert.
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil || !strings.Contains(err.Error(), "image validator not wired") {
		t.Errorf("nil backend: want validator-unwired error, got %v", err)
	}
}
func TestUpdate_ExecuteRejectsPartialBackendBeforeMutation(t *testing.T) {
	backend, state := stubBackends(t)
	backend.Docker = nil

	err := NewUpdateExecutor(backend).Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil || !strings.Contains(err.Error(), "missing dependencies: docker") {
		t.Fatalf("partial backend error = %v, want fail-closed docker diagnostic", err)
	}
	if len(state.drainCalls) != 0 {
		t.Fatalf("partial backend triggered worker mutation: drain calls=%v", state.drainCalls)
	}
}
func TestUpdate_UnregisteredWorker(t *testing.T) {
	backend, st := stubBackends(t)
	st.registeredWorker = false
	e := NewUpdateExecutor(backend)
	op := mkOp("wkr-ghost", validImageRef(), "")
	err := e.Execute(context.Background(), op)
	if err == nil || !errors.Is(err, ErrUnregisteredWorker) {
		t.Errorf("unregistered: want ErrUnregisteredWorker, got %v", err)
	}
	if !strings.Contains(err.Error(), "wkr-ghost") {
		t.Errorf("err must mention worker_id; got %v", err)
	}
}
func TestUpdate_EmptyRegistry(t *testing.T) {
	backend, st := stubBackends(t)
	st.prevDigest = "" // signals ErrDeploymentNotFound
	// The ErrEmptyRegistry path is the partially composed legacy surface: no
	// authenticated runtime reader available to bootstrap a baseline.
	backend.Runtime = nil
	e := NewUpdateExecutor(backend)
	op := mkOp("wkr-1", validImageRef(), "")
	err := e.Execute(context.Background(), op)
	if err == nil || !errors.Is(err, ErrEmptyRegistry) {
		t.Errorf("empty registry: want ErrEmptyRegistry, got %v", err)
	}
	if !strings.Contains(err.Error(), "wkr-1") {
		t.Errorf("err must mention worker_id; got %v", err)
	}
}
