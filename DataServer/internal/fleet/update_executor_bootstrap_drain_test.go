package fleet

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
	"velox-server/internal/store"
	"velox-server/internal/workers"
)

func TestUpdate_BootstrapLedgerFromAuthenticatedRuntime(t *testing.T) {
	backend, st := stubBackends(t)
	backend.Runtime = st
	target := validImageRef()
	st.prevDigest = ""
	st.runtimeDigest = target

	if err := NewUpdateExecutor(backend).Execute(context.Background(), mkOp("wkr-1", target, "")); err != nil {
		t.Fatalf("bootstrap returned err %v", err)
	}
	if len(st.baselineRows) != 1 {
		t.Fatalf("baseline rows = %d, want 1", len(st.baselineRows))
	}
	row := st.baselineRows[0]
	if row.Status != store.DeployStatusSucceeded || row.TargetDigest != target {
		t.Fatalf("baseline row = %+v, want SUCCEEDED target=%s", row, target)
	}
	if row.PreviousDigest != "" || row.FinishedAt == nil {
		t.Fatalf("baseline rollback provenance = %q/%v, want missing provenance and finished_at", row.PreviousDigest, row.FinishedAt)
	}
	if len(st.insertedRows) != 0 || len(st.drainCalls) != 0 {
		t.Fatalf("bootstrap performed rollout mutations: rows=%d drain=%v", len(st.insertedRows), st.drainCalls)
	}
}
func TestUpdate_BootstrapDigestMismatchDoesNotMutate(t *testing.T) {
	backend, st := stubBackends(t)
	backend.Runtime = st
	st.prevDigest = ""
	st.runtimeDigest = "sha256:" + strings.Repeat("b", 64)
	err := NewUpdateExecutor(backend).Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil || !errors.Is(err, ErrBootstrapDigestMismatch) {
		t.Fatalf("digest mismatch err = %v, want ErrBootstrapDigestMismatch", err)
	}
	if len(st.baselineRows) != 0 || len(st.insertedRows) != 0 || len(st.drainCalls) != 0 {
		t.Fatalf("digest mismatch mutated state: baseline=%d rows=%d drain=%v", len(st.baselineRows), len(st.insertedRows), st.drainCalls)
	}
}
func TestUpdate_BootstrapMissingDigestFailsClosed(t *testing.T) {
	backend, st := stubBackends(t)
	backend.Runtime = st
	st.prevDigest = ""
	err := NewUpdateExecutor(backend).Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil || !errors.Is(err, ErrBootstrapUnverifiable) {
		t.Fatalf("missing runtime digest err = %v, want ErrBootstrapUnverifiable", err)
	}
	if len(st.baselineRows) != 0 || len(st.drainCalls) != 0 {
		t.Fatalf("missing digest mutated state: baseline=%d drain=%v", len(st.baselineRows), st.drainCalls)
	}
}
func TestUpdate_BootstrapDisconnectedFailsClosed(t *testing.T) {
	backend, st := stubBackends(t)
	backend.Runtime = st
	st.prevDigest = ""
	st.runtimeDigest = validImageRef()
	st.connectionState = workers.ConnectionOffline
	err := NewUpdateExecutor(backend).Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil || !errors.Is(err, ErrBootstrapWorkerDisconnected) {
		t.Fatalf("disconnected bootstrap err = %v, want ErrBootstrapWorkerDisconnected", err)
	}
	if len(st.baselineRows) != 0 || len(st.drainCalls) != 0 {
		t.Fatalf("disconnected bootstrap mutated state: baseline=%d drain=%v", len(st.baselineRows), st.drainCalls)
	}
}
func TestUpdate_IdempotentAlreadyCurrentSkipsRollout(t *testing.T) {
	backend, st := stubBackends(t)
	backend.Runtime = st
	target := validImageRef()
	st.prevDigest = target
	st.runtimeDigest = target

	if err := NewUpdateExecutor(backend).Execute(context.Background(), mkOp("wkr-1", target, "")); err != nil {
		t.Fatalf("already-current update returned err %v", err)
	}
	if len(st.baselineRows) != 0 || len(st.insertedRows) != 0 || len(st.drainCalls) != 0 {
		t.Fatalf("already-current ran rollout: baseline=%d rows=%d drain=%v", len(st.baselineRows), len(st.insertedRows), st.drainCalls)
	}
}
func TestUpdate_NormalRolloutStillUsesLedgerPreviousDigest(t *testing.T) {
	backend, st := stubBackends(t)
	backend.Runtime = st
	previous := "ghcr.io/marcuss-ops/velox-worker@sha256:" + strings.Repeat("b", 64)
	st.prevDigest = previous
	target := validImageRef()
	st.runtimeDigest = target // post-restart authenticated digest matches the target

	if err := NewUpdateExecutor(backend).Execute(context.Background(), mkOp("wkr-1", target, "")); err != nil {
		t.Fatalf("normal rollout returned err %v", err)
	}
	if len(st.insertedRows) != 1 || st.insertedRows[0].PreviousDigest != previous {
		t.Fatalf("normal rollout row = %+v, want previous=%s", st.insertedRows, previous)
	}
}
func TestUpdate_DrainsAndReleasesOnSuccess(t *testing.T) {
	backend, st := stubBackends(t)
	st.runtimeDigest = validImageRef() // authenticated digest matches target
	e := NewUpdateExecutor(backend)
	if err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), "")); err != nil {
		t.Fatalf("happy path Execute returned err %v", err)
	}
	if got, want := st.drainCalls, []bool{true, false}; !reflect.DeepEqual(got, want) {
		t.Fatalf("drain calls = %v, want %v", got, want)
	}
	if st.drain {
		t.Fatal("worker remained drained after successful update")
	}
}
func TestUpdate_PreservesExistingDrain(t *testing.T) {
	backend, st := stubBackends(t)
	st.drain = true
	st.runtimeDigest = validImageRef()
	e := NewUpdateExecutor(backend)
	if err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), "")); err != nil {
		t.Fatalf("pre-drained worker update returned err %v", err)
	}
	if len(st.drainCalls) != 0 {
		t.Fatalf("executor changed an operator-owned drain: calls=%v", st.drainCalls)
	}
	if !st.drain {
		t.Fatal("executor cleared an operator-owned drain")
	}
}
func TestUpdate_ActiveTasksTimeoutReleasesOwnedDrain(t *testing.T) {
	backend, st := stubBackends(t)
	st.activeTasksZero = false // stays busy
	e := NewUpdateExecutor(backend)
	e.drainTimeout = 10 * time.Millisecond
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil || !strings.Contains(err.Error(), "did not reach DRAINING") {
		t.Errorf("active-tasks timeout: want drain fail, got %v", err)
	}
	if got, want := st.drainCalls, []bool{true, false}; !reflect.DeepEqual(got, want) {
		t.Fatalf("drain calls after timeout = %v, want %v", got, want)
	}
	if st.drain {
		t.Fatal("worker remained drained after pre-forward timeout")
	}
}
func TestUpdate_DrainNotReflectedFailsClosed(t *testing.T) {
	// SetDrainMode returns nil but the registry read model never
	// reflects drain=true (drainNotApplied). active_tasks is already
	// 0 — yet the rollout MUST NOT proceed: DRAINING requires the
	// authoritative drain flag, not the drain() call's return value.
	backend, st := stubBackends(t)
	st.drainNotApplied = true
	e := NewUpdateExecutor(backend)
	e.drainTimeout = 10 * time.Millisecond
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil || !strings.Contains(err.Error(), "did not reach DRAINING") {
		t.Errorf("drain-not-reflected: want DRAINING fail, got %v", err)
	}
	// No deployment row may be inserted: the rollout must not start.
	if len(st.insertedRows) != 0 {
		t.Fatalf("rollout started despite worker not in DRAINING: rows=%d", len(st.insertedRows))
	}
	// The owned drain is still released on the failed gate.
	if got, want := st.drainCalls, []bool{true, false}; !reflect.DeepEqual(got, want) {
		t.Fatalf("drain calls after failed DRAINING gate = %v, want %v", got, want)
	}
	if st.drain {
		t.Fatal("worker remained drained after pre-forward DRAINING gate failure")
	}
}
