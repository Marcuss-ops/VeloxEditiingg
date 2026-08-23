package fleet

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
	"velox-server/internal/store"
)

func TestUpdate_DigestMismatchFailsClosed(t *testing.T) {
	backend, st := stubBackends(t)
	st.runtimeDigest = "sha256:" + strings.Repeat("c", 64) // worker runs C, target is A
	e := NewUpdateExecutor(backend)
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil {
		t.Fatal("digest mismatch must fail the update")
	}
	if !errors.Is(err, ErrRollbackSucceeded) {
		t.Fatalf("digest mismatch err = %v, want ErrRollbackSucceeded (rollback restores previous)", err)
	}
	if !strings.Contains(err.Error(), "digest_mismatch") || !strings.Contains(err.Error(), "expected=") || !strings.Contains(err.Error(), "observed=") {
		t.Errorf("digest mismatch err must carry error_code digest_mismatch with expected/observed; got %v", err)
	}
	if len(st.insertedRows) < 2 {
		t.Fatalf("rows = %d, want forward+rollback", len(st.insertedRows))
	}
	forward := st.insertedRows[0]
	if st.markedStatuses[forward.DeploymentID] != store.DeployStatusFailed {
		t.Fatalf("forward row status = %q, want FAILED (never SUCCEEDED unverified)", st.markedStatuses[forward.DeploymentID])
	}
	if st.observedVerifiedDigest != "" {
		t.Fatalf("MarkVerifiedSucceeded was reached on a mismatched digest: observed=%q", st.observedVerifiedDigest)
	}
	// The journal FAILED write carries the stable code separately from the
	// human-readable message (migration 153).
	if code := st.markedErrorCodes[forward.DeploymentID]; code != DeploymentErrorCodeDigestMismatch {
		t.Fatalf("forward row error_code = %q, want DIGEST_MISMATCH", code)
	}
}
func TestUpdate_StaleSessionCannotAdvance(t *testing.T) {
	backend, st := stubBackends(t)
	st.runtimeDigest = validImageRef()
	st.reconnectSessionID = "" // activate restarts the worker but it never reconnects
	e := NewUpdateExecutor(backend)
	e.waitReadyTimeout = 10 * time.Millisecond
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil {
		t.Fatal("stale-session rollout must fail")
	}
	if !strings.Contains(err.Error(), "NEW authenticated session") {
		t.Errorf("err must cite the new-session gate; got %v", err)
	}
	if !errors.Is(err, ErrRollbackSucceeded) {
		t.Fatalf("stale-session err = %v, want ErrRollbackSucceeded", err)
	}
	if len(st.insertedRows) < 2 {
		t.Fatalf("rows = %d, want forward+rollback", len(st.insertedRows))
	}
	if st.markedStatuses[st.insertedRows[0].DeploymentID] != store.DeployStatusFailed {
		t.Fatalf("forward row status = %q, want FAILED", st.markedStatuses[st.insertedRows[0].DeploymentID])
	}
}
func TestUpdate_PhaseSequenceRecorded(t *testing.T) {
	backend, st := stubBackends(t)
	st.runtimeDigest = validImageRef()
	e := NewUpdateExecutor(backend)
	if err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), "")); err != nil {
		t.Fatalf("happy path Execute returned err %v", err)
	}
	want := []string{RolloutPhaseDraining, RolloutPhaseDeploying, RolloutPhaseRestarting,
		RolloutPhaseWaitingReady, RolloutPhaseVerifyingDigest}
	if !reflect.DeepEqual(st.phases, want) {
		t.Fatalf("recorded phases = %v, want %v", st.phases, want)
	}
	if st.observedVerifiedDigest != validImageRef() {
		t.Fatalf("MarkVerifiedSucceeded observed digest = %q, want %q", st.observedVerifiedDigest, validImageRef())
	}
}
func TestUpdate_RecoveryReentryAfterCrashDuringDeployingNeverAssumesSuccess(t *testing.T) {
	backend, st := stubBackends(t)
	// Pre-restart executor already drained the worker; the flag persisted
	// through the crash. Re-entry must neither re-assert nor clear it.
	st.drain = true
	// The worker came back advertising C — neither the target nor the
	// previous digest.
	st.runtimeDigest = "sha256:" + strings.Repeat("c", 64)
	// A fresh Hello IS observed (new session) — the refusal is purely the
	// digest comparison, not the WAITING_READY session gate.
	st.reconnectSessionID = "grpc-session-2"

	e := NewUpdateExecutor(backend)
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil {
		t.Fatal("re-entry on a mismatched digest must fail the update")
	}
	if !errors.Is(err, ErrRollbackSucceeded) {
		t.Fatalf("re-entry mismatch err = %v, want ErrRollbackSucceeded (rollback restores previous)", err)
	}
	// No duplicated destructive side effect: the pre-restart-owned drain is
	// never re-asserted (no SetDrainMode(true)) and never released.
	if len(st.drainCalls) != 0 {
		t.Fatalf("re-entry re-touched the drain: calls=%v (pre-restart-owned drain must be left alone)", st.drainCalls)
	}
	if !st.drain {
		t.Fatal("re-entry cleared a drain it does not own")
	}
	// Never SUCCEEDED unverified: the reconciler compared running vs target
	// before deciding, and the verified terminal write was never reached.
	if st.observedVerifiedDigest != "" {
		t.Fatalf("MarkVerifiedSucceeded was reached on a mismatched digest: observed=%q", st.observedVerifiedDigest)
	}
	// The interrupted forward row is FAILED with the stable DIGEST_MISMATCH
	// code; the rollback row restored the previous digest.
	if len(st.insertedRows) < 2 {
		t.Fatalf("rows = %d, want forward+rollback", len(st.insertedRows))
	}
	forward := st.insertedRows[0]
	if st.markedStatuses[forward.DeploymentID] != store.DeployStatusFailed {
		t.Fatalf("forward row status = %q, want FAILED (never SUCCEEDED unverified)", st.markedStatuses[forward.DeploymentID])
	}
	if code := st.markedErrorCodes[forward.DeploymentID]; code != DeploymentErrorCodeDigestMismatch {
		t.Fatalf("forward row error_code = %q, want DIGEST_MISMATCH", code)
	}
}
func TestUpdate_RecoveryReentryAfterCrashDuringDeployingResumesOnMatchingDigest(t *testing.T) {
	backend, st := stubBackends(t)
	st.drain = true // the pre-restart executor's drain survived the crash
	st.runtimeDigest = validImageRef()
	st.reconnectSessionID = "grpc-session-2"

	e := NewUpdateExecutor(backend)
	if err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), "")); err != nil {
		t.Fatalf("re-entry resume returned err %v", err)
	}
	if len(st.drainCalls) != 0 {
		t.Fatalf("re-entry re-touched the drain: calls=%v", st.drainCalls)
	}
	if !st.drain {
		t.Fatal("re-entry cleared a drain it does not own (no-clobber contract)")
	}
	if len(st.insertedRows) != 1 {
		t.Fatalf("forward rows = %d, want exactly 1 (no rollback on a clean resume)", len(st.insertedRows))
	}
	if st.markedStatuses[st.insertedRows[0].DeploymentID] != store.DeployStatusSucceeded {
		t.Fatalf("forward row status = %q, want SUCCEEDED (verified)", st.markedStatuses[st.insertedRows[0].DeploymentID])
	}
	if st.observedVerifiedDigest != validImageRef() {
		t.Fatalf("MarkVerifiedSucceeded observed = %q, want %q (only the verified digest advances last-known-good)", st.observedVerifiedDigest, validImageRef())
	}
	// The full phase trail is recorded after restart: the resume is a real
	// rollout through every gate, not a shortcut to success.
	want := []string{RolloutPhaseDraining, RolloutPhaseDeploying, RolloutPhaseRestarting,
		RolloutPhaseWaitingReady, RolloutPhaseVerifyingDigest}
	if !reflect.DeepEqual(st.phases, want) {
		t.Fatalf("recorded phases = %v, want %v", st.phases, want)
	}
}
