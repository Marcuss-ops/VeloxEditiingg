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

func TestUpdate_CosignFail_RollsBack(t *testing.T) {
	backend, _ := stubBackends(t)
	// Override Cosign BEFORE NewUpdateExecutor — the constructor
	// stores UpdateBackend by value, so a post-construction
	// interface override would not reach the executor's stored
	// surface. Other tests (e.g. TestUpdate_RollbackSucceeds
	// AfterForwardFail) mutate the shared state via st.cosignErr
	// because the stubBackendsState is the SAME pointer held in
	// the interface; an interface OVERRIDE like this one needs
	// to happen BEFORE the value-copy constructor.
	backend.Cosign = stubBackendCosignErr{errors.New("invalid signature")}
	e := NewUpdateExecutor(backend)
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil || !errors.Is(err, ErrRollbackSucceeded) {
		t.Errorf("cosign fail: want ErrRollbackSucceeded wrap, got %v", err)
	}
	if !strings.Contains(err.Error(), "rollback_ok to") {
		t.Errorf("cosign fail err must surface rollback_ok to <digest>; got %v", err)
	}
}
func TestUpdate_PullImageFail_RollsBack(t *testing.T) {
	backend, st := stubBackends(t)
	st.pullErr = errors.New("network unreachable")
	e := NewUpdateExecutor(backend)
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	// Pull failure cascades through forward AND rollback because
	// both share the same docker client (production realistic:
	// a hard network outage hits both pulls). Surface as
	// ErrRollbackFailed so the operator gets the "real" signal.
	if err == nil || !errors.Is(err, ErrRollbackFailed) {
		t.Errorf("pull fail: want ErrRollbackFailed (cascade), got %v", err)
	}
	if !strings.Contains(err.Error(), "activate image") {
		t.Errorf("pull fail err must mention step name; got %v", err)
	}
}
func TestUpdate_ComposeRestartFail_RollsBack(t *testing.T) {
	backend, st := stubBackends(t)
	st.composeErr = errors.New("compose file missing")
	e := NewUpdateExecutor(backend)
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	// Compose restart failure cascades through forward AND rollback
	// (production realistic: a broken compose.yml hits both the
	// target and previous-digest restart). ErrRollbackFailed.
	if err == nil || !errors.Is(err, ErrRollbackFailed) {
		t.Errorf("compose fail: want ErrRollbackFailed (cascade), got %v", err)
	}
}
func TestUpdate_ContainerUnhealthy_RollsBack(t *testing.T) {
	backend, st := stubBackends(t)
	st.containerRunning = false
	e := NewUpdateExecutor(backend)
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	// Container-running check is the same BEFORE and AFTER
	// rollback (the stub state isn't cleared), so both forward
	// and rollback's container check fail. ErrRollbackFailed.
	if err == nil || !errors.Is(err, ErrRollbackFailed) {
		t.Errorf("container unhealthy: want ErrRollbackFailed (cascade), got %v", err)
	}
	if !strings.Contains(err.Error(), ErrContainerUnhealthy.Error()) {
		t.Errorf("err must mention ErrContainerUnhealthy; got %v", err)
	}
}
func TestUpdate_HealthReadyFail_RollsBack(t *testing.T) {
	backend, st := stubBackends(t)
	st.healthErr = errors.New("curl exit 7")
	e := NewUpdateExecutor(backend)
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	// /health/ready via SSHClient.Run is BOTH forward AND
	// rollback's check, sharing the same surface state. A
	// curl/network failure cascades — the rollback's health
	// poll also fails. ErrRollbackFailed (same pattern as
	// pull/compose/container cascade).
	if err == nil || !errors.Is(err, ErrRollbackFailed) {
		t.Errorf("health fail: want ErrRollbackFailed (cascade), got %v", err)
	}
}
func TestUpdate_MasterDisconnected_RollsBack(t *testing.T) {
	backend, st := stubBackends(t)
	st.sessionActive = false // the worker never reconnects on a new session
	e := NewUpdateExecutor(backend)
	e.waitReadyTimeout = 10 * time.Millisecond
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil || !errors.Is(err, ErrRollbackSucceeded) {
		t.Errorf("master disconnected: want ErrRollbackSucceeded wrap, got %v", err)
	}
}
func TestUpdate_SmokeFail_RollsBack(t *testing.T) {
	backend, st := stubBackends(t)
	st.runtimeDigest = validImageRef() // digest verifies; the smoke is the rejecting gate
	st.smokeErr = errors.New("ffmpeg rc=1")
	e := NewUpdateExecutor(backend)
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil || !errors.Is(err, ErrRollbackSucceeded) {
		t.Errorf("smoke fail: want ErrRollbackSucceeded wrap, got %v", err)
	}
	if !strings.Contains(err.Error(), ErrSmokeFailed.Error()) {
		t.Errorf("err must mention ErrSmokeFailed; got %v", err)
	}
}
func TestUpdate_DriveDeliveryMissing_RollsBack(t *testing.T) {
	backend, st := stubBackends(t)
	st.runtimeDigest = validImageRef() // digest verifies; the Drive check is the rejecting gate
	st.driveErr = ErrDriveDeliveryMissing
	e := NewUpdateExecutor(backend)
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	// Drive verify is FORWARD-ONLY (not in the rollback
	// cascade), so rollback succeeds → ErrRollbackSucceeded.
	// runRollback uses %v (not %w) for forwardErr when wrapping
	// ErrRollbackSucceeded, so the original sentinel is NOT
	// in the error chain — assert its string-Error() form
	// instead (matches TestUpdate_SmokeFail_RollsBack pattern).
	if err == nil || !errors.Is(err, ErrRollbackSucceeded) {
		t.Errorf("drive missing: want ErrRollbackSucceeded wrap, got %v", err)
	}
	if !strings.Contains(err.Error(), ErrDriveDeliveryMissing.Error()) {
		t.Errorf("err message must mention ErrDriveDeliveryMissing; got %v", err)
	}
}
func TestUpdate_RollbackSucceedsAfterForwardFail(t *testing.T) {
	backend, st := stubBackends(t)
	st.cosignErr = errors.New("sig fail")
	e := NewUpdateExecutor(backend)
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil {
		t.Fatal("forward fail with rollback OK should return wrapped error")
	}
	if !errors.Is(err, ErrRollbackSucceeded) {
		t.Errorf("expected ErrRollbackSucceeded, got %v", err)
	}
	// Three rows expected: PENDING forward + PENDING rollback + 2x terminal
	// transitions. Forward row gets status=FAILED; rollback row gets status=ROLLED_BACK.
	if len(st.insertedRows) != 2 {
		t.Errorf("expected 2 inserted rows, got %d", len(st.insertedRows))
	}
	if st.insertedRows[0].IsRollback || st.insertedRows[1].IsRollback == false {
		t.Errorf("expected rows: forward(0, is_rollback=false) + rollback(1, is_rollback=true); got is_rollback=[%v,%v]",
			st.insertedRows[0].IsRollback, st.insertedRows[1].IsRollback)
	}
	// Forward row should be marked FAILED; rollback row should be marked ROLLED_BACK.
	var forwardID, rollbackID string
	for _, r := range st.insertedRows {
		if !r.IsRollback {
			forwardID = r.DeploymentID
		} else {
			rollbackID = r.DeploymentID
		}
	}
	if got := st.markedStatuses[forwardID]; got != store.DeployStatusFailed {
		t.Errorf("forward row status = %q, want FAILED", got)
	}
	if ok, found := st.rolledBack[rollbackID]; !found || !ok {
		t.Errorf("rollback row rolledBack = (%v, %v), want (true, true)", found, ok)
	}
	if got, want := st.drainCalls, []bool{true, false}; !reflect.DeepEqual(got, want) {
		t.Errorf("rollback-success drain calls = %v, want %v", got, want)
	}
	if st.drain {
		t.Error("worker remained drained after successful rollback")
	}
}
func TestUpdate_RollbackFailsAfterForwardFail(t *testing.T) {
	backend, st := stubBackends(t)
	st.cosignErr = errors.New("sig fail")
	st.pullErr = errors.New("rollback pull fail")
	// Make the rollback's compose + container + health steps
	// succeed so the only rollback-side failure is the first
	// pull (matching real-world: stale runner pulling a missing
	// image deep in a network outage).
	e := NewUpdateExecutor(backend)
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil {
		t.Fatal("forward fail + rollback fail must return non-nil err")
	}
	if !errors.Is(err, ErrRollbackFailed) {
		t.Errorf("expected ErrRollbackFailed, got %v", err)
	}
	if got, want := st.drainCalls, []bool{true}; !reflect.DeepEqual(got, want) {
		t.Errorf("rollback-failure drain calls = %v, want %v", got, want)
	}
	if !st.drain {
		t.Error("worker was undrained after failed rollback")
	}
	// The failed rollback cascade records its own stable error code on the
	// rollback row (migration 153): forward FAILED + rollback FAILED are
	// distinguishable in the audit.
	var rollbackID string
	for _, r := range st.insertedRows {
		if r.IsRollback {
			rollbackID = r.DeploymentID
		}
	}
	if rollbackID == "" {
		t.Fatal("no rollback row inserted")
	}
	if code := st.rolledBackErrorCodes[rollbackID]; code != DeploymentErrorCodeRollbackFailed {
		t.Errorf("rollback row error_code = %q, want ROLLBACK_FAILED", code)
	}
}
func TestUpdate_PayloadSuppliesPreviousDigest(t *testing.T) {
	backend, st := stubBackends(t)
	// Make DB snapshot empty so any caller-supplied previous_digest
	// would otherwise trigger ErrEmptyRegistry.
	st.prevDigest = ""
	st.runtimeDigest = validImageRef() // authenticated digest matches target
	e := NewUpdateExecutor(backend)
	op := mkOp("wkr-1", validImageRef(), "sha256:caller-supplied")
	err := e.Execute(context.Background(), op)
	if err != nil {
		t.Errorf("payload-supplied previous_digest should bypass DB snapshot; got %v", err)
	}
}
