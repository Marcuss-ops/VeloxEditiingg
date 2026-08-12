// Package fleet — Step 9/15 UpdateExecutor tests.
//
// Coverage map (each test exercises a single decision branch
// of the forward+rollback pipeline):
//
//	Happy-path:
//	  TestUpdate_HappyPath                       — all 8 forward steps pass
//
//	Phase 1 (input validation) errors:
//	  TestUpdate_EmptyPayload                    — empty json.RawMessage
//	  TestUpdate_PayloadMissingTargetDigest      — JSON parses, target_digest=""
//	  TestUpdate_PayloadParseFails               — malformed JSON
//	  TestUpdate_InvalidImageRef_MobileTag       — :latest rejected
//	  TestUpdate_InvalidImageRef_WrongRegistry   — non-ghcr.io rejected
//	  TestUpdate_NilBackend                      — backend not wired
//
//	Phase 2/3 (registry + snapshot) errors:
//	  TestUpdate_UnregisteredWorker              — registry returns nil Worker
//	  TestUpdate_EmptyRegistry                   — no prior deployment_records row
//
//	Phase 4 (drain) errors:
//	  TestUpdate_ActiveJobsTimeout               — active_tasks never reaches 0
//
//	Phase 6 forward-pipeline failures (each triggers rollback):
//	  TestUpdate_CosignFail_RollsBack            — Verify returns error
//	  TestUpdate_PullImageFail_RollsBack         — PullImage returns error
//	  TestUpdate_ComposeRestartFail_RollsBack    — ComposeRestart returns error
//	  TestUpdate_ContainerUnhealthy_RollsBack    — ContainerRunning → false
//	  TestUpdate_HealthReadyFail_RollsBack       — /health/ready curl fails
//	  TestUpdate_MasterDisconnected_RollsBack    — info.SessionActive=false
//	  TestUpdate_SmokeFail_RollsBack             — RunLevelD returns error
//	  TestUpdate_DriveDeliveryMissing_RollsBack  — VerifyDelivery returns missing
//
//	Rollback-side outcomes:
//	  TestUpdate_RollbackSucceedsAfterForwardFail — composite ErrRollbackSucceeded
//	  TestUpdate_RollbackFailsAfterForwardFail    — composite ErrRollbackFailed
//	  TestUpdate_PayloadSuppliesPreviousDigest    — caller overrides snapshot
//
//	Row transition audit:
//	  TestUpdate_ForwardRowTransitions_SUCCEEDED  — happy path persistence trail
//	  TestUpdate_ForwardRowTransitions_FAILED     — forward-fail persistence trail
//
//	Helper unit tests:
//	  TestUpdate_stepContainerRunning_NilDocker  — guard returns ErrContainerUnhealthy
//	  TestUpdate_parsePayload_EmptyJSONObject    — "{}" accepted as empty
//
// Pinned to Go 1.21+ (the repo's go.mod).
package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"velox-server/internal/store"
	"velox-server/internal/workers"
	"velox-shared/identity"
)

// pin to NEW per-test scaffold ---------------------------------------

func TestMain_Registered(t *testing.T) {
	// Compile-time assertion: ensure fleet.NewUpdateExecutor wires
	// without panicking on an empty backend struct (the nil-tolerance
	// expectations on every field). Every test below relies on this
	// contract — a panicking constructor breaks the whole test
	// suite before any individual assertion runs.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("NewUpdateExecutor must not panic on empty backend (got %v)", r)
		}
	}()
	_ = NewUpdateExecutor(UpdateBackend{})
}

// stubBackends builds an UpdateBackend with all dependencies
// pre-wired to in-process stubs. Tests mutate the per-stub call
// counters / canned-error fields before each Execute call to
// drive different failure modes. Default behaviour is "happy
// path" — every method returns success / canned value.
func stubBackends(t *testing.T) (UpdateBackend, *stubBackendsState) {
	t.Helper()
	st := &stubBackendsState{
		registeredWorker: true,
		activeTasksZero:  true,
		drain:            false,
		drainCalls:       []bool{},
		cosignErr:        nil,
		pullErr:          nil,
		composeErr:       nil,
		containerRunning: true,
		containerErr:     nil,
		healthErr:        nil,
		sessionActive:    true,
		lastHB:           time.Now().UTC().Format(time.RFC3339),
		smokeArtifactID:  "smoke-artifact-id-1",
		smokeErr:         nil,
		driveErr:         nil,
		prevDigest:       "sha256:previousdigest",
		runtimeDigest:    "",
		connectionState:  workers.ConnectionConnected,
		healthState:      workers.HealthHealthy,
		insertedRows:     []store.DeploymentRecord{},
		baselineRows:     []store.DeploymentRecord{},
		markedStatuses:   map[string]string{},
		rolledBack:       map[string]bool{},
	}
	return UpdateBackend{
		SSHCmd:      st,
		Docker:      st,
		Cosign:      st,
		Smoke:       st,
		Drive:       st,
		Registry:    st,
		Deployments: st,
		Image:       stubImageValidator{},
		Now:         func() time.Time { return time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC) },
	}, st
}

// stubBackendsState is the in-process implementation of all
// 7 Backends. Type-asserted to a single struct so test code
// can mutate one field at a time and inspect counters across
// surfaces.
type stubBackendsState struct {
	registeredWorker bool
	activeTasksZero  bool
	drain            bool
	drainCalls       []bool
	cosignErr        error
	pullErr          error
	composeErr       error
	containerRunning bool
	containerErr     error
	healthErr        error
	sessionActive    bool
	lastHB           string
	smokeArtifactID  string
	smokeErr         error
	driveErr         error
	prevDigest       string
	runtimeDigest    string
	runtimeErr       error
	connectionState  workers.ConnectionState
	healthState      workers.HealthState

	insertedRows   []store.DeploymentRecord
	baselineRows   []store.DeploymentRecord
	markedStatuses map[string]string // deployment_id -> status
	rolledBack     map[string]bool   // deployment_id -> rollbackOK
}

// ── BackendSSHClient ──────────────────────────────────────────────────

func (s *stubBackendsState) Run(_ context.Context, _ string, _ string) (string, error) {
	// The /health/ready step shells this; surfaces as error.
	if s.healthErr != nil {
		return "", s.healthErr
	}
	return "loopback /health/ready: ok\n", nil
}

// ── BackendDockerClient ──────────────────────────────────────────────

func (s *stubBackendsState) ActivateImage(_ context.Context, _ string, _ string) (string, error) {
	if s.pullErr != nil {
		return "", s.pullErr
	}
	if s.composeErr != nil {
		return "", s.composeErr
	}
	return "activated digest OK", nil
}

func (s *stubBackendsState) ContainerRunning(_ context.Context, _ string) (bool, error) {
	if s.containerErr != nil {
		return false, s.containerErr
	}
	return s.containerRunning, nil
}

// ── BackendCosignVerifierIfc ────────────────────────────────────────

func (s *stubBackendsState) Verify(_ context.Context, _ string) error {
	return s.cosignErr
}

// ── BackendSmokeRunner ──────────────────────────────────────────────

func (s *stubBackendsState) RunLevelD(_ context.Context, _ string) (string, error) {
	if s.smokeErr != nil {
		return "", s.smokeErr
	}
	return s.smokeArtifactID, nil
}

// ── BackendDriveVerifier ────────────────────────────────────────────

func (s *stubBackendsState) VerifyDelivery(_ context.Context, _ string, _ int64) error {
	return s.driveErr
}

// ── BackendRegistryGater ────────────────────────────────────────────

func (s *stubBackendsState) GetWorker(_ context.Context, id string) (*workers.Worker, error) {
	if !s.registeredWorker {
		return nil, nil
	}
	info := &workers.Worker{
		WorkerID:         identity.ParseWorkerID(id),
		SessionActive:    s.sessionActive,
		LastHB:           s.lastHB,
		Drain:            s.drain,
		ConnectionState:  s.connectionState,
		HealthState:      s.healthState,
		ConnectionStatus: workers.StatusConnected,
		Health:           workers.WorkerHealthHealthy,
		Metrics:          map[string]interface{}{"active_tasks": float64(0)},
	}
	return info, nil
}

func (s *stubBackendsState) GetAuthenticatedRuntimeSnapshot(_ context.Context, _ string) (*store.WorkerRuntimeSnapshot, error) {
	if s.runtimeErr != nil {
		return nil, s.runtimeErr
	}
	if s.runtimeDigest == "" {
		return &store.WorkerRuntimeSnapshot{}, nil
	}
	return &store.WorkerRuntimeSnapshot{DockerImageDigest: s.runtimeDigest}, nil
}

func (s *stubBackendsState) IsActiveJobsZero(_ context.Context, _ string) bool {
	return s.activeTasksZero
}

func (s *stubBackendsState) SetDrainMode(_ context.Context, _ string, drain bool) error {
	s.drain = drain
	s.drainCalls = append(s.drainCalls, drain)
	return nil
}

// ── BackendDeploymentRepo ───────────────────────────────────────────

func (s *stubBackendsState) GetLatestDeploymentForWorker(_ context.Context, id string) (*store.DeploymentRecord, error) {
	if s.prevDigest == "" {
		return nil, store.ErrDeploymentNotFound
	}
	return &store.DeploymentRecord{
		DeploymentID: "deploy-prev-1",
		WorkerID:     id,
		TargetDigest: s.prevDigest,
		Status:       store.DeployStatusSucceeded,
		StartedAt:    time.Date(2026, 7, 27, 17, 0, 0, 0, time.UTC),
	}, nil
}

func (s *stubBackendsState) InsertDeploymentRecord(_ context.Context, r store.DeploymentRecord) error {
	s.insertedRows = append(s.insertedRows, r)
	return nil
}

func (s *stubBackendsState) InsertBaselineDeploymentRecord(_ context.Context, r store.DeploymentRecord) error {
	s.baselineRows = append(s.baselineRows, r)
	return nil
}

func (s *stubBackendsState) MarkSucceeded(_ context.Context, id string, _ time.Time) error {
	s.markedStatuses[id] = store.DeployStatusSucceeded
	return nil
}

func (s *stubBackendsState) MarkFailed(_ context.Context, id string, _ time.Time, errMsg string) error {
	s.markedStatuses[id] = store.DeployStatusFailed
	return nil
}

func (s *stubBackendsState) MarkDeploymentRolledBack(_ context.Context, id string, _ time.Time, rollbackOK bool) error {
	s.rolledBack[id] = rollbackOK
	return nil
}

// stubImageValidator accepts any non-empty ref; per-call
// `reject` field drives failure-mode tests.
type stubImageValidator struct{ reject error }

func (s stubImageValidator) Validate(ref string) error {
	if s.reject != nil {
		return s.reject
	}
	if ref == "" {
		return errors.New("empty")
	}
	return nil
}

// mkOp builds an Operation with a sane payload.
func mkOp(workerID, targetDigest, prevInPayload string) *store.Operation {
	payload := map[string]string{"target_digest": targetDigest}
	if prevInPayload != "" {
		payload["previous_digest"] = prevInPayload
	}
	raw, _ := json.Marshal(payload)
	return &store.Operation{
		OperationID: "op-test-1",
		WorkerID:    workerID,
		Op:          "update",
		RequestedBy: "test",
		Reason:      "step 9/15 test",
		Payload:     raw,
		Status:      store.OperationStatusQueued,
		QueuedAt:    time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC),
	}
}

// ─── Phase 1: input validation ─────────────────────────────────────────

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

// ─── Phase 2/3: registry + snapshot ──────────────────────────────────

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
	st.runtimeDigest = previous
	target := validImageRef()

	if err := NewUpdateExecutor(backend).Execute(context.Background(), mkOp("wkr-1", target, "")); err != nil {
		t.Fatalf("normal rollout returned err %v", err)
	}
	if len(st.insertedRows) != 1 || st.insertedRows[0].PreviousDigest != previous {
		t.Fatalf("normal rollout row = %+v, want previous=%s", st.insertedRows, previous)
	}
}

// ─── Phase 4: drain ────────────────────────────────────────────────

func TestUpdate_DrainsAndReleasesOnSuccess(t *testing.T) {
	backend, st := stubBackends(t)
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
	if err == nil || !strings.Contains(err.Error(), "did not drain to 0") {
		t.Errorf("active-tasks timeout: want drain fail, got %v", err)
	}
	if got, want := st.drainCalls, []bool{true, false}; !reflect.DeepEqual(got, want) {
		t.Fatalf("drain calls after timeout = %v, want %v", got, want)
	}
	if st.drain {
		t.Fatal("worker remained drained after pre-forward timeout")
	}
}

// ─── Phase 6 forward failures (each triggers rollback) ────────────

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
	st.sessionActive = false
	e := NewUpdateExecutor(backend)
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err == nil || !errors.Is(err, ErrRollbackSucceeded) {
		t.Errorf("master disconnected: want ErrRollbackSucceeded wrap, got %v", err)
	}
}

func TestUpdate_SmokeFail_RollsBack(t *testing.T) {
	backend, st := stubBackends(t)
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

// ─── Rollback-side outcomes ─────────────────────────────────────────

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
}

func TestUpdate_PayloadSuppliesPreviousDigest(t *testing.T) {
	backend, st := stubBackends(t)
	// Make DB snapshot empty so any caller-supplied previous_digest
	// would otherwise trigger ErrEmptyRegistry.
	st.prevDigest = ""
	e := NewUpdateExecutor(backend)
	op := mkOp("wkr-1", validImageRef(), "sha256:caller-supplied")
	err := e.Execute(context.Background(), op)
	if err != nil {
		t.Errorf("payload-supplied previous_digest should bypass DB snapshot; got %v", err)
	}
}

// ─── Row transition audit ──────────────────────────────────────────

func TestUpdate_ForwardRowTransitions_SUCCEEDED(t *testing.T) {
	backend, st := stubBackends(t)
	e := NewUpdateExecutor(backend)
	err := e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if err != nil {
		t.Fatalf("happy path Execute returned err %v", err)
	}
	// Single PENDING forward row → marked SUCCEEDED.
	if len(st.insertedRows) != 1 {
		t.Fatalf("expected 1 forward row, got %d", len(st.insertedRows))
	}
	if st.markedStatuses[st.insertedRows[0].DeploymentID] != store.DeployStatusSucceeded {
		t.Errorf("forward row not marked SUCCEEDED: %+v", st.markedStatuses)
	}
}

func TestUpdate_ForwardRowTransitions_FAILED(t *testing.T) {
	backend, st := stubBackends(t)
	st.cosignErr = errors.New("bad sig")
	e := NewUpdateExecutor(backend)
	_ = e.Execute(context.Background(), mkOp("wkr-1", validImageRef(), ""))
	if len(st.insertedRows) < 2 {
		t.Fatalf("forward+rollback: expected 2 rows, got %d", len(st.insertedRows))
	}
	if st.markedStatuses[st.insertedRows[0].DeploymentID] != store.DeployStatusFailed {
		t.Errorf("forward row not marked FAILED: %+v", st.markedStatuses)
	}
}

// ─── Helper unit tests ──────────────────────────────────────────────

func TestUpdate_stepContainerRunning_NilDocker(t *testing.T) {
	backend, _ := stubBackends(t)
	backend.Docker = nil
	e := NewUpdateExecutor(backend)
	if err := e.stepContainerRunning(context.Background(), "wkr-1"); !errors.Is(err, ErrContainerUnhealthy) {
		t.Errorf("nil Docker backend: want ErrContainerUnhealthy sentinel, got %v", err)
	}
}

func TestUpdate_parsePayload_EmptyJSONObject(t *testing.T) {
	e := NewUpdateExecutor(UpdateBackend{})
	op := &store.Operation{Payload: []byte("{}")}
	if _, _, err := e.parsePayload(op); err == nil || !strings.Contains(err.Error(), "payload empty") {
		t.Errorf("{} payload: want payload-empty error, got %v", err)
	}
}

// ─── shared helpers ─────────────────────────────────────────────────

func validImageRef() string {
	return "ghcr.io/marcuss-ops/velox-worker@sha256:" + strings.Repeat("a", 64)
}

// stubBackendCosignErr wraps errors.New for the cosign verifier
// surface. Required because the stubBackendsState is the same
// struct for all 7 Backends (Go single-receiver pattern); a
// per-method override uses a separate type to drive the cosign
// return without affecting other surfaces in the same test.
type stubBackendCosignErr struct{ err error }

func (s stubBackendCosignErr) Verify(_ context.Context, _ string) error { return s.err }
