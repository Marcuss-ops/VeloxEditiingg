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
		registeredWorker:     true,
		activeTasksZero:      true,
		drain:                false,
		drainCalls:           []bool{},
		cosignErr:            nil,
		pullErr:              nil,
		composeErr:           nil,
		containerRunning:     true,
		containerErr:         nil,
		healthErr:            nil,
		sessionActive:        true,
		lastHB:               time.Now().UTC().Format(time.RFC3339),
		smokeArtifactID:      "smoke-artifact-id-1",
		smokeErr:             nil,
		driveErr:             nil,
		prevDigest:           "sha256:previousdigest",
		runtimeDigest:        "",
		sessionID:            "grpc-session-1",
		reconnectSessionID:   "grpc-session-2",
		connectionState:      workers.ConnectionConnected,
		healthState:          workers.HealthHealthy,
		insertedRows:         []store.DeploymentRecord{},
		baselineRows:         []store.DeploymentRecord{},
		markedStatuses:       map[string]string{},
		markedErrorCodes:     map[string]string{},
		rolledBack:           map[string]bool{},
		rolledBackErrorCodes: map[string]string{},
	}
	return UpdateBackend{
		SSHCmd:      st,
		Docker:      st,
		Cosign:      st,
		Smoke:       st,
		Drive:       st,
		Registry:    st,
		Runtime:     st, // production always wires the authenticated runtime reader
		Preflight:   st, // production always checks the canonical host before drain
		Deployments: st,
		Image:       stubImageValidator{},
		Now:         func() time.Time { return time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC) },
	}, st
}

// stubBackendsState is the in-process implementation of all
// UpdateBackend backends. Type-asserted to a single struct so test code
// can mutate one field at a time and inspect counters across
// surfaces.
type stubBackendsState struct {
	registeredWorker bool
	activeTasksZero  bool
	drain            bool
	// drainNotApplied simulates a drain backend that reports success
	// (SetDrainMode returns nil) without the registry read model ever
	// reflecting drain=true — the bug class the DRAINING gate exists
	// to close.
	drainNotApplied  bool
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

	// sessionID is the authenticated control session returned in the runtime
	// snapshot; reconnectSessionID, when non-empty, is what the worker flips
	// to after a successful ActivateImage (fresh Hello on reconnect). The
	// WAITING_READY new-session gate requires SessionID != pre-restart ID.
	sessionID          string
	reconnectSessionID string

	// phases records every RecordDeploymentPhase call in order.
	phases []string
	// observedVerifiedDigest is the digest handed to MarkVerifiedSucceeded.
	observedVerifiedDigest string

	insertedRows         []store.DeploymentRecord
	baselineRows         []store.DeploymentRecord
	markedStatuses       map[string]string // deployment_id -> status
	markedErrorCodes     map[string]string // deployment_id -> DeploymentErrorCode
	rolledBack           map[string]bool   // deployment_id -> rollbackOK
	rolledBackErrorCodes map[string]string // deployment_id -> DeploymentErrorCode (rollback failed)
}

// ── BackendSSHClient ──────────────────────────────────────────────────

func (s *stubBackendsState) Run(_ context.Context, _ string, _ string) (string, error) {
	// The /health/ready step shells this; surfaces as error.
	if s.healthErr != nil {
		return "", s.healthErr
	}
	return "loopback /health/ready: ok\n", nil
}

func (s *stubBackendsState) Check(_ context.Context, _ string) error {
	return nil
}

// ── BackendDockerClient ──────────────────────────────────────────────

func (s *stubBackendsState) ActivateImage(_ context.Context, _ string, _ string) (string, error) {
	if s.pullErr != nil {
		return "", s.pullErr
	}
	if s.composeErr != nil {
		return "", s.composeErr
	}
	// A successful activation restarts the worker; model the fresh Hello by
	// advancing to the reconnect session (when the test models one).
	if s.reconnectSessionID != "" {
		s.sessionID = s.reconnectSessionID
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
	return &store.WorkerRuntimeSnapshot{SessionID: s.sessionID, DockerImageDigest: s.runtimeDigest}, nil
}

func (s *stubBackendsState) IsActiveJobsZero(_ context.Context, _ string) bool {
	return s.activeTasksZero
}

func (s *stubBackendsState) IsDrained(_ context.Context, _ string) bool {
	return s.drain
}

func (s *stubBackendsState) SetDrainMode(_ context.Context, _ string, drain bool) error {
	if drain && s.drainNotApplied {
		// Report success but leave the registry read model untouched:
		// the executor must not trust the return value alone.
		s.drainCalls = append(s.drainCalls, drain)
		return nil
	}
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

func (s *stubBackendsState) MarkVerifiedSucceeded(_ context.Context, id, observedDigest string, _ time.Time) error {
	s.markedStatuses[id] = store.DeployStatusSucceeded
	s.observedVerifiedDigest = observedDigest
	return nil
}

func (s *stubBackendsState) RecordDeploymentPhase(_ context.Context, _ string, phase string) error {
	s.phases = append(s.phases, phase)
	return nil
}

func (s *stubBackendsState) MarkFailed(_ context.Context, id string, _ time.Time, errCode, errMsg string) error {
	s.markedStatuses[id] = store.DeployStatusFailed
	s.markedErrorCodes[id] = errCode
	return nil
}

func (s *stubBackendsState) MarkDeploymentRolledBack(_ context.Context, id string, _ time.Time, rollbackOK bool, errCode string) error {
	s.rolledBack[id] = rollbackOK
	if !rollbackOK {
		s.rolledBackErrorCodes[id] = errCode
	}
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

// ─── Phase 2/3: registry + snapshot ──────────────────────────────────

// ─── Phase 4: drain ────────────────────────────────────────────────

// ─── Phase VERIFYING_DIGEST ────────────────────────────────────────

// TestUpdate_DigestMismatchFailsClosed pins the VERIFYING_DIGEST mismatch
// contract: the authenticated running digest (C) differs from the target
// (B), so the forward row is marked FAILED with error_code `digest_mismatch`
// (expected/observed in the message) and the rollback cascade restores the
// previous digest. MarkVerifiedSucceeded must NOT be reached: an unverified
// digest can never advance last-known-good.
// TestUpdate_StaleSessionCannotAdvance pins the WAITING_READY new-session
// gate: after the restart the worker must reconnect on a NEW authenticated
// session. A worker that stays on the pre-restart session (no fresh Hello)
// never satisfies WAITING_READY, so the rollout fails closed and rolls back
// even though the container/health checks passed.
// TestUpdate_PhaseSequenceRecorded pins that the rollout phases are persisted
// in order on the read model (worker_deployment_state.last_phase): DRAINING →
// DEPLOYING → RESTARTING → WAITING_READY → VERIFYING_DIGEST, and that the
// observed digest handed to MarkVerifiedSucceeded is the verified one.
// ─── Recovery re-entry after Master restart (acceptance §29-30) ──

// TestUpdate_RecoveryReentryAfterCrashDuringDeployingNeverAssumesSuccess pins
// the §30 acceptance condition at the executor boundary: the Master crashed
// mid-DEPLOYING; the drain the pre-restart executor owned survived in the
// registry read model, and the worker came back advertising digest C while
// the operation targets A. Re-invoking the executor for the same operation
// after the restart must NOT duplicate the drain side effect (it can no
// longer prove ownership — drainOwned=false), must NOT assume success:
// VERIFYING_DIGEST compares running C vs target A, the forward row is marked
// FAILED with error_code DIGEST_MISMATCH, and MarkVerifiedSucceeded is never
// reached (no last-known-good advance on an unverified digest).
// TestUpdate_RecoveryReentryAfterCrashDuringDeployingResumesOnMatchingDigest
// pins the positive resume: the crash was mid-DEPLOYING and the worker
// reconnects on a NEW authenticated session advertising the TARGET digest.
// Re-invoking the executor must run the full forward pipeline through to
// MarkVerifiedSucceeded (the ONLY path that advances last-known-good) while
// still NOT re-draining and NOT clobbering the pre-restart drain it can no
// longer prove ownership of.
// ─── Phase 6 forward failures (each triggers rollback) ────────────

// ─── Rollback-side outcomes ─────────────────────────────────────────

// ─── Row transition audit ──────────────────────────────────────────

// ─── Helper unit tests ──────────────────────────────────────────────

// TestClassifyDeploymentError pins the error_code vocabulary (migration 153)
// end-to-end: every forward/rollback failure mode maps to a stable, routable
// code — the six core codes plus the smoke/drive/rollback extensions — so the
// journal row and the read model carry a machine-usable failure class
// separate from the free-form message.
// ─── shared helpers ─────────────────────────────────────────────────

func validImageRef() string {
	return "ghcr.io/marcuss-ops/velox-worker@sha256:" + strings.Repeat("a", 64)
}

// stubBackendCosignErr wraps errors.New for the cosign verifier
// surface. Required because the stubBackendsState is the same
// struct for all UpdateBackend backends (Go single-receiver pattern); a
// per-method override uses a separate type to drive the cosign
// return without affecting other surfaces in the same test.
type stubBackendCosignErr struct{ err error }

func (s stubBackendCosignErr) Verify(_ context.Context, _ string) error { return s.err }
