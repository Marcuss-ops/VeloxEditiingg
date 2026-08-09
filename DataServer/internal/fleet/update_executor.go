// Package fleet — Step 9/15 UpdateExecutor implementation.
//
// Implements the atomic update+rollback procedure promised by
// the rollout headers:
//
//  1. parse payload (target_digest required, previous_digest
//     optional — read from deployment_records if absent).
//  2. validate worker_id registered AND not in a torn state.
//  3. snapshot previous_digest from deployment_records, or adopt a verified
//     authenticated runtime digest when the ledger is empty.
//  4. set drain=true and wait for active_tasks=0. The executor
//     owns this transition so callers cannot start an update while
//     relying on a documentation-only drain step.

//  5. INSERT deployment_records row, status=PENDING,
//     is_rollback=false. Health() flips to UPDATING via
//     DeriveDeploymentHealthState precedence rank 4.
//  6. forward pipeline:
//     a. cosign verify target_digest (with VELOX_SKIP_*
//     override sentinel).
//     b. ssh: activate <target_digest> on the worker through the
//     canonical privileged helper (/usr/local/sbin/
//     velox-worker-activate-image): pull + atomic
//     VELOX_WORKER_IMAGE swap + velox-worker.service restart.
//     c. ContainerRunning poll on the fixed `velox-worker`
//     container (canonical project/service/container name).
//     d. /health/ready poll via ssh curl on the worker.
//     e. master-connection check via Registry.SessionActive
//     + heartbeat recency.
//     f. RunLevelD smoke (returns smoke artifact_id).
//     g. Drive verifier confirms the smoke artifact landed.
//     h. release an executor-owned drain and verify it is released.
//     i. MarkSucceeded on the PENDING row → Health HEALTHY.
//  7. on any forward failure (cosign fail / pull fail /
//     container unhealthy / health non-200 / master offline
//     / smoke fail / Drive fail), UPDATES the PENDING row to
//     status=FAILED and runs the rollback cascade:
//     a. INSERT a SECOND deployment_records row, status=
//     PENDING, is_rollback=true. Health flips to
//     ROLLBACK via rank-3 precedence (beats UPDATING).
//     b. ssh: activate the previous digest through the canonical helper.
//     c. systemd restarts the canonical service.
//     d. ContainerRunning poll.
//     e. /health/ready poll.
//     f. health CONTAINER-restart path does NOT re-run
//     smoke — the previous_digest has already passed
//     smoke historically; a re-smoke is unnecessary
//     and would double the rollback latency.
//     g. MarkDeploymentRolledBack on the rollback row:
//     rollbackOK=true → status=ROLLED_BACK (terminal),
//     rollbackOK=false → status=FAILED — the worker is
//     now in an inconsistent state and operator
//     intervention is required.
//
// Return semantics (Execute → FleetController):
//   - forward OK       → nil            → FCO marks SUCCEEDED
//   - forward fail,
//     rollback OK      → ErrForward + ErrRollbackSucceeded wrap
//     → FCO marks FAILED, error_message
//     surfaces "rollback_ok to <digest>"; an executor-owned
//     drain is released after the worker is restored.

//   - forward fail,
//     rollback fail    → ErrForward + ErrRollbackFailed wrap,
//     rollback err wrapped.
//     → FCO marks FAILED, error_message
//     surfaces "rollback_failed: <err>"
//   - empty registry without an authenticated runtime reader → ErrEmptyRegistry
//     (production bootstraps a verified baseline instead).
//   - unregistered     → ErrUnregisteredWorker
//     → FCO marks FAILED — NO rollback
//     (forward never started)
//
// Per-step timeouts (forward budget 30min; configured via
// fleet.NewFleetController opTimeout). Each step's helper
// uses context.WithTimeout(ctx, budget) so a slow step
// fails fast rather than running away.
//
// File split by responsibility:
//   - update_executor.go        → struct, Execute, parsePayload, waitForIdle
//   - update_executor_pipeline.go → runForward / runRollback / runRollbackSteps
//   - update_executor_steps.go  → per-step helpers (cosign, pull, restart, ...)
package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"velox-server/internal/store"
	"velox-server/internal/workers"
)

// Sentinel errors returned by Execute. Each maps to a
// grep-friendly error_message prefix in the audit ledger so
// operator dashboards can route on them.
var (
	// ErrEmptyRegistry is retained for partially composed legacy callers that
	// have no authenticated runtime reader. Production uses the reader to
	// bootstrap a verified baseline instead. Forward never starts.
	ErrEmptyRegistry = errors.New("empty_registry")

	// ErrBootstrapUnverifiable means a missing ledger could not be safely
	// adopted because the authenticated runtime digest was absent or the
	// bootstrap dependency was not wired.
	ErrBootstrapUnverifiable = errors.New("bootstrap_unverifiable")

	// ErrBootstrapDigestMismatch means the operator target does not match the
	// digest reported by the currently authenticated worker session.
	ErrBootstrapDigestMismatch = errors.New("bootstrap_digest_mismatch")

	// ErrBootstrapWorkerDisconnected and ErrBootstrapWorkerUnhealthy are
	// fail-closed bootstrap guards. Adoption never changes worker state.
	ErrBootstrapWorkerDisconnected = errors.New("bootstrap_worker_disconnected")
	ErrBootstrapWorkerUnhealthy    = errors.New("bootstrap_worker_unhealthy")

	// ErrUnregisteredWorker is returned when the worker_id is
	// not present in the in-process registry. Forward never
	// started; NO rollback is attempted.
	ErrUnregisteredWorker = errors.New("unregistered_worker")

	// ErrRollbackSucceeded is the marker for "forward failed
	// but the rollback cascade recovered the worker". Wrapped
	// around the forward error so the audit dashboard's error
	// field reads "<forward> (rollback_ok to <prev>)".
	ErrRollbackSucceeded = errors.New("rollback_ok")

	// ErrRollbackFailed is the marker for "forward failed AND
	// the rollback cascade also failed". The audit dashboard
	// surfaces this as a hard error; operator intervention
	// required.
	ErrRollbackFailed = errors.New("rollback_failed")

	// ErrContainerUnhealthy is returned by the deps backend
	// when the container is broken. The executor treats this
	// as a forward-failure trigger and rolls back.
	ErrContainerUnhealthy = errors.New("container_unhealthy")

	// ErrDriveDeliveryMissing is returned by BackendDriveVerifier
	// when the smoke artifact is not reachable on Drive.
	ErrDriveDeliveryMissing = errors.New("drive_delivery_missing")

	// ErrDriveDeliverySize is returned by BackendDriveVerifier
	// when the smoke artifact's Drive-resident size does not
	// match the expected byte count.
	ErrDriveDeliverySize = errors.New("drive_delivery_size_mismatch")

	// ErrSmokeFailed is returned by BackendSmokeRunner on
	// Level D smoke test failure.
	ErrSmokeFailed = errors.New("smoke_failed")
)

// Per-step timeouts. Each step uses context.WithTimeout to
// bound its own budget — a runaway step fails fast rather
// than pinning the FleetController's opTimeout for the
// entire update.
//
// Total forward budget: 25-30 min. Total rollback budget:
// 15-20 min. Confirms the user's spec detail
// ("DRAINING → /health/ready → health → Master connection
// → Level D smoke → Drive delivery verification").
const (
	timeoutSnapshot       = 10 * time.Second
	timeoutDrainVerify    = 5 * time.Second
	timeoutActiveJobsIdle = 5 * time.Minute
	timeoutCosign         = 30 * time.Second
	timeoutDockerPull     = 10 * time.Minute
	timeoutComposeRestart = 2 * time.Minute
	timeoutContainerCheck = 30 * time.Second
	timeoutHealthReady    = 60 * time.Second
	timeoutMasterCheck    = 30 * time.Second
	timeoutSmokeRun       = 5 * time.Minute
	timeoutDriveVerify    = 60 * time.Second
)

// UpdateExecutor is the Step 9/15 OperationExecutor binding
// for fleet.OperationKindUpdate. It owns the forward pipeline
// + rollback cascade.
//
// Construction takes a single bundled UpdateBackend interface
// (defined in update_executor_deps.go) so the production wiring
// at buildFleet can supply a single dependency object. Tests
// construct UpdateBackend manually with stub implementations.
type UpdateExecutor struct {
	backend      UpdateBackend
	drainTimeout time.Duration
}

// UpdateBackend is the bundled dependency surface for
// UpdateExecutor. Defined in update_executor_deps.go but
// referenced here so the executor struct's field types are
// obvious to readers of this file.
//
// All fields are required for production. nil-tolerant in the
// constructor so a misconfigured boot surfaces Execute
// failures (not panic on first call).
type UpdateBackend struct {
	SSHCmd      BackendSSHClient
	Docker      BackendDockerClient
	Cosign      BackendCosignVerifierIfc
	Smoke       BackendSmokeRunner
	Drive       BackendDriveVerifier
	Registry    BackendRegistryGater
	Deployments BackendDeploymentRepo
	Runtime     BackendRuntimeSnapshotReader
	Image       BackendImageRefValidator
	Now         NowFunc
}

// NewUpdateExecutor returns an UpdateExecutor ready for
// ExecutorRegistry.Register(fleet.OperationKindUpdate, exec).
// nil `now` defaults to time.Now.
func NewUpdateExecutor(b UpdateBackend) *UpdateExecutor {
	if b.Now == nil {
		b.Now = func() time.Time { return time.Now().UTC() }
	}
	return &UpdateExecutor{backend: b, drainTimeout: timeoutActiveJobsIdle}
}

// AttachRuntimeBackends completes the production composition after the
// canonical Level-D executor and Drive service have been constructed. It is
// intentionally atomic at bootstrap time: an update executor with any
// missing runtime backend is rejected by the composition root.
func (e *UpdateExecutor) AttachRuntimeBackends(smoke BackendSmokeRunner, drive BackendDriveVerifier) error {
	if e == nil {
		return errors.New("update: nil executor")
	}
	if smoke == nil || drive == nil {
		return errors.New("update: fresh smoke and drive verifier are required")
	}
	e.backend.Smoke = smoke
	e.backend.Drive = drive
	return nil
}

// UpdateCapability is the fail-closed boot verdict for the update
// path (AZIONE 2: no "docker client not wired" discovered 30s after
// a POST). Ready is true ONLY when every critical backend is wired:
// SSH, Docker, Cosign, Registry, Deployments, Image, Smoke and Drive.
// Missing lists the names of the absent backends so the operator log,
// the /ready probe and the 503 gate detail all surface the same
// grep-friendly vocabulary.
type UpdateCapability struct {
	Ready   bool
	Missing []string
}

// Capability computes the current fail-closed verdict from the
// executor's live backend surface. Nil-receiver safe so the /ready
// probe and the POST /update gate can call it on a partially
// composed executor without a panic.
func (e *UpdateExecutor) Capability() UpdateCapability {
	if e == nil {
		return UpdateCapability{Missing: []string{"executor"}}
	}
	missing := make([]string, 0, 8)
	if e.backend.SSHCmd == nil {
		missing = append(missing, "ssh")
	}
	if e.backend.Docker == nil {
		missing = append(missing, "docker")
	}
	if e.backend.Deployments == nil {
		missing = append(missing, "deployments")
	}
	if e.backend.Cosign == nil {
		missing = append(missing, "cosign")
	}
	if e.backend.Image == nil {
		missing = append(missing, "image")
	}
	if e.backend.Registry == nil {
		missing = append(missing, "registry")
	}
	if e.backend.Smoke == nil {
		missing = append(missing, "smoke")
	}
	if e.backend.Drive == nil {
		missing = append(missing, "drive")
	}
	return UpdateCapability{Ready: len(missing) == 0, Missing: missing}
}

// Ready reports whether the update capability is fully wired. This
// is the fail-closed boot signal: while Ready is false, /ready stays
// red (update-capability probe) and POST /update refuses with 503
// instead of accepting an operation that would fail mid-pipeline.
func (e *UpdateExecutor) Ready() bool { return e.Capability().Ready }

// ValidateProductionBackends prevents the update endpoint from being
// exposed with a partially wired production backend. Delegates to
// Capability so boot validation and the runtime gate share exactly
// one definition of "ready". Error text keeps the
// "missing dependencies: <list>" shape consumed by dashboards/tests.
func (e *UpdateExecutor) ValidateProductionBackends() error {
	if e == nil {
		return errors.New("update: nil executor")
	}
	capability := e.Capability()
	if !capability.Ready {
		return fmt.Errorf("missing dependencies: %s", strings.Join(capability.Missing, ", "))
	}
	return nil
}

// BackendCosignVerifierIfc is the local alias for
// velox-server/internal/deploy/cosign.CosignVerifier so the
// executor struct definition here doesn't import the cosign
// package twice (once in the deps file, once again here).
//
// Go allows declaring the type alias in either location; placed
// here because the executor is the consumer.
type BackendCosignVerifierIfc interface {
	Verify(ctx context.Context, ref string) error
}

// Execute runs the forward pipeline (with rollback on any
// forward failure). Returns nil only on a clean forward
// success; any other terminal state returns a wrapped error
// the FleetController maps to a FAILED Operation row with
// a grep-friendly error message.
func (e *UpdateExecutor) Execute(ctx context.Context, op *store.Operation) error {
	if op == nil {
		return errors.New("update: nil operation")
	}
	if op.WorkerID == "" {
		return errors.New("update: worker_id empty")
	}

	// ── Phase 1: parse payload ──────────────────────────────────────
	targetDigest, explicitPrev, err := e.parsePayload(op)
	if err != nil {
		return fmt.Errorf("update: parse payload: %w", err)
	}

	if e.backend.Image == nil {
		return errors.New("update: image validator not wired")
	}
	if err := e.backend.Image.Validate(targetDigest); err != nil {
		return fmt.Errorf("update: target_digest validation: %w", err)
	}

	// ── Phase 2: worker lookup ──────────────────────────────────────
	if e.backend.Registry == nil {
		return errors.New("update: registry gater not wired")
	}
	info, err := e.backend.Registry.GetWorker(ctx, op.WorkerID)
	if err != nil {
		return fmt.Errorf("update: registry lookup: %w", err)
	}
	if info == nil {
		return fmt.Errorf("%w: %s", ErrUnregisteredWorker, op.WorkerID)
	}

	// ── Phase 3: snapshot previous_digest ───────────────────────────
	previousDigest := explicitPrev
	if previousDigest == "" {
		if e.backend.Deployments == nil {
			return errors.New("update: deployments repo not wired")
		}
		rec, err := e.backend.Deployments.GetLatestDeploymentForWorker(ctx, op.WorkerID)
		if err != nil {
			if errors.Is(err, store.ErrDeploymentNotFound) {
				if e.backend.Runtime == nil {
					// Preserve the explicit failure for partially composed or
					// legacy test backends. Production wiring always provides
					// the authenticated runtime reader for adoption.
					return fmt.Errorf("%w: no previous deployment_records row for worker %s", ErrEmptyRegistry, op.WorkerID)
				}
				return e.bootstrapLedger(ctx, op, targetDigest, info)
			}
			return fmt.Errorf("update: snapshot previous_digest: %w", err)
		}
		if rec == nil {
			return fmt.Errorf("update: snapshot previous_digest: nil deployment record for worker %s", op.WorkerID)
		}
		previousDigest = rec.TargetDigest

		// The authenticated runtime snapshot also enables an idempotent
		// no-op. If desired, requested, and authenticated running digests
		// already agree, no drain, Docker, SSH, smoke, or Drive step is
		// needed.
		if runningDigest, readErr := e.authenticatedRunningDigest(ctx, op.WorkerID); readErr != nil {
			return readErr
		} else if runningDigest != "" && normalizeDigest(rec.TargetDigest) == normalizeDigest(targetDigest) &&
			normalizeDigest(runningDigest) == normalizeDigest(targetDigest) {
			log.Printf("[UPDATE] worker=%s target=%s ALREADY_CURRENT", op.WorkerID, targetDigest)
			return nil
		}
	}

	// Logged at every operator-observable transition.
	log.Printf("[UPDATE] worker=%s target=%s previous=%s", op.WorkerID, targetDigest, previousDigest)

	// ── Phase 4: own the drain and wait for active_tasks=0 ───────────
	// Preserve an operator-owned drain. Only undo the transition that
	// this executor made; callers must serialize worker mutations while
	// an update operation is active.
	drainOwned := !info.Drain
	if drainOwned {
		if err := e.backend.Registry.SetDrainMode(ctx, op.WorkerID, true); err != nil {
			return fmt.Errorf("update: set drain: %w", err)
		}
	}
	if err := e.waitForIdle(ctx, op.WorkerID); err != nil {
		if releaseErr := e.releaseOwnedDrain(ctx, op.WorkerID, drainOwned); releaseErr != nil {
			return fmt.Errorf("update: drain wait: %w (release drain: %v)", err, releaseErr)
		}
		return fmt.Errorf("update: drain wait: %w", err)
	}

	// ── Phase 5: insert PENDING deployment_records row ───────────────
	now := e.backend.Now()
	deploymentID := fmt.Sprintf("deploy-%s-%d", op.WorkerID, now.UnixNano())
	if err := e.backend.Deployments.InsertDeploymentRecord(ctx, store.DeploymentRecord{
		DeploymentID:   deploymentID,
		WorkerID:       op.WorkerID,
		PreviousDigest: previousDigest,
		TargetDigest:   targetDigest,
		StartedAt:      now,
		Status:         store.DeployStatusPending,
		AppliedBy:      op.RequestedBy,
		IsRollback:     false,
	}); err != nil {
		if releaseErr := e.releaseOwnedDrain(ctx, op.WorkerID, drainOwned); releaseErr != nil {
			return fmt.Errorf("update: insert PENDING: %w (release drain: %v)", err, releaseErr)
		}
		return fmt.Errorf("update: insert PENDING: %w", err)
	}

	// ── Phase 6: forward pipeline ───────────────────────────────────
	if runErr := e.runForward(ctx, op, targetDigest); runErr != nil {
		// Mark forward row FAILED first; THEN run rollback.
		if uerr := e.backend.Deployments.MarkFailed(ctx, deploymentID, e.backend.Now(), runErr.Error()); uerr != nil {
			log.Printf("[UPDATE] mark FAILED for %s: %v", deploymentID, uerr)
		}
		rollbackErr := e.runRollback(ctx, op, previousDigest, runErr)
		if errors.Is(rollbackErr, ErrRollbackSucceeded) {
			if releaseErr := e.releaseOwnedDrain(ctx, op.WorkerID, drainOwned); releaseErr != nil {
				return fmt.Errorf("%w: rollback completed but release drain failed: %v", ErrRollbackFailed, releaseErr)
			}
		}
		return rollbackErr
	}

	// ── Phase 7: release drain, then mark terminal ──────────────────
	// A successful deployment must never be published while the worker
	// remains DRAINING. The drain is part of the success invariant, not
	// cleanup after success; otherwise the fleet ledger and live routing
	// state can disagree.
	if err := e.releaseOwnedDrain(ctx, op.WorkerID, drainOwned); err != nil {
		return fmt.Errorf("update: release drain before success: %w", err)
	}
	if err := e.backend.Deployments.MarkSucceeded(ctx, deploymentID, e.backend.Now()); err != nil {
		return fmt.Errorf("update: mark SUCCEEDED for %s: %w", deploymentID, err)
	}
	log.Printf("[UPDATE] worker=%s target=%s SUCCEEDED", op.WorkerID, targetDigest)
	return nil
}

func (e *UpdateExecutor) authenticatedRunningDigest(ctx context.Context, workerID string) (string, error) {
	if e.backend.Runtime == nil {
		return "", nil
	}
	snapshot, err := e.backend.Runtime.GetAuthenticatedRuntimeSnapshot(ctx, workerID)
	if err != nil {
		return "", fmt.Errorf("update: authenticated runtime snapshot: %w", err)
	}
	if snapshot == nil {
		return "", nil
	}
	return strings.TrimSpace(snapshot.DockerImageDigest), nil
}

func (e *UpdateExecutor) bootstrapLedger(ctx context.Context, op *store.Operation, targetDigest string, info *workers.Worker) error {
	runningDigest, err := e.authenticatedRunningDigest(ctx, op.WorkerID)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBootstrapUnverifiable, err)
	}
	if runningDigest == "" {
		return fmt.Errorf("%w: authenticated runtime digest missing for worker %s", ErrBootstrapUnverifiable, op.WorkerID)
	}
	if normalizeDigest(runningDigest) != normalizeDigest(targetDigest) {
		return fmt.Errorf("%w: requested=%s running=%s", ErrBootstrapDigestMismatch, targetDigest, runningDigest)
	}
	if !workerConnected(info) {
		return fmt.Errorf("%w: worker %s is not CONNECTED", ErrBootstrapWorkerDisconnected, op.WorkerID)
	}
	if !workerHealthy(info) {
		return fmt.Errorf("%w: worker %s is not HEALTHY", ErrBootstrapWorkerUnhealthy, op.WorkerID)
	}

	baselineRepo, ok := e.backend.Deployments.(BackendDeploymentBaselineRepo)
	if !ok {
		return fmt.Errorf("%w: deployment baseline writer not wired", ErrBootstrapUnverifiable)
	}
	now := e.backend.Now()
	if err := baselineRepo.InsertBaselineDeploymentRecord(ctx, store.DeploymentRecord{
		DeploymentID:   fmt.Sprintf("bootstrap-%s-%d", op.WorkerID, now.UnixNano()),
		WorkerID:       op.WorkerID,
		PreviousDigest: "", // missing provenance is truthful; never invent it
		TargetDigest:   targetDigest,
		StartedAt:      now,
		FinishedAt:     &now,
		Status:         store.DeployStatusSucceeded,
		AppliedBy:      op.RequestedBy,
		IsRollback:     false,
	}); err != nil {
		return fmt.Errorf("%w: insert baseline: %v", ErrBootstrapUnverifiable, err)
	}
	log.Printf("[UPDATE] worker=%s target=%s BOOTSTRAPPED (authenticated runtime; no worker mutation)", op.WorkerID, targetDigest)
	return nil
}

func normalizeDigest(ref string) string {
	ref = strings.ToLower(strings.TrimSpace(ref))
	if at := strings.LastIndexByte(ref, '@'); at >= 0 {
		return ref[at+1:]
	}
	return ref
}

func workerConnected(info *workers.Worker) bool {
	if info == nil {
		return false
	}
	if info.ConnectionState != "" {
		return info.ConnectionState == workers.ConnectionConnected
	}
	return info.ConnectionStatus == workers.StatusConnected && info.SessionActive
}

func workerHealthy(info *workers.Worker) bool {
	if info == nil {
		return false
	}
	if info.HealthState != "" {
		return info.HealthState == workers.HealthHealthy
	}
	return info.Health == workers.WorkerHealthHealthy
}

func (e *UpdateExecutor) releaseOwnedDrain(ctx context.Context, workerID string, owned bool) error {
	if !owned {
		return nil
	}
	if e.backend.Registry == nil {
		return errors.New("update: registry gater not wired (cannot release drain)")
	}
	if err := e.backend.Registry.SetDrainMode(ctx, workerID, false); err != nil {
		return err
	}
	return nil
}

// parsePayload unwraps the Operation.Payload into the typed
// UpdatePayload schema. Returns the target_digest (required,
// non-empty after Validate), the explicitPreviousDigest
// (caller-supplied snapshot), and a parse error if the
// payload is malformed or invalid.
func (e *UpdateExecutor) parsePayload(op *store.Operation) (string, string, error) {
	if len(op.Payload) == 0 || string(op.Payload) == "{}" {
		return "", "", errors.New("update: payload empty (target_digest required)")
	}
	var p UpdatePayload
	if err := json.Unmarshal(op.Payload, &p); err != nil {
		return "", "", fmt.Errorf("update: payload parse: %w", err)
	}
	if p.TargetDigest == "" {
		return "", "", errors.New("update: target_digest missing")
	}
	return strings.TrimSpace(p.TargetDigest), strings.TrimSpace(p.PreviousDigest), nil
}

// waitForIdle polls the registry's canonical active_tasks signal until
// the value reaches 0, or until the poll budget elapses. The default
// budget is timeoutActiveJobsIdle (5min).

// Polling is intentional rather than event-driven because
// the registry's in-memory heartbeat metric is the canonical
// active-task count and polling exposes a simple deadline to
// the operator's audit trail.
func (e *UpdateExecutor) waitForIdle(ctx context.Context, workerID string) error {
	if e.backend.Registry == nil {
		// Defensive: callers should pass a wired gater, but
		// a missing dependency surfaces the failure explicitly.
		return errors.New("update: registry gater not wired (cannot confirm drain)")
	}
	drainTimeout := e.drainTimeout
	if drainTimeout <= 0 {
		drainTimeout = timeoutActiveJobsIdle
	}
	deadline := time.Now().Add(drainTimeout)
	for {
		if e.backend.Registry.IsActiveJobsZero(ctx, workerID) {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("active_tasks (active_jobs) did not drain to 0 within budget")
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait_for_idle: ctx cancelled: %w", ctx.Err())
		case <-time.After(1 * time.Second):
		}
	}
}
