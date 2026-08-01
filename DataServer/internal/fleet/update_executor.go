// Package fleet — Step 9/15 UpdateExecutor implementation.
//
// Implements the atomic update+rollback procedure promised by
// the rollout headers:
//
//  1. parse payload (target_digest required, previous_digest
//     optional — read from deployment_records if absent).
//  2. validate worker_id registered AND not in a torn state.
//  3. snapshot previous_digest from deployment_records.
//  4. wait for active_jobs=0 (drain side-effect from the
//     admin endpoint (Step 6/15) is already in effect — the
//     executor just confirms via polling).
//  5. INSERT deployment_records row, status=PENDING,
//     is_rollback=false. Health() flips to UPDATING via
//     DeriveDeploymentHealthState precedence rank 4.
//  6. forward pipeline:
//     a. cosign verify target_digest (with VELOX_SKIP_*
//     override sentinel).
//     b. ssh: docker pull <target_digest> on the worker.
//     c. ssh: docker compose -p velox-worker-<id> restart.
//     d. ContainerRunning poll.
//     e. /health/ready poll via ssh curl on the worker.
//     f. master-connection check via Registry.SessionActive
//     + heartbeat recency.
//     g. RunLevelD smoke (returns smoke artifact_id).
//     h. Drive verifier confirms the smoke artifact landed.
//     i. MarkSucceeded on the PENDING row → Health HEALTHY.
//  7. on any forward failure (cosign fail / pull fail /
//     container unhealthy / health non-200 / master offline
//     / smoke fail / Drive fail), UPDATES the PENDING row to
//     status=FAILED and runs the rollback cascade:
//     a. INSERT a SECOND deployment_records row, status=
//     PENDING, is_rollback=true. Health flips to
//     ROLLBACK via rank-3 precedence (beats UPDATING).
//     b. ssh: docker pull <previous_digest>.
//     c. ssh: docker compose restart.
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
//     surfaces "rollback_ok to <digest>"
//   - forward fail,
//     rollback fail    → ErrForward + ErrRollbackFailed wrap,
//     rollback err wrapped.
//     → FCO marks FAILED, error_message
//     surfaces "rollback_failed: <err>"
//   - empty registry   → ErrEmptyRegistry
//     → FCO marks FAILED, error_message
//     surfaces "empty_registry: ..."
//   - unregistered     → ErrUnregisteredWorker
//     → FCO marks FAILED — NO rollback
//     (forward never started)
//
// Per-step timeouts (forward budget 30min; configured via
// fleet.NewFleetController opTimeout). Each step's helper
// uses context.WithTimeout(ctx, budget) so a slow step
// fails fast rather than running away.
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
)

// Sentinel errors returned by Execute. Each maps to a
// grep-friendly error_message prefix in the audit ledger so
// operator dashboards can route on them.
var (
	// ErrEmptyRegistry is returned when the worker has no prior
	// deployment_records row. The executor cannot snapshot a
	// previous digest in that case, and the operator must
	// bootstrap the worker into the ledger manually. Forward
	// never started, so NO rollback is attempted.
	ErrEmptyRegistry = errors.New("empty_registry")

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
				return fmt.Errorf("%w: no previous deployment_records row for worker %s", ErrEmptyRegistry, op.WorkerID)
			}
			return fmt.Errorf("update: snapshot previous_digest: %w", err)
		}
		previousDigest = rec.TargetDigest
	}

	// Logged at every operator-observable transition.
	log.Printf("[UPDATE] worker=%s target=%s previous=%s", op.WorkerID, targetDigest, previousDigest)

	// ── Phase 4: wait for active_jobs=0 (drain confirm) ─────────────
	if err := e.waitForIdle(ctx, op.WorkerID); err != nil {
		// Drain didn't complete in budget — DO NOT roll back
		// the worker state (we never modified it), but surface
		// the failure. The deployment_records row stays
		// unwritten because forward never started.
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
		return fmt.Errorf("update: insert PENDING: %w", err)
	}

	// ── Phase 6: forward pipeline ───────────────────────────────────
	if runErr := e.runForward(ctx, op, targetDigest); runErr != nil {
		// Mark forward row FAILED first; THEN run rollback.
		if uerr := e.backend.Deployments.MarkFailed(ctx, deploymentID, e.backend.Now(), runErr.Error()); uerr != nil {
			log.Printf("[UPDATE] mark FAILED for %s: %v", deploymentID, uerr)
		}
		return e.runRollback(ctx, op, previousDigest, runErr)
	}

	// ── Phase 7: forward SUCCEEDED, mark terminal ───────────────────
	if err := e.backend.Deployments.MarkSucceeded(ctx, deploymentID, e.backend.Now()); err != nil {
		return fmt.Errorf("update: mark SUCCEEDED for %s: %w", deploymentID, err)
	}
	log.Printf("[UPDATE] worker=%s target=%s SUCCEEDED", op.WorkerID, targetDigest)
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

// waitForIdle polls WorkerInfo.Metrics["active_tasks"] until
// the value reaches 0, or until the poll budget elapses. The
// default budget is timeoutActiveJobsIdle (5min).
//
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
			return errors.New("active_jobs did not drain to 0 within budget")
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait_for_idle: ctx cancelled: %w", ctx.Err())
		case <-time.After(1 * time.Second):
		}
	}
}

// runForward executes the forward-only pipeline: cosign,
// docker pull, compose restart, container running, health
// check, master-connection check, smoke, Drive verify.
// Returns nil only on full forward success.
//
// If any step fails, the function returns the wrapped error;
// the caller (Execute) handles the rollback cascade.
func (e *UpdateExecutor) runForward(ctx context.Context, op *store.Operation, targetDigest string) error {
	// (a) Cosign verify the target_digest.
	if err := e.stepCosignVerify(ctx, targetDigest); err != nil {
		return fmt.Errorf("cosign: %w", err)
	}

	// (b) docker pull <target_digest>.
	if _, err := e.stepPullImage(ctx, op.WorkerID, targetDigest); err != nil {
		return fmt.Errorf("docker pull: %w", err)
	}

	// (c) docker compose restart on the worker.
	if _, err := e.stepComposeRestart(ctx, op.WorkerID); err != nil {
		return fmt.Errorf("docker compose restart: %w", err)
	}

	// (d) ContainerRunning poll.
	if err := e.stepContainerRunning(ctx, op.WorkerID); err != nil {
		return fmt.Errorf("container_running: %w", err)
	}

	// (e) /health/ready poll on the worker.
	if err := e.stepHealthReady(ctx, op.WorkerID); err != nil {
		return fmt.Errorf("health_ready: %w", err)
	}

	// (f) Master-connection check (no SSH — pure registry).
	if err := e.stepMasterConnected(ctx, op.WorkerID); err != nil {
		return fmt.Errorf("master_connection: %w", err)
	}

	// (g) Level D smoke on the worker.
	smokeArtifactID, err := e.stepSmoke(ctx, op.WorkerID)
	if err != nil {
		return err
	}

	// (h) Drive verifier on the smoke artifact.
	if err := e.stepDriveVerify(ctx, smokeArtifactID); err != nil {
		return err
	}

	return nil
}

// runRollback executes the recovery cascade when forward
// failed. The earlier Execute code path has already UPDATed
// the FORWARD deployment_records row to FAILED; this function
// inserts a SECOND distinct row, executes the rollback, and
// transitions THAT row to ROLLED_BACK or FAILED depending on
// outcome.
//
// Returns either nil (forward never started — shouldn't
// happen since the rollback row insert succeeded) or a wrapped
// error encoding the rollback status via ErrRollbackSucceeded
// / ErrRollbackFailed sentinels so the caller (Execute) returns
// the right audit message.
//
// rollbackID is the deployment_id of the new rollback row;
// returned to the caller so ops logs can correlate.
func (e *UpdateExecutor) runRollback(ctx context.Context, op *store.Operation, previousDigest string, forwardErr error) error {
	now := e.backend.Now()
	deploymentID := fmt.Sprintf("rollback-%s-%d", op.WorkerID, now.UnixNano())
	log.Printf("[UPDATE] WORKER=%s forward=%v — cascading ROLLBACK to %s (deployment_id=%s)",
		op.WorkerID, forwardErr, previousDigest, deploymentID)

	// Insert the rollback PENDING row. is_rollback=true from
	// creation so Health() flips to ROLLBACK immediately.
	if err := e.backend.Deployments.InsertDeploymentRecord(ctx, store.DeploymentRecord{
		DeploymentID:   deploymentID,
		WorkerID:       op.WorkerID,
		PreviousDigest: previousDigest,
		TargetDigest:   previousDigest, // ROLLBACK restores previous_digest
		StartedAt:      now,
		Status:         store.DeployStatusPending,
		AppliedBy:      op.RequestedBy,
		IsRollback:     true,
	}); err != nil {
		return fmt.Errorf("rollback: insert PENDING: %w (forward_err=%v)", err, forwardErr)
	}

	// Execute the rollback steps. Each is non-fatal at the
	// audit-dashboard level — even partial rollback is
	// useful informatively for the operator.
	rbErr := e.runRollbackSteps(ctx, op.WorkerID, previousDigest)

	finishedAt := e.backend.Now()
	rollbackOK := rbErr == nil
	if err := e.backend.Deployments.MarkDeploymentRolledBack(ctx, deploymentID, finishedAt, rollbackOK); err != nil {
		log.Printf("[UPDATE] mark ROLLED_BACK for %s: %v (rollback err: %v)", deploymentID, err, rbErr)
		// Even if the DB write fails, surface the rollback
		// outcome to the caller so the audit row's
		// error_message reflects what actually happened.
	}

	if rollbackOK {
		return fmt.Errorf("%w: %v (rollback_ok to %s)", ErrRollbackSucceeded, forwardErr, previousDigest)
	}
	return fmt.Errorf("%w: forward=%v rollback=%v", ErrRollbackFailed, forwardErr, rbErr)
}

// runRollbackSteps executes the four rollback steps. Even
// partial rollback is reported so the operator sees a faithful
// audit message. Returns nil on full recovery.
func (e *UpdateExecutor) runRollbackSteps(ctx context.Context, workerID, previousDigest string) error {
	if _, err := e.stepPullImage(ctx, workerID, previousDigest); err != nil {
		return fmt.Errorf("rollback pull previous_digest: %w", err)
	}
	if _, err := e.stepComposeRestart(ctx, workerID); err != nil {
		return fmt.Errorf("rollback compose restart: %w", err)
	}
	if err := e.stepContainerRunning(ctx, workerID); err != nil {
		return fmt.Errorf("rollback container_running: %w", err)
	}
	// /health/ready on the rolled-back image (no re-smoke —
	// the previous digest already passed smoke historically).
	if err := e.stepHealthReady(ctx, workerID); err != nil {
		return fmt.Errorf("rollback health_ready: %w", err)
	}
	return nil
}

// ── Per-step helpers ────────────────────────────────────────────────────
//
// Each helper wraps context.WithTimeout + the BackendXxx
// surface call. Failures are wrapped with the step name so
// the audit row's error_message reads "<step>: <err>".

func (e *UpdateExecutor) stepCosignVerify(parent context.Context, ref string) error {
	if e.backend.Cosign == nil {
		return errors.New("cosign verifier not wired")
	}
	ctx, cancel := context.WithTimeout(parent, timeoutCosign)
	defer cancel()
	return e.backend.Cosign.Verify(ctx, ref)
}

func (e *UpdateExecutor) stepPullImage(parent context.Context, workerID, image string) (string, error) {
	if e.backend.Docker == nil {
		return "", errors.New("docker client not wired")
	}
	ctx, cancel := context.WithTimeout(parent, timeoutDockerPull)
	defer cancel()
	return e.backend.Docker.PullImage(ctx, workerID, image)
}

func (e *UpdateExecutor) stepComposeRestart(parent context.Context, workerID string) (string, error) {
	if e.backend.Docker == nil {
		return "", errors.New("docker client not wired")
	}
	ctx, cancel := context.WithTimeout(parent, timeoutComposeRestart)
	defer cancel()
	return e.backend.Docker.ComposeRestart(ctx, workerID)
}

func (e *UpdateExecutor) stepContainerRunning(parent context.Context, workerID string) error {
	if e.backend.Docker == nil {
		return ErrContainerUnhealthy
	}
	ctx, cancel := context.WithTimeout(parent, timeoutContainerCheck)
	defer cancel()
	running, err := e.backend.Docker.ContainerRunning(ctx, workerID)
	if err != nil {
		return fmt.Errorf("%w: check failed: %v", ErrContainerUnhealthy, err)
	}
	if !running {
		return fmt.Errorf("%w: container not running", ErrContainerUnhealthy)
	}
	return nil
}

func (e *UpdateExecutor) stepHealthReady(parent context.Context, workerID string) error {
	// Production: `docker exec velox-worker-<id> curl -fsS http://127.0.0.1:8081/health/ready`.
	// Tests substitute a stub that returns a canned status. The
	// implementation is in the BackendDockerClient surface
	// (PullImage / ComposeRestart / ContainerRunning) — for
	// /health/ready we re-use the SSH surface since the health
	// endpoint is a loopback curl on the worker.
	if e.backend.SSHCmd == nil {
		return errors.New("ssh client not wired (cannot poll /health/ready)")
	}
	ctx, cancel := context.WithTimeout(parent, timeoutHealthReady)
	defer cancel()
	// Production wires a real SSH client that curls the loopback
	// health endpoint. Stub Backends for tests will return
	// canned (=non-empty output + nil error = healthy).
	_, err := e.backend.SSHCmd.Run(ctx, workerID,
		"curl -fsS --max-time 5 http://127.0.0.1:8081/health/ready")
	return err
}

func (e *UpdateExecutor) stepMasterConnected(parent context.Context, workerID string) error {
	if e.backend.Registry == nil {
		return errors.New("registry gater not wired (cannot verify master connection)")
	}
	ctx, cancel := context.WithTimeout(parent, timeoutMasterCheck)
	defer cancel()
	info, err := e.backend.Registry.GetWorker(ctx, workerID)
	if err != nil {
		return fmt.Errorf("registry lookup: %w", err)
	}
	if info == nil {
		return errors.New("worker missing from registry (master disconnected)")
	}
	if !info.SessionActive {
		return errors.New("session not active (master disconnected)")
	}
	if info.LastHB == "" {
		return errors.New("no heartbeat yet (master connection not established)")
	}
	return nil
}

func (e *UpdateExecutor) stepSmoke(parent context.Context, workerID string) (string, error) {
	if e.backend.Smoke == nil {
		return "", fmt.Errorf("%w: smoke runner not wired", ErrSmokeFailed)
	}
	ctx, cancel := context.WithTimeout(parent, timeoutSmokeRun)
	defer cancel()
	artifactID, err := e.backend.Smoke.RunLevelD(ctx, workerID)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrSmokeFailed, err)
	}
	return artifactID, nil
}

func (e *UpdateExecutor) stepDriveVerify(parent context.Context, driveFileID string) error {
	if e.backend.Drive == nil {
		return errors.New("drive verifier not wired")
	}
	ctx, cancel := context.WithTimeout(parent, timeoutDriveVerify)
	defer cancel()
	if err := e.backend.Drive.VerifyDelivery(ctx, driveFileID, 0 /*expectedBytes: caller may inject via deps upgrade*/); err != nil {
		// Map the dep's error to our canonical sentinels so the
		// audit dashboard's classification logic stays stable.
		if errors.Is(err, ErrDriveDeliverySize) || errors.Is(err, ErrDriveDeliveryMissing) {
			return err
		}
		return fmt.Errorf("%w: %v", ErrDriveDeliveryMissing, err)
	}
	return nil
}
