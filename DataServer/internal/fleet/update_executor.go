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
//  4. set drain=true and wait for the authoritative DRAINING
//     condition: the registry read model reflects drain=true AND
//     active_tasks=0. A drain() call that returns nil without the
//     worker entering DRAINING never advances the rollout. The
//     executor owns this transition so callers cannot start an
//     update while relying on a documentation-only drain step.

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
//     e. WAITING_READY — gated on the NEW authenticated session:
//     the authenticated runtime snapshot must be bound to a session
//     DIFFERENT from the pre-restart one (fresh Hello) AND the
//     registry session active + heartbeat fresh. A stale READY from
//     the pre-restart session never advances the rollout.
//     f. VERIFYING_DIGEST — authenticated running digest must equal
//     the target. Mismatch fails forward with error_code
//     `digest_mismatch`; running_digest is never touched by the
//     control plane.
//     g. RunLevelD smoke (returns smoke artifact_id).
//     h. Drive verifier confirms the smoke artifact landed.
//     i. release an executor-owned drain and verify it is released.
//     j. MarkVerifiedSucceeded on the PENDING row → Health HEALTHY.
//     This is the ONLY path that advances last_successful_digest:
//     the store re-verifies the observed digest against the target
//     inside the transition transaction.
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
//   - update_executor.go            → struct, Execute, and core lifecycle
//   - update_executor_defs.go       → sentinels, timeouts, and interfaces
//   - update_executor_capabilities.go → capability and production wiring
//   - update_executor_bootstrap.go  → runtime digest and ledger adoption
//   - update_executor_helpers.go    → payload, drain, and idle helpers
//   - update_executor_pipeline.go   → runForward / runRollback / runRollbackSteps
//   - update_executor_steps.go      → per-step helpers (cosign, pull, restart, ...)
package fleet

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"velox-server/internal/store"
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
	backend          UpdateBackend
	drainTimeout     time.Duration
	waitReadyTimeout time.Duration
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
	return &UpdateExecutor{
		backend:          b,
		drainTimeout:     timeoutActiveJobsIdle,
		waitReadyTimeout: timeoutWaitReady,
	}
}

// AttachRuntimeBackends completes the production composition after the
// canonical Level-D executor and Drive service have been constructed. It is
// intentionally atomic at bootstrap time: an update executor with any
// missing runtime backend is rejected by the composition root.
// Capability computes the current fail-closed verdict from the
// executor's live backend surface. Nil-receiver safe so the /ready
// probe and the POST /update gate can call it on a partially
// composed executor without a panic.
// Ready reports whether the update capability is fully wired. This
// is the fail-closed boot signal: while Ready is false, /ready stays
// red (update-capability probe) and POST /update refuses with 503
// instead of accepting an operation that would fail mid-pipeline.
// ValidateProductionBackends prevents the update endpoint from being
// exposed with a partially wired production backend. Delegates to
// Capability so boot validation and the runtime gate share exactly
// one definition of "ready". Error text keeps the
// "missing dependencies: <list>" shape consumed by dashboards/tests.
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
	// Bootstrap normally rejects a partially wired executor before it is
	// registered. Keep the execution boundary fail-closed as well: tests,
	// recovery paths, or a future caller must not reach a nil backend after
	// validation and turn a configuration error into a panic.
	if capability := e.Capability(); !capability.Ready {
		return fmt.Errorf("update: capability misconfigured: missing dependencies: %s",
			strings.Join(capability.Missing, ", "))
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

	// ── Phase 4: own the drain and wait for authoritative DRAINING ───
	// DRAINING means the registry read model reports drain=true AND
	// active_tasks=0 — never merely that SetDrainMode returned nil.
	// Preserve an operator-owned drain. Only undo the transition that
	// this executor made; callers must serialize worker mutations while
	// an update operation is active.
	e.recordPhase(ctx, op.WorkerID, RolloutPhaseDraining)
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
	e.recordPhase(ctx, op.WorkerID, RolloutPhaseDeploying)

	// ── Phase 6: forward pipeline ───────────────────────────────────
	// Snapshot the authenticated session bound to the worker BEFORE the
	// restart. WAITING_READY will only accept a DIFFERENT (new) session
	// proving the worker came back up through a fresh Hello — a cached
	// READY from the pre-restart session can never advance the rollout.
	// Fail closed: if the authenticated session cannot be read, the
	// new-session gate has no baseline to compare against.
	preRestartSessionID := ""
	if snap, snapErr := e.authenticatedRuntimeSnapshot(ctx, op.WorkerID); snapErr != nil {
		if releaseErr := e.releaseOwnedDrain(ctx, op.WorkerID, drainOwned); releaseErr != nil {
			return fmt.Errorf("update: pre-restart session snapshot: %w (release drain: %v)", snapErr, releaseErr)
		}
		return fmt.Errorf("update: pre-restart session snapshot: %w", snapErr)
	} else if snap != nil {
		preRestartSessionID = snap.SessionID
	}
	observedDigest, runErr := e.runForward(ctx, op, targetDigest, preRestartSessionID)
	if runErr != nil {
		// Mark forward row FAILED first; THEN run rollback. The stable
		// error code (DIGEST_MISMATCH, DRAIN_TIMEOUT, …) is written
		// separately from the human-readable message so operators and
		// metrics can route on the code (migration 153).
		if uerr := e.backend.Deployments.MarkFailed(ctx, deploymentID, e.backend.Now(), classifyDeploymentError(runErr), runErr.Error()); uerr != nil {
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

	// ── Phase 7: release drain, then mark verified terminal ──────────
	// A successful deployment must never be published while the worker
	// remains DRAINING. The drain is part of the success invariant, not
	// cleanup after success; otherwise the fleet ledger and live routing
	// state can disagree. The terminal SUCCEEDED write is MarkVerifiedSucceeded
	// — the ONLY path that advances last_successful_digest — and it re-verifies
	// the observed digest against the target inside the transition transaction.
	if err := e.releaseOwnedDrain(ctx, op.WorkerID, drainOwned); err != nil {
		return fmt.Errorf("update: release drain before success: %w", err)
	}
	if err := e.backend.Deployments.MarkVerifiedSucceeded(ctx, deploymentID, observedDigest, e.backend.Now()); err != nil {
		return fmt.Errorf("update: mark verified SUCCEEDED for %s: %w", deploymentID, err)
	}
	log.Printf("[UPDATE] worker=%s target=%s SUCCEEDED (digest verified)", op.WorkerID, targetDigest)
	return nil
}

// parsePayload unwraps the Operation.Payload into the typed
// UpdatePayload schema. Returns the target_digest (required,
// non-empty after Validate), the explicitPreviousDigest
// (caller-supplied snapshot), and a parse error if the
// payload is malformed or invalid.
// waitForIdle polls until the registry read model reflects the
// authoritative DRAINING condition (drain=true AND active_tasks=0)
// or the poll budget elapses. The default budget is
// timeoutActiveJobsIdle (5min). The full contract lives in
// update_executor_helpers.go.

// Polling is intentional rather than event-driven because
// the registry's in-memory heartbeat metric is the canonical
// active-task count and polling exposes a simple deadline to
// the operator's audit trail.
