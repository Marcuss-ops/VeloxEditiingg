package api

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"

	"velox-server/internal/fleet"
	workersreg "velox-server/internal/workers"
)

// errAlreadyInDesiredState is the sentinel returned by the per-op
// action closure when the worker is already in the desired
// state (drain ⇒ already drain=true; resume ⇒ already
// healthy; etc.). The mutationHandler maps this to 409 Conflict
// rather than publishing a noop ledger row — keeps the audit
// table clean and gives the operator a clear "your click was
// redundant" signal.
//
// Reason shape: "worker is already DRAINING (no-op)" /
// "worker is already HEALTHY (no-op)" / "worker is already
// QUARANTINED (no-op)". The current-state vocabulary is
// canonical and matches Health()'s 9-state enum (Step 3/15).
type errAlreadyInDesiredState struct {
	desired string
	current string
}

func (e errAlreadyInDesiredState) Error() string {
	// State vocabulary stays in canonical uppercase (matches
	// Health()'s 9-state enum from registry_health.go) so the
	// operator-facing error message reads "worker is already
	// DRAINING" not "worker is already draining" — the
	// dashboard parser matches the dashboard's vocabulary
	// verbatim across surfaces.
	return fmt.Sprintf("worker is already %s (no-op)", e.current)
}

// mutationAction is the per-op closure the unified handler
// dispatches against. Receives the freshly-looked-up
// Worker (snapshot); mutates the registry (Drain /
// Quarantined flag) and returns:
//
//   - nil                  — state flipped; proceed to publish
//   - errAlreadyInDesiredState — already in target state; 409
//   - any other error      — unexpected; 500
type mutationAction func(ctx context.Context, info *workersreg.Worker) error

// DrainWorker returns POST /api/v1/admin/workers/:worker_id/drain.
//
// Behavior (user spec verbatim: "il Master rifiuta nuovi lease per
// quel worker e attende active_jobs=0 prima di considerare la
// transizione"):
//
//   - Synchronously: Worker.Drain := true so costmodel.Score
//     excludes the worker from GetEligibleWorkers on the next
//     placement call (immediate lease refusal).
//   - Asynchronously: publish a fleet_operations op="drain" row.
//     Future executor (Step 7+) waits for active_jobs=0 before
//     transitioning the worker to a confirmed DRAINING state.
//     Step 6 ships with the noop executor, so the audit row
//     transitions QUEUED→RUNNING→SUCCEEDED immediately — the
//     in-process flag flip is the user-visible effect.
//
// Idempotency:
//   - Already-draining worker → 409 (errAlreadyInDesiredState).
//   - In-flight drain op → 409 (ErrOperationInFlight).
func (h *AdminWorkersMutationsHandler) DrainWorker() gin.HandlerFunc {
	return h.mutationHandler(fleet.OperationKindDrain, func(ctx context.Context, info *workersreg.Worker) error {
		if info.Drain {
			return errAlreadyInDesiredState{desired: "DRAINING", current: "DRAINING"}
		}
		return h.reg.SetWorkerDrain(ctx, info.WorkerID.String(), true)
	})
}

// QuarantineWorker returns POST /api/v1/admin/workers/:worker_id/quarantine.
//
// Behavior (user spec verbatim: "stesso + escluso dal placement"):
//
//   - Synchronously: Worker.Quarantined := true. Health()
//     precedence rank-1 in registry_health.go means QUARANTINED
//     wins over DRAINING/BUSY/HEALTHY/etc., so the operator
//     dashboard surfaces the worker as QUARANTINED immediately.
//     costmodel.Score (or future placement gates) treats
//     Quarantined=true as excluded from placement.
//   - Asynchronously: publish a fleet_operations op="quarantine"
//     row for audit.
//
// Idempotency:
//   - Already-quarantined worker → 409 (errAlreadyInDesiredState).
//   - In-flight quarantine op → 409 (ErrOperationInFlight).
func (h *AdminWorkersMutationsHandler) QuarantineWorker() gin.HandlerFunc {
	return h.mutationHandler(fleet.OperationKindQuarantine, func(ctx context.Context, info *workersreg.Worker) error {
		if info.Quarantined {
			return errAlreadyInDesiredState{desired: "QUARANTINED", current: "QUARANTINED"}
		}
		return h.reg.SetWorkerQuarantine(ctx, info.WorkerID.String(), true)
	})
}

// UpdateWorker returns POST /api/v1/admin/workers/:worker_id/update.
//
// Accepts {target_digest: "ghcr.io/<owner>/<repo>@sha256:<64hex>", reason: "..."}.
// The target_digest is validated with deploy.ValidateImageRef before the
// operation is published. Unlike drain/resume/
// quarantine, update has no synchronous state flag to flip — the
// handler publishes the operation and the FleetController's tick
// goroutine (Step 7+) dispatches it to the UpdateExecutor which
// handles docker pull + compose restart on the worker host via SSH.
//
// Idempotency:
//   - In-flight update op for the same worker → 409 (ErrOperationInFlight).
//   - Malformed or missing target_digest → 400.
func (h *AdminWorkersMutationsHandler) UpdateWorker() gin.HandlerFunc {
	return h.mutationHandler(fleet.OperationKindUpdate, func(ctx context.Context, info *workersreg.Worker) error {
		// No synchronous state flag to flip — the update is purely
		// async via the FleetController tick goroutine.
		return nil
	})
}

// ResumeWorker returns POST /api/v1/admin/workers/:worker_id/resume.
//
// Behavior (user spec verbatim: "ritorna a HEALTHY se smoke verde"):
//
//   - Synchronously: keep Worker.Drain and Worker.Quarantined
//     unchanged, so the worker remains excluded from placement
//     while the asynchronous executor runs the Level D smoke gate.
//
//   - Asynchronously: publish a fleet_operations op="resume" row.
//     The ResumeExecutor clears both exclusion flags only after
//     the smoke gate succeeds; a failed smoke leaves the worker
//     excluded and the operation FAILED.
//
// Idempotency:
//   - Already-healthy worker (no drain, not quarantined) → 409.
//   - In-flight resume op → 409 (ErrOperationInFlight).
//
// A smoke failure is recorded by the asynchronous operation runner;
// the handler itself does not make the worker eligible or clear either
// exclusion flag.
func (h *AdminWorkersMutationsHandler) ResumeWorker() gin.HandlerFunc {
	return h.mutationHandler(fleet.OperationKindResume, func(ctx context.Context, info *workersreg.Worker) error {
		if !info.Drain && !info.Quarantined {
			return errAlreadyInDesiredState{desired: "HEALTHY", current: "HEALTHY"}
		}
		// RESUMING is claimed atomically by mutationHandler before this
		// action. Keep the closure as a no-op to preserve the common
		// mutation shape.
		return nil
	})
}
