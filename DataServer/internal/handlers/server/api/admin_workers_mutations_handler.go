// Package api — Step 6/15 fleet-operator: admin worker mutation
// endpoints (drain / resume / quarantine).
//
// SCOPE/DISCIPLINE (mirrors admin_operations_handler.go §4/15):
//
//   - POST-only mutation handlers. Per AGENTS.md §1 spec
//     ("Le mutation admin pubblicano operazioni, NON eseguono SSH/sudo
//     — è il worker della coda l'unico a parlare con Ansible/SSH"),
//     the HTTP layer NEVER executes anything directly. Each
//     handler:
//
//     1. Validates the worker_id and looks up the worker via
//     the in-process registry (404 on miss, 400 on empty).
//     2. Synchronously flips the worker-card flag (Drain /
//     Quarantined) via Registry.SetWorkerDrain /
//     SetWorkerQuarantine so the placement matcher
//     immediately reflects the change (no lease can be
//     granted mid-transition).
//     3. Publishes a fleet_operations ledger row via
//     ControllerPublisher.PublishOperation so the audit
//     dashboard renders the row's QUEUED→RUNNING→
//     SUCCEEDED lifecycle.
//     4. Returns 202 Accepted with the new operation_id.
//
//     The async Operations Runner (FleetController tick goroutine
//     — Step 4/15 wires the abstraction; the supervisor wiring
//     lands in Step 7+) actually drives the SSH/Ansible path:
//     waits for active_jobs=0, runs smoke, etc. Step 6 ships with
//     the noop executor only, so the audit row demonstrates the
//     abstraction end-to-end without an SSH/Ansible dependency.
//
//   - The handler reads through a ControllerPublisher interface
//     seam so tests substitute a stub controller without standing
//     up SQLite or running the migration sweep. Production wires
//     *fleet.FleetController which satisfies the seam via
//     structural typing (no explicit declaration anywhere).
//
//   - Handler-side schema conformance:
//     op ∈ {drain, resume, quarantine}: enforced by the schema
//     CHECK constraint on fleet_operations + by the partial
//     UNIQUE INDEX on (worker_id, op) WHERE status IN
//     ('QUEUED','RUNNING'). A re-issue during the same in-flight
//     window trips ErrOperationInFlight → 409 Conflict.
//
// URL/auth convention matches admin_workers_handler.go (Step 1/15):
//
//	/api/v1/admin/workers/:worker_id/{drain,resume,quarantine} → adminAuth
//
// Operator-facing: a click on the "drain" button publishes a
// ledger row AND immediately marks the worker Drain=true. The
// WorkerCard's health derivation (Step 3/15) reflects DRAINING
// on the next poll. The tick goroutine's noop executor
// transitions the ledger row to SUCCEEDED in the next dispatch,
// giving the dashboard the QUEUED→RUNNING→SUCCEEDED audit
// trail without an SSH handshake.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/deploy"
	"velox-server/internal/fleet"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// ControllerPublisher is the slice of *fleet.FleetController this
// mutation handler depends on. Defined locally so Step 6/15's new
// handler is isolated from Step 4/15's ControllerAudit seam — no
// widening of the audit-only interface (which would force Step 6
// to touch the Step 4 file and violate atomic-commit discipline).
//
// *fleet.FleetController satisfies this interface via structural
// typing (it has PublishOperation(ctx, *store.Operation) error from
// Step 4/15). Tests substitute a stub to inject canned responses
// without standing up SQLite.
type ControllerPublisher interface {
	PublishOperation(ctx context.Context, op *store.Operation) error
}

// MutationRequest is the JSON body shape for drain/resume/quarantine
// POSTs. Reason is the operator's intent text rendered verbatim in
// the audit dashboard's audit log; falls back to a constant when
// the operator omits the field (the admin auth context does not yet
// carry an operator identity in Step 6).
//
// Body is OPTIONAL — handlers tolerate an absent body, an empty
// body, and a body without the `reason` field. The
// TrimWhitespace+empty-fallback chain ensures the schema's
// `length(reason) > 0` CHECK never trips on handler output.
type MutationRequest struct {
	Reason       string `json:"reason"`
	TargetDigest string `json:"target_digest"`
}

// validateAdminTargetDigest is the API boundary for worker updates. Reuse
// deploy.ValidateImageRef so the HTTP path, UpdateExecutor, and worker-side
// prepare-host validation accept exactly the same immutable GHCR reference.
// Keep this validation before worker lookup, registry mutation, and operation
// publication so rejected requests have no observable side effects.
func validateAdminTargetDigest(ref string) error {
	return deploy.ValidateImageRef(ref)
}

// MutationResponse is the unified 202 envelope for all 3 mutations.
// Includes worker_id (echoed) + operation_id (newly-minted UUIDv4
// row id) + op + status (always QUEUED post-publish) + queued_at
// (RFC3339) + reason. The operation row is then transitioned by
// the FleetController tick goroutine through RUNNING →
// SUCCEEDED/FAILED over time; the audit dashboard polls
// GET /api/v1/admin/operations/{operation_id} for the terminal
// state.
type MutationResponse struct {
	WorkerID    string `json:"worker_id"`
	OperationID string `json:"operation_id"`
	Op          string `json:"op"`
	Status      string `json:"status"`
	QueuedAt    string `json:"queued_at"`
	Reason      string `json:"reason,omitempty"`
}

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

// AdminWorkersMutationsHandler holds the dependencies for 3 admin
// mutation endpoints under /api/v1/admin/workers/:worker_id/.
// Construction-tolerant of nil deps so a partial-boot config
// can surface 503 on the first request rather than crashing at
// route registration time.
type AdminWorkersMutationsHandler struct {
	reg       *workersreg.Registry
	publisher ControllerPublisher

	// updateGate is the fail-closed UpdateExecutor capability gate
	// (AZIONE 2). nil disables the gate (unit-test / legacy wiring).
	// When set, POST /update refuses with 503 while the gate returns
	// non-nil — the master no longer accepts an update operation it
	// will fail ~30s later inside the executor. The closure reads the
	// LIVE UpdateExecutor capability at request time so backends
	// attached later during bootstrap are reflected immediately.
	updateGate func() error
}

// NewAdminWorkersMutationsHandler wires the mutation handler to the
// in-process worker registry (Drain/Quarantined flags) + the
// FleetController publisher (audit ledger).
//
// Production wiring (cmd/server/bootstrap_composition.go) passes
// (*fleet.FleetController) as the publisher and (*workersreg.Registry)
// as the registry; both pointers are stable for the master's
// lifetime.
func NewAdminWorkersMutationsHandler(reg *workersreg.Registry, pub ControllerPublisher) *AdminWorkersMutationsHandler {
	return &AdminWorkersMutationsHandler{reg: reg, publisher: pub}
}

// SetUpdateGate installs the fail-closed update capability gate.
// Idempotent; nil disables the gate. The gate is evaluated per
// request, so wiring it before the UpdateExecutor's runtime backends
// are attached (fresh Level-D smoke + Drive verifier) is safe: the
// closure reads live state at POST time.
//
// MUST be called during bootstrap before the router starts serving:
// the field is a plain (unlocked) reference read on every POST /update
// (same contract as the Set*Handler setters — no mutation after
// RegisterRoutes).
func (h *AdminWorkersMutationsHandler) SetUpdateGate(gate func() error) {
	h.updateGate = gate
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

// mutationHandler is the unified request→publish path used by all
// 3 mutation endpoints. Each handler is a thin wrapper that
// supplies the OperationKind + the state-changing action closure.
//
// Failure modes (mirrors admin_workers_handler.go):
//
//	publisher==nil or reg==nil → 503 Service Unavailable
//	empty worker_id (after trim) → 400 Bad Request
//	unknown worker_id           → 404 Not Found
//	already in desired state    → 409 Conflict (no publish)
//	store.ErrOperationInFlight  → 409 Conflict (in-flight de-dup)
//	other publisher error       → 500 Internal Server Error
//
// Success: 202 Accepted with MutationResponse. The audit row
// appears in GET /api/v1/admin/operations/{operation_id} on the
// next dashboard poll.
func (h *AdminWorkersMutationsHandler) mutationHandler(kind string, action mutationAction) gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.reg == nil || h.publisher == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "fleet mutation dependencies unavailable",
			})
			return
		}

		// ── Fail-closed update capability gate (AZIONE 2) ────────────
		// Refuse to publish an update operation while the
		// UpdateExecutor's critical backends are not fully wired.
		// Without this gate the master would accept the POST, then
		// fail ~30s later inside the executor ("docker client not
		// wired" surfaced only after the tick goroutine dispatched
		// the op). The gate is update-only: drain / resume /
		// quarantine have no UpdateExecutor dependency and stay
		// available even while the update capability is NOT READY.
		if kind == fleet.OperationKindUpdate && h.updateGate != nil {
			if err := h.updateGate(); err != nil {
				c.JSON(http.StatusServiceUnavailable, gin.H{
					"error":  "update capability not ready",
					"detail": err.Error(),
				})
				return
			}
		}

		// ── Path-param validation ─────────────────────────────────────
		workerID := strings.TrimSpace(c.Param("worker_id"))
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id path parameter is required"})
			return
		}

		// ── Body validation (optional) ────────────────────────────────
		// Tolerate absent body, empty body, and body without `reason`.
		// Default reason keeps the schema's `length(reason)>0` CHECK
		// satisfied without forcing the operator to type a custom
		// string for every click.
		var req MutationRequest
		if err := c.ShouldBindJSON(&req); err != nil && !errors.Is(err, io.EOF) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON request body"})
			return
		}
		req.Reason = strings.TrimSpace(req.Reason)
		req.TargetDigest = strings.TrimSpace(req.TargetDigest)
		if req.Reason == "" {
			req.Reason = "triggered via admin API"
		}
		if kind == fleet.OperationKindUpdate {
			if err := validateAdminTargetDigest(req.TargetDigest); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"error": "target_digest is required and must be a pinned ghcr.io image digest",
				})
				return
			}
		}

		// ── Worker lookup ─────────────────────────────────────────────
		info := h.reg.GetWorker(c.Request.Context(), workerID)
		if info == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "worker not found"})
			return
		}
		if info.Resuming {
			c.JSON(http.StatusConflict, gin.H{"error": "worker resume smoke gate is already in-flight"})
			return
		}

		// ── Synchronous state change (immediate placement exclusion) ─
		// Per Q2 design: the handler updates Worker.Drain /
		// Quarantined BEFORE publishing so the placement matcher
		// (costmodel.Score in registry_query.go:GetEligibleWorkers)
		// excludes the worker from the next match. The async tick
		// goroutine + future executor handle the SSH/Ansible path.
		op := &store.Operation{WorkerID: workerID}
		if kind == fleet.OperationKindResume {
			// Generate the operation ID before claiming RESUMING so the
			// persisted gate has an owner even if publication is concurrent.
			if opID := fleet.NewOperationID(); opID != "" {
				op.OperationID = opID
			}
			if err := h.reg.SetWorkerResumingIfClear(c.Request.Context(), workerID, op.OperationID); err != nil {
				if errors.Is(err, workersreg.ErrWorkerResumeInFlight) {
					c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
					return
				}
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		} else if err := action(c.Request.Context(), info); err != nil {
			var already errAlreadyInDesiredState
			if errors.As(err, &already) {
				c.JSON(http.StatusConflict, gin.H{"error": already.Error()})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if kind == fleet.OperationKindResume && !info.Drain && !info.Quarantined {
			_ = h.reg.ClearWorkerResumingIfOwner(c.Request.Context(), workerID, op.OperationID)
			c.JSON(http.StatusConflict, gin.H{"error": "worker is already HEALTHY (no-op)"})
			return
		}

		// ── Publish operation (audit ledger + downstream executor) ──
		now := time.Now().UTC()
		payload := json.RawMessage("{}")
		if kind == fleet.OperationKindUpdate {
			payload, _ = json.Marshal(map[string]string{"target_digest": req.TargetDigest})
		}
		op.WorkerID = workerID
		op.Op = kind
		op.RequestedBy = "admin" // Step 6/15: admin auth context does not yet carry an operator identity
		op.Reason = req.Reason
		op.Payload = payload
		op.QueuedAt = now

		if err := h.publisher.PublishOperation(c.Request.Context(), op); err != nil {
			// Resume marks the worker RESUMING before publication so
			// placement is fail-closed immediately. Every failed
			// publication, including ErrOperationInFlight, must release
			// only this request's claim: no operation was accepted from
			// this handler invocation, so leaving the gate set would
			// permanently block a retry.
			if kind == fleet.OperationKindResume {
				if cleanupErr := h.reg.ClearWorkerResumingIfOwner(c.Request.Context(), workerID, op.OperationID); cleanupErr != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("%v; resume gate cleanup failed: %v", err, cleanupErr)})
					return
				}
			}
			if errors.Is(err, store.ErrOperationInFlight) {
				// Per Q3 design: 409 with structured payload so the
				// operator UI can render "your click was acknowledged,
				// but we are already doing this" instead of a flat
				// error string.
				c.JSON(http.StatusConflict, gin.H{
					"error":        "operation is already in-flight for this worker",
					"worker_id":    workerID,
					"op":           kind,
					"operation_id": op.OperationID,
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// ── 202 Accepted with the audit row's envelope ───────────────
		c.JSON(http.StatusAccepted, MutationResponse{
			WorkerID:    workerID,
			OperationID: op.OperationID,
			Op:          kind,
			Status:      store.OperationStatusQueued,
			QueuedAt:    op.QueuedAt.Format(time.RFC3339),
			Reason:      req.Reason,
		})
	}
}

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
