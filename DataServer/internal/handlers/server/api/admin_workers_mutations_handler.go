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
//     The async Operations Runner (FleetController tick goroutine)
//     drives the concrete executor path: it waits for active_jobs=0,
//     runs the configured smoke flow where required, and records the
//     terminal audit state. A capability gate rejects operations whose
//     production dependencies are not fully wired.
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
// on the next poll. The tick goroutine's registered concrete
// executor transitions the ledger row through its auditable
// QUEUED→RUNNING→SUCCEEDED/FAILED lifecycle.
package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

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

// AdminWorkersMutationsHandler holds the dependencies for 3 admin
// mutation endpoints under /api/v1/admin/workers/:worker_id/.
// Construction-tolerant of nil deps so a partial-boot config
// can surface 503 on the first request rather than crashing at
// route registration time.
type AdminWorkersMutationsHandler struct {
	reg       *workersreg.Registry
	publisher ControllerPublisher

	// updateGate is the fail-closed UpdateExecutor capability gate.
	updateGate func() error

	// resumeGate prevents POST /resume from claiming a worker when the
	// fresh Level-D smoke capability is disabled or misconfigured.
	resumeGate func() error
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

// SetResumeGate installs the fail-closed Level-D smoke gate for resume.
func (h *AdminWorkersMutationsHandler) SetResumeGate(gate func() error) {
	h.resumeGate = gate
}

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

		if !h.authorizeMutation(c, kind) {
			return
		}

		// ── Path-param validation ─────────────────────────────────────
		workerID := strings.TrimSpace(c.Param("worker_id"))
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id path parameter is required"})
			return
		}

		req, err := bindMutationRequest(c, kind)
		if err != nil {
			var invalidJSON invalidMutationJSON
			if errors.As(err, &invalidJSON) {
				c.JSON(http.StatusBadRequest, gin.H{"error": invalidJSON.Error()})
				return
			}
			var invalidDigest invalidMutationDigest
			if errors.As(err, &invalidDigest) {
				c.JSON(http.StatusBadRequest, gin.H{"error": invalidDigest.Error()})
				return
			}
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
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
		op = newMutationOperation(workerID, kind, req, time.Now().UTC(), op.OperationID)
		publishErr, cleanupErr := h.publishMutation(c.Request.Context(), kind, workerID, op)
		if cleanupErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("%v; resume gate cleanup failed: %v", publishErr, cleanupErr)})
			return
		}
		if publishErr != nil {
			if errors.Is(publishErr, store.ErrOperationInFlight) {
				c.JSON(http.StatusConflict, gin.H{
					"error":        "operation is already in-flight for this worker",
					"worker_id":    workerID,
					"op":           kind,
					"operation_id": op.OperationID,
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": publishErr.Error()})
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
