// Package api — Step 12/15 fleet-operator: admin worker smoke
// endpoint.
//
// SCOPE/DISCIPLINE (mirrors admin_workers_mutations_handler.go §6/15
// and admin_operations_handler.go §4/15):
//
//   - POST-only mutation handler. Per AGENTS.md §1 spec
//     ("Le mutation admin pubblicano operazioni, NON eseguono SSH/sudo
//     — è il worker della coda l'unico a parlare con Ansible/SSH"),
//     the HTTP layer NEVER executes smoke steps directly. The
//     handler:
//
//     1. Validates the worker_id and looks up the worker via
//     the in-process registry (404 on miss, 400 on empty).
//     2. Parses an OPTIONAL SmokeRequest body (asset_id,
//     render_plan, timeout_sec, reason).
//     3. Publishes a fleet_operations ledger row with
//     kind=OperationKindSmoke via ControllerPublisher.
//     4. Returns 202 Accepted with MutationResponse shape
//     (operation_id, queued_at, asset_id echoed).
//
//   - The async FleetController tick goroutine (Step 4/15 wires
//     the abstraction; Step 9/15 wires the UpdateExecutor for
//     `update`; Step 12/15 wires LevelDSmokeExecutor for `smoke`)
//     actually drives the 6-phase pipeline + duration baseline.
//     A future dashboard polls GET /api/v1/admin/operations/{id}
//     for terminal state AND GET /api/v1/admin/workers/{id}/smokes
//     for the duration analytics (Step 14+ candidate).
//
//   - The handler reads through a ControllerPublisher interface
//     seam so tests substitute a stub controller without standing
//     up SQLite or running the migration sweep. Production wires
//     *fleet.FleetController which satisfies the seam via
//     structural typing.
//
// URL/auth convention matches admin_workers_handler.go (Step 1/15)
// + admin_workers_mutations_handler.go (Step 6/15):
//
// \t/api/v1/admin/workers/:worker_id/smoke → adminAuth
package api

import (
	"context"
	"encoding/json"
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

// SmokeRequest is the JSON body shape for POST /smoke. Optional
// per AGENTS.md §1 — empty body / absent body / body without
// fields all fall through to default behavior. The asset_id is
// required by the executor's parsePayload but at the handler
// layer we tolerate its absence and let the executor surface the
// "payload empty" error in the audit ledger — keeps the operator's
// re-publish pattern symmetric with the Step 6/15 mutations.
//
// Reason falls back to "triggered via admin API" when omitted;
// matches Step 6/15 default.
//
// TimeoutSec<=0 falls through to the executor's defaultSmokeBudget
// (10 minutes), per-step timeouts bounded inside the executor.
type SmokeRequest struct {
	AssetID    string `json:"asset_id"`
	RenderPlan string `json:"render_plan,omitempty"`
	TimeoutSec int    `json:"timeout_sec,omitempty"`
	Reason     string `json:"reason"`
}

// errAssetIDEmpty is the sentinel SurfaceD as 400 Bad Request
// when the operator provides a body without asset_id. Distinct
// from the executor's "payload empty (asset_id required)" guard
// because the handler-side error can give a tighter message
// ("asset_id is required") and the operator UI can render it
// as a per-field error rather than the ledger's per-row error.
type errAssetIDEmpty struct{}

func (errAssetIDEmpty) Error() string { return "asset_id is required in body" }

// AdminWorkersSmokeHandler holds the dependencies for the smoke
// endpoint. Construction-tolerant of nil deps so a partial-boot
// config can surface 503 on the first request rather than crashing
// at route registration time.
type AdminWorkersSmokeHandler struct {
	reg       *workersreg.Registry
	publisher ControllerPublisher
	now       func() time.Time
}

// NewAdminWorkersSmokeHandler wires the handler to the in-process
// worker registry (for the 404 path) + the FleetController
// publisher (audit ledger for kind=smoke).
func NewAdminWorkersSmokeHandler(reg *workersreg.Registry, pub ControllerPublisher) *AdminWorkersSmokeHandler {
	return &AdminWorkersSmokeHandler{reg: reg, publisher: pub, now: func() time.Time { return time.Now().UTC() }}
}

// TriggerSmoke returns POST /api/v1/admin/workers/:worker_id/smoke.
// Validates the worker + body, builds the SmokePayload, publishes
// the operation, returns 202 Accepted with MutationResponse.
//
// Failure modes:
//
// \tpublisher==nil or reg==nil → 503 Service Unavailable
// \tempty worker_id (after trim) → 400 Bad Request
// \tunknown worker                → 404 Not Found
// \tzero asset_id in body         → 400 Bad Request (handler-side;
//
//	tighter than the executor's
//	"payload empty" guard)
//
// \tpublisher error other than
//
//	ErrOperationInFlight         → 500 Internal Server Error
//
// \tErrOperationInFlight          → 409 Conflict (in-flight de-dup)
//
// Success: 202 Accepted with MutationResponse. The audit row's
// QUEUED→RUNNING→SUCCEEDED/FAILED lifecycle renders through
// GET /api/v1/admin/operations/{operation_id}, and the duration
// analytics shows up in smoke_runs (sorted by started_at DESC).
func (h *AdminWorkersSmokeHandler) TriggerSmoke() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.reg == nil || h.publisher == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "smoke dependencies unavailable"})
			return
		}
		workerID := strings.TrimSpace(c.Param("worker_id"))
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id path parameter is required"})
			return
		}
		if h.reg.GetWorker(c.Request.Context(), workerID) == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "worker not found"})
			return
		}
		// ── Body validation (optional) ──────────────────────────────
		var req SmokeRequest
		_ = c.ShouldBindJSON(&req)
		req.AssetID = strings.TrimSpace(req.AssetID)
		req.RenderPlan = strings.TrimSpace(req.RenderPlan)
		req.Reason = strings.TrimSpace(req.Reason)
		if req.AssetID == "" {
			// Render the handler-side 400 with a per-field message
			// so the operator UI can highlight the asset_id field
			// without looking up the ledger row.
			c.JSON(http.StatusBadRequest, gin.H{
				"error":     errAssetIDEmpty{}.Error(),
				"field":     "asset_id",
				"worker_id": workerID,
			})
			return
		}
		if req.Reason == "" {
			req.Reason = "triggered via admin API"
		}
		// ── Encode payload + publish ───────────────────────────────
		payloadBytes, _ := json.Marshal(SmokePayload{
			AssetID:    req.AssetID,
			RenderPlan: req.RenderPlan,
			TimeoutSec: req.TimeoutSec,
			Reason:     req.Reason,
		})
		now := h.now()
		op := &store.Operation{
			WorkerID:    workerID,
			Op:          fleet.OperationKindSmoke,
			RequestedBy: "admin", // Step 6/15: admin auth context does not yet carry an operator identity
			Reason:      req.Reason,
			Payload:     payloadBytes,
			QueuedAt:    now,
		}
		if err := h.publisher.PublishOperation(c.Request.Context(), op); err != nil {
			if errors.Is(err, store.ErrOperationInFlight) {
				c.JSON(http.StatusConflict, gin.H{
					"error":        "smoke operation already in-flight for this worker",
					"worker_id":    workerID,
					"op":           fleet.OperationKindSmoke,
					"operation_id": op.OperationID,
				})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, MutationResponse{
			WorkerID:    workerID,
			OperationID: op.OperationID,
			Op:          fleet.OperationKindSmoke,
			Status:      store.OperationStatusQueued,
			QueuedAt:    op.QueuedAt.Format(time.RFC3339),
			Reason:      req.Reason,
		})
	}
}

// SmokePayload mirrors the fleet.SmokePayload schema but is
// shaped at the API boundary so the handler can marshal from
// the SmokeRequest without exposing the fleet typed-shape to
// the operator UI (and vice versa — fleet.SmokePayload is the
// canonical internal schema).
//
// MarshalFrom is a sentinel struct in the smoke handler's API
// package; the field name conflict with the canonical
// fleet.SmokePayload is intentional — these two structs live
// in different layers (api vs fleet) and JSON marshalling
// happens across the layer boundary.
type SmokePayload struct {
	AssetID    string `json:"asset_id"`
	RenderPlan string `json:"render_plan,omitempty"`
	TimeoutSec int    `json:"timeout_sec,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// The "_ = ctx" pattern below suppresses unused-import warnings
// in case future revisions wire context-aware hooks (e.g., per-
// handler trace propagation). Kept as a comment block rather
// than an executable line so the lint pass stays clean.
var _ = func(ctx context.Context) {} //nolint:staticcheck // symmetry with handler pattern in mutations_handler.go
var _ = fmt.Sprintf                  //nolint:staticcheck
