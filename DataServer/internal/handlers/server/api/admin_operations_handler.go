// Package api — Step 4/15 fleet-operator: admin audit handler for
// the fleet_operations ledger.
//
// SCOPE/DISCIPLINE:
//
//   - GET-only on this surface. Mutation POSTs (drain, resume,
//     restart, update, rollback, quarantine, smoke) come in
//     later steps. When they do, they call
//     FleetController.PublishOperation and return 202 Accepted
//     with operation_id — the HTTP boundary NEVER executes
//     anything directly; the queue consumer does.
//
//   - The handler reads through a ControllerAudit interface
//     seam so tests substitute a stub without standing up
//     SQLite or running the migration sweep. Production wires
//     fleet.FleetController which satisfies the seam.
//
// URL/auth convention matches admin_workers_handler.go:
//
//   /api/v1/admin/operations  → adminAuth (VELOX_ADMIN_TOKEN)
//
// Operator-facing dashboard enumeration: GET list (with optional
// ?worker_id= and ?status= filters) + GET by id.
package api

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

// ControllerAudit is the slice of *fleet.FleetController this
// handler depends on. Defined as an interface in this package so
// tests can swap a stub controller at construction time. The
// interface name reflects the audit-only responsibility: future
// mutation handlers will depend on a wider ControllerPublisher
// interface that adds PublishOperation.
type ControllerAudit interface {
	AuditList(ctx context.Context, workerID, statusFilter string, limit int) ([]store.Operation, error)
	AuditGet(ctx context.Context, operationID string) (*store.Operation, error)
}

// AdminOperationsHandler holds the controller dependency. The
// handler does not embed a context: gin:Context is the per-
// request context, and the audit calls are independent of
// stateful instance data.
type AdminOperationsHandler struct {
	controller ControllerAudit
}

// NewAdminOperationsHandler wires an AdminOperationsHandler to a
// ControllerAudit source (production: *fleet.FleetController).
//
// Construction guards against a nil source so the production
// router can call this with a partial-boot controller and
// surface a 503 on the first request rather than crashing.
func NewAdminOperationsHandler(c ControllerAudit) *AdminOperationsHandler {
	return &AdminOperationsHandler{controller: c}
}

// ListAdminOperations returns GET /api/v1/admin/operations.
// Supports ?worker_id= and ?status= query filters (exact-match;
// partial / regex / LIKE support lands if the dashboard asks).
// The dispatch list is capped at 200 rows; an unbounded
// enumeration is a Step 7+ concern (cursor-paginated audit
// reads, when the fleet grows to 30+ workers).
//
// Sort: OperationID at the handler boundary so the underlying
// store-layer sort (queued_at DESC) can evolve without churn
// (e.g. future chronological sort).
//
// Failure modes:
//
//	controller == nil  → 503 Service Unavailable
//	(empty result)     → 200 with `count=0, operations=[]`
//	                     (canonical empty envelope, NOT null array)
//	internal error     → 500 with the error message verbatim
func (h *AdminOperationsHandler) ListAdminOperations() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.controller == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "fleet controller not available"})
			return
		}
		workerID := strings.TrimSpace(c.Query("worker_id"))
		statusFilter := strings.TrimSpace(c.Query("status"))
		ops, err := h.controller.AuditList(c.Request.Context(), workerID, statusFilter, 200)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		cards := make([]OperationCard, 0, len(ops))
		for i := range ops {
			cards = append(cards, buildOperationCard(&ops[i]))
		}
		sort.Slice(cards, func(i, j int) bool {
			return cards[i].OperationID < cards[j].OperationID
		})
		c.JSON(http.StatusOK, AdminOperationsListResponse{
			Count:      len(cards),
			Operations: cards,
		})
	}
}

// GetAdminOperation returns GET /api/v1/admin/operations/:operation_id.
//
// WorkerID trim-then-empty semantic mirrors admin_workers_handler.go:
// surrounding whitespace is stripped; an empty-after-trim
// operation_id is treated as 400 (path syntactically valid,
// semantically empty) rather than 404 to give the operator a
// faster diagnostic signal.
//
// Failure modes:
//
//	controller == nil  → 503 Service Unavailable
//	empty operation_id → 400 Bad Request
//	unknown id         → 404 Not Found (ErrOperationNotFound)
//	internal error     → 500 Internal Server Error
func (h *AdminOperationsHandler) GetAdminOperation() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.controller == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "fleet controller not available"})
			return
		}
		opID := strings.TrimSpace(c.Param("operation_id"))
		if opID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "operation_id path parameter is required"})
			return
		}
		op, err := h.controller.AuditGet(c.Request.Context(), opID)
		if err != nil {
			if errors.Is(err, store.ErrOperationNotFound) {
				c.JSON(http.StatusNotFound, gin.H{"error": "operation not found"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, buildOperationCard(op))
	}
}

// buildOperationCard translates the storage Operation into the
// admin audit shape. Pure function — no I/O, no auth — so the
// mapper is unit-test driven (TestBuildOperationCard_AllFields
// etc. in admin_operations_handler_test.go).
//
// Time fields marshaled as RFC3339 strings (matching
// WorkerCard.LastHeartbeatAt convention) so the dashboard parser
// is unchanged across the worker / operation surfaces.
//
// json.RawMessage payload is coerced to a string verbatim so the
// JSON envelope renders it unchanged: the dashboard parses the
// string as a JSON object using whatever client-side path it's
// already written.
func buildOperationCard(op *store.Operation) OperationCard {
	if op == nil {
		return OperationCard{}
	}
	card := OperationCard{
		OperationID: op.OperationID,
		WorkerID:    op.WorkerID,
		Op:          op.Op,
		RequestedBy: op.RequestedBy,
		Reason:      op.Reason,
		Status:      op.Status,
		QueuedAt:    op.QueuedAt.UTC().Format(time.RFC3339),
	}
	if op.StartedAt != nil {
		card.StartedAt = op.StartedAt.UTC().Format(time.RFC3339)
	}
	if op.FinishedAt != nil {
		card.FinishedAt = op.FinishedAt.UTC().Format(time.RFC3339)
	}
	if len(op.Payload) > 0 {
		card.Payload = string(op.Payload)
	}
	card.ErrorMessage = op.ErrorMessage
	return card
}
