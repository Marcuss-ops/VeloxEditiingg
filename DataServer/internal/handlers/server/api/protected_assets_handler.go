// Package api — Pass 6 of the Velox Protected-Asset Snapshot feature.
//
// protected_assets_handler.go maps the in-memory `protectedasset.Service`
// to the worker-facing surface:
//
//	GET /api/v1/workers/cache/protected-assets
//
// Auth is delegated to the upstream `WorkerOrAdminAuthMiddleware` that the
// router layers on this path — the handler itself does NOT inspect tokens,
// so unreachable callers (invalid/Origin/bearers) never reach this code.
// Pass 6.5 (router_wiring, tracked separately) will mount this handler
// under that middleware in cmd/server/router.go.
//
// Snapshot semantics (delegated to protectedasset.Service.Snapshot):
//
//   - Version == 0  → 503 "snapshot not yet generated" (master has
//     not run the periodic loop yet, or the loop is broken)
//   - Version > 0   → 200 + JSON identical to protectedasset.Snapshot
//
// The handler does no DB work per request; the snapshot is already in
// memory thanks to the Service.Run goroutine started at master boot.
// The Service.Snapshot() method is concurrent-safe (RWMutex + struct
// value copy) so multiple workers polling simultaneously do not
// contend with the writer.
package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"velox-server/internal/protectedasset"
)

// ProtectedAssetsHandler is the HTTP layer over the in-memory snapshot.
// Kept intentionally thin: the only job is "introspect Service, render
// the right status code", all business logic lives in protectedasset.
type ProtectedAssetsHandler struct {
	svc *protectedasset.Service
}

// NewProtectedAssetsHandler wires the handler to the in-memory service.
// Returns nil when svc is nil so callers running a no-cache configuration
// (tests, partial bootstrap) can safely skip route registration without
// a nil-pointer panic. The same pattern is used by NewMetricsHandler.
func NewProtectedAssetsHandler(svc *protectedasset.Service) *ProtectedAssetsHandler {
	if svc == nil {
		return nil
	}
	return &ProtectedAssetsHandler{svc: svc}
}

// Snapshot returns GET /api/v1/workers/cache/protected-assets.
//
// Status mapping:
//
//	200 → JSON object with the snapshot fields
//	      {"version": uint64, "generated_at": RFC3339,
//	       "lookahead_jobs": int, "drive_file_ids": []string}
//	503 → service nil OR snapshot not yet generated (Version == 0)
//	401/403 → never seen here; upstream WorkerOrAdminAuthMiddleware
//	          aborts the chain before reaching this handler
//
// Content-Type: application/json (set implicitly by c.JSON).
//
// Nil-receiver contract (IMPORTANT): Snapshot is intentionally
// nil-tolerant so a route mistakenly wired with a nil handler does
// NOT panic. Maintainers MUST NOT dereference `h` before the
// `if h == nil || h.svc == nil` guard — adding e.g.
// `h.logger.Info(...)` at the top silently breaks
// TestPass6_NilHandler_Returns503 and panics in production.
//
// @Summary       Read the master-side protected-asset snapshot
// @Description   Returns the most recent in-memory snapshot of "the next
// @Description   N dispatchable jobs' Drive clip IDs". Workers poll this
// @Description   endpoint to decide which cached assets they MUST keep
// @Description   before their periodic cleanup pass. The snapshot is
// @Description   pre-computed by a periodic loop running on the master —
// @Description   this handler does NOT query the database per request.
// @Tags          workers
// @Produce       json
// @Success       200 {object} protectedasset.Snapshot
// @Failure       503 {object} map[string]string
// @Router        /api/v1/workers/cache/protected-assets [get]
func (h *ProtectedAssetsHandler) Snapshot() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h == nil || h.svc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "protected-asset service not available",
			})
			return
		}
		snap := h.svc.Snapshot()
		if snap.Version == 0 {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "protected-asset snapshot not yet generated",
			})
			return
		}
		c.JSON(http.StatusOK, snap)
	}
}
