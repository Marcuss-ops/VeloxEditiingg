// Package api — Step 10/15 fleet-operator: 4-level worker
// health probe endpoint.
//
// Surface:
//
//   GET /api/v1/admin/workers/:worker_id/health
//     ?level=A        → return HealthReport{level:"A"}
//     ?level=B        → return HealthReport{level:"B"}
//     ?level=C        → return HealthReport{level:"C"}
//     ?level=D        → return HealthReport{level:"D"}
//     (no level)      → aggregate {worker_id, reports: [A,B,C,D]}
//     ?level=invalid  → 400 Bad Request
//
// Auth: mounted under the existing adminWorkers adminAuth-gated
// group (adminAuth middleware from VELOX_ADMIN_TOKEN). Same
// gating contract as the Step 1/15 GET endpoints (read-only
// operator view).
//
// Failure modes (mirrors admin_workers_handler.go §1/15):
//
//   reg == nil            → 503 Service Unavailable
//   empty worker_id       → 400 Bad Request
//   unknown worker_id     → 404 Not Found
//   invalid ?level=       → 400 Bad Request
//
// Each per-level probe is a PURE function in fleet.ProbeLevelX;
// the handler is a thin shell that picks the right probe based
// on the query-param. Production wiring (in
// bootstrap_composition.go after buildFleet) leaves SSH and
// Smoke nil for Step 10+; the probe surfaces the missing dep
// in CheckResult{passed:false, detail:"...not wired..."} so
// the operator sees the gap rather than a silent 503.
//
// Handler dependency surface is bundled in a single
// HealthProbeDeps struct mirroring the Step 9/15
// UpdateBackend pattern so the bootstrap call site is
// symmetric (one happy parameter object per handler).
package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/fleet"
	workersreg "velox-server/internal/workers"
)

// HealthProbeDeps is the bundled dependency surface for the
// 4-level probe runner. nil-tolerant on every field: each
// ProbeLevelX surfaces a "<dep> not wired" CheckResult when
// its specific dep is nil so the operator sees the gap. The
// bundle shape mirrors fleet.UpdateBackend (Step 9/15) for
// visual+test symmetry.
type HealthProbeDeps struct {
	// SSH — required for ProbeLevelA and the docker-inspect
	// sub-checks of ProbeLevelB. nil → audit-only (Step 11+
	// wires the real SSH client).
	SSH fleet.BackendSSHClient
	// Deployments — required for ProbeLevelB's
	// image_digest_match sub-check. nil → check surfaces
	// "no deployment_records row to compare".
	Deployments fleet.BackendDeploymentRepo
	// Registry — required for ProbeLevelC (in-process
	// WorkerInfo read). Production wires
	// *fleet.RealRegistryLevelCGater.
	Registry fleet.HealthLevelCGater
	// Smoke — required for ProbeLevelD (level-d smoke test).
	// nil → audit-only (Step 12+ wires the real SmokeRunner).
	Smoke fleet.BackendSmokeRunner
}

// AdminWorkersHealthHandler exposes
// GET /api/v1/admin/workers/:worker_id/health with optional
// ?level=A|B|C|D.
//
// Construction-tolerant of nil reg/deps so a misconfigured
// bootstrap returns 503 on first request rather than
// panicking at route registration time. The SetHealthHandler
// setter on WorkersModule nil-guards the route in
// app/workers.go.
type AdminWorkersHealthHandler struct {
	reg  *workersreg.Registry
	deps HealthProbeDeps
	now  func() time.Time
}

// NewAdminWorkersHealthHandler wires the handler to the
// in-process registry (for the 404 path) and the HealthProbeDeps
// bundle (for the level-specific runs).
//
// Production wiring (cmd/server/bootstrap_composition.go)
// calls this with workersModule.Registry() as the reg and
// a partly-nil deps bundle (registry wired; SSH+Smoke nil
// audit-only for Step 10+).
func NewAdminWorkersHealthHandler(reg *workersreg.Registry, deps HealthProbeDeps) *AdminWorkersHealthHandler {
	return &AdminWorkersHealthHandler{reg: reg, deps: deps, now: func() time.Time { return time.Now().UTC() }}
}

// AggregatedHealth is the JSON envelope returned when ?level
// is absent. Concatenates the 4 reports in canonical A,B,C,D
// order so dashboards can iterate without sorting.
type AggregatedHealth struct {
	WorkerID string               `json:"worker_id"`
	Reports  []fleet.HealthReport `json:"reports"`
}

// GetWorkerHealth returns GET /api/v1/admin/workers/
// :worker_id/health. The query-param ?level selects one
// probe; absent returns the aggregated envelope.
//
// Worker existence is checked via the registry BEFORE calling
// any probe so a 404 surfaces without side effects. Probes are
// called after the existence check passes — if a probe's
// dep is nil, the operator sees the audit row in the
// CheckResult map; we do NOT 503 the whole response.
//
// Return semantics (the JSON envelope shape):
//   200 + AggregatedHealth       — no ?level=
//   200 + fleet.HealthReport     — ?level=A|B|C|D
//   400 Bad Request              — empty worker_id OR invalid level
//   404 Not Found                — unknown worker
//   503 Service Unavailable      — registry not wired
func (h *AdminWorkersHealthHandler) GetWorkerHealth() gin.HandlerFunc {
	return func(c *gin.Context) {
		// ── handler-level preconditions ─────────────────────────
		if h.reg == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "worker registry not available"})
			return
		}
		workerID := strings.TrimSpace(c.Param("worker_id"))
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id path parameter is required"})
			return
		}
		// ── worker existence (404 check before probe runs) ─────
		// The probe functions themselves tolerate missing workers
		// (ProbeLevelC reports worker_present=false) but we want
		// an HTTP 404 to match the GET /:worker_id convention
		// (Step 1/15) — a uniform 404 across the URL surface
		// keeps the operator dashboard's auth/error handling
		// single-source-of-truth.
		if h.reg.GetWorker(c.Request.Context(), workerID) == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "worker not found"})
			return
		}
		now := h.now()
		levelParam := strings.TrimSpace(c.Query("level"))
		// ── aggregated path (no ?level=) ────────────────────────
		if levelParam == "" {
			c.JSON(http.StatusOK, AggregatedHealth{
				WorkerID: workerID,
				Reports: fleet.ProbeAll(
					c.Request.Context(),
					h.deps.SSH,
					h.deps.Deployments,
					h.deps.Registry,
					h.deps.Smoke,
					workerID,
					now,
				),
			})
			return
		}
		// ── single-level path ────────────────────────────────────
		level := fleet.HealthLevel(levelParam)
		if !level.Valid() {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "level must be one of A|B|C|D",
				"got":   levelParam,
			})
			return
		}
		var report fleet.HealthReport
		switch level {
		case fleet.HealthLevelA:
			report = fleet.ProbeLevelA(c.Request.Context(), h.deps.SSH, workerID, now)
		case fleet.HealthLevelB:
			report = fleet.ProbeLevelB(c.Request.Context(), h.deps.SSH, h.deps.Deployments, workerID, now)
		case fleet.HealthLevelC:
			report = fleet.ProbeLevelC(c.Request.Context(), h.deps.Registry, workerID, now)
		case fleet.HealthLevelD:
			report = fleet.ProbeLevelD(c.Request.Context(), h.deps.Smoke, workerID, now)
		}
		c.JSON(http.StatusOK, report)
	}
}
