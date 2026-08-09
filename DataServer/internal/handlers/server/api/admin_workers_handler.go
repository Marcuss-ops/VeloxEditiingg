// Package api — Step 1/15 fleet operator HTTP entry points.
//
// Two endpoints, distinct URL/auth from the diagnostic surface:
//
//	GET /api/v1/admin/workers           — operator fleet overview
//	GET /api/v1/admin/workers/:worker_id — operator per-worker card
//
// URL and auth contrast:
//
//	/api/v1/workers             — diagnostic allowlist (operator UI)
//	/api/v1/admin/workers       — adminAuth (VELOX_ADMIN_TOKEN)
//
// Both endpoints read from the SAME canonical source
// (`workers.Registry`); only the field shape and the auth surface
// differ. Keeping the two handlers in parallel preserves the security
// posture of the diagnostic surface (no sensitive PII leak through the
// allowlist bypass) while letting the operator dashboard safely drive
// sequencing decisions through adminAuth.
//
// The handler is a thin shell — the `buildWorkerCard` mapper is pure
// (no I/O, no auth) so the mapper is unit-test driven and any future
// state-machine / digest derivation can land there without churn on
// the HTTP layer.
package api

import (
	"context"
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// DeploymentReader is the read-only ledger seam used to make the desired
// image visible beside the worker-reported running image. Keeping this seam
// small lets tests use an in-memory fake and keeps the handler independent of
// the FleetController mutation path.
type DeploymentReader interface {
	GetLatestDeploymentForWorker(context.Context, string) (*store.DeploymentRecord, error)
}

// AdminWorkersHandler holds the registry dependency for the operator-
// facing GET /api/v1/admin/workers endpoints.
//
// Constructor takes a non-nil Registry; tests pass `nil` to exercise
// the 503 path explicitly. Route registration in app/workers.go
// skips the GET when the handler itself is nil so a misconfigured
// bootstrap never accidentally mounts an admin endpoint.
type AdminWorkersHandler struct {
	reg         *workersreg.Registry
	deployments DeploymentReader
}

// NewAdminWorkersHandler wires an AdminWorkersHandler to the worker
// registry read model.
func NewAdminWorkersHandler(reg *workersreg.Registry) *AdminWorkersHandler {
	return &AdminWorkersHandler{reg: reg}
}

// SetDeploymentReader wires the deployment ledger used by production status
// checks. A nil reader is allowed for lightweight/unit-only deployments, but
// production consumers must treat missing ledger data as unverified.
func (h *AdminWorkersHandler) SetDeploymentReader(reader DeploymentReader) {
	if h != nil {
		h.deployments = reader
	}
}

// ListAdminWorkers returns GET /api/v1/admin/workers — the canonical
// fleet operator's view of every registered worker.
//
// Output is sorted by WorkerID (stable alpha) so dashboard consumers
// do not flicker on each poll; the sort lives in the handler-boundary
// so the underlying mapper can be re-ordered (e.g. by Health desc)
// without re-churning the sort key.
//
// Failure modes:
//
//	reg == nil  → 503 Service Unavailable
//	(empty reg) → 200 with `count=0, workers=[]` (legitimate empty
//	              fleet state; the schema is stable for an empty array
//	              so dashboards never see an envelope-only drift)
func (h *AdminWorkersHandler) ListAdminWorkers() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.reg == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "worker registry not available"})
			return
		}
		list := h.reg.List(c.Request.Context())
		cards := make([]WorkerCard, 0, len(list))
		for i := range list {
			cards = append(cards, h.card(c.Request.Context(), &list[i]))
		}
		sort.Slice(cards, func(i, j int) bool {
			return cards[i].WorkerID < cards[j].WorkerID
		})
		c.JSON(http.StatusOK, AdminWorkersListResponse{
			Count:   len(cards),
			Workers: cards,
		})
	}
}

// GetAdminWorker returns GET /api/v1/admin/workers/:worker_id — the
// canonical fleet operator's view of a single worker, or 404 when
// the worker is not registered.
//
// worker_id is trimmed because gin's path-param decoder passes
// surrounding whitespace through verbatim; an empty trim is treated
// as 400 (the path is syntactically valid but semantically empty)
// rather than 404 to give the operator a faster diagnostic signal.
//
// Failure modes:
//
//	reg == nil                → 503 Service Unavailable
//	empty worker_id (after trim) → 400 Bad Request
//	unknown worker_id          → 404 Not Found
func (h *AdminWorkersHandler) GetAdminWorker() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.reg == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "worker registry not available"})
			return
		}
		workerID := strings.TrimSpace(c.Param("worker_id"))
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id path parameter is required"})
			return
		}
		info := h.reg.GetWorker(c.Request.Context(), workerID)
		if info == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "worker not found"})
			return
		}
		c.JSON(http.StatusOK, h.card(c.Request.Context(), info))
	}
}

func (h *AdminWorkersHandler) card(ctx context.Context, info *workersreg.Worker) WorkerCard {
	card := buildWorkerCard(info)
	if h == nil || h.deployments == nil || info == nil {
		return card
	}
	rec, err := h.deployments.GetLatestDeploymentForWorker(ctx, info.WorkerID.String())
	if err != nil || rec == nil {
		return card
	}
	card.TargetDigest = rec.TargetDigest
	card.PreviousDigest = rec.PreviousDigest
	card.DigestState = rec.Status
	return card
}

// buildWorkerCard translates the registry read model into the
// canonical WorkerCard. Pure function — no I/O, no auth — so the
// mapper is unit-test driven and any future state-machine / digest
// derivation can land here without churn on the HTTP layer.
//
// Source map documented in admin_workers_dto.go (WorkerCard doc).
//
// `software_version ← info.CodeVersion` (NOT BundleVersion) because
// the operator's question is "what software is the worker running
// right now", which is the worker-reported code version. The
// staging-bundle label remains available through the diagnostic
// WorkerResponse.BundleVersion field.
//
// executor flattening: deterministic by the first entry of the typed
// registry's canonical (ID, Version) ordering. This keeps the operator
// projection stable and consistent with the master capability registry.
func buildWorkerCard(info *workersreg.Worker) WorkerCard {
	if info == nil {
		return WorkerCard{}
	}
	metrics := ParseWorkerMetrics(info.Metrics)
	var execID string
	var execVer int32
	if exs := extractExecutors(info.ExecutorRegistrySnapshot()); len(exs) > 0 {
		execID = exs[0].ID
		execVer = exs[0].Version
	}
	return WorkerCard{
		WorkerID:            info.WorkerID.String(),
		WorkerName:          sanitiseHostname(info.WorkerName),
		Hostname:            sanitiseHostname(info.WorkerName),
		Host:                sanitiseHostname(info.IPAddress),
		Status:              info.ConnectionStatus,
		ConnectionState:     string(info.ConnectionState),
		SchedulingState:     string(info.SchedulingState),
		DeploymentState:     string(info.DeploymentState),
		HealthState:         string(info.HealthState),
		SessionActive:       info.SessionActive,
		Executor:            execID,
		ExecutorVersion:     execVer,
		ImageDigest:         info.ImageDigest,
		SoftwareVersion:     info.CodeVersion,
		DesiredVersion:      info.DesiredVersion,
		LastHeartbeatAt:     info.LastHB,
		ActiveJobs:          int32(info.Capacity.ActiveSlots),
		MaxActiveJobs:       int32(info.Capacity.MaxSlots),
		ActiveSlots:         int32(info.Capacity.ActiveSlots),
		MaxSlots:            int32(info.Capacity.MaxSlots),
		AvailableSlots:      int32(info.Capacity.AvailableSlots),
		CPUUtilizationRatio: metrics.CPUUtilizationRatio,
		MemoryUsedBytes:     metrics.MemoryUsedBytes,
		DiskFreeBytes:       metrics.DiskFreeBytes,
		Load1:               metrics.Load1,
		CurrentJob:          info.CurrentJob,
		Health:              info.Health, // Step 3/15 — 9-state operator-facing health
	}
}
