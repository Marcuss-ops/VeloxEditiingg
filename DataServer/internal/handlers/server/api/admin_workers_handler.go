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
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// DeploymentReader is the read-only journal seam for the worker's deployment
// operation history. It feeds the card's OPERATION-HISTORY section
// (PreviousDigest + the last operation row) only — current-state digest
// fields come exclusively from WorkerDeploymentStateReader. Keeping this
// seam small lets tests use an in-memory fake and keeps the handler
// independent of the FleetController mutation path.
type DeploymentReader interface {
	GetLatestDeploymentForWorker(context.Context, string) (*store.DeploymentRecord, error)
}

// OperationLedgerReader is the read-only audit seam for the worker's last
// fleet operation row. It feeds WorkerOperationState.Error (the failure
// reason) so the operator sees WHY the last update/rollback failed without
// the operation status contaminating the image match view. Optional: when
// not wired (lightweight/unit deployments), Error is omitted rather than
// fabricated.
type OperationLedgerReader interface {
	ListOperations(context.Context, string, string, int) ([]store.Operation, error)
}

// WorkerDeploymentStateReader is the read-only seam for the durable
// worker_deployment_state projection. When wired, the admin card derives
// its CURRENT-STATE digest fields (desired/running/last_successful) from
// this read model and NEVER from deployment_records history — the journal
// is audit history, not current truth. Optional: when not wired
// (lightweight/unit deployments) or when a worker has no state row
// (pre-migration 151), the journal reconstruction remains as fallback.
type WorkerDeploymentStateReader interface {
	GetWorkerDeploymentState(context.Context, string) (*store.WorkerDeploymentState, error)
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
	operations  OperationLedgerReader
	state       WorkerDeploymentStateReader
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

// SetOperationLedgerReader wires the fleet_operations audit ledger used to
// enrich WorkerOperationState.Error. A nil reader is allowed for
// lightweight/unit-only deployments; Error is then simply omitted.
func (h *AdminWorkersHandler) SetOperationLedgerReader(reader OperationLedgerReader) {
	if h != nil {
		h.operations = reader
	}
}

// SetWorkerDeploymentStateReader wires the durable read model used for the
// card's current-state digest fields. When a state row exists it is
// authoritative; the deployment_records journal is never used to rebuild
// desired/running/last_successful.
func (h *AdminWorkersHandler) SetWorkerDeploymentStateReader(reader WorkerDeploymentStateReader) {
	if h != nil {
		h.state = reader
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
			card, err := h.cardWithError(c.Request.Context(), &list[i])
			if err != nil {
				c.JSON(http.StatusServiceUnavailable, gin.H{"error": "worker deployment projection unavailable"})
				return
			}
			cards = append(cards, card)
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
		card, err := h.cardWithError(c.Request.Context(), info)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "worker deployment projection unavailable"})
			return
		}
		c.JSON(http.StatusOK, card)
	}
}

func (h *AdminWorkersHandler) card(ctx context.Context, info *workersreg.Worker) WorkerCard {
	card, _ := h.cardWithError(ctx, info)
	return card
}

func (h *AdminWorkersHandler) cardWithError(ctx context.Context, info *workersreg.Worker) (WorkerCard, error) {
	card := buildWorkerCard(info)
	card.RunningDigest = card.ImageDigest
	if h == nil || info == nil {
		return card, nil
	}

	// CURRENT-STATE section — the durable worker_deployment_state read
	// model is the ONLY source of truth for what the worker is running,
	// what the fleet wants, and the last verified digest. The API never
	// reconstructs these fields from deployment_records history: the
	// journal is audit history, not current truth (a newer FAILED rollout
	// keeps DESIRED=B / RUNNING=A visible as drift). A worker without a
	// state row (pre-migration 151) gets empty digest fields rather than
	// an invented value — UNKNOWN is honest, reconstruction is not.
	if h.state != nil {
		state, err := h.state.GetWorkerDeploymentState(ctx, info.WorkerID.String())
		if err != nil {
			if !errors.Is(err, store.ErrWorkerDeploymentStateNotFound) {
				return WorkerCard{}, err
			}
		} else if state != nil {
			card.DesiredDigest = state.DesiredDigest
			card.TargetDigest = state.DesiredDigest
			if state.RunningDigest != "" {
				card.RunningDigest = state.RunningDigest
			}
			card.LastSuccessfulDigest = state.LastSuccessfulDigest
			card.LastPhase = state.LastPhase
		}
	}
	// IMAGE section — real-time state only: what is running vs what the
	// fleet wants, and whether they match. No operation-history fields.
	if card.RunningDigest != "" && card.TargetDigest != "" {
		card.ImageState = &WorkerImageState{
			RunningDigest: card.RunningDigest,
			TargetDigest:  card.TargetDigest,
			Match:         card.RunningDigest == card.TargetDigest,
		}
	}

	// OPERATION-HISTORY section — the append-only journal. This is a
	// history view ONLY (PreviousDigest + the last operation row); it
	// never feeds back into the current-state digest fields above.
	if h.deployments == nil {
		return card, nil
	}
	rec, err := h.deployments.GetLatestDeploymentForWorker(ctx, info.WorkerID.String())
	if err != nil {
		if errors.Is(err, store.ErrDeploymentNotFound) {
			return card, nil
		}
		return WorkerCard{}, err
	}
	if rec == nil {
		return card, nil
	}
	card.PreviousDigest = rec.PreviousDigest
	// LAST UPDATE OPERATION section — the operation history, deliberately
	// separate from ImageState: an old FAILED rollout must not make a
	// worker with a matching digest look unhealthy.
	opType := "update"
	if rec.IsRollback {
		opType = "rollback"
	}
	operation := &WorkerOperationState{
		OperationID: rec.DeploymentID,
		Type:        opType,
		Status:      rec.Status,
		StartedAt:   rec.StartedAt.UTC().Format(time.RFC3339Nano),
		FinishedAt:  rec.FinishedAt,
	}
	// Enrich with the failure reason from the fleet_operations audit
	// ledger when available (optional seam — lightweight deployments
	// simply omit Error). Only an update/rollback row that actually
	// FAILED contributes an error: the ledger also carries smoke/drain/
	// resume rows, and a smoke failure must never surface under
	// "LAST UPDATE OPERATION" for a healthy image rollout.
	if h.operations != nil && (rec.Status == store.DeployStatusFailed || rec.Status == store.DeployStatusRolledBack) {
		ops, opErr := h.operations.ListOperations(ctx, info.WorkerID.String(), "", 10)
		if opErr != nil {
			return WorkerCard{}, opErr
		}
		for i := range ops {
			candidate := ops[i]
			if candidate.Op != "update" && candidate.Op != "rollback" {
				continue
			}
			if candidate.Status == store.OperationStatusFailed && candidate.ErrorMessage != "" {
				operation.Error = candidate.ErrorMessage
				break
			}
		}
	}
	card.OperationState = operation
	return card, nil
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
		Readiness:           cloneAnyMap(info.Readiness),
		Runtime:             runtimeSnapshot(info.Metrics),
		RecentErrors:        append([]string(nil), info.RecentErrors...),
	}
}

func cloneAnyMap(in map[string]interface{}) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

// runtimeSnapshot projects only runtime-oriented heartbeat keys. This keeps
// the WorkerCard compact while allowing agents that already report
// systemd/container/ffmpeg facts to make them visible without SSH.
func runtimeSnapshot(metrics map[string]interface{}) map[string]any {
	if len(metrics) == 0 {
		return nil
	}
	allowed := map[string]bool{
		"systemd_unit": true, "systemd_state": true, "container_state": true,
		"container_started_at": true, "container_restart_count": true,
		"image_digest": true, "worker_pid": true, "ffmpeg_processes": true,
		"ffmpeg_version": true, "uptime_seconds": true,
	}
	out := make(map[string]any)
	for key, value := range metrics {
		if allowed[key] {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
