// Package instaedit exposes the InstaEdit BFF route group on the
// Velox master. Every endpoint in this group is protected by the
// instaeditauth JWT middleware, which verifies signature, issuer,
// audience, expiry, and required scopes.
//
// The routes mounted here are the canonical surface the InstaEdit
// BFF (internal/veloxclient) calls. Handlers scope every read to
// the workspace_id carried in the signed JWT and stamp the
// workspace_id on jobs created through this surface.
package instaedit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"velox-shared/contract"

	"velox-server/internal/costmodel"
	"velox-server/internal/instaeditauth"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/store"
)

// Scope constants used by the JWT-protected route group.
const (
	ScopeJobsRead    = "velox:jobs:read"
	ScopeJobsWrite   = "velox:jobs:write"
	ScopeWorkersRead = "velox:workers:read"
	ScopeAssetsRead  = "velox:assets:read"
)

// defaultDeliveryRetryBudget is the retry budget stamped on each
// delivery-plan entry when the caller does not override it.
const defaultDeliveryRetryBudget = 5

// storeReader is the minimal store surface consumed by the InstaEdit
// BFF handlers. It lets tests and future service layers supply a
// fake or failing implementation without depending on SQLite directly.
type storeReader interface {
	ListJobsByWorkspace(ctx context.Context, workspaceID int64, limit int) ([]map[string]any, error)
	GetJobByWorkspace(ctx context.Context, jobID string, workspaceID int64) (map[string]any, error)
	ListWorkersByWorkspace(workspaceID int64) ([]map[string]any, error)
	GetWorkerByWorkspace(workerID string, workspaceID int64) (map[string]any, error)
	GetDeliveryDestinationByExternalID(ctx context.Context, externalID string) (*store.DeliveryDestination, error)
	ListJobDeliveriesByJob(jobID string) ([]store.JobDelivery, error)
	GetDeliveryDestination(ctx context.Context, destID string) (*store.DeliveryDestination, error)
}

// HandlerDeps carries the dependencies required by the InstaEdit BFF
// handlers. All fields are required for the route group to be mounted;
// the composition root skips the group when the verifier is nil.
type HandlerDeps struct {
	Verifier *instaeditauth.Verifier
	Enqueuer *enqueue.Enqueuer
	Store    storeReader
	Jobs     jobs.Repository
	Assets   store.AssetRepository
}

// Handler holds the dependencies for the InstaEdit BFF endpoints.
type Handler struct {
	deps HandlerDeps
}

// NewHandler creates a Handler wired to the given dependencies.
func NewHandler(deps HandlerDeps) *Handler {
	return &Handler{deps: deps}
}

// RegisterRoutes mounts the /api/v1/instaedit/* routes on the given
// engine. All routes require a valid InstaEdit JWT and the
// appropriate scope.
func (h *Handler) RegisterRoutes(r *gin.Engine) {
	g := r.Group("/api/v1/instaedit")

	jobs := g.Group("/jobs")
	{
		jobs.GET("", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeJobsRead}), h.listJobs())
		jobs.POST("", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeJobsWrite}), h.createJob())
		jobs.GET("/:id", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeJobsRead}), h.getJob())
		jobs.POST("/:id/cancel", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeJobsWrite}), h.cancelJob())
		jobs.GET("/:id/deliveries", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeJobsRead}), h.listJobDeliveries())
	}

	workers := g.Group("/workers")
	{
		workers.GET("", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeWorkersRead}), h.listWorkers())
		workers.GET("/:id", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeWorkersRead}), h.getWorker())
	}

	assets := g.Group("/assets")
	{
		assets.GET("/:id", instaeditauth.Middleware(h.deps.Verifier, []string{ScopeAssetsRead}), h.getAsset())
	}
}

// claimsFromContext is a small helper that extracts the verified JWT
// claims. Handlers should treat a nil return as an unexpected error
// because the middleware aborts the request when verification fails.
func (h *Handler) claimsFromContext(c *gin.Context) *instaeditauth.Claims {
	return instaeditauth.FromContext(c)
}

// --- Wire types -----------------------------------------------------------

type createJobRequest struct {
	ProjectID    string          `json:"project_id"`
	RenderSpec   json.RawMessage `json:"render_spec"`
	DeliveryPlan deliveryPlanReq `json:"delivery_plan"`
}

type deliveryPlanReq struct {
	Destinations []deliveryDestinationReq `json:"destinations"`
}

type deliveryDestinationReq struct {
	ExternalDestinationID string          `json:"external_destination_id"`
	Metadata              json.RawMessage `json:"metadata"`
}

type jobResponse struct {
	ID           string    `json:"id"`
	WorkspaceID  int64     `json:"workspace_id"`
	ProjectID    string    `json:"project_id,omitempty"`
	RenderStatus string    `json:"render_status"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type deliveryResponse struct {
	ExternalDestinationID string `json:"external_destination_id"`
	SocialDeliveryID      string `json:"social_delivery_id"`
	Status                string `json:"status"`
	PlatformMediaID       string `json:"platform_media_id,omitempty"`
	PlatformURL           string `json:"platform_url,omitempty"`
}

type jobDetailResponse struct {
	Job        jobResponse      `json:"job"`
	Deliveries []deliveryResponse `json:"deliveries"`
}

type workerResponse struct {
	ID          string `json:"id"`
	WorkspaceID int64  `json:"workspace_id"`
	Status      string `json:"status"`
	CPU         int    `json:"cpu,omitempty"`
	RAMMB       int    `json:"ram_mb,omitempty"`
	GPU         string `json:"gpu,omitempty"`
	DiskGB      int    `json:"disk_gb,omitempty"`
}

type assetResponse struct {
	ID          string `json:"id"`
	WorkspaceID int64  `json:"workspace_id"`
	SHA256      string `json:"sha256"`
	SizeBytes   int64  `json:"size_bytes"`
	MimeType    string `json:"mime_type"`
	DownloadURL string `json:"download_url,omitempty"`
}

type listJobsResponse struct {
	Jobs []jobResponse `json:"jobs"`
}

type listWorkersResponse struct {
	Workers []workerResponse `json:"workers"`
}

type listDeliveriesResponse struct {
	Deliveries []deliveryResponse `json:"deliveries"`
}

// --- Handlers -------------------------------------------------------------

func (h *Handler) listJobs() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		limit := 100
		if l := c.Query("limit"); l != "" {
			if n, err := parseLimit(l); err == nil {
				limit = n
			}
		}
		rows, err := h.deps.Store.ListJobsByWorkspace(c.Request.Context(), claims.WorkspaceID, limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list jobs"})
			return
		}
		resp := listJobsResponse{Jobs: make([]jobResponse, 0, len(rows))}
		for _, row := range rows {
			resp.Jobs = append(resp.Jobs, h.mapJob(row, claims.WorkspaceID))
		}
		c.JSON(http.StatusOK, resp)
	}
}

func (h *Handler) createJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		var req createJobRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON: " + err.Error()})
			return
		}
		if req.ProjectID == "" {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "project_id is required"})
			return
		}
		if len(req.DeliveryPlan.Destinations) == 0 {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "delivery_plan.destinations is required"})
			return
		}

		// Build the canonical payload using the shared JobPayloadV2 contract.
		// render_spec must be valid JSON, must conform to the canonical V2
		// keyset, and must not contain legacy aliases or unknown top-level
		// keys. We do NOT keep the raw bytes on failure.
		var renderSpec map[string]interface{}
		if len(req.RenderSpec) > 0 {
			if err := json.Unmarshal(req.RenderSpec, &renderSpec); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid render_spec JSON: " + err.Error()})
				return
			}
			if err := contract.StrictValidatePayload(renderSpec); err != nil {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "invalid render_spec: " + err.Error()})
				return
			}
		} else {
			renderSpec = map[string]interface{}{}
		}

		// Resolve each external_destination_id to the internal Velox
		// destination_id. The BFF works with opaque InstaEdit identifiers;
		// Velox still stores its own destination rows.
		deliveryPlan := make([]map[string]interface{}, 0, len(req.DeliveryPlan.Destinations))
		for i, d := range req.DeliveryPlan.Destinations {
			if strings.TrimSpace(d.ExternalDestinationID) == "" {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "delivery_plan destination external_destination_id is required"})
				return
			}
			dest, err := h.deps.Store.GetDeliveryDestinationByExternalID(c.Request.Context(), d.ExternalDestinationID)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to resolve destination"})
				return
			}
			if dest == nil {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "unknown external_destination_id"})
				return
			}
			if !dest.Enabled {
				c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "destination is disabled"})
				return
			}

			metadata := map[string]interface{}{}
			if len(d.Metadata) > 0 {
				if err := json.Unmarshal(d.Metadata, &metadata); err != nil {
					c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "invalid metadata for destination[" + string(rune('0'+i)) + "]"})
					return
				}
			}

			deliveryPlan = append(deliveryPlan, map[string]interface{}{
				"destination_id": dest.DestinationID,
				"priority":       i,
				"retry_budget":   defaultDeliveryRetryBudget,
				"metadata":       metadata,
			})
		}

		// InstaEdit scopes by project; use the project_id as a sensible
		// default video_name when the caller does not supply one. The
		// enqueue normalizer will derive script_text from video_name.
		if _, ok := renderSpec["video_name"]; !ok {
			renderSpec["video_name"] = req.ProjectID
		}

		// The delivery plan resolved from opaque InstaEdit identifiers
		// becomes part of the canonical payload.
		renderSpec["delivery_plan"] = deliveryPlan

		typedPayload := contract.NewJobPayloadV2(renderSpec)
		payload, err := typedPayload.ToMap()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to build canonical payload: " + err.Error()})
			return
		}
		// project_id is InstaEdit-specific and is not part of the
		// canonical worker payload; preserve it for downstream
		// InstaEdit-specific bookkeeping.
		payload["project_id"] = req.ProjectID

		resp, err := h.deps.Enqueuer.Enqueue(c.Request.Context(), payload, costmodel.JobRequirements{}, enqueue.WithWorkspaceID(claims.WorkspaceID))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create job: " + err.Error()})
			return
		}
		jobID, _ := resp["job_id"].(string)
		row, err := h.deps.Store.GetJobByWorkspace(c.Request.Context(), jobID, claims.WorkspaceID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "job created but could not be retrieved"})
			return
		}
		if row == nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "job created but not found"})
			return
		}
		c.JSON(http.StatusCreated, h.mapJob(row, claims.WorkspaceID))
	}
}

func (h *Handler) getJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		jobID := c.Param("id")
		row, err := h.deps.Store.GetJobByWorkspace(c.Request.Context(), jobID, claims.WorkspaceID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get job"})
			return
		}
		if row == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		deliveries, err := h.loadDeliveries(c.Request.Context(), jobID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load deliveries"})
			return
		}
		detail := jobDetailResponse{
			Job:        h.mapJob(row, claims.WorkspaceID),
			Deliveries: deliveries,
		}
		c.JSON(http.StatusOK, detail)
	}
}

func (h *Handler) cancelJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		jobID := c.Param("id")
		// Verify ownership before cancelling.
		row, err := h.deps.Store.GetJobByWorkspace(c.Request.Context(), jobID, claims.WorkspaceID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get job"})
			return
		}
		if row == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		if err := h.deps.Jobs.Cancel(c.Request.Context(), jobID, "cancelled via InstaEdit BFF", 0); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to cancel job"})
			return
		}
		c.Status(http.StatusNoContent)
	}
}

func (h *Handler) listJobDeliveries() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		jobID := c.Param("id")
		// Verify ownership before listing deliveries.
		row, err := h.deps.Store.GetJobByWorkspace(c.Request.Context(), jobID, claims.WorkspaceID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get job"})
			return
		}
		if row == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		deliveries, err := h.loadDeliveries(c.Request.Context(), jobID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load deliveries"})
			return
		}
		c.JSON(http.StatusOK, listDeliveriesResponse{Deliveries: deliveries})
	}
}

func (h *Handler) listWorkers() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		rows, err := h.deps.Store.ListWorkersByWorkspace(claims.WorkspaceID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list workers"})
			return
		}
		resp := listWorkersResponse{Workers: make([]workerResponse, 0, len(rows))}
		for _, row := range rows {
			resp.Workers = append(resp.Workers, h.mapWorker(row, claims.WorkspaceID))
		}
		c.JSON(http.StatusOK, resp)
	}
}

func (h *Handler) getWorker() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		workerID := c.Param("id")
		row, err := h.deps.Store.GetWorkerByWorkspace(workerID, claims.WorkspaceID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		c.JSON(http.StatusOK, h.mapWorker(row, claims.WorkspaceID))
	}
}

func (h *Handler) getAsset() gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := h.claimsFromContext(c)
		assetID := c.Param("id")
		asset, err := h.deps.Assets.GetByIDAndWorkspace(c.Request.Context(), assetID, claims.WorkspaceID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get asset"})
			return
		}
		if asset == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		c.JSON(http.StatusOK, h.mapAsset(asset, claims.WorkspaceID))
	}
}

// --- Mapping helpers -------------------------------------------------------

func (h *Handler) mapJob(row map[string]any, workspaceID int64) jobResponse {
	j := jobResponse{
		ID:           asString(row["job_id"]),
		WorkspaceID:  workspaceID,
		ProjectID:    asString(row["project_id"]),
		RenderStatus: asString(row["status"]),
		CreatedAt:    parseTime(row["created_at"]),
		UpdatedAt:    parseTime(row["updated_at"]),
	}
	if j.RenderStatus == "" {
		j.RenderStatus = "PENDING"
	}
	return j
}

func (h *Handler) mapWorker(row map[string]any, workspaceID int64) workerResponse {
	w := workerResponse{
		ID:          asString(row["worker_id"]),
		WorkspaceID: workspaceID,
		Status:      asString(row["status"]),
	}
	if w.ID == "" {
		w.ID = asString(row["id"])
	}
	if metrics, ok := row["metrics"].(map[string]any); ok {
		if cpu, ok := metrics["cpu_count"].(int64); ok {
			w.CPU = int(cpu)
		} else if cpu, ok := metrics["cpu_count"].(float64); ok {
			w.CPU = int(cpu)
		}
		if ram, ok := metrics["ram_bytes"].(int64); ok {
			w.RAMMB = int(ram / 1024 / 1024)
		} else if ram, ok := metrics["ram_bytes"].(float64); ok {
			w.RAMMB = int(ram / 1024 / 1024)
		}
		if disk, ok := metrics["disk_free_bytes"].(int64); ok {
			w.DiskGB = int(disk / 1024 / 1024 / 1024)
		} else if disk, ok := metrics["disk_free_bytes"].(float64); ok {
			w.DiskGB = int(disk / 1024 / 1024 / 1024)
		}
		w.GPU = asString(metrics["gpu"])
	}
	return w
}

func (h *Handler) mapAsset(a *store.AssetRecord, workspaceID int64) assetResponse {
	return assetResponse{
		ID:          a.AssetID,
		WorkspaceID: workspaceID,
		SHA256:      a.SHA256,
		SizeBytes:   a.SizeBytes,
		MimeType:    a.MimeType,
		DownloadURL: "",
	}
}

func (h *Handler) loadDeliveries(ctx context.Context, jobID string) ([]deliveryResponse, error) {
	rows, err := h.deps.Store.ListJobDeliveriesByJob(jobID)
	if err != nil {
		return nil, err
	}
	out := make([]deliveryResponse, 0, len(rows))
	for _, row := range rows {
		dest, err := h.deps.Store.GetDeliveryDestination(ctx, row.DestinationID)
		if err != nil {
			return nil, err
		}
		externalID := ""
		if dest != nil {
			externalID = dest.ExternalDestinationID
		}
		out = append(out, deliveryResponse{
			ExternalDestinationID: externalID,
			SocialDeliveryID:      row.DeliveryID,
			Status:                row.Status,
			PlatformMediaID:       row.RemoteID,
			PlatformURL:           row.RemoteURL,
		})
	}
	return out, nil
}

// --- Helpers ---------------------------------------------------------------

func asString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	if s, ok := v.([]byte); ok {
		return string(s)
	}
	return fmt.Sprintf("%v", v)
}

func parseTime(v any) time.Time {
	s := asString(v)
	if s == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	return time.Time{}
}

func parseLimit(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid limit")
	}
	if n > 500 {
		n = 500
	}
	return n, nil
}
