package jobs

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/queue"
	jobservice "velox-server/internal/services/jobs"
	"velox-server/internal/store"
)

// RegisterV2JobRoutes registra le nuove route pulite /api/v1/jobs/{id}/*
// che delegano allo stesso service layer delle route legacy.
// Le route legacy rimangono attive per compatibilità.
func RegisterV2JobRoutes(rg *gin.RouterGroup, cfg *config.Config, fileQ *queue.FileQueue, dbStore *store.SQLiteStore, svc *jobservice.Service) {
	if rg == nil {
		return
	}

	h := &jobV2Handler{cfg: cfg, fileQ: fileQ, dbStore: dbStore, svc: svc}

	rg.POST("/jobs/:id/lease", h.RenewLease())
	rg.POST("/jobs/:id/complete", h.CompleteJob())
	rg.POST("/jobs/:id/fail", h.FailJob())
	rg.POST("/jobs/:id/result", h.SubmitResult())
	rg.POST("/jobs/:id/progress", h.Progress())
	rg.GET("/jobs/:id/attempts", h.ListAttempts())
	rg.GET("/jobs/:id/artifacts", h.ListArtifacts())
	rg.GET("/jobs/:id/events", h.ListEvents())
}

type jobV2Handler struct {
	cfg     *config.Config
	fileQ   *queue.FileQueue
	dbStore *store.SQLiteStore
	svc     *jobservice.Service
}

func (h *jobV2Handler) RenewLease() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("id")
		var body struct {
			WorkerID        string `json:"worker_id"`
			LeaseID         string `json:"lease_id"`
			LeaseExpiresAt  string `json:"lease_expires_at"`
			Attempt         int    `json:"attempt"`
			ContractVersion int    `json:"contract_version"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid request"})
			return
		}
		if jobID == "" || body.LeaseID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id and lease_id required"})
			return
		}
		if body.ContractVersion != 0 && body.ContractVersion != 2 {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "unsupported contract_version"})
			return
		}
		if h.fileQ == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "queue not available"})
			return
		}

		leaseExpiry := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)
		if strings.TrimSpace(body.LeaseExpiresAt) != "" {
			leaseExpiry = strings.TrimSpace(body.LeaseExpiresAt)
		}

		job, err := h.fileQ.GetJobAsMap(c.Request.Context(), jobID)
		if err != nil || job == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "job not found"})
			return
		}

		currentLease := asJobString(job["lease_id"])
		if currentLease != "" && !strings.EqualFold(strings.TrimSpace(currentLease), strings.TrimSpace(body.LeaseID)) {
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "lease mismatch"})
			return
		}

		if err := h.fileQ.RenewJobLease(c.Request.Context(), jobID, body.WorkerID, body.LeaseID, parseLeaseExpiry(leaseExpiry)); err != nil {
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": err.Error()})
			return
		}

		// Auto-log event
		if h.dbStore != nil {
			_ = h.dbStore.LogJobEvent(jobID, "lease_renewed", map[string]interface{}{
				"worker_id": body.WorkerID, "lease_id": body.LeaseID,
				"lease_expires_at": leaseExpiry,
			})
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":               true,
			"job_id":           jobID,
			"lease_id":         body.LeaseID,
			"lease_expires_at": leaseExpiry,
		})
	}
}

func (h *jobV2Handler) CompleteJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("id")
		var body struct {
			WorkerID string `json:"worker_id"`
			LeaseID  string `json:"lease_id"`
			Attempt  int    `json:"attempt"`
		}
		_ = c.ShouldBindJSON(&body)

		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id required"})
			return
		}

		if body.WorkerID == "" {
			body.WorkerID = c.Query("worker_id")
		}
		if body.LeaseID == "" {
			body.LeaseID = c.Query("lease_id")
		}

		// Validate lease
		if err := h.svc.ValidateJobLease(c.Request.Context(), jobID, body.WorkerID, body.LeaseID); err != nil {
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": err.Error()})
			return
		}

		if err := h.svc.CompleteJob(c.Request.Context(), jobID, body.WorkerID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}

		// Auto-log event + update attempt
		if h.dbStore != nil {
			_ = h.dbStore.LogJobEvent(jobID, "job_completed", map[string]interface{}{
				"worker_id": body.WorkerID, "lease_id": body.LeaseID, "attempt": body.Attempt,
			})
			if attempt, _ := h.dbStore.GetLatestJobAttempt(jobID); attempt != nil {
				_ = h.dbStore.UpdateJobAttemptStatus(attempt.ID, "completed", "")
			}
		}

		c.JSON(http.StatusOK, gin.H{"ok": true, "job_id": jobID})
	}
}

func (h *jobV2Handler) SubmitResult() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("id")
		var body struct {
			WorkerID        string                 `json:"worker_id"`
			LeaseID         string                 `json:"lease_id"`
			Status          string                 `json:"status"`
			Error           string                 `json:"error"`
			Output          map[string]interface{} `json:"output"`
			Attempt         int                    `json:"attempt"`
			ContractVersion int                    `json:"contract_version"`
			ArtifactID      string                 `json:"artifact_id"`
			OutputSHA256    string                 `json:"output_sha256"`
			IdempotencyKey  string                 `json:"idempotency_key"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid request"})
			return
		}
		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id required"})
			return
		}

		// Validate lease
		if err := h.svc.ValidateJobLease(c.Request.Context(), jobID, body.WorkerID, body.LeaseID); err != nil {
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": err.Error()})
			return
		}

		req := jobservice.SubmitResultRequest{
			JobID:           jobID,
			WorkerID:        body.WorkerID,
			Status:          body.Status,
			Error:           body.Error,
			Output:          body.Output,
			LeaseID:         body.LeaseID,
			Attempt:         body.Attempt,
			ContractVersion: body.ContractVersion,
			ArtifactID:      body.ArtifactID,
			OutputSHA256:    body.OutputSHA256,
			IdempotencyKey:  body.IdempotencyKey,
			EndTime:         time.Now().UTC().Format(time.RFC3339),
		}
		ok, err := h.svc.SubmitResult(c.Request.Context(), req)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}

		// Auto-log event
		if h.dbStore != nil {
			_ = h.dbStore.LogJobEvent(jobID, "job_result_submitted", map[string]interface{}{
				"worker_id": body.WorkerID, "lease_id": body.LeaseID,
				"status": body.Status, "artifact_id": body.ArtifactID,
				"output_sha256": body.OutputSHA256,
			})
		}

		c.JSON(http.StatusOK, gin.H{"ok": true, "job_id": jobID, "completed": ok})
	}
}

func (h *jobV2Handler) FailJob() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("id")
		var body struct {
			WorkerID string `json:"worker_id"`
			LeaseID  string `json:"lease_id"`
			Error    string `json:"error"`
		}
		_ = c.ShouldBindJSON(&body)

		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id required"})
			return
		}

		// Validate lease
		if err := h.svc.ValidateJobLease(c.Request.Context(), jobID, body.WorkerID, body.LeaseID); err != nil {
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": err.Error()})
			return
		}

		if err := h.svc.FailJob(c.Request.Context(), jobID, body.WorkerID, body.Error); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}

		// Auto-log event + update attempt
		if h.dbStore != nil {
			_ = h.dbStore.LogJobEvent(jobID, "job_failed", map[string]interface{}{
				"worker_id": body.WorkerID, "lease_id": body.LeaseID, "error": body.Error,
			})
			if attempt, _ := h.dbStore.GetLatestJobAttempt(jobID); attempt != nil {
				_ = h.dbStore.UpdateJobAttemptStatus(attempt.ID, "failed", body.Error)
			}
		}

		c.JSON(http.StatusOK, gin.H{"ok": true, "job_id": jobID})
	}
}

// Progress handles POST /api/v1/jobs/:id/progress
func (h *jobV2Handler) Progress() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("id")
		var body struct {
			WorkerID string  `json:"worker_id"`
			LeaseID  string  `json:"lease_id"`
			Percent  float64 `json:"percent"`
			Stage    string  `json:"stage"`
			Message  string  `json:"message"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid request"})
			return
		}

		if h.dbStore != nil {
			extra := map[string]interface{}{
				"worker_id": body.WorkerID,
				"percent":   body.Percent,
				"stage":     body.Stage,
			}
			if body.Message != "" {
				extra["message"] = body.Message
			}
			_ = h.dbStore.LogJobEvent(jobID, "progress", extra)
		}

		// Update job fields with progress
		if h.fileQ != nil && body.Percent > 0 {
			_ = h.fileQ.UpdateJobFields(c.Request.Context(), jobID, map[string]interface{}{
				"progress_percent": body.Percent,
				"progress_stage":   body.Stage,
			})
		}

		c.JSON(http.StatusOK, gin.H{"ok": true, "job_id": jobID, "percent": body.Percent})
	}
}

func (h *jobV2Handler) ListAttempts() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("id")
		if h.dbStore == nil {
			c.JSON(http.StatusOK, gin.H{"attempts": []interface{}{}})
			return
		}
		attempts, err := h.dbStore.GetJobAttempts(jobID, 20)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if attempts == nil {
			attempts = []store.JobAttempt{}
		}
		c.JSON(http.StatusOK, gin.H{"job_id": jobID, "attempts": attempts})
	}
}

func (h *jobV2Handler) ListArtifacts() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("id")
		if h.dbStore == nil {
			c.JSON(http.StatusOK, gin.H{"artifacts": []interface{}{}})
			return
		}
		artifacts, err := h.dbStore.GetArtifactsByJob(jobID, 20)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if artifacts == nil {
			artifacts = []store.Artifact{}
		}
		c.JSON(http.StatusOK, gin.H{"job_id": jobID, "artifacts": artifacts})
	}
}

func (h *jobV2Handler) ListEvents() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("id")
		if h.dbStore == nil {
			c.JSON(http.StatusOK, gin.H{"events": []interface{}{}})
			return
		}
		events, err := h.dbStore.ListJobEvents(jobID, 100)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if events == nil {
			events = []map[string]any{}
		}
		c.JSON(http.StatusOK, gin.H{"job_id": jobID, "events": events})
	}
}
