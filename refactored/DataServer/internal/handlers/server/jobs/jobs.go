package jobs

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
	"velox-server/internal/queue"
	jobservice "velox-server/internal/services/jobs"
	"velox-server/internal/workers"
)

// JobAPI provides handlers for job management
type JobAPI struct {
	cfg      *config.Config
	fileQ    *queue.FileQueue
	tokenMgr *workers.TokenManager
	service  *jobservice.Service
}

// NewJobAPI creates a new JobAPI
func NewJobAPI(cfg *config.Config, fileQ *queue.FileQueue, tokenMgr *workers.TokenManager, service *jobservice.Service) *JobAPI {
	return &JobAPI{
		cfg:      cfg,
		fileQ:    fileQ,
		tokenMgr: tokenMgr,
		service:  service,
	}
}

// GetJobHandler handles GET /api/v1/queue/job (worker polls for jobs)
func (api *JobAPI) GetJobHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := strings.TrimSpace(c.Query("worker_id"))
		workerName := strings.TrimSpace(c.Query("worker_name"))
		drain := c.Query("drain") == "true"
		schedulable := c.Query("schedulable") != "false"

		if workerID == "" {
			c.JSON(http.StatusOK, gin.H{"job": nil, "reason": "missing worker_id"})
			return
		}
		result, err := api.service.ClaimNextJob(c.Request.Context(), jobservice.ClaimRequest{
			WorkerID:    workerID,
			WorkerName:  workerName,
			ClientIP:    c.ClientIP(),
			Drain:       drain,
			Schedulable: schedulable,
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"job": nil, "reason": err.Error()})
			return
		}
		if result == nil || result.JobID == "" {
			if result != nil && result.Reason != "" {
				c.JSON(http.StatusOK, gin.H{"job": nil, "reason": result.Reason})
				return
			}
			c.JSON(http.StatusOK, gin.H{"job": nil})
			return
		}
		c.JSON(http.StatusOK, gin.H{"job": result.Payload})
	}
}

// GetJobCompatHandler handles POST /api/jobs/get and returns the Go worker-agent response shape.
func (api *JobAPI) GetJobCompatHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			WorkerID   string `json:"worker_id"`
			WorkerName string `json:"worker_name"`
		}
		_ = c.ShouldBindJSON(&req)

		workerID := strings.TrimSpace(c.Query("worker_id"))
		if workerID == "" {
			workerID = strings.TrimSpace(req.WorkerID)
		}
		workerName := strings.TrimSpace(c.Query("worker_name"))
		if workerName == "" {
			workerName = strings.TrimSpace(req.WorkerName)
		}
		if workerID == "" {
			c.JSON(http.StatusOK, gin.H{"success": false, "message": "missing worker_id"})
			return
		}
		result, err := api.service.ClaimNextJob(c.Request.Context(), jobservice.ClaimRequest{
			WorkerID:    workerID,
			WorkerName:  workerName,
			ClientIP:    c.ClientIP(),
			Schedulable: true,
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"success": false, "message": err.Error()})
			return
		}
		if result == nil || result.JobID == "" {
			message := "no job"
			if result != nil && result.Reason != "" {
				message = result.Reason
			}
			c.JSON(http.StatusOK, gin.H{"success": false, "message": message})
			return
		}
		payload := result.Payload
		if payload != nil {
			if ic, ok := payload["intro_clips"]; ok && payload["intro_clip_paths"] == nil {
				payload["intro_clip_paths"] = ic
			}
			if sc, ok := payload["stock_clips"]; ok && payload["stock_clip_paths"] == nil {
				payload["stock_clip_paths"] = sc
			}
			if vo, ok := payload["voiceovers"]; ok && payload["voiceover_paths"] == nil {
				payload["voiceover_paths"] = vo
			}
		}

		// Return worker-agent-compatible envelope for new_api mode.
		jobType := "render"
		if jt, ok := payload["job_type"].(string); ok && strings.TrimSpace(jt) != "" {
			jobType = strings.TrimSpace(jt)
		}
		timeoutSecs := 0
		switch v := payload["timeout_secs"].(type) {
		case int:
			timeoutSecs = v
		case int64:
			timeoutSecs = int(v)
		case float64:
			timeoutSecs = int(v)
		}
		createdAt := time.Now().UTC().Format(time.RFC3339)
		if s, ok := payload["created_at"].(string); ok && strings.TrimSpace(s) != "" {
			createdAt = s
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"message": "job available",
			"data": gin.H{
				"job_id":           result.JobID,
				"job_type":         jobType,
				"priority":         0,
				"parameters":       payload,
				"created_at":       createdAt,
				"timeout_secs":     timeoutSecs,
				"lease_id":         result.LeaseID,
				"lease_expiry":     result.LeaseExpiresAt,
				"lease_expires_at": result.LeaseExpiresAt,
				"attempt":          result.Attempt,
				"contract_version": result.ContractVersion,
			},
		})
	}
}

// SubmitResultCompatHandler handles POST /api/jobs/result for Go worker-agent.
func (api *JobAPI) SubmitResultCompatHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			JobID           string                 `json:"job_id"`
			WorkerID        string                 `json:"worker_id"`
			Status          string                 `json:"status"`
			Error           string                 `json:"error"`
			Output          map[string]interface{} `json:"output"`
			StartTime       string                 `json:"start_time"`
			EndTime         string                 `json:"end_time"`
			LeaseID         string                 `json:"lease_id"`
			Attempt         int                    `json:"attempt"`
			ContractVersion int                    `json:"contract_version"`
			ArtifactID      string                 `json:"artifact_id"`
			OutputSHA256    string                 `json:"output_sha256"`
			IdempotencyKey  string                 `json:"idempotency_key"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": err.Error()})
			return
		}
		if body.JobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "missing job_id"})
			return
		}
		triggerFallback, err := api.service.SubmitResult(c.Request.Context(), jobservice.SubmitResultRequest{
			JobID:           body.JobID,
			WorkerID:        body.WorkerID,
			Status:          body.Status,
			Error:           body.Error,
			Output:          body.Output,
			EndTime:         body.EndTime,
			LeaseID:         body.LeaseID,
			Attempt:         body.Attempt,
			ContractVersion: body.ContractVersion,
		})
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
			return
		}
		if triggerFallback {
			go api.tryDriveFallbackUpload(body.JobID)
		}
		c.JSON(http.StatusOK, gin.H{"success": true, "message": "result accepted"})
	}
}

// StartJobHandler handles POST /api/v1/queue/start
func (api *JobAPI) StartJobHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body map[string]interface{}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	}
}

// CompleteJobHandler handles POST /complete_job
func (api *JobAPI) CompleteJobHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body map[string]interface{}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}
		jobID, _ := body["job_id"].(string)
		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing job_id"})
			return
		}

		workerID, _ := body["worker_id"].(string)
		leaseID, _ := body["lease_id"].(string)
		if err := api.service.ValidateJobLease(c.Request.Context(), jobID, workerID, leaseID); err != nil {
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if err := api.service.CompleteJob(c.Request.Context(), jobID, workerID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	}
}

// RenewLeaseHandler handles POST /jobs/:id/lease
func (api *JobAPI) RenewLeaseHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			JobID           string `json:"job_id"`
			WorkerID        string `json:"worker_id"`
			LeaseID         string `json:"lease_id"`
			LeaseExpiresAt  string `json:"lease_expires_at"`
			Attempt         int    `json:"attempt"`
			ContractVersion int    `json:"contract_version"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if body.JobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing job_id"})
			return
		}
		if body.ContractVersion != 0 && body.ContractVersion != 2 {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "unsupported contract_version"})
			return
		}
		if api.fileQ == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "job not found"})
			return
		}
		leaseExpiry := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)
		if strings.TrimSpace(body.LeaseExpiresAt) != "" {
			leaseExpiry = strings.TrimSpace(body.LeaseExpiresAt)
		}
		job, err := api.fileQ.GetJobAsMap(c.Request.Context(), body.JobID)
		if err != nil || job == nil {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "error": "job not found"})
			return
		}
		currentLease := asJobString(job["lease_id"])
		if currentLease != "" && body.LeaseID != "" && !strings.EqualFold(strings.TrimSpace(currentLease), strings.TrimSpace(body.LeaseID)) {
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "lease mismatch"})
			return
		}
		if err := api.fileQ.RenewJobLease(c.Request.Context(), body.JobID, body.WorkerID, body.LeaseID, parseLeaseExpiry(leaseExpiry)); err != nil {
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"ok":               true,
			"job_id":           body.JobID,
			"lease_id":         body.LeaseID,
			"lease_expires_at": leaseExpiry,
		})
	}
}

// FailJobHandler handles POST /fail_job
func (api *JobAPI) FailJobHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			JobID    string `json:"job_id"`
			WorkerID string `json:"worker_id"`
			LeaseID  string `json:"lease_id"`
			Error    string `json:"error"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if body.JobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "missing job_id"})
			return
		}
		if err := api.service.ValidateJobLease(c.Request.Context(), body.JobID, body.WorkerID, body.LeaseID); err != nil {
			c.JSON(http.StatusConflict, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if err := api.service.FailJob(c.Request.Context(), body.JobID, body.WorkerID, body.Error); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
	}
}

// GetJobsHandler handles GET /jobs
func (api *JobAPI) GetJobsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		limit := 500
		if l := c.Query("limit"); l != "" {
			if n, err := parseJobInt(l, 1, 5000); err == nil {
				limit = n
			}
		}
		jobsList, err := api.service.ListJobs(c.Request.Context(), limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"jobs": jobsList})
	}
}

// GetJobStatusHandler handles GET /jobs/:id
func (api *JobAPI) GetJobStatusHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("id")
		job, found, err := api.service.GetJob(c.Request.Context(), jobID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if !found {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
			return
		}
		c.JSON(http.StatusOK, job)
	}
}

// GetJobsSummaryHandler handles GET /jobs/summary
func (api *JobAPI) GetJobsSummaryHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		limit := 50
		if l := c.Query("limit"); l != "" {
			if n, err := parseJobInt(l, 1, 200); err == nil {
				limit = n
			}
		}
		summary, err := api.service.GetSummary(c.Request.Context(), limit, time.Now())
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, summary)
	}
}

func asJobString(v interface{}) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func parseLeaseExpiry(raw string) time.Time {
	expiry := time.Now().UTC().Add(30 * time.Minute)
	if strings.TrimSpace(raw) == "" {
		return expiry
	}
	if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(raw)); err == nil {
		return parsed
	}
	return expiry
}

// DeleteJobHandler handles DELETE /jobs/:id
func (api *JobAPI) DeleteJobHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Param("id")

		if api.fileQ == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
			return
		}

		err := api.fileQ.DeleteJob(c.Request.Context(), jobID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"status": "cancelled", "job_id": jobID})
	}
}

// UpdateJobLogsHandler handles POST /update_job_logs
func (api *JobAPI) UpdateJobLogsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			JobID    string              `json:"job_id"`
			WorkerID string              `json:"worker_id"`
			Logs     []queue.JobLogEntry `json:"logs"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if body.JobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "job_id is required"})
			return
		}

		if api.fileQ == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
			return
		}

		// Normalize logs
		nowISO := time.Now().UTC().Format(time.RFC3339) + "Z"
		var normalized []queue.JobLogEntry
		for _, entry := range body.Logs {
			if entry.Timestamp == "" && entry.Time == "" {
				entry.Timestamp = nowISO
			}
			if entry.WorkerID == "" {
				entry.WorkerID = body.WorkerID
			}
			normalized = append(normalized, entry)
		}

		err := api.fileQ.UpdateJobLogs(c.Request.Context(), body.JobID, normalized)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
			"job_id": body.JobID,
			"added":  len(normalized),
		})
	}
}

// GetJobEventsHandler handles GET /job_events
func (api *JobAPI) GetJobEventsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := c.Query("job_id")
		limit := 200
		if l := c.Query("limit"); l != "" {
			if n, err := parseJobInt(l, 1, 5000); err == nil {
				limit = n
			}
		}

		events := make([]map[string]interface{}, 0, limit)
		var err error
		events, err = api.service.GetJobEvents(c.Request.Context(), jobID, limit)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"ok": false, "error": err.Error(), "events": []interface{}{}})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true, "events": events})
	}
}

// parseJobInt parses a string to int with bounds
func parseJobInt(s string, min, max int) (int, error) {
	var n int
	for _, c := range s {
		if c < '0' || c > '9' {
			return min, nil
		}
		n = n*10 + int(c-'0')
	}
	if n < min {
		return min, nil
	}
	if n > max {
		return max, nil
	}
	return n, nil
}
