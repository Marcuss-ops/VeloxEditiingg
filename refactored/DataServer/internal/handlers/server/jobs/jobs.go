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
	"velox-server/internal/workers"
)

// JobAPI provides handlers for job management
type JobAPI struct {
	cfg      *config.Config
	fileQ    *queue.FileQueue
	tokenMgr *workers.TokenManager
	service  *jobservice.Service
	dbStore  *store.SQLiteStore
}

// NewJobAPI creates a new JobAPI
func NewJobAPI(cfg *config.Config, fileQ *queue.FileQueue, tokenMgr *workers.TokenManager, service *jobservice.Service, dbStore *store.SQLiteStore) *JobAPI {
	return &JobAPI{
		cfg:      cfg,
		fileQ:    fileQ,
		tokenMgr: tokenMgr,
		service:  service,
		dbStore:  dbStore,
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
