package workers

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	workersreg "velox-server/internal/workers"
)

func parseLogLimit(c *gin.Context, defaultLimit int) int {
	limit := defaultLimit
	if v := strings.TrimSpace(c.Query("tail")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	if v := strings.TrimSpace(c.Query("limit")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > 1000 {
		limit = 1000
	}
	return limit
}

func findWorker(list []workersreg.WorkerInfo, key string) *workersreg.WorkerInfo {
	for i := range list {
		w := &list[i]
		if w.WorkerID == key || w.WorkerName == key || w.DisplayName == key || w.Host == key || w.IPAddress == key {
			return w
		}
	}
	return nil
}

func workerLogsResponse(c *gin.Context, found *workersreg.WorkerInfo, limit int) {
	logs := found.RecentLogs
	if limit > 0 && len(logs) > limit {
		logs = logs[len(logs)-limit:]
	}

	msg := ""
	if len(logs) == 0 {
		msg = "Nessun log showlog disponibile. I log vengono generati quando il worker esegue job."
	}

	c.JSON(http.StatusOK, gin.H{
		"worker_id":   found.WorkerID,
		"worker_name": found.WorkerName,
		"count":       len(logs),
		"logs":        logs,
		"message":     msg,
	})
}

// ShowlogHandler returns recent worker logs (showlog push) from registry.
// Query params:
// - worker (preferred), or worker_id / worker_name / ip
// - limit (default 200, max 1000)
func ShowlogHandler(reg *workersreg.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		worker := strings.TrimSpace(c.Query("worker"))
		if worker == "" {
			worker = strings.TrimSpace(c.Query("worker_id"))
		}
		if worker == "" {
			worker = strings.TrimSpace(c.Query("worker_name"))
		}
		if worker == "" {
			worker = strings.TrimSpace(c.Query("ip"))
		}

		if worker == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker (or worker_id/worker_name/ip) required"})
			return
		}

		found := findWorker(reg.List(c.Request.Context()), worker)
		if found == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "worker not found", "worker": worker})
			return
		}

		workerLogsResponse(c, found, parseLogLimit(c, 200))
	}
}

// WorkerLogsHandler returns worker logs from path-param route:
// GET /api/v1/workers/:id/logs?tail=200
func WorkerLogsHandler(reg *workersreg.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := strings.TrimSpace(c.Param("id"))
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker id required"})
			return
		}

		found := findWorker(reg.List(c.Request.Context()), workerID)
		if found == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "worker not found", "worker_id": workerID})
			return
		}

		workerLogsResponse(c, found, parseLogLimit(c, 200))
	}
}
