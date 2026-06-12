package workers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/queue"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// WorkersList same response shape as Python GET /workers
func WorkersList(reg *workersreg.Registry, workersRepo store.WorkersRepository) gin.HandlerFunc {
	return func(c *gin.Context) {
		if workersRepo != nil {
			if dbWorkers, err := workersRepo.ListWorkers(); err == nil && len(dbWorkers) > 0 {
				c.JSON(http.StatusOK, gin.H{"workers": dbWorkers})
				return
			}
		}
		list := reg.List(c.Request.Context())
		c.JSON(http.StatusOK, gin.H{"workers": list})
	}
}

// WorkersStatus returns same shape as Python GET /workers_status for installer/dashboard
func WorkersStatus(reg *workersreg.Registry, q *queue.Queue) gin.HandlerFunc {
	const heartbeatTimeoutSec = 900 // 15 min like Python
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		list := reg.List(ctx)
		now := time.Now().UTC()
		var workersList []gin.H
		activeCount := 0
		for _, w := range list {
			var since float64
			if w.LastHB != "" {
				if t, err := time.Parse(time.RFC3339, w.LastHB); err == nil {
					since = now.Sub(t.UTC()).Seconds()
				}
			}
			active := since < heartbeatTimeoutSec
			if active {
				activeCount++
			}
			workersList = append(workersList, gin.H{
				"worker_id":            w.WorkerID,
				"worker_name":          w.WorkerName,
				"display_name":         w.WorkerName,
				"name":                 w.WorkerName,
				"status":               w.Status,
				"last_heartbeat":       w.LastHB,
				"time_since_heartbeat": since,
				"active":               active,
				"current_job":          w.CurrentJob,
				"code_version":         w.CodeVersion,
				"bundle_version":       w.BundleVersion,
				"drain":                w.Drain,
				"schedulable":          w.Schedulable,
				"worker_group":         w.WorkerGroup,
				"first_seen":           w.FirstSeen,
				"ip_address":           w.IPAddress,
				"readiness":            w.Readiness,
			})
		}
		pending, _ := q.ReadyCount(ctx)
		processing, _ := q.LeasedCount(ctx)

		// Include revoked workers count for dashboard awareness
		revokedList := reg.ListRevoked()

		c.JSON(http.StatusOK, gin.H{
			"workers":          workersList,
			"active_workers":   activeCount,
			"total_workers":    len(workersList),
			"revoked_workers":  len(revokedList),
			"revoked_ids":      revokedList,
			"pending_jobs":     pending,
			"processing_jobs":  processing,
			"completed_jobs":   0,
			"error_jobs":       0,
			"total_jobs":       pending + processing,
		})
	}
}
