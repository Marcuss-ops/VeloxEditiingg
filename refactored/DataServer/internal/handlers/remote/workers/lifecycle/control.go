package lifecycle

import (
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func (h *Handler) RestartWorkerHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		h.cmdMgr.PushCommand(body.WorkerID, "restart_worker", nil)

		log.Printf("[CONTROL] Restart requested for worker %s", body.WorkerID[:min(16, len(body.WorkerID))]+"...")

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"message": "Restart scheduled",
		})
	}
}

func (h *Handler) RevokeWorkerHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		ctx := c.Request.Context()

		h.reg.RevokeWorker(ctx, body.WorkerID)
		h.tokenMgr.RevokeWorkerTokens(body.WorkerID)

		log.Printf("Worker revoked: %s", body.WorkerID[:min(16, len(body.WorkerID))]+"...")

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"message": "Worker revoked",
		})
	}
}

func (h *Handler) UnrevokeWorkerHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		h.reg.UnrevokeWorker(body.WorkerID)

		log.Printf("Worker unrevoked: %s", body.WorkerID[:min(16, len(body.WorkerID))]+"...")

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"message": "Worker unrevoked",
		})
	}
}

func (h *Handler) DrainWorkerHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
			Drain    bool   `json:"drain"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		ctx := c.Request.Context()
		if err := h.reg.SetWorkerDrain(ctx, body.WorkerID, body.Drain); err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "worker not found"})
			return
		}

		action := "drain"
		if !body.Drain {
			action = "undrain"
		}
		log.Printf("[CONTROL] Worker %s set to %s", body.WorkerID[:min(16, len(body.WorkerID))]+"...", action)

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"drain":   body.Drain,
			"message": "Worker drain status updated",
		})
	}
}

func (h *Handler) GetWorkerDetailsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := c.Param("id")
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		ctx := c.Request.Context()
		worker := h.reg.GetWorker(ctx, workerID)
		if worker == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "worker not found"})
			return
		}

		c.JSON(http.StatusOK, worker)
	}
}

func (h *Handler) CleanupStaleWorkersHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			MaxAgeMinutes int `json:"max_age_minutes"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			body.MaxAgeMinutes = 30
		}

		maxAge := time.Duration(body.MaxAgeMinutes) * time.Minute
		if maxAge <= 0 {
			maxAge = 30 * time.Minute
		}

		ctx := c.Request.Context()
		count := h.reg.CleanupStaleWorkers(ctx, maxAge)

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"removed": count,
			"message": "Stale workers cleaned up",
		})
	}
}

// ListRevokedWorkersHandler returns a list of all revoked worker IDs and their details.
func (h *Handler) ListRevokedWorkersHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		revokedIDs := h.reg.ListRevoked()

		type revokedInfo struct {
			WorkerID string `json:"worker_id"`
		}
		workers := make([]revokedInfo, 0, len(revokedIDs))
		for _, id := range revokedIDs {
			workers = append(workers, revokedInfo{WorkerID: id})
		}

		c.JSON(http.StatusOK, gin.H{
			"workers": workers,
			"count":   len(workers),
		})
	}
}
