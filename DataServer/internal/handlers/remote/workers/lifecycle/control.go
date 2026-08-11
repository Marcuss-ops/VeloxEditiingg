package lifecycle

import (
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// workerIDFromAdminPath extracts the canonical worker_id from the
// /api/v1/admin/workers/:worker_id path namespace. The legacy /worker/*
// group identified workers via a JSON body; the canonical operator
// surface (Phase 6 split) uses the path param exclusively.
func workerIDFromAdminPath(c *gin.Context) string {
	return strings.TrimSpace(c.Param("worker_id"))
}

func (h *Handler) RestartWorkerHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := workerIDFromAdminPath(c)
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		commandID, err := h.cmdMgr.PushCommandWithError(workerID, "restart_worker", nil)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to persist restart command", "details": err.Error()})
			return
		}

		log.Printf("[CONTROL] Restart requested for worker %s", workerID[:min(16, len(workerID))]+"...")

		c.JSON(http.StatusOK, gin.H{
			"ok":         true,
			"message":    "Restart scheduled",
			"command_id": commandID,
		})
	}
}

func (h *Handler) RevokeWorkerHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := workerIDFromAdminPath(c)
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		ctx := c.Request.Context()

		h.reg.RevokeWorker(ctx, workerID)
		h.tokenMgr.RevokeWorkerTokens(workerID)

		log.Printf("Worker revoked: %s", workerID[:min(16, len(workerID))]+"...")

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"message": "Worker revoked",
		})
	}
}

func (h *Handler) UnrevokeWorkerHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := workerIDFromAdminPath(c)
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}

		h.reg.UnrevokeWorker(workerID)

		log.Printf("Worker unrevoked: %s", workerID[:min(16, len(workerID))]+"...")

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"message": "Worker unrevoked",
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
