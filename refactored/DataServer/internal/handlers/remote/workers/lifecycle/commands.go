package lifecycle

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

func (h *Handler) RequestUpdateHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
			Version  string `json:"version"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}
		version := body.Version
		if version == "" {
			version = h.codeVersion
		}

		h.cmdMgr.PushCommand(body.WorkerID, "update_code", map[string]interface{}{
			"version": version,
		})
		h.updateMgr.RequestUpdate(body.WorkerID, version)

		log.Printf("[UPDATE] Update requested for worker %s (version: %s)", body.WorkerID[:min(16, len(body.WorkerID))]+"...", version)

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"message": "Update scheduled",
			"version": version,
		})
	}
}
