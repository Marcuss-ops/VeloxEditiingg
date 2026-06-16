package lifecycle

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

func (h *Handler) GetCommandsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := c.Query("worker_id")
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "worker_id required"})
			return
		}
		if !h.authorizeWorkerRequest(c, workerID) {
			return
		}

		pending := h.cmdMgr.GetPendingCommands(workerID)
		commands := make([]gin.H, 0, len(pending))
		for _, cmd := range pending {
			commands = append(commands, gin.H{
				"command":   cmd.Command,
				"timestamp": cmd.Timestamp,
				"payload":   cmd.Params,
			})
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"data":    commands,
		})
	}
}

func (h *Handler) AckCommandHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
			Command  string `json:"command"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "invalid JSON body"})
			return
		}
		if body.WorkerID == "" || body.Command == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "worker_id and command required"})
			return
		}
		if !h.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		h.cmdMgr.AckCommand(body.WorkerID, body.Command)
		c.JSON(http.StatusOK, gin.H{"success": true, "message": "acknowledged"})
	}
}

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
		if !h.authorizeWorkerRequest(c, body.WorkerID) {
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
