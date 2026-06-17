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

		// Fetch pending commands and mark them as delivered
		pending := h.cmdMgr.GetPendingCommandsAndMarkDelivered(workerID)
		commands := make([]gin.H, 0, len(pending))
		for _, cmd := range pending {
			commands = append(commands, gin.H{
				"command_id":   cmd.CommandID,
				"command":      cmd.Command,
				"timestamp":    cmd.Timestamp,
				"payload":      cmd.Params,
				"sequence_num": cmd.SequenceNum,
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
			WorkerID  string `json:"worker_id"`
			Command   string `json:"command"`
			CommandID string `json:"command_id"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "invalid JSON body"})
			return
		}
		if body.WorkerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "worker_id required"})
			return
		}
		if !h.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		// Prefer ACK by command_id for precise acknowledgement
		if body.CommandID != "" {
			if err := h.cmdMgr.AckCommandByID(body.CommandID); err != nil {
				log.Printf("[CMD_ACK] Failed to ack by id %s: %v", body.CommandID, err)
				c.JSON(http.StatusNotFound, gin.H{"success": false, "error": err.Error()})
				return
			}
		} else if body.Command != "" {
			// Legacy: ack by type
			h.cmdMgr.AckCommand(body.WorkerID, body.Command)
		} else {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "command or command_id required"})
			return
		}

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
