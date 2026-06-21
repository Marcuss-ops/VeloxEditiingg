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
		// Phase 4.4: no in-memory mirror; persistent worker_commands is the
		// single source of truth.

		log.Printf("[UPDATE] Update requested for worker %s (version: %s)", body.WorkerID[:min(16, len(body.WorkerID))]+"...", version)

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"message": "Update scheduled",
			"version": version,
		})
	}
}

// GetCommandsHandler returns all pending commands for a worker via HTTP.
// The response includes command_id, command type, timestamp, payload, and
// sequence_num — the worker uses command_id to ack individual commands
// via AckCommandHandler (or the gRPC CommandAck path).
func (h *Handler) GetCommandsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := c.Query("worker_id")
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "worker_id required"})
			return
		}

		cmds := h.cmdMgr.GetPendingCommands(workerID)

		type commandResponse struct {
			CommandID   string                 `json:"command_id"`
			Command     string                 `json:"command"`
			Timestamp   string                 `json:"timestamp"`
			Payload     map[string]interface{} `json:"payload,omitempty"`
			SequenceNum int64                  `json:"sequence_num"`
		}

		data := make([]commandResponse, 0, len(cmds))
		for _, cmd := range cmds {
			data = append(data, commandResponse{
				CommandID:   cmd.CommandID,
				Command:     cmd.Command,
				Timestamp:   cmd.Timestamp,
				Payload:     cmd.Params,
				SequenceNum: cmd.SequenceNum,
			})
		}

		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"data":    data,
		})
	}
}

// AckCommandHandler acknowledges a command by command_id via HTTP.
//
// JSON body shape (both variants accepted):
//
//	{"worker_id": "w1", "command_id": "cmd-w1-drain-123"}
//	{"worker_id": "w1", "command": "drain"}  → 400 (legacy type-based ACK removed in Phase 4.5)
//
// If command_id is present, the handler calls cmdMgr.AckCommandByID (scoped to
// the owning worker). If only the legacy "command" field is present (no
// command_id), the request is rejected with 400 — the type-based ACK path
// was a footgun that could ack the wrong command when two pending commands
// of the same type coexisted.
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

		// Phase 4.5: ACK by command_id ONLY.  Legacy type-based path is
		// rejected — without a command_id we cannot disambiguate which
		// pending command (of possibly several of the same type) is being
		// acknowledged.
		if body.CommandID == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"success": false,
				"error":   "command_id required — legacy type-based ACK removed in Phase 4.5",
			})
			return
		}

		if err := h.cmdMgr.AckCommandByID(body.WorkerID, body.CommandID); err != nil {
			log.Printf("[ACK] Command ACK failed for %s (worker %s): %v",
				body.CommandID, body.WorkerID[:min(16, len(body.WorkerID))]+"...", err)
			c.JSON(http.StatusNotFound, gin.H{"success": false, "error": err.Error()})
			return
		}

		log.Printf("[ACK] Command ACK'd by ID: %s (worker %s)",
			body.CommandID, body.WorkerID[:min(16, len(body.WorkerID))]+"...")

		c.JSON(http.StatusOK, gin.H{
			"success":    true,
			"command_id": body.CommandID,
			"message":    "command acknowledged",
		})
	}
}
