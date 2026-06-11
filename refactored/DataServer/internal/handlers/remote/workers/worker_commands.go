package workers

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

func (wl *WorkerLifecycle) GetCommandsCompatHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := c.Query("worker_id")
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "worker_id required"})
			return
		}
		if !wl.authorizeWorkerRequest(c, workerID) {
			return
		}

		pending := wl.cmdMgr.GetPendingCommands(workerID)
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

func (wl *WorkerLifecycle) AckCommandCompatHandler() gin.HandlerFunc {
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
		if !wl.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		wl.cmdMgr.AckCommand(body.WorkerID, body.Command)
		c.JSON(http.StatusOK, gin.H{"success": true, "message": "acknowledged"})
	}
}

func (wl *WorkerLifecycle) WorkerCommandHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		workerID := c.Query("worker_id")
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id required"})
			return
		}
		if !wl.authorizeWorkerRequest(c, workerID) {
			return
		}

		commands := wl.cmdMgr.GetPendingCommands(workerID)

		c.JSON(http.StatusOK, gin.H{
			"commands": commands,
		})
	}
}

func (wl *WorkerLifecycle) WorkerCommandAckHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			WorkerID string `json:"worker_id"`
			Command  string `json:"command"`
			Error    string `json:"error"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON body"})
			return
		}

		if body.WorkerID == "" || body.Command == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "worker_id and command required"})
			return
		}
		if !wl.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		wl.cmdMgr.AckCommand(body.WorkerID, body.Command)

		if body.Command == "update_code" && body.Error == "" {
			wl.updateMgr.AckUpdate(body.WorkerID, wl.codeVersion)
		}

		c.JSON(http.StatusOK, gin.H{"ok": true})
	}
}

func (wl *WorkerLifecycle) RequestUpdateHandler() gin.HandlerFunc {
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
		if !wl.authorizeWorkerRequest(c, body.WorkerID) {
			return
		}

		version := body.Version
		if version == "" {
			version = wl.codeVersion
		}

		wl.cmdMgr.PushCommand(body.WorkerID, "update_code", map[string]interface{}{
			"version": version,
		})
		wl.updateMgr.RequestUpdate(body.WorkerID, version)

		log.Printf("[UPDATE] Update requested for worker %s (version: %s)", body.WorkerID[:min(16, len(body.WorkerID))]+"...", version)

		c.JSON(http.StatusOK, gin.H{
			"ok":      true,
			"message": "Update scheduled",
			"version": version,
		})
	}
}
