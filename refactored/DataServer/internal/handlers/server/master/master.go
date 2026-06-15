package master

import (
	"net/http"
	"time"

	"velox-shared/payload"

	"github.com/gin-gonic/gin"

	"velox-server/internal/state"
)

// MasterState returns the current master state
func MasterState() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"ok":    true,
			"state": state.Global.GetState(),
		})
	}
}

// PauseNewJobs toggles pausing new job submissions
// POST /master/pause_new_jobs { "pause": true } or { "pause": false }
func PauseNewJobs() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			Pause bool `json:"pause"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}

		state.Global.PauseNewJobs(body.Pause)

		action := "resumed"
		if body.Pause {
			action = "paused"
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":              true,
			"new_jobs_paused": body.Pause,
			"message":         "New job submissions " + action,
		})
	}
}

// PauseScheduling toggles pausing job scheduling to workers
// POST /master/pause_scheduling { "pause": true } or { "pause": false }
func PauseScheduling() gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			Pause bool `json:"pause"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
			return
		}

		state.Global.PauseScheduling(body.Pause)

		action := "resumed"
		if body.Pause {
			action = "paused"
		}

		c.JSON(http.StatusOK, gin.H{
			"ok":                true,
			"scheduling_paused": body.Pause,
			"message":           "Job scheduling " + action,
		})
	}
}

// GetAPIRequestsLog returns recent API requests for monitoring
func GetAPIRequestsLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		limit := 100
		if l := c.Query("limit"); l != "" {
			if parsed, err := payload.ParseIntParam(l, 0); err == nil && parsed > 0 {
				limit = parsed
			}
		}

		entries := state.Global.GetAPIRequestsLog(limit)
		c.JSON(http.StatusOK, gin.H{
			"ok":    true,
			"count": len(entries),
			"log":   entries,
		})
	}
}

// Middleware to log API requests
func APIRequestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		duration := time.Since(start)
		status := c.Writer.Status()

		entry := state.APIRequestEntry{
			Timestamp:  time.Now().UTC().Format(time.RFC3339),
			Path:       c.Request.URL.Path,
			Status:     status,
			DurationMs: int(duration.Milliseconds()),
		}

		if status >= 400 {
			entry.ErrorType = "HTTP_ERROR"
		}

		state.Global.LogAPIRequest(entry)
	}
}
