package darkeditor

import (
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

// GetLogs returns recent log entries
func (h *Handler) GetLogs(c *gin.Context) {
	if h.logger == nil {
		c.JSON(http.StatusOK, gin.H{
			"logs":  []LogEntry{},
			"count": 0,
			"error": "Logger not initialized",
		})
		return
	}

	level := LogLevel(c.Query("level"))
	limit := 100
	if l := c.Query("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}

	entries := h.logger.GetEntries(limit, level)
	c.JSON(http.StatusOK, gin.H{
		"logs":  entries,
		"count": len(entries),
	})
}

// ClientLog receives client-side log messages
func (h *Handler) ClientLog(c *gin.Context) {
	var req ClientLogRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	log.Printf("[CLIENT-%s] %s", strings.ToUpper(req.Level), req.Message)

	if h.logger != nil {
		h.logger.Log(LogLevel(req.Level), req.Message, req.Metadata, "client")
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
