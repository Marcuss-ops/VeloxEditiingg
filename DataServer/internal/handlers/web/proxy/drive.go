package proxy

import (
	"bytes"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
)

// DriveProxy handles all routes /api/drive/* by proxying to Job Master backend
func DriveProxy(cfg *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		if cfg.JobMasterURL == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"ok":    false,
				"error": "Drive not available - configure VELOX_JOB_MASTER_URL",
			})
			return
		}

		// Build the backend URL
		path := c.Request.URL.Path
		backendURL := cfg.JobMasterURL + path
		if c.Request.URL.RawQuery != "" {
			backendURL += "?" + c.Request.URL.RawQuery
		}

		// Read body for POST/PUT requests
		var body []byte
		if c.Request.Method == "POST" || c.Request.Method == "PUT" {
			body, _ = io.ReadAll(c.Request.Body)
		}

		req, err := http.NewRequest(c.Request.Method, backendURL, bytes.NewReader(body))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "failed to create proxy request"})
			return
		}

		// Copy headers
		req.Header.Set("Content-Type", c.GetHeader("Content-Type"))
		for k, v := range c.Request.Header {
			if k != "Content-Type" {
				req.Header.Set(k, v[0])
			}
		}

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": "failed to proxy to backend"})
			return
		}
		defer resp.Body.Close()

		respBody, _ := io.ReadAll(resp.Body)
		c.Data(resp.StatusCode, "application/json", respBody)
	}
}


