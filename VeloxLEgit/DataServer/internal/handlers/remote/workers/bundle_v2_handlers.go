package workers

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
)

// GetManifestV2Handler handles GET /api/worker/v2/manifest
func (h *WorkerUpdateHandler) GetManifestV2Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		manifestPath := filepath.Join(h.bundleDir, "manifest_v2.json")
		if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Manifest V2 not found"})
			return
		}
		c.File(manifestPath)
	}
}

// GetChunkV2Handler handles GET /api/worker/v2/chunk/:chunkName
func (h *WorkerUpdateHandler) GetChunkV2Handler() gin.HandlerFunc {
	return func(c *gin.Context) {
		chunkName := c.Param("chunkName")
		if chunkName == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Chunk name required"})
			return
		}

		if strings.Contains(chunkName, "/") || strings.Contains(chunkName, "\\") {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid chunk name"})
			return
		}

		chunkPath := filepath.Join(h.bundleDir, chunkName)
		stat, err := os.Stat(chunkPath)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Chunk not found"})
			return
		}

		if stat.IsDir() {
			c.JSON(http.StatusForbidden, gin.H{"error": "Cannot serve directory"})
			return
		}

		file, err := os.Open(chunkPath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to read chunk"})
			return
		}
		defer file.Close()

		etag := fmt.Sprintf(`"%x-%x"`, stat.Size(), stat.ModTime().UnixNano())
		c.Header("ETag", etag)

		if match := c.GetHeader("If-None-Match"); match == etag {
			c.Status(http.StatusNotModified)
			return
		}

		http.ServeContent(c.Writer, c.Request, filepath.Base(chunkPath), stat.ModTime(), file)
	}
}
