package workers

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"velox-server/internal/config"
)

// WorkerAssetHandler serves master-staged media assets to remote workers.
type WorkerAssetHandler struct {
	dataDir string
}

func NewWorkerAssetHandler(cfg *config.Config) *WorkerAssetHandler {
	dataDir := ""
	if cfg != nil {
		dataDir = strings.TrimSpace(cfg.DataDir)
		if dataDir == "" {
			dataDir = strings.TrimSpace(cfg.Runtime.DataDir)
		}
	}
	return &WorkerAssetHandler{dataDir: dataDir}
}

func (h *WorkerAssetHandler) ServeVoiceoverAsset() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h == nil || strings.TrimSpace(h.dataDir) == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "asset storage unavailable"})
			return
		}

		jobID := strings.TrimSpace(c.Param("job_id"))
		filename := strings.TrimSpace(c.Param("filename"))
		if jobID == "" || filename == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "job_id and filename required"})
			return
		}

		filename = filepath.Base(filename)
		if filename == "." || filename == string(filepath.Separator) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid filename"})
			return
		}

		filePath := filepath.Join(h.dataDir, "worker_downloads", "script_assets", jobID, filename)
		if !strings.HasPrefix(filepath.Clean(filePath), filepath.Clean(filepath.Join(h.dataDir, "worker_downloads", "script_assets"))) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid asset path"})
			return
		}

		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "asset not found"})
			return
		}

		c.File(filePath)
	}
}
