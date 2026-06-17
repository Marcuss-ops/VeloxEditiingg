package assets

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
)

// Handler serves master-staged media assets to remote workers.
type Handler struct {
	dataDir string
}

// NewHandler creates a new assets Handler.
func NewHandler(cfg *config.Config) *Handler {
	dataDir := ""
	if cfg != nil {
		dataDir = strings.TrimSpace(cfg.Runtime.DataDir)
	}
	return &Handler{dataDir: dataDir}
}

// ServeVoiceoverAsset serves voiceover audio files to workers.
func (h *Handler) ServeVoiceoverAsset() gin.HandlerFunc {
	return h.serveScriptAsset("voiceover")
}

// ServeSceneImageAsset serves scene image files to workers.
func (h *Handler) ServeSceneImageAsset() gin.HandlerFunc {
	return h.serveScriptAsset("scene-image")
}

func (h *Handler) serveScriptAsset(kind string) gin.HandlerFunc {
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

		baseDir := filepath.Clean(filepath.Join(h.dataDir, "worker_downloads", "script_assets"))
		filePath := filepath.Join(baseDir, jobID, filename)
		if !strings.HasPrefix(filepath.Clean(filePath), baseDir) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid asset path"})
			return
		}

		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": kind + " asset not found"})
			return
		}

		c.File(filePath)
	}
}
