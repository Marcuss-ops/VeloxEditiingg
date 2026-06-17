package assets

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	workersreg "velox-server/internal/workers"
)

// Handler serves master-staged media assets to remote workers.
type Handler struct {
	dataDir  string
	tokenMgr *workersreg.TokenManager
}

// NewHandler creates a new assets Handler.
func NewHandler(cfg *config.Config, tokenMgr *workersreg.TokenManager) *Handler {
	dataDir := ""
	if cfg != nil {
		dataDir = strings.TrimSpace(cfg.Runtime.DataDir)
	}
	return &Handler{dataDir: dataDir, tokenMgr: tokenMgr}
}

// ServeVoiceoverAsset serves voiceover audio files to workers.
func (h *Handler) ServeVoiceoverAsset() gin.HandlerFunc {
	return h.serveScriptAsset("voiceover")
}

// ServeSceneImageAsset serves scene image files to workers.
func (h *Handler) ServeSceneImageAsset() gin.HandlerFunc {
	return h.serveScriptAsset("scene-image")
}

// ServeAsset serves canonical voiceover assets addressed by asset ID.
func (h *Handler) ServeAsset() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !h.authorizeWorker(c) {
			return
		}

		if strings.TrimSpace(h.dataDir) == "" {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "asset storage unavailable"})
			return
		}

		assetID := strings.TrimSpace(c.Param("asset_id"))
		if assetID == "" || strings.ContainsAny(assetID, `/\`) || assetID != filepath.Base(assetID) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid asset id"})
			return
		}

		store := voiceoverassets.NewStore(h.dataDir, 256*1024*1024, []string{h.dataDir})
		resolved, err := store.Lookup(assetID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "asset not found"})
			return
		}

		file, err := os.Open(resolved.LocalPath)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "asset not found"})
			return
		}
		defer file.Close()

		info, err := file.Stat()
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "asset not found"})
			return
		}

		contentType := strings.TrimSpace(resolved.MediaType)
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		c.Header("Content-Type", contentType)
		c.Header("Content-Length", fmt.Sprintf("%d", info.Size()))
		http.ServeContent(c.Writer, c.Request, filepath.Base(resolved.LocalPath), info.ModTime(), file)
	}
}

func (h *Handler) authorizeWorker(c *gin.Context) bool {
	token := workersreg.ExtractBearerToken(
		c.GetHeader("Authorization"),
		c.GetHeader("X-Worker-Token"),
		c.Query("token"),
	)
	if h.tokenMgr == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "worker authentication unavailable"})
		return false
	}
	if token == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "worker authentication required"})
		return false
	}
	if _, ok := h.tokenMgr.ValidateToken(token); !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid worker token"})
		return false
	}
	return true
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
