package assets

import (
	"fmt"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	driveintegration "velox-server/internal/integrations/drive"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// Handler serves master-staged media assets to remote workers.
//
// Uses the canonical AssetService (DB as source of truth) + BlobStore
// for all asset resolution and serving.
type Handler struct {
	tokenMgr  *workersreg.TokenManager
	assetSvc  *voiceoverassets.AssetService
	blobStore store.BlobStore
	driveSvc  *driveintegration.Service
}

// NewHandler creates a new assets Handler.
func NewHandler(cfg *config.Config, tokenMgr *workersreg.TokenManager, assetSvc *voiceoverassets.AssetService, blobStore store.BlobStore, driveSvcs ...*driveintegration.Service) *Handler {
	h := &Handler{
		tokenMgr:  tokenMgr,
		assetSvc:  assetSvc,
		blobStore: blobStore,
	}
	if len(driveSvcs) > 0 {
		h.driveSvc = driveSvcs[0]
	}
	return h
}

// ServeAsset serves canonical assets addressed by asset ID.
//
// assetSvc.Get(ctx, assetID) → blobStore.ReadFinal(storageKey)
func (h *Handler) ServeAsset() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !h.authorizeWorker(c) {
			return
		}

		assetID := strings.TrimSpace(c.Param("asset_id"))
		if assetID == "" || strings.ContainsAny(assetID, `/\`) || assetID != filepath.Base(assetID) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid asset id"})
			return
		}

		if h.assetSvc == nil || h.blobStore == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "asset service unavailable"})
			return
		}

		asset, err := h.assetSvc.Get(c.Request.Context(), assetID)
		if err != nil {
			asset = nil
		}
		if asset != nil && asset.StorageKey != "" {
			file, openErr := h.blobStore.ReadFinal(asset.StorageKey)
			if openErr == nil {
				defer file.Close()
				info, statErr := file.Stat()
				if statErr != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"error": "asset stat error"})
					return
				}
				contentType := strings.TrimSpace(asset.MimeType)
				if contentType == "" {
					contentType = "application/octet-stream"
				}
				c.Header("Content-Type", contentType)
				c.Header("Content-Length", fmt.Sprintf("%d", info.Size()))
				http.ServeContent(c.Writer, c.Request, filepath.Base(asset.StorageKey), info.ModTime(), file)
				return
			}
		}

		if h.driveSvc == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "asset not found"})
			return
		}
		tmp, tempErr := os.CreateTemp("", "velox-drive-asset-*")
		if tempErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "asset staging unavailable"})
			return
		}
		tmpPath := tmp.Name()
		_ = tmp.Close()
		defer os.Remove(tmpPath)
		if downloadErr := h.driveSvc.DownloadFile(c.Request.Context(), assetID, tmpPath); downloadErr != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "drive asset not found"})
			return
		}
		file, openErr := os.Open(tmpPath)
		if openErr != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "drive asset unavailable"})
			return
		}
		defer file.Close()
		info, statErr := file.Stat()
		if statErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "drive asset stat error"})
			return
		}
		contentType := mime.TypeByExtension(filepath.Ext(assetID))
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		c.Header("Content-Type", contentType)
		c.Header("Content-Length", fmt.Sprintf("%d", info.Size()))
		http.ServeContent(c.Writer, c.Request, assetID, info.ModTime(), file)
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
	if _, ok := h.tokenMgr.ValidateWorkerCommandToken(token); !ok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid worker token"})
		return false
	}
	return true
}
