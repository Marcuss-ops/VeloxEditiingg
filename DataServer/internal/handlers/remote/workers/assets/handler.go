package assets

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
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
}

// NewHandler creates a new assets Handler.
func NewHandler(cfg *config.Config, tokenMgr *workersreg.TokenManager, assetSvc *voiceoverassets.AssetService, blobStore store.BlobStore) *Handler {
	return &Handler{
		tokenMgr:  tokenMgr,
		assetSvc:  assetSvc,
		blobStore: blobStore,
	}
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
			c.JSON(http.StatusNotFound, gin.H{"error": "asset not found"})
			return
		}
		if asset == nil || asset.StorageKey == "" {
			c.JSON(http.StatusNotFound, gin.H{"error": "asset not found"})
			return
		}

		file, openErr := h.blobStore.ReadFinal(asset.StorageKey)
		if openErr != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "asset not found"})
			return
		}
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
