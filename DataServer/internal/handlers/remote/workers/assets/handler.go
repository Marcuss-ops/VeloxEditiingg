package assets

import (
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	driveintegration "velox-server/internal/integrations/drive"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// Handler serves registered assets to remote workers. A local final blob is an
// optional fast path; otherwise the saved external source (for example Drive)
// is resolved and streamed at execution time.
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
// assetSvc.Get(ctx, assetID) → local final blob or asset_sources resolver
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

		if h.assetSvc == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "asset service unavailable"})
			return
		}

		asset, err := h.assetSvc.Get(c.Request.Context(), assetID)
		if err != nil {
			asset = nil
		}
		if asset != nil && h.blobStore != nil && asset.StorageKey != "" {
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

		source, resolveErr := h.assetSvc.ResolveExternalSource(c.Request.Context(), assetID)
		if resolveErr != nil || source == nil || source.Reader == nil {
			// Worker references for deferred Drive assets carry only the opaque
			// file ID at this HTTP boundary. If no local registry row exists,
			// materialize through the authenticated Drive resolver at execution
			// time; local assets remain the fast path above.
			source, resolveErr = h.assetSvc.ResolveDeferredDriveSource(c.Request.Context(), assetID)
		}
		if resolveErr != nil || source == nil || source.Reader == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "asset source unavailable"})
			return
		}
		defer source.Reader.Close()
		contentType := "application/octet-stream"
		if source.MIMEType != "" {
			contentType = source.MIMEType
		} else if asset != nil && asset.MimeType != "" {
			contentType = asset.MimeType
		}
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		c.Header("Content-Type", contentType)
		if source.ExpectedSize > 0 {
			c.Header("Content-Length", fmt.Sprintf("%d", source.ExpectedSize))
		}
		_, _ = io.Copy(c.Writer, source.Reader)
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
