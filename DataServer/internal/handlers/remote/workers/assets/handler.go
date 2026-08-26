package assets

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"
	driveintegration "velox-server/internal/integrations/drive"
	"velox-server/internal/repository"
	workersreg "velox-server/internal/workers"
)

// Handler serves registered assets to remote workers. A local final blob is an
// optional fast path; otherwise the saved external source (for example Drive)
// is resolved and streamed at execution time.
//
// Uses the canonical AssetService (DB as source of truth) + BlobStore
// for all asset resolution and serving.
type Handler struct {
	tokenMgr      *workersreg.TokenManager
	assetSvc      *voiceoverassets.AssetService
	blobStore     repository.BlobStore
	driveSvc      *driveintegration.Service
	materializeMu sync.Mutex
	materializing map[string]*materializationCall
}

type materializationCall struct {
	done  chan struct{}
	asset *voiceoverassets.Asset
	err   error
}

// NewHandler creates a new assets Handler.
func NewHandler(cfg *config.Config, tokenMgr *workersreg.TokenManager, assetSvc *voiceoverassets.AssetService, blobStore repository.BlobStore, driveSvcs ...*driveintegration.Service) *Handler {
	h := &Handler{
		tokenMgr:      tokenMgr,
		assetSvc:      assetSvc,
		blobStore:     blobStore,
		materializing: make(map[string]*materializationCall),
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

		// Deferred provider IDs are materialized once into the verified local
		// registry. This removes repeated Drive downloads and coalesces the
		// first request when multiple workers ask for the same clip.
		if asset == nil {
			if deferredReference, refErr := voiceoverassets.DeferredDriveReference(assetID); refErr == nil {
				asset, _ = h.materializeDeferred(c.Request.Context(), deferredReference)
				if asset != nil && h.serveLocal(c, asset) {
					return
				}
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

		// External and Drive sources are staged in seekable temp files (see
		// the resolver implementations in internal/assets and
		// verifyExternalSource), so the same http.ServeContent used for local
		// final blobs can honor Range requests (206 Partial Content) here too
		// — enabling resumable and future parallel-chunked downloads for
		// Drive-backed assets. A non-seekable reader (a future streaming
		// resolver) degrades to the full-body copy without Range support.
		if seeker, ok := source.Reader.(io.ReadSeeker); ok {
			http.ServeContent(c.Writer, c.Request, source.SuggestedName, time.Time{}, seeker)
			return
		}
		if source.ExpectedSize > 0 {
			c.Header("Content-Length", fmt.Sprintf("%d", source.ExpectedSize))
		}
		_, _ = io.Copy(c.Writer, source.Reader)
	}
}

func (h *Handler) serveLocal(c *gin.Context, asset *voiceoverassets.Asset) bool {
	if asset == nil || h.blobStore == nil || asset.StorageKey == "" {
		return false
	}
	file, err := h.blobStore.ReadFinal(asset.StorageKey)
	if err != nil {
		return false
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return false
	}
	contentType := strings.TrimSpace(asset.MimeType)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	c.Header("Content-Type", contentType)
	c.Header("Content-Length", fmt.Sprintf("%d", info.Size()))
	http.ServeContent(c.Writer, c.Request, filepath.Base(asset.StorageKey), info.ModTime(), file)
	return true
}

func (h *Handler) materializeDeferred(ctx context.Context, reference string) (*voiceoverassets.Asset, error) {
	if existing, err := h.assetSvc.GetBySourceReference(ctx, reference); err == nil && existing != nil {
		return existing, nil
	}
	h.materializeMu.Lock()
	if call, ok := h.materializing[reference]; ok {
		h.materializeMu.Unlock()
		select {
		case <-call.done:
			return call.asset, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	call := &materializationCall{done: make(chan struct{})}
	h.materializing[reference] = call
	h.materializeMu.Unlock()
	call.asset, call.err = h.assetSvc.ResolveAndRegister(ctx, voiceoverassets.ResolveAssetCommand{
		Kind: "clip", Reference: reference, SourceType: "drive_deferred",
	})
	h.materializeMu.Lock()
	delete(h.materializing, reference)
	close(call.done)
	h.materializeMu.Unlock()
	return call.asset, call.err
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
