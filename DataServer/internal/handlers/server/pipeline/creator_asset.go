package pipeline

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	voiceoverassets "velox-server/internal/assets"
)

// CreatorAssetUpload registers one multipart asset in the canonical,
// content-addressed registry and returns the velox-asset URI for job payloads.
func (h *Handlers) CreatorAssetUpload() gin.HandlerFunc {
	return func(c *gin.Context) {
		if h.assetService == nil || h.cfg == nil {
			creatorPushError(c, http.StatusServiceUnavailable, "asset_service_unavailable", "asset service is not configured", nil)
			return
		}
		kind := strings.TrimSpace(c.PostForm("kind"))
		if kind == "" {
			kind = voiceoverassets.RoleProjectFile
		}
		header, err := c.FormFile("file")
		if err != nil {
			creatorPushError(c, http.StatusBadRequest, "asset_file_required", "multipart field 'file' is required", nil)
			return
		}
		maxBytes := h.cfg.Runtime.MaxVoiceoverBytes
		if maxBytes <= 0 {
			maxBytes = 256 * 1024 * 1024
		}
		tmp, err := os.CreateTemp(h.cfg.Runtime.DataDir, "creator-asset-*")
		if err != nil {
			creatorPushError(c, http.StatusInternalServerError, "asset_stage_failed", "could not stage asset", nil)
			return
		}
		tmpPath := tmp.Name()
		defer os.Remove(tmpPath)
		in, err := header.Open()
		if err != nil {
			_ = tmp.Close()
			creatorPushError(c, http.StatusBadRequest, "asset_read_failed", "could not read uploaded asset", nil)
			return
		}
		n, copyErr := io.Copy(tmp, io.LimitReader(in, maxBytes+1))
		_ = in.Close()
		closeErr := tmp.Close()
		if copyErr != nil || closeErr != nil {
			creatorPushError(c, http.StatusBadRequest, "asset_read_failed", "could not stage uploaded asset", nil)
			return
		}
		if n == 0 || n > maxBytes {
			creatorPushError(c, http.StatusRequestEntityTooLarge, "asset_too_large", fmt.Sprintf("asset must be between 1 and %d bytes", maxBytes), nil)
			return
		}
		asset, err := h.assetService.ResolveAndRegister(c.Request.Context(), voiceoverassets.ResolveAssetCommand{
			Kind: kind, Reference: filepath.Clean(tmpPath), SourceType: "creator_upload",
		})
		if err != nil || asset == nil {
			creatorPushError(c, http.StatusUnprocessableEntity, "asset_registration_failed", "uploaded asset could not be registered", nil)
			return
		}
		c.JSON(http.StatusCreated, gin.H{
			"ok": true, "asset_id": asset.AssetID, "reference": asset.Reference(),
			"sha256": asset.SHA256, "size_bytes": asset.SizeBytes, "mime_type": asset.MimeType,
		})
	}
}
