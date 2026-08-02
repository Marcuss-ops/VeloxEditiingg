package pipeline

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/inputsecurity"
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
		policy := h.assetService.SecurityPolicy()
		maxBytes := policy.MaxBytes
		if maxBytes <= 0 {
			maxBytes = 256 * 1024 * 1024
		}
		tempDir := strings.TrimSpace(policy.TempDir)
		if tempDir == "" {
			tempDir = strings.TrimSpace(h.cfg.Runtime.DataDir)
		}
		if err := os.MkdirAll(tempDir, 0o700); err != nil {
			creatorPushError(c, http.StatusInternalServerError, string(inputsecurity.ErrReadFailed), "could not create secure asset staging directory", nil)
			return
		}
		tmp, err := os.CreateTemp(tempDir, ".creator-input-*")
		if err != nil {
			creatorPushError(c, http.StatusInternalServerError, string(inputsecurity.ErrReadFailed), "could not stage asset", nil)
			return
		}
		tmpPath := tmp.Name()
		defer os.Remove(tmpPath)
		if err := tmp.Chmod(0o600); err != nil {
			_ = tmp.Close()
			creatorPushError(c, http.StatusInternalServerError, string(inputsecurity.ErrReadFailed), "could not protect staged asset", nil)
			return
		}
		in, err := header.Open()
		if err != nil {
			_ = tmp.Close()
			creatorPushError(c, http.StatusBadRequest, string(inputsecurity.ErrReadFailed), "could not read uploaded asset", nil)
			return
		}
		if header.Size > maxBytes {
			_ = in.Close()
			_ = tmp.Close()
			creatorPushError(c, http.StatusRequestEntityTooLarge, string(inputsecurity.ErrDownloadTooLarge), "uploaded asset exceeds the input byte limit", nil)
			return
		}
		n, copyErr := io.Copy(tmp, io.LimitReader(in, maxBytes+1))
		_ = in.Close()
		closeErr := tmp.Close()
		if copyErr != nil || closeErr != nil {
			creatorPushError(c, http.StatusBadRequest, string(inputsecurity.ErrReadFailed), "could not stage uploaded asset", nil)
			return
		}
		if n == 0 || n > maxBytes {
			code := inputsecurity.ErrDownloadTooLarge
			if n == 0 {
				code = inputsecurity.ErrEmptyFile
			}
			creatorPushError(c, http.StatusRequestEntityTooLarge, string(code), fmt.Sprintf("asset must be between 1 and %d bytes", maxBytes), nil)
			return
		}
		asset, err := h.assetService.ResolveAndRegister(c.Request.Context(), voiceoverassets.ResolveAssetCommand{
			Kind: kind, Reference: tmpPath, SourceType: "creator_upload",
		})
		if err != nil || asset == nil {
			if code := inputsecurity.CodeOf(err); code != "" {
				creatorPushError(c, http.StatusUnprocessableEntity, string(code), "uploaded asset could not be registered", nil)
			} else {
				creatorPushError(c, http.StatusUnprocessableEntity, "asset_registration_failed", "uploaded asset could not be registered", nil)
			}
			return
		}
		c.JSON(http.StatusCreated, gin.H{
			"ok": true, "asset_id": asset.AssetID, "reference": asset.Reference(),
			"sha256": asset.SHA256, "size_bytes": asset.SizeBytes, "mime_type": asset.MimeType,
		})
	}
}
