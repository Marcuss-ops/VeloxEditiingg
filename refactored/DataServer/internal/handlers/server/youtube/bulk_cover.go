package youtube

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"image"
	"image/jpeg"
	_ "image/png"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/disintegration/imaging"
	"github.com/gin-gonic/gin"
	"velox-server/internal/integrations/youtube"
)

type BulkCoverApplyRequest struct {
	ChannelID     string   `json:"channel_id" binding:"required"`
	VideoIDs      []string `json:"video_ids" binding:"required"`
	VariantID     string   `json:"variant_id,omitempty"`
	CoverBase64   string   `json:"cover_base64" binding:"required"`
	CoverFilename string   `json:"cover_filename,omitempty"`
	Publish       bool     `json:"publish,omitempty"`
	Privacy       string   `json:"privacy,omitempty"`
	MaxSizeMB     float64  `json:"max_size_mb,omitempty"`
}

type BulkCoverItemResult struct {
	VideoID      string `json:"video_id"`
	OK           bool   `json:"ok"`
	ThumbnailURL string `json:"thumbnail_url,omitempty"`
	Privacy      string `json:"privacy,omitempty"`
	SizeBytes    int    `json:"size_bytes,omitempty"`
	Error        string `json:"error,omitempty"`
}

type BulkCoverApplyResponse struct {
	OK           bool                  `json:"ok"`
	ChannelID    string                `json:"channel_id"`
	VariantID    string                `json:"variant_id,omitempty"`
	CoverFile    string                `json:"cover_file,omitempty"`
	CoverSizeMB  float64               `json:"cover_size_mb,omitempty"`
	Privacy      string                `json:"privacy,omitempty"`
	Results      []BulkCoverItemResult `json:"results"`
	AppliedCount int                   `json:"applied_count"`
	FailedCount  int                   `json:"failed_count"`
	Message      string                `json:"message"`
}

// ApplyBulkCover applies the same cover to multiple videos and optionally changes visibility.
// POST /api/v1/youtube/videos/bulk-cover
func (h *YouTubeHandlers) ApplyBulkCover(c *gin.Context) {
	var req BulkCoverApplyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": err.Error()})
		return
	}

	if req.MaxSizeMB <= 0 {
		req.MaxSizeMB = 2.0
	}
	maxBytes := int(req.MaxSizeMB * 1024 * 1024)
	if maxBytes <= 0 {
		maxBytes = 2 * 1024 * 1024
	}

	decoded, err := base64.StdEncoding.DecodeString(stripDataURLPrefix(req.CoverBase64))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid cover_base64"})
		return
	}

	compressed, ext, err := compressCoverToLimit(decoded, maxBytes)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}

	tempFile, err := h.writeTempCoverFile(req.CoverFilename, compressed, ext)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": err.Error()})
		return
	}
	defer os.Remove(tempFile)

	results := make([]BulkCoverItemResult, 0, len(req.VideoIDs))
	appliedCount := 0
	failedCount := 0

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Minute)
	defer cancel()

	for _, videoID := range req.VideoIDs {
		videoID = strings.TrimSpace(videoID)
		if videoID == "" {
			continue
		}

		item := BulkCoverItemResult{VideoID: videoID, SizeBytes: len(compressed)}
		thumbnailURL, setErr := h.service.SetThumbnail(ctx, req.ChannelID, videoID, tempFile)
		if setErr != nil {
			item.OK = false
			item.Error = setErr.Error()
			failedCount++
			results = append(results, item)
			continue
		}

		item.ThumbnailURL = thumbnailURL
		item.OK = true

		targetPrivacy := strings.ToLower(strings.TrimSpace(req.Privacy))
		if req.Publish && targetPrivacy == "" {
			targetPrivacy = "public"
		}
		if targetPrivacy != "" && targetPrivacy != "private" {
			if err := h.service.UpdateVideoMetadata(ctx, req.ChannelID, videoID, youtube.UploadConfig{PrivacyStatus: targetPrivacy}); err != nil {
				item.OK = false
				item.Error = fmt.Sprintf("thumbnail applied, privacy update failed: %v", err)
				item.Privacy = "private"
				failedCount++
				results = append(results, item)
				continue
			}
			item.Privacy = targetPrivacy
		} else {
			item.Privacy = "private"
		}

		appliedCount++
		results = append(results, item)
	}

	c.JSON(http.StatusOK, BulkCoverApplyResponse{
		OK:           true,
		ChannelID:    req.ChannelID,
		VariantID:    req.VariantID,
		CoverFile:    filepath.Base(tempFile),
		CoverSizeMB:  float64(len(compressed)) / (1024 * 1024),
		Privacy:      req.Privacy,
		Results:      results,
		AppliedCount: appliedCount,
		FailedCount:  failedCount,
		Message:      fmt.Sprintf("Applied cover to %d videos", appliedCount),
	})
}

func stripDataURLPrefix(raw string) string {
	if idx := strings.Index(raw, ","); idx > -1 && strings.HasPrefix(raw, "data:") {
		return raw[idx+1:]
	}
	return raw
}

func (h *YouTubeHandlers) writeTempCoverFile(filename string, data []byte, ext string) (string, error) {
	if filename == "" {
		filename = fmt.Sprintf("cover_%d.%s", time.Now().UnixNano(), ext)
	}
	if filepath.Ext(filename) == "" {
		filename = filename + "." + ext
	}

	dir := h.getCoverTempDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create temp cover dir: %w", err)
	}
	fullPath := filepath.Join(dir, filename)
	if err := os.WriteFile(fullPath, data, 0o644); err != nil {
		return "", fmt.Errorf("failed to write temp cover: %w", err)
	}
	return fullPath, nil
}

func compressCoverToLimit(src []byte, maxBytes int) ([]byte, string, error) {
	img, _, err := image.Decode(bytes.NewReader(src))
	if err != nil {
		return nil, "", fmt.Errorf("failed to decode cover image: %w", err)
	}

	// Start from the source resolution and progressively reduce size/quality.
	best := src
	bestExt := "jpg"

	baseW := img.Bounds().Dx()
	baseH := img.Bounds().Dy()
	if baseW <= 0 || baseH <= 0 {
		return nil, "", fmt.Errorf("invalid cover image size")
	}

	scaleSteps := []float64{1.0, 0.92, 0.84, 0.76, 0.68, 0.6, 0.52}
	qualities := []int{92, 88, 84, 80, 76, 72, 68, 64}

	for _, scale := range scaleSteps {
		width := int(float64(baseW) * scale)
		height := int(float64(baseH) * scale)
		if width < 640 {
			width = 640
		}
		if height < 360 {
			height = 360
		}

		resized := imaging.Resize(img, width, height, imaging.Lanczos)

		for _, quality := range qualities {
			var buf bytes.Buffer
			if err := jpeg.Encode(&buf, resized, &jpeg.Options{Quality: quality}); err != nil {
				continue
			}
			if maxBytes <= 0 || buf.Len() <= maxBytes {
				return buf.Bytes(), "jpg", nil
			}
			if len(best) == 0 || buf.Len() < len(best) {
				best = append([]byte(nil), buf.Bytes()...)
				bestExt = "jpg"
			}
		}
	}

	// Fallback best effort: return the smallest JPEG we found.
	if len(best) > 0 {
		return best, bestExt, nil
	}
	return nil, "", fmt.Errorf("unable to compress cover below size limit")
}


