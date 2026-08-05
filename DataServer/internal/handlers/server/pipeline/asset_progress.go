package pipeline

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"velox-server/internal/store"
)

// AssetDownloadProgress returns the latest worker-side asset state projected
// into the requested job. The aggregate is weighted by bytes rather than by
// asset count, so a small completed asset cannot make a large pending asset
// look half complete.
func (h *Handlers) AssetDownloadProgress() gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID := strings.TrimSpace(c.Param("id"))
		if jobID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "job_id_required"})
			return
		}
		if h.store == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "asset_progress_unavailable"})
			return
		}
		assets, err := h.store.ListAssetDownloadProgressForJob(c.Request.Context(), jobID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "asset_progress_store_failure"})
			return
		}

		var downloaded, total int64
		active := make([]store.AssetDownloadProgressView, 0, len(assets))
		ready, downloading, queued, failed, cacheHits := 0, 0, 0, 0, 0
		for _, asset := range assets {
			total += asset.BytesTotal
			downloaded += asset.BytesDownloaded
			switch asset.State {
			case "READY", "CACHE_HIT":
				ready++
				if asset.CacheHit {
					cacheHits++
				}
			case "DOWNLOADING":
				downloading++
				active = append(active, asset)
			case "QUEUED", "CACHE_CHECK", "RETRY_WAIT":
				queued++
			case "FAILED", "CANCELLED":
				failed++
			}
		}
		if total > 0 {
			// A READY cache hit has zero transferred bytes by contract, but
			// its full size is available and therefore counts as complete.
			for _, asset := range assets {
				if (asset.State == "READY" || asset.State == "CACHE_HIT") && asset.BytesTotal > asset.BytesDownloaded {
					downloaded += asset.BytesTotal - asset.BytesDownloaded
				}
			}
		}
		percent := float64(0)
		if total > 0 {
			percent = float64(downloaded) / float64(total) * 100
		}
		c.JSON(http.StatusOK, gin.H{
			"ok":               true,
			"job_id":           jobID,
			"progress_percent": percent,
			"bytes_downloaded": downloaded,
			"bytes_total":      total,
			"assets": gin.H{
				"total":       len(assets),
				"ready":       ready,
				"downloading": downloading,
				"queued":      queued,
				"failed":      failed,
				"cache_hits":  cacheHits,
			},
			"active": active,
		})
	}
}
