package analytics

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	analyticsService "velox-server/internal/services/analytics"
)

type DashboardHandler struct {
	dataDir      string
	analyticsSvc analyticsService.AnalyticsService
}

func NewDashboardHandler(dataDir string, svc analyticsService.AnalyticsService) *DashboardHandler {
	return &DashboardHandler{
		dataDir:      dataDir,
		analyticsSvc: svc,
	}
}

func (h *DashboardHandler) DashboardSummary(c *gin.Context) {
	jobCounts := make(map[string]int64)
	if h.analyticsSvc != nil {
		if counts, err := h.analyticsSvc.GetJobCounts(c.Request.Context()); err == nil {
			jobCounts = counts
		}
	}

	var analyticsSummary map[string]any
	if h.analyticsSvc != nil {
		if data, err := h.analyticsSvc.GetAnalyticsTotals("30"); err == nil {
			analyticsSummary = data
		}
	}

	var realtimeData map[string]any
	if h.analyticsSvc != nil {
		if data, err := h.analyticsSvc.GetAnalyticsCache("realtime"); err == nil {
			realtimeData = data
		}
	}

	response := gin.H{
		"ok": true,
		"jobs": gin.H{
			"total":      jobCounts["total"],
			"pending":    jobCounts["pending"],
			"processing": jobCounts["processing"],
			"completed":  jobCounts["completed"],
			"error":      jobCounts["error"],
		},
		"analytics": gin.H{
			"total_views":   0,
			"total_revenue": 0.0,
			"total_videos":  0,
			"channels":      0,
		},
		"realtime": gin.H{
			"views_24h": 0,
			"views_1h":  0,
		},
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}

	if analyticsSummary != nil {
		if views, ok := analyticsSummary["views"]; ok {
			response["analytics"].(gin.H)["total_views"] = views
		}
		if revenue, ok := analyticsSummary["revenue"]; ok {
			response["analytics"].(gin.H)["total_revenue"] = revenue
		}
		if videos, ok := analyticsSummary["videos"]; ok {
			response["analytics"].(gin.H)["total_videos"] = videos
		}
		if channels, ok := analyticsSummary["channels"]; ok {
			response["analytics"].(gin.H)["channels"] = channels
		}
	}

	if realtimeData != nil {
		if views24h, ok := realtimeData["total_views_24h"]; ok {
			response["realtime"].(gin.H)["views_24h"] = views24h
		}
		if views1h, ok := realtimeData["total_views_1h"]; ok {
			response["realtime"].(gin.H)["views_1h"] = views1h
		}
	}

	c.JSON(http.StatusOK, response)
}

func (h *DashboardHandler) DashboardHealth(c *gin.Context) {
	serviceStatus := "ok"
	if h.analyticsSvc == nil {
		serviceStatus = "unavailable"
	}

	dataStatus := "ok"
	if h.dataDir == "" {
		dataStatus = "not_configured"
	} else {
		analyticsPath := filepath.Join(h.dataDir, "analytics")
		if _, err := os.Stat(analyticsPath); os.IsNotExist(err) {
			dataStatus = "missing_analytics_dir"
		}
	}

	lastUpdate := ""
	if h.analyticsSvc != nil {
		if data, err := h.analyticsSvc.GetAnalyticsCache("30"); err == nil {
			if ts, ok := data["cached_at"].(float64); ok {
				lastUpdate = time.Unix(int64(ts), 0).UTC().Format(time.RFC3339)
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"ok": true,
		"health": gin.H{
			"service":     serviceStatus,
			"data_dir":    dataStatus,
			"last_update": lastUpdate,
		},
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func RegisterDashboardRoutes(r *gin.Engine, dataDir string, svc analyticsService.AnalyticsService) {
	handler := NewDashboardHandler(dataDir, svc)

	dashboard := r.Group("/api/v1/dashboard")
	{
		dashboard.GET("/summary", handler.DashboardSummary)
		dashboard.GET("/finance", handler.DashboardFinance)
		dashboard.GET("/performance", handler.DashboardPerformance)
		dashboard.GET("/realtime", handler.DashboardRealtime)
		dashboard.GET("/channels", handler.DashboardChannels)
		dashboard.GET("/groups", handler.DashboardGroups)
		dashboard.GET("/timeline", handler.DashboardTimeline)
		dashboard.GET("/comparison", handler.DashboardComparison)
		dashboard.GET("/export", handler.DashboardExport)
		dashboard.GET("/health", handler.DashboardHealth)
	}

	log.Printf("[OK] Dashboard routes registered at /api/v1/dashboard/*")
}
