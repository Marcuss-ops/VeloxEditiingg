package analytics

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/store"
)

func (h *DashboardHandler) DashboardRealtime(c *gin.Context) {
	var data map[string]any
	if h.analyticsSvc != nil {
		data, _ = h.analyticsSvc.GetAnalyticsCache("realtime")
	}

	if data == nil {
		data = make(map[string]any)
	}

	if _, ok := data["chart_data"]; !ok {
		data["chart_data"] = generateEstimatedChartData()
	}

	if _, ok := data["top_videos"]; !ok {
		data["top_videos"] = []any{}
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":        true,
		"data":      data,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *DashboardHandler) DashboardTimeline(c *gin.Context) {
	days := 30
	if d := c.Query("days"); d != "" {
		if parsed := parseIntDef(d, 30); parsed > 0 && parsed <= 365 {
			days = parsed
		}
	}

	var dailyStats []store.DailyStat
	if h.analyticsSvc != nil {
		dailyStats, _ = h.analyticsSvc.GetDailyStats(days)
	}

	cumulative := 0
	chartData := make([]gin.H, 0, len(dailyStats))
	for _, ds := range dailyStats {
		cumulative += ds.Views
		chartData = append(chartData, gin.H{
			"date":       ds.Date,
			"views":      ds.Views,
			"revenue":    ds.Revenue,
			"cumulative": cumulative,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":          true,
		"chart_data":  chartData,
		"period_days": days,
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
	})
}

func generateEstimatedChartData() []gin.H {
	chart := make([]gin.H, 24)
	now := time.Now()
	for i := 0; i < 24; i++ {
		t := now.Add(-time.Duration(i) * time.Hour)
		chart[i] = gin.H{
			"time":  t.Format("2006-01-02 15:04"),
			"views": 0,
			"type":  "estimated",
		}
	}
	return chart
}
