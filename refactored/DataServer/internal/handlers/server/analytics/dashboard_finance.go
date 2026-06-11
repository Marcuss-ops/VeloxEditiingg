package analytics

import (
	"net/http"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
)

func (h *DashboardHandler) DashboardFinance(c *gin.Context) {
	period := c.DefaultQuery("period", "30")

	var data map[string]any
	var err error

	if h.analyticsSvc != nil {
		data, err = h.analyticsSvc.GetAnalyticsCache(period)
		if err != nil {
			data, err = h.analyticsSvc.GetAnalyticsCache("30")
		}
	}

	if data == nil {
		data = make(map[string]any)
	}

	totals, _ := data["totals"].(map[string]any)
	channels, _ := data["channels"].([]any)
	dailyStats, _ := data["daily_stats"].([]any)

	totalRevenue := 0.0
	totalViews := 0
	if totals != nil {
		if r, ok := totals["revenue"].(float64); ok {
			totalRevenue = r
		}
		if v, ok := totals["views"].(float64); ok {
			totalViews = int(v)
		}
	}

	type ChannelRevenue struct {
		Name    string  `json:"name"`
		Revenue float64 `json:"revenue"`
		Views   int     `json:"views"`
	}

	channelRevenues := make([]ChannelRevenue, 0)
	if channels != nil {
		for _, ch := range channels {
			chMap, ok := ch.(map[string]any)
			if !ok {
				continue
			}
			channelRevenues = append(channelRevenues, ChannelRevenue{
				Name:    asStr(chMap["name"]),
				Revenue: asFloatFromAny(chMap["revenue"]),
				Views:   int(asFloatFromAny(chMap["views"])),
			})
		}
	}

	sort.Slice(channelRevenues, func(i, j int) bool {
		return channelRevenues[i].Revenue > channelRevenues[j].Revenue
	})

	if len(channelRevenues) > 10 {
		channelRevenues = channelRevenues[:10]
	}

	chartData := make([]gin.H, 0)
	if dailyStats != nil {
		for _, ds := range dailyStats {
			dsMap, ok := ds.(map[string]any)
			if !ok {
				continue
			}
			chartData = append(chartData, gin.H{
				"date":    asStr(dsMap["date"]),
				"views":   int(asFloatFromAny(dsMap["views"])),
				"revenue": asFloatFromAny(dsMap["revenue"]),
			})
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"ok": true,
		"summary": gin.H{
			"total_revenue": totalRevenue,
			"total_views":   totalViews,
			"period":        period,
			"channels":      len(channelRevenues),
		},
		"top_channels": channelRevenues,
		"chart_data":   chartData,
		"timestamp":    time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *DashboardHandler) DashboardComparison(c *gin.Context) {
	period1 := c.DefaultQuery("period1", "7")
	period2 := c.DefaultQuery("period2", "30")

	var data1, data2 map[string]any
	if h.analyticsSvc != nil {
		data1, _ = h.analyticsSvc.GetAnalyticsTotals(period1)
		data2, _ = h.analyticsSvc.GetAnalyticsTotals(period2)
	}

	extractValue := func(data map[string]any, key string) int {
		if data == nil {
			return 0
		}
		if v, ok := data[key]; ok {
			switch n := v.(type) {
			case int:
				return n
			case int64:
				return int(n)
			case float64:
				return int(n)
			}
		}
		return 0
	}

	views1 := extractValue(data1, "views")
	views2 := extractValue(data2, "views")
	revenue1 := extractFloat(data1, "revenue")
	revenue2 := extractFloat(data2, "revenue")

	c.JSON(http.StatusOK, gin.H{
		"ok": true,
		"period1": gin.H{
			"days":    period1,
			"views":   views1,
			"revenue": revenue1,
		},
		"period2": gin.H{
			"days":    period2,
			"views":   views2,
			"revenue": revenue2,
		},
		"comparison": gin.H{
			"views_diff":   views1 - views2,
			"revenue_diff": revenue1 - revenue2,
		},
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *DashboardHandler) DashboardExport(c *gin.Context) {
	format := c.DefaultQuery("format", "json")
	period := c.DefaultQuery("period", "30")

	var data map[string]any
	if h.analyticsSvc != nil {
		data, _ = h.analyticsSvc.GetAnalyticsCache(period)
	}

	if data == nil {
		data = make(map[string]any)
	}

	switch format {
	case "csv":
		c.Header("Content-Type", "text/csv")
		c.Header("Content-Disposition", "attachment; filename=analytics_export.csv")

		dailyStats, _ := data["daily_stats"].([]any)
		csv := "date,views,revenue\n"
		for _, ds := range dailyStats {
			dsMap, ok := ds.(map[string]any)
			if !ok {
				continue
			}
			csv += asStr(dsMap["date"]) + "," +
				asStr(dsMap["views"]) + "," +
				asStr(dsMap["revenue"]) + "\n"
		}
		c.String(http.StatusOK, csv)

	default:
		c.Header("Content-Disposition", "attachment; filename=analytics_export.json")
		c.JSON(http.StatusOK, data)
	}
}
