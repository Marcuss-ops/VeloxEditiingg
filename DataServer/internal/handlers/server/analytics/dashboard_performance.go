package analytics

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/store"
)

func (h *DashboardHandler) DashboardPerformance(c *gin.Context) {
	limit := 10
	if l := c.Query("limit"); l != "" {
		if parsed := parseIntDef(l, 10); parsed > 0 {
			limit = parsed
		}
	}

	var topVideos []store.VideoStat
	if h.analyticsSvc != nil {
		if videos, err := h.analyticsSvc.GetTopVideos(limit, "30d"); err == nil {
			topVideos = videos
		}
	}

	var topChannels []store.ChannelStat
	if h.analyticsSvc != nil {
		if channels, err := h.analyticsSvc.GetTopChannels(limit, "30d"); err == nil {
			topChannels = channels
		}
	}

	var realtimeViews map[string]any
	if h.analyticsSvc != nil {
		if data, err := h.analyticsSvc.GetAnalyticsCache("realtime"); err == nil {
			realtimeViews = data
		}
	}

	avgViews := 0
	if len(topVideos) > 0 {
		total := 0
		for _, v := range topVideos {
			total += v.Views30d
		}
		avgViews = total / len(topVideos)
	}

	c.JSON(http.StatusOK, gin.H{
		"ok": true,
		"top_videos": gin.H{
			"items": topVideos,
			"count": len(topVideos),
		},
		"top_channels": gin.H{
			"items": topChannels,
			"count": len(topChannels),
		},
		"metrics": gin.H{
			"avg_views_per_video": avgViews,
			"total_videos":        len(topVideos),
		},
		"realtime":  realtimeViews,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *DashboardHandler) DashboardChannels(c *gin.Context) {
	limit := 20
	if l := c.Query("limit"); l != "" {
		if parsed := parseIntDef(l, 20); parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}

	period := c.DefaultQuery("period", "30d")

	var channels []store.ChannelStat
	if h.analyticsSvc != nil {
		channels, _ = h.analyticsSvc.GetTopChannels(limit, period)
	}

	authErrors := 0
	for _, ch := range channels {
		if ch.AuthError {
			authErrors++
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"ok": true,
		"channels": gin.H{
			"items":       channels,
			"count":       len(channels),
			"auth_errors": authErrors,
		},
		"period":    period,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *DashboardHandler) DashboardGroups(c *gin.Context) {
	var data map[string]any
	if h.analyticsSvc != nil {
		data, _ = h.analyticsSvc.GetAnalyticsCache("30")
	}
	if data == nil {
		data = make(map[string]any)
	}

	groupMap := make(map[string]*struct {
		Views   int
		Count   int
		Revenue float64
	})

	channels, _ := data["channels"].([]any)
	for _, ch := range channels {
		chMap, ok := ch.(map[string]any)
		if !ok {
			continue
		}

		name := asStr(chMap["name"])
		group := "Ungrouped"

		if idx := strings.LastIndex(name, " - "); idx > 0 {
			group = strings.TrimSpace(name[idx+3:])
		} else if idx := strings.LastIndex(name, " "); idx > 0 {
			group = strings.TrimSpace(name[idx+1:])
		}

		if _, exists := groupMap[group]; !exists {
			groupMap[group] = &struct {
				Views   int
				Count   int
				Revenue float64
			}{}
		}

		groupMap[group].Views += int(asFloatFromAny(chMap["views"]))
		groupMap[group].Count++
		groupMap[group].Revenue += asFloatFromAny(chMap["revenue"])
	}

	type GroupStat struct {
		GroupName        string  `json:"group_name"`
		TotalViews       int     `json:"total_views"`
		VideoCount       int     `json:"video_count"`
		AvgViewsPerVideo int     `json:"avg_views_per_video"`
		TotalRevenue     float64 `json:"total_revenue"`
		ChannelCount     int     `json:"channel_count"`
	}

	groups := make([]GroupStat, 0, len(groupMap))
	for name, g := range groupMap {
		avg := 0
		if g.Count > 0 {
			avg = g.Views / g.Count
		}
		groups = append(groups, GroupStat{
			GroupName:        name,
			TotalViews:       g.Views,
			VideoCount:       g.Count,
			AvgViewsPerVideo: avg,
			TotalRevenue:     g.Revenue,
			ChannelCount:     g.Count,
		})
	}

	sort.Slice(groups, func(i, j int) bool {
		return groups[i].TotalViews > groups[j].TotalViews
	})

	limit := 10
	if l := c.Query("limit"); l != "" {
		if parsed := parseIntDef(l, 10); parsed > 0 {
			limit = parsed
		}
	}
	if len(groups) > limit {
		groups = groups[:limit]
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":        true,
		"groups":    groups,
		"count":     len(groups),
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}
