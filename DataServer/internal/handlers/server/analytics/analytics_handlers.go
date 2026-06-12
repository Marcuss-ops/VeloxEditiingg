package analytics

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-shared/payload"
)

func AnalyticsSummaryHandler(c *gin.Context) {
	data := loadAnalyticsCache("30")
	totals, _ := data["totals"].(map[string]any)
	channels, _ := data["channels"].([]any)
	views := toInt(totals["views"])
	revenue := toFloat(totals["revenue"])
	totalVideos := len(channels)
	avgViews := 0
	if totalVideos > 0 {
		avgViews = views / totalVideos
	}

	views48h := 0
	revenue48h := 0.0
	mom := gin.H{
		"current_month_revenue": 0.0,
		"prev_month_revenue":    0.0,
		"revenue_growth":        0.0,
		"current_month_views":   0,
		"prev_month_views":      0,
		"views_growth":          0.0,
	}

	if analyticsStore != nil {
		cutoff := time.Now().Add(-48 * time.Hour).Format("2006-01-02")
		rows, err := analyticsStore.GetYouTubeHistoricalStats(2)
		if err == nil {
			for _, ds := range rows {
				if ds.Date >= cutoff {
					views48h += ds.Views
					revenue48h += ds.Revenue
				}
			}
		}

		curr, prev, err := analyticsStore.GetYouTubeMoMStats()
		if err == nil {
			mom["current_month_revenue"] = curr.Revenue
			mom["prev_month_revenue"] = prev.Revenue
			if prev.Revenue > 0 {
				mom["revenue_growth"] = (curr.Revenue - prev.Revenue) / prev.Revenue * 100
			}
			mom["current_month_views"] = curr.Views
			mom["prev_month_views"] = prev.Views
			if prev.Views > 0 {
				mom["views_growth"] = float64(curr.Views-prev.Views) / float64(prev.Views) * 100
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"total_views":   views,
		"total_revenue": revenue,
		"avg_views":     avgViews,
		"total_videos":  totalVideos,
		"views_48h":     views48h,
		"revenue_48h":   revenue48h,
		"mom":           mom,
	})
}

func AnalyticsTimelineHandler(c *gin.Context) {
	days := c.DefaultQuery("days", "30")
	data := loadAnalyticsCache(days)
	daily, _ := data["daily_stats"].([]any)
	out := make([]gin.H, 0, len(daily))
	for _, v := range daily {
		d, ok := v.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, gin.H{
			"date":    toStr(d["date"]),
			"views":   toInt(d["views"]),
			"revenue": toFloat(d["revenue"]),
		})
	}
	c.JSON(http.StatusOK, out)
}

func AnalyticsTopVideosHandler(c *gin.Context) {
	limit := toInt(c.DefaultQuery("limit", "20"))
	days := parseIntDef(c.DefaultQuery("days", "7"), 7)

	if analyticsStore != nil {
		videos, err := analyticsStore.GetTopVideosFromDB(days, limit)
		if err == nil && len(videos) > 0 {
			out := make([]gin.H, len(videos))
			for i, v := range videos {
				out[i] = gin.H{
					"video_id":      v.VideoID,
					"title":         v.Title,
					"thumbnail_url": v.ThumbnailURL,
					"views":         v.Views30d,
					"revenue":       v.Revenue,
				}
			}
			c.JSON(http.StatusOK, gin.H{"videos": out})
			return
		}
	}

	realtime := loadRealtimeCache()
	top, _ := realtime["top_videos"].([]any)
	videos := make([]gin.H, 0)
	for _, v := range top {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}
		videos = append(videos, gin.H{
			"title":         toStr(m["title"]),
			"channel_title": toStr(m["channel_title"]),
			"thumbnail_url": toStr(m["thumbnail_url"]),
			"views_24h":     toInt(m["views_24h"]),
			"views_7d":      toInt(m["views_7d"]),
			"views_30d":     toInt(m["views_30d"]),
		})
	}
	if limit > 0 && len(videos) > limit {
		videos = videos[:limit]
	}
	c.JSON(http.StatusOK, gin.H{"videos": videos})
}

func asString(v any) string {
	return payload.AsString(v)
}

func AnalyticsTopChannelsHandler(c *gin.Context) {
	limit := toInt(c.DefaultQuery("limit", "5"))
	data := loadAnalyticsCache("30")
	channelsAny, _ := data["channels"].([]any)
	channels := make([]gin.H, 0, len(channelsAny))
	for _, v := range channelsAny {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}
		channels = append(channels, gin.H{
			"channel_title":   toStr(m["name"]),
			"total_views":     toInt(m["views"]),
			"views_last_hour": 0,
		})
	}
	sort.Slice(channels, func(i, j int) bool {
		return toInt(channels[i]["total_views"]) > toInt(channels[j]["total_views"])
	})
	if limit > 0 && len(channels) > limit {
		channels = channels[:limit]
	}
	c.JSON(http.StatusOK, gin.H{"channels": channels})
}

func AnalyticsTopGroupsHandler(c *gin.Context) {
	limit := toInt(c.DefaultQuery("limit", "5"))
	data := loadAnalyticsCache("30")
	channelsAny, _ := data["channels"].([]any)
	groupMap := map[string]*struct {
		views int
		count int
	}{}

	for _, v := range channelsAny {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}
		name := toStr(m["name"])
		group := "Ungrouped"
		if idx := strings.LastIndex(name, " "); idx > 0 {
			group = strings.TrimSpace(name[idx+1:])
		}
		if _, ok := groupMap[group]; !ok {
			groupMap[group] = &struct {
				views int
				count int
			}{0, 0}
		}
		groupMap[group].views += toInt(m["views"])
		groupMap[group].count++
	}

	groups := make([]gin.H, 0, len(groupMap))
	for name, g := range groupMap {
		avg := 0
		if g.count > 0 {
			avg = g.views / g.count
		}
		groups = append(groups, gin.H{
			"group_name":          name,
			"total_views":         g.views,
			"video_count":         g.count,
			"avg_views_per_video": avg,
		})
	}
	sort.Slice(groups, func(i, j int) bool {
		return toInt(groups[i]["total_views"]) > toInt(groups[j]["total_views"])
	})
	if limit > 0 && len(groups) > limit {
		groups = groups[:limit]
	}
	c.JSON(http.StatusOK, gin.H{"groups": groups})
}

func AnalyticsRealtimeV1Handler(c *gin.Context) {
	cache := loadRealtimeCache()
	if len(cache) == 0 {
		cache = map[string]any{}
	}

	data30 := loadAnalyticsCache("30")
	totals, _ := data30["totals"].(map[string]any)
	if _, ok := cache["totals"]; !ok {
		cache["totals"] = map[string]any{
			"revenue": toFloat(totals["revenue"]),
			"views":   toInt(totals["views"]),
		}
	}
	if _, ok := cache["channels"]; !ok {
		cache["channels"] = data30["channels"]
	}
	if _, ok := cache["daily_stats"]; !ok {
		cache["daily_stats"] = data30["daily_stats"]
	}
	if _, ok := cache["total_views_24h"]; !ok {
		cache["total_views_24h"] = toInt(totals["views"])
	}
	if _, ok := cache["total_views_1h"]; !ok {
		cache["total_views_1h"] = 0
	}

	c.JSON(http.StatusOK, gin.H{
		"ok":   true,
		"ts":   time.Now().UTC().Format(time.RFC3339),
		"data": cache,
	})
}
