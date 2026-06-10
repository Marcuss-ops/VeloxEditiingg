package analytics

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/store"
)

type analyticsCacheState struct {
	DataDir string
}

var analyticsState analyticsCacheState
var analyticsStore *store.SQLiteStore

// InitAnalyticsCache initializes data directory for analytics/channels compatibility handlers.
func InitAnalyticsCache(dataDirectory string, s *store.SQLiteStore) {
	analyticsState.DataDir = dataDirectory
	analyticsStore = s
	// One-time deep hydration from JSON into SQLite
	if analyticsStore != nil && dataDirectory != "" {
		path := filepath.Join(dataDirectory, "analytics", "analytics_cache.json")
		var raw map[string]any
		if err := readJSONFile(path, &raw); err == nil {
			log.Printf("Starting deep migration from %s to SQLite...", path)
			for _, entry := range raw {
				em, ok := entry.(map[string]any)
				if !ok {
					continue
				}
				dataMap, _ := em["data"].(map[string]any)
				if dataMap == nil {
					continue
				}

				// 1. Migrate Global Daily Stats (Platform level)
				if daily, ok := dataMap["daily_stats"].([]any); ok {
					for _, d := range daily {
						if m, ok := d.(map[string]any); ok {
							dateStr := toStr(m["date"])
							if dateStr == "" {
								continue
							}
							dt, err := time.Parse("2006-01-02", dateStr)
							if err != nil {
								continue
							}
							// Save as a "global" entry or simply ensure it's in the DB for aggregate queries
							_ = analyticsStore.SaveYouTubeRevenueMetric(store.YouTubeRevenueMetric{
								ChannelID:        "GLOBAL_TOTAL",
								Date:             dt,
								EstimatedRevenue: toFloat(m["revenue"]),
								Views:            int64(toInt(m["views"])),
								Currency:         "EUR",
							})
						}
					}
				}

				// 2. Migrate Per-Channel Daily Stats
				if channels, ok := dataMap["channels"].([]any); ok {
					for _, ch := range channels {
						cm, ok := ch.(map[string]any)
						if !ok {
							continue
						}
						channelID := toStr(cm["channel_id"])
						if daily, ok := cm["daily_stats"].(map[string]any); ok {
							for dateStr, stats := range daily {
								sm, ok := stats.(map[string]any)
								if !ok {
									continue
								}
								dt, err := time.Parse("2006-01-02", dateStr)
								if err != nil {
									continue
								}
								_ = analyticsStore.SaveYouTubeRevenueMetric(store.YouTubeRevenueMetric{
									ChannelID:        channelID,
									Date:             dt,
									EstimatedRevenue: toFloat(sm["revenue"]),
									Views:            int64(toInt(sm["views"])),
									Currency:         "EUR",
								})
							}
						}
					}
				}
			}
			log.Printf("Deep migration completed.")
		}
	}
}

func readJSONFile(path string, out any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}

func loadYouTubeGroups() map[string]any {
	if analyticsState.DataDir == "" {
		return map[string]any{}
	}
	// Read from groups.json (array format)
	path := filepath.Join(analyticsState.DataDir, "youtube", "groups.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return map[string]any{}
	}

	// Parse array format
	var groupsArray []map[string]any
	if err := json.Unmarshal(data, &groupsArray); err != nil {
		return map[string]any{}
	}

	// Convert to object format for compatibility
	groupsObj := make(map[string]any)
	for _, g := range groupsArray {
		if name, ok := g["name"].(string); ok {
			groupsObj[name] = g
		}
	}

	return map[string]any{"groups": groupsObj}
}

func flattenChannels(groupsRaw map[string]any) []map[string]any {
	groupsAny, ok := groupsRaw["groups"].(map[string]any)
	if !ok {
		return []map[string]any{}
	}
	res := make([]map[string]any, 0)
	for groupName, gv := range groupsAny {
		gmap, ok := gv.(map[string]any)
		if !ok {
			continue
		}
		channels, ok := gmap["channels"].([]any)
		if !ok {
			continue
		}
		for _, chv := range channels {
			ch, ok := chv.(map[string]any)
			if !ok {
				continue
			}
			channelID := toStr(ch["channelId"])
			if channelID == "" {
				channelID = toStr(ch["channel_id"])
			}
			if channelID == "" {
				channelID = toStr(ch["id"])
			}
			res = append(res, map[string]any{
				"id":            toStr(ch["id"]),
				"channelId":     channelID,
				"channel_id":    channelID,
				"title":         toStr(ch["title"]),
				"url":           toStr(ch["url"]),
				"thumbnail":     toStr(ch["thumbnail"]),
				"thumbnail_url": toStr(ch["thumbnail"]),
				"group":         groupName,
				"group_name":    groupName,
			})
		}
	}
	return res
}

func toFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case json.Number:
		n, _ := t.Float64()
		return n
	case string:
		n, _ := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return n
	default:
		return 0
	}
}

func toInt(v any) int {
	return int(toFloat(v))
}

func toStr(v any) string {
	if v == nil {
		return ""
	}
	s, ok := v.(string)
	if ok {
		return s
	}
	return ""
}

func loadAnalyticsCache(period string) map[string]any {
	if analyticsStore != nil {
		if data, err := analyticsStore.GetAnalyticsCache(period); err == nil && len(data) > 0 {
			return data
		}
	}
	if analyticsState.DataDir == "" {
		return map[string]any{}
	}
	path := filepath.Join(analyticsState.DataDir, "analytics", "analytics_cache.json")
	var raw map[string]any
	if err := readJSONFile(path, &raw); err != nil {
		return map[string]any{}
	}
	entry, ok := raw[period].(map[string]any)
	if !ok {
		entry = map[string]any{}
	}
	data, ok := entry["data"].(map[string]any)
	if ok {
		// Write-through to DB for future DB-first reads.
		if analyticsStore != nil {
			if b, err := json.Marshal(data); err == nil {
				ts := toFloat(entry["ts"])
				if ts == 0 {
					ts = float64(time.Now().Unix())
				}
				if err := analyticsStore.UpsertAnalyticsCache(period, ts, b); err != nil {
					log.Printf("sqlite upsert analytics cache failed: %v", err)
				}
			}
		}
		return data
	}
	return map[string]any{}
}

func loadRealtimeCache() map[string]any {
	if analyticsStore != nil {
		if data, err := analyticsStore.GetAnalyticsCache("realtime"); err == nil && len(data) > 0 {
			return data
		}
	}
	if analyticsState.DataDir == "" {
		return map[string]any{}
	}
	path := filepath.Join(analyticsState.DataDir, "analytics", "analytics_realtime_cache.json")
	var raw map[string]any
	if err := readJSONFile(path, &raw); err != nil {
		return map[string]any{}
	}
	if data, ok := raw["data"].(map[string]any); ok {
		if analyticsStore != nil {
			if b, err := json.Marshal(data); err == nil {
				if err := analyticsStore.UpsertAnalyticsCache("realtime", float64(time.Now().Unix()), b); err != nil {
					log.Printf("sqlite upsert realtime cache failed: %v", err)
				}
			}
		}
		return data
	}
	return map[string]any{}
}

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

	// Calculate last 48h and MoM from structured data if available
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
		rows, err := analyticsStore.GetYouTubeHistoricalStats(2) // 2 days ~ 48h
		if err == nil {
			for _, ds := range rows {
				if ds.Date >= cutoff {
					views48h += ds.Views
					revenue48h += ds.Revenue
				}
			}
		}

		// MoM stats
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

	// Fallback to legacy realtime cache
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
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
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

	// merge financial data so Finance view can always render
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

func ChannelsListHandler(c *gin.Context) {
	groups := loadYouTubeGroups()
	channels := flattenChannels(groups)
	c.JSON(http.StatusOK, channels)
}

func ChannelsSimpleHandler(c *gin.Context) {
	groups := loadYouTubeGroups()
	channels := flattenChannels(groups)
	out := make([]gin.H, 0, len(channels))
	for _, ch := range channels {
		out = append(out, gin.H{
			"channelId": toStr(ch["channelId"]),
			"title":     toStr(ch["title"]),
		})
	}
	c.JSON(http.StatusOK, out)
}

func YouTubePendingTasksHandler(c *gin.Context) {
	// Compatibility endpoint used by dashboard cards and the YouTube Orchestrator.
	// Counts are derived from the local upload log so private uploads are visible
	// even when the live feed API returns empty results.
	dataDir := resolveAnalyticsDataDir()
	if dataDir == "" {
		c.JSON(http.StatusOK, gin.H{
			"ok":             true,
			"missing_covers": 0,
			"to_publish":     0,
			"groups":         []any{},
		})
		return
	}

	groupChannels := loadYouTubeGroupChannels(dataDir)

	// Honour an explicit ?days= override, otherwise match the Dark Editor
	// "last 3 months" default so the orchestrator counter stays in sync with
	// the private videos list shown in the UI.
	days := 90
	if daysStr := c.Query("days"); daysStr != "" {
		if parsed, err := strconv.Atoi(daysStr); err == nil && parsed > 0 {
			days = parsed
		}
	}
	cutoff := time.Now().AddDate(0, 0, -days)
	privateCounts := countPrivateUploadsByGroup(
		filepath.Join(dataDir, "youtube", "youtube_upload_log.jsonl"),
		cutoff,
	)

	groupSummaries := make([]gin.H, 0, len(groupChannels))
	totalPending := 0
	for groupName, channels := range groupChannels {
		count := privateCounts[groupName]
		totalPending += count

		logoChannelID, logoChannelTitle, logoThumbnail := pickGroupLogo(groupName, channels)
		groupSummaries = append(groupSummaries, gin.H{
			"group_name":         groupName,
			"pending_count":      count,
			"channel_count":      len(channels),
			"logo_channel_id":    logoChannelID,
			"logo_channel_title": logoChannelTitle,
			"logo_thumbnail":     logoThumbnail,
		})
	}

	sort.Slice(groupSummaries, func(i, j int) bool {
		ai := toInt(groupSummaries[i]["pending_count"])
		aj := toInt(groupSummaries[j]["pending_count"])
		if ai != aj {
			return ai > aj
		}
		return toStr(groupSummaries[i]["group_name"]) < toStr(groupSummaries[j]["group_name"])
	})

	c.JSON(http.StatusOK, gin.H{
		"ok":             true,
		"missing_covers": 0,
		"to_publish":     totalPending,
		"total_pending":  totalPending,
		"groups":         groupSummaries,
		"days":           days,
	})
}

func resolveAnalyticsDataDir() string {
	candidates := []string{
		analyticsState.DataDir,
		"./data",
		"../data",
		"../../data",
	}
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		info, err := os.Stat(filepath.Join(candidate, "youtube"))
		if err == nil && info.IsDir() {
			return candidate
		}
	}
	return ""
}

func loadYouTubeGroupChannels(dataDir string) map[string][]map[string]string {
	out := make(map[string][]map[string]string)
	if dataDir == "" {
		return out
	}

	path := filepath.Join(dataDir, "youtube", "GroupYoutubeManager", "ChannelsSaved.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return out
	}

	var decoded struct {
		Groups map[string]struct {
			Name     string `json:"name"`
			Channels []struct {
				ID        string `json:"id"`
				Title     string `json:"title"`
				Name      string `json:"name"`
				Thumbnail string `json:"thumbnail"`
			} `json:"channels"`
		} `json:"groups"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return out
	}

	for groupName, group := range decoded.Groups {
		channels := make([]map[string]string, 0, len(group.Channels))
		for _, ch := range group.Channels {
			title := ch.Title
			if title == "" {
				title = ch.Name
			}
			channels = append(channels, map[string]string{
				"id":        ch.ID,
				"title":     title,
				"thumbnail": ch.Thumbnail,
			})
		}
		out[groupName] = channels
	}

	return out
}

func countPrivateUploadsByGroup(path string, cutoff time.Time) map[string]int {
	out := make(map[string]int)
	file, err := os.Open(path)
	if err != nil {
		return out
	}
	defer file.Close()

	type entry struct {
		TS            string `json:"ts"`
		Status        string `json:"status"`
		Privacy       string `json:"privacy"`
		YoutubeGroup  string `json:"youtube_group"`
		YoutubeVideoID string `json:"youtube_video_id"`
		OutputVideoID string `json:"output_video_id"`
		ChannelID     string `json:"channel_id"`
		JobID         string `json:"job_id"`
	}

	scanner := bufio.NewScanner(file)
	// Some lines can be fairly large; keep a larger buffer.
	scanner.Buffer(make([]byte, 1024*64), 1024*1024*8)

	seen := make(map[string]bool)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var e entry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			continue
		}
		if e.Privacy != "private" {
			continue
		}
		if e.Status != "UPLOADED" && e.Status != "IDEMPOTENT" {
			continue
		}

		// Apply the same 3-month window used by ListGroupPrivateVideos so the
		// orchestrator counter and the Dark Editor never disagree.
		if ts := strings.TrimSpace(e.TS); ts != "" {
			if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
				if t.Before(cutoff) {
					continue
				}
			} else if t, err := time.Parse(time.RFC3339, ts); err == nil {
				if t.Before(cutoff) {
					continue
				}
			}
		}

		key := e.YoutubeVideoID
		if key == "" {
			key = e.OutputVideoID
		}
		if key == "" {
			key = e.JobID + ":" + e.ChannelID
		}
		if key == "" {
			continue
		}
		if seen[key] {
			continue
		}
		seen[key] = true

		groupName := strings.TrimSpace(e.YoutubeGroup)
		if groupName == "" {
			groupName = "Ungrouped"
		}
		out[groupName]++
	}

	return out
}

func pickGroupLogo(groupName string, channels []map[string]string) (string, string, string) {
	if len(channels) == 0 {
		return "", "", ""
	}

	thumbChannels := make([]map[string]string, 0, len(channels))
	for _, ch := range channels {
		if strings.TrimSpace(ch["thumbnail"]) != "" {
			thumbChannels = append(thumbChannels, ch)
		}
	}

	source := channels
	if len(thumbChannels) > 0 {
		source = thumbChannels
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(strings.ToLower(strings.TrimSpace(groupName))))
	idx := int(h.Sum32()) % len(source)
	ch := source[idx]
	return ch["id"], ch["title"], ch["thumbnail"]
}
