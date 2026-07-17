package store

import (
	"encoding/json"
	"fmt"
	"time"

	"velox-shared/payload"
)

func (s *SQLiteStore) UpsertAnalyticsCache(cacheKey string, ts float64, dataJSON []byte) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`INSERT INTO analytics_cache (cache_key, ts, data_json, migrated_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(cache_key) DO UPDATE SET
		   ts=excluded.ts,
		   data_json=excluded.data_json,
		   migrated_at=excluded.migrated_at`,
		cacheKey, ts, string(dataJSON), now,
	)
	return err
}

func (s *SQLiteStore) GetAnalyticsCache(cacheKey string) (map[string]any, error) {
	row := s.db.QueryRow(`SELECT data_json FROM analytics_cache WHERE cache_key = ?`, cacheKey)
	var data string
	if err := row.Scan(&data); err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(data), &out); err != nil {
		return nil, err
	}
	return out, nil
}

type VideoStat struct {
	VideoID      string  `json:"video_id"`
	Title        string  `json:"title"`
	ChannelTitle string  `json:"channel_title"`
	ThumbnailURL string  `json:"thumbnail_url"`
	Views24h     int     `json:"views_24h"`
	Views7d      int     `json:"views_7d"`
	Views30d     int     `json:"views_30d"`
	Revenue      float64 `json:"revenue"`
}

type ChannelStat struct {
	ChannelID     string  `json:"channel_id"`
	ChannelTitle  string  `json:"channel_title"`
	ThumbnailURL  string  `json:"thumbnail_url"`
	TotalViews    int     `json:"total_views"`
	ViewsLastHour int     `json:"views_last_hour"`
	ViewsLast24h  int     `json:"views_24h"`
	TotalRevenue  float64 `json:"total_revenue"`
	VideoCount    int     `json:"video_count"`
	AuthError     bool    `json:"auth_error"`
}

type GroupStat struct {
	GroupName        string  `json:"group_name"`
	TotalViews       int     `json:"total_views"`
	VideoCount       int     `json:"video_count"`
	AvgViewsPerVideo int     `json:"avg_views_per_video"`
	TotalRevenue     float64 `json:"total_revenue"`
	ChannelCount     int     `json:"channel_count"`
}

type DailyStat struct {
	Date    string  `json:"date"`
	Views   int     `json:"views"`
	Revenue float64 `json:"revenue"`
}

func (s *SQLiteStore) GetTopVideos(limit int, period string) ([]VideoStat, error) {
	if limit <= 0 {
		limit = 10
	}
	if period == "24h" || period == "1h" {
		data, err := s.GetAnalyticsCache("realtime")
		if err == nil {
			return extractTopVideosFromRealtime(data, limit)
		}
	}
	days := "30"
	if period == "7d" {
		days = "7"
	} else if period == "14d" {
		days = "14"
	}
	data, err := s.GetAnalyticsCache(days)
	if err != nil {
		return nil, err
	}
	return extractTopVideosFromCache(data, limit)
}

func extractTopVideosFromRealtime(data map[string]any, limit int) ([]VideoStat, error) {
	topAny, ok := data["top_videos"].([]any)
	if !ok {
		return nil, nil
	}
	videos := make([]VideoStat, 0, limit)
	for i, v := range topAny {
		if i >= limit {
			break
		}
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}
		videos = append(videos, VideoStat{
			VideoID:      asString(m["video_id"]),
			Title:        asString(m["title"]),
			ChannelTitle: asString(m["channel_title"]),
			ThumbnailURL: asString(m["thumbnail_url"]),
			Views24h:     asInt(m["views_24h"]),
			Views7d:      asInt(m["views_7d"]),
			Views30d:     asInt(m["views_30d"]),
		})
	}
	return videos, nil
}

func extractTopVideosFromCache(data map[string]any, limit int) ([]VideoStat, error) {
	channelsAny, ok := data["channels"].([]any)
	if !ok {
		return nil, nil
	}
	videos := make([]VideoStat, 0)
	for _, ch := range channelsAny {
		chMap, ok := ch.(map[string]any)
		if !ok {
			continue
		}
		channelTitle := asString(chMap["name"])
		if topVideos, ok := chMap["top_videos"].([]any); ok {
			for _, v := range topVideos {
				vMap, ok := v.(map[string]any)
				if !ok {
					continue
				}
				videos = append(videos, VideoStat{
					VideoID:      asString(vMap["video_id"]),
					Title:        asString(vMap["title"]),
					ChannelTitle: channelTitle,
					ThumbnailURL: asString(vMap["thumbnail_url"]),
					Views30d:     asInt(vMap["views"]),
					Views24h:     asInt(vMap["views_24h"]),
					Views7d:      asInt(vMap["views_7d"]),
				})
			}
		}
	}
	if len(videos) > limit {
		videos = videos[:limit]
	}
	return videos, nil
}

func (s *SQLiteStore) GetTopChannels(limit int, period string) ([]ChannelStat, error) {
	if limit <= 0 {
		limit = 10
	}
	daysStr := period
	if period == "7d" {
		daysStr = "7"
	} else if period == "1d" || period == "24h" {
		daysStr = "1"
	}
	data, err := s.GetAnalyticsCache(daysStr)
	if err != nil {
		return nil, err
	}
	channelsAny, ok := data["channels"].([]any)
	if !ok {
		return nil, nil
	}

	channels := make([]ChannelStat, 0, len(channelsAny))
	for _, ch := range channelsAny {
		chMap, ok := ch.(map[string]any)
		if !ok {
			continue
		}
		authErr := false
		if b, ok := chMap["auth_error"].(bool); ok && b {
			authErr = true
		}

		id := asString(chMap["channel_id"])
		views := asInt(chMap["views"])
		revenue := asFloat(chMap["revenue"])

		channels = append(channels, ChannelStat{
			ChannelID:    id,
			ChannelTitle: asString(chMap["name"]),
			ThumbnailURL: asString(chMap["thumbnail_url"]),
			TotalViews:   views,
			TotalRevenue: revenue,
			VideoCount:   asInt(chMap["video_count"]),
			AuthError:    authErr,
		})
	}
	if len(channels) > limit {
		channels = channels[:limit]
	}
	return channels, nil
}

func (s *SQLiteStore) GetDailyStats(days int) ([]DailyStat, error) {
	daysStr := "30"
	if days > 0 && days <= 365 {
		daysStr = fmt.Sprintf("%d", days)
	}
	data, err := s.GetAnalyticsCache(daysStr)
	if err != nil {
		return nil, err
	}
	dailyAny, ok := data["daily_stats"].([]any)
	if !ok {
		return nil, nil
	}
	stats := make([]DailyStat, 0, len(dailyAny))
	for _, d := range dailyAny {
		dMap, ok := d.(map[string]any)
		if !ok {
			continue
		}
		stats = append(stats, DailyStat{
			Date:    asString(dMap["date"]),
			Views:   asInt(dMap["views"]),
			Revenue: asFloat(dMap["revenue"]),
		})
	}
	return stats, nil
}

func (s *SQLiteStore) GetAnalyticsTotals(period string) (map[string]any, error) {
	daysStr := period
	if period == "7d" {
		daysStr = "7"
	} else if period == "1d" || period == "24h" {
		daysStr = "1"
	}
	data, err := s.GetAnalyticsCache(daysStr)
	if err != nil {
		return nil, err
	}
	totals, ok := data["totals"].(map[string]any)
	if !ok {
		return map[string]any{
			"views":    0,
			"revenue":  0.0,
			"videos":   0,
			"channels": 0,
		}, nil
	}
	channelsAny, _ := data["channels"].([]any)
	return map[string]any{
		"views":              asInt(totals["views"]),
		"revenue":            asFloat(totals["revenue"]),
		"subscribers_gained": asInt(totals["subscribersGained"]),
		"minutes_watched":    asInt(totals["estimatedMinutesWatched"]),
		"videos":             asInt(totals["videos"]),
		"channels":           len(channelsAny),
		"period":             period,
		"source":             "cache",
	}, nil
}

func parseIntDef(s string, def int) int {
	return payload.ParseIntDef(s, def)
}

func asFloat(v any) float64 {
	return payload.AsFloat(v)
}
