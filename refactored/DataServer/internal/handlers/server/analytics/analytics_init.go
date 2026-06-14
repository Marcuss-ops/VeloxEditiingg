package analytics

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"time"

	"velox-server/internal/store"
)

type analyticsCacheState struct {
	DataDir string
}

var analyticsState analyticsCacheState
var analyticsStore *store.SQLiteStore

func InitAnalyticsCache(dataDirectory string, s *store.SQLiteStore) {
	analyticsState.DataDir = dataDirectory
	analyticsStore = s
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
	if analyticsStore == nil {
		return map[string]any{}
	}

	rows, err := analyticsStore.ListYouTubeGroupsV2()
	if err != nil || len(rows) == 0 {
		return map[string]any{}
	}

	groupsObj := make(map[string]any)
	for _, row := range rows {
		name, _ := row["name"].(string)
		if name == "" {
			continue
		}
		gid, _ := row["id"].(int64)

		var channelIDs []string
		if gid > 0 {
			if ids, err := analyticsStore.ListGroupChannelsV2(gid); err == nil {
				channelIDs = ids
			}
		}

		channels := make([]any, 0, len(channelIDs))
		for _, id := range channelIDs {
			channels = append(channels, map[string]any{"id": id, "channel_id": id})
		}
		groupsObj[name] = map[string]any{
			"name":     name,
			"channels": channels,
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
