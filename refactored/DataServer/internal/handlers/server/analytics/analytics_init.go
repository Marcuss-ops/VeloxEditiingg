package analytics

import (
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
