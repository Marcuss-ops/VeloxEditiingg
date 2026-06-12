package analytics

import (
	"bufio"
	"encoding/json"
	"hash/fnv"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

func YouTubePendingTasksHandler(c *gin.Context) {
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
		TS             string `json:"ts"`
		Status         string `json:"status"`
		Privacy        string `json:"privacy"`
		YoutubeGroup   string `json:"youtube_group"`
		YoutubeVideoID string `json:"youtube_video_id"`
		OutputVideoID  string `json:"output_video_id"`
		ChannelID      string `json:"channel_id"`
		JobID          string `json:"job_id"`
	}

	scanner := bufio.NewScanner(file)
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
