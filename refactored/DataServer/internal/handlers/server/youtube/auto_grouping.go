package youtube

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	yt "velox-server/internal/integrations/youtube"
)

type topicRule struct {
	name     string
	keywords []string
}

var autoGroupRules = []topicRule{
	{name: "Tech", keywords: []string{"tech", "ai", "software", "developer", "coding", "programming", "react", "python", "tutorial", "review", "android", "iphone", "gadget", "pc", "laptop", "app"}},
	{name: "Gaming", keywords: []string{"gaming", "game", "gameplay", "stream", "streamer", "minecraft", "fortnite", "roblox", "valorant", "gta", "esports", "console"}},
	{name: "Finance", keywords: []string{"finance", "money", "invest", "investing", "stock", "stocks", "trading", "trader", "market", "business", "economy", "startup"}},
	{name: "Crypto", keywords: []string{"crypto", "bitcoin", "ethereum", "blockchain", "web3", "nft", "defi", "altcoin"}},
	{name: "News", keywords: []string{"news", "breaking", "politics", "report", "update", "latest", "world", "current", "analysis"}},
	{name: "Education", keywords: []string{"education", "learn", "learning", "course", "lesson", "explained", "guide", "howto", "how to", "study", "school", "university"}},
	{name: "Fitness", keywords: []string{"fitness", "workout", "gym", "health", "wellness", "running", "diet", "bodybuilding", "training"}},
	{name: "Entertainment", keywords: []string{"vlog", "comedy", "reaction", "funny", "entertainment", "lifestyle", "prank", "challenge"}},
	{name: "Music", keywords: []string{"music", "song", "album", "beat", "lyrics", "remix", "live", "dj", "producer"}},
	{name: "Discovery", keywords: []string{"travel", "documentary", "science", "nature", "history", "space", "explore", "discovery", "wildlife"}},
	{name: "Crime", keywords: []string{"crime", "true crime", "investigation", "police", "case", "murder", "mystery"}},
	{name: "Boxe", keywords: []string{"boxing", "boxe", "mma", "ufc", "fight", "fighter", "knockout"}},
	{name: "Wwe", keywords: []string{"wwe", "wrestling", "raw", "smackdown", "wrestler"}},
	{name: "Pop", keywords: []string{"pop", "celebrity", "gossip", "trend", "viral", "culture"}},
}

// AutoOrganizeUndefinedChannels groups unassigned channels into topic-based upload groups.
// POST /api/v1/youtube/manager/groups/auto-organize-undefined
func (ym *YouTubeManager) AutoOrganizeUndefinedChannelsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		if ym.service == nil || ym.storage == nil {
			c.JSON(http.StatusInternalServerError, yt.APIResponse{
				OK:    false,
				Error: "YouTube services are not initialized",
			})
			return
		}

		undefined := ym.service.GetUndefinedChannels()
		if len(undefined) == 0 {
			c.JSON(http.StatusOK, gin.H{
				"ok":             true,
				"moved":          0,
				"skipped":        0,
				"created_groups": []string{},
				"assignments":    []gin.H{},
				"message":        "No undefined channels to organize",
			})
			return
		}

		existingGroups, _ := ym.storage.ListGroups()
		existingNames := make([]string, 0, len(existingGroups))
		for name, group := range existingGroups {
			if group.GroupType != "" && group.GroupType != "upload" {
				continue
			}
			existingNames = append(existingNames, name)
		}
		sort.Strings(existingNames)

		createdGroups := make(map[string]bool)
		assigned := make([]gin.H, 0, len(undefined))
		moved := 0
		skipped := 0

		for _, ch := range undefined {
			target, reason, score := chooseAutoGroup(ch, existingNames)
			if target == "" || score < 2 {
				skipped++
				continue
			}

			if !createdGroups[target] {
				if _, ok := ym.storage.GetGroup(target); !ok {
					if err := ym.storage.CreateGroup(target, "upload"); err != nil && err != yt.ErrGroupExists {
						skipped++
						continue
					}
				}
				createdGroups[target] = true
			}

			channel := yt.Channel{
				ID:        ch.ID,
				URL:       fmt.Sprintf("https://www.youtube.com/channel/%s", ch.ID),
				Title:     firstNonEmpty(ch.Title, ch.Name, ch.ID),
				Name:      firstNonEmpty(ch.Name, ch.Title, ch.ID),
				Thumbnail: ch.Thumbnail,
				AddedAt:   time.Now(),
				Language:  ch.Language,
				Notes:     "Auto-organized from undefined channels",
			}

			if err := ym.storage.AddChannel(target, channel); err != nil {
				if err == yt.ErrChannelExists {
					skipped++
					continue
				}
				skipped++
				continue
			}

			moved++
			assigned = append(assigned, gin.H{
				"id":     ch.ID,
				"title":  channel.Title,
				"name":   channel.Name,
				"target": target,
				"reason": reason,
				"score":  score,
			})
		}

		if moved > 0 {
			ym.feedCache.Clear()
		}

		createdList := make([]string, 0, len(createdGroups))
		for name := range createdGroups {
			createdList = append(createdList, name)
		}
		sort.Strings(createdList)

		c.JSON(http.StatusOK, gin.H{
			"ok":             true,
			"moved":          moved,
			"skipped":        skipped,
			"created_groups": createdList,
			"assignments":    assigned,
			"undefined_left": len(ym.service.GetUndefinedChannels()),
		})
	}
}

func chooseAutoGroup(ch *yt.Channel, existingGroups []string) (string, string, int) {
	textParts := []string{ch.ID, ch.Name, ch.Title, ch.Language}
	text := strings.ToLower(strings.Join(textParts, " "))
	bestName := ""
	bestScore := 0
	bestReason := ""

	for _, groupName := range existingGroups {
		if groupName == "" {
			continue
		}
		score := scoreTopicMatch(groupName, text)
		if score > bestScore {
			bestScore = score
			bestName = groupName
			bestReason = fmt.Sprintf("matched existing group %q", groupName)
		}
	}

	for _, rule := range autoGroupRules {
		score := scoreTopicMatch(rule.name, text)
		for _, keyword := range rule.keywords {
			if strings.Contains(text, strings.ToLower(keyword)) {
				score += 2
			}
		}
		if score > bestScore {
			bestScore = score
			bestName = rule.name
			bestReason = fmt.Sprintf("matched topic keywords for %s", rule.name)
		}
	}

	return bestName, bestReason, bestScore
}

func scoreTopicMatch(seed, text string) int {
	seed = strings.ToLower(strings.TrimSpace(seed))
	if seed == "" {
		return 0
	}

	score := 0
	if strings.Contains(text, seed) {
		score += 4
	}

	tokens := extractKeywords(seed)
	for _, token := range tokens {
		if strings.Contains(text, token) {
			score++
		}
	}
	return score
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
