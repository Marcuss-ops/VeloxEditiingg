package youtube

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	yt "velox-server/internal/integrations/youtube"
	"velox-shared/payload"
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

// AutoOrganizeResult contains the summary of the auto-organization process.
type AutoOrganizeResult struct {
	Moved         int                      `json:"moved"`
	Skipped       int                      `json:"skipped"`
	CreatedGroups []string                 `json:"created_groups"`
	Assignments   []map[string]interface{} `json:"assignments"`
	UndefinedLeft int                      `json:"undefined_left"`
}

// AutoOrganizeUndefinedChannels groups unassigned channels into topic-based upload groups.
//
// PR-YT-REPO: s.storage.<Method>() is gone — the storage facade was deleted.
// All persistence routes through s.ytService (which owns the merged
// Repository). GetGroups/GetGroup return *ChannelGroup, so the existing
// nil-check idiom (cg == nil) replaces the (Group, bool) pair.
func (s *Service) AutoOrganizeUndefinedChannels(ctx context.Context) (*AutoOrganizeResult, error) {
	if s.ytService == nil {
		return nil, fmt.Errorf("YouTube services are not initialized")
	}

	undefined := s.GetUndefinedChannels()
	if len(undefined) == 0 {
		return &AutoOrganizeResult{
			Moved:         0,
			Skipped:       0,
			CreatedGroups: []string{},
			Assignments:   []map[string]interface{}{},
			UndefinedLeft: 0,
		}, nil
	}

	existingGroups := s.ytService.GetGroups()
	existingNames := make([]string, 0, len(existingGroups))
	for name, group := range existingGroups {
		if group == nil {
			continue
		}
		if group.GroupType != "" && group.GroupType != "upload" {
			continue
		}
		existingNames = append(existingNames, name)
	}
	sort.Strings(existingNames)

	createdGroups := make(map[string]bool)
	assignments := make([]map[string]interface{}, 0, len(undefined))
	moved := 0
	skipped := 0

	for _, ch := range undefined {
		target, reason, score := chooseAutoGroup(ch, existingNames)
		if target == "" || score < 2 {
			skipped++
			continue
		}

		if !createdGroups[target] {
			if s.ytService.GetGroup(target) == nil {
				if err := s.ytService.CreateGroup(target, "", nil); err != nil && err != yt.ErrGroupExists {
					skipped++
					continue
				}
			}
			createdGroups[target] = true
		}

		channel := yt.Channel{
			ID:        ch.ID,
			URL:       fmt.Sprintf("https://www.youtube.com/channel/%s", ch.ID),
			Title:     payload.FirstNonEmpty(ch.Title, ch.Name, ch.ID),
			Name:      payload.FirstNonEmpty(ch.Name, ch.Title, ch.ID),
			Thumbnail: ch.Thumbnail,
			AddedAt:   time.Now(),
			Language:  ch.Language,
			Notes:     "Auto-organized from undefined channels",
		}

		if err := s.ytService.AddChannel(target, channel); err != nil {
			if err == yt.ErrChannelExists {
				skipped++
				continue
			}
			skipped++
			continue
		}

		moved++
		assignments = append(assignments, map[string]interface{}{
			"id":     ch.ID,
			"title":  channel.Title,
			"name":   channel.Name,
			"target": target,
			"reason": reason,
			"score":  score,
		})
	}

	if moved > 0 {
		s.feedCache.Clear()
	}

	createdList := make([]string, 0, len(createdGroups))
	for name := range createdGroups {
		createdList = append(createdList, name)
	}
	sort.Strings(createdList)

	return &AutoOrganizeResult{
		Moved:         moved,
		Skipped:       skipped,
		CreatedGroups: createdList,
		Assignments:   assignments,
		UndefinedLeft: len(s.GetUndefinedChannels()),
	}, nil
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

// extractKeywords extracts keywords from a string
func extractKeywords(s string) []string {
	s = strings.ToLower(s)
	words := strings.FieldsFunc(s, func(r rune) bool {
		return r == ' ' || r == ',' || r == '.' || r == '!' || r == '?' || r == '-' || r == '_'
	})

	var keywords []string
	for _, word := range words {
		word = strings.TrimSpace(word)
		if len(word) > 3 { // Skip short words
			keywords = append(keywords, word)
		}
	}

	if len(keywords) > 10 {
		keywords = keywords[:10]
	}

	return keywords
}
