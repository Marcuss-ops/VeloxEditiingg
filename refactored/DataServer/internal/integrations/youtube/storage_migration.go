package youtube

import (
	"encoding/json"
	"log"
	"os"
	"time"
)

// MigrateFromGroupsJSON migrates upload groups from the old groups.json format
// into the unified Storage with GroupType="upload".
// It enriches channels with OAuth metadata from channels.json when available.
// Returns the number of groups migrated or an error.
func (s *Storage) MigrateFromGroupsJSON(groupsJSONPath, channelsJSONPath string) (int, error) {
	data, err := os.ReadFile(groupsJSONPath)
	if err != nil {
		return 0, err
	}

	// Try array format first (current Service format)
	var groupsArray []struct {
		Name        string   `json:"name"`
		Channels    []string `json:"channels"`
		Description string   `json:"description,omitempty"`
		Privacy     string   `json:"privacy,omitempty"`
	}
	if err := json.Unmarshal(data, &groupsArray); err == nil && len(groupsArray) > 0 {
		return s.migrateGroupsArray(groupsArray, channelsJSONPath)
	}

	// Try map format
	var groupsMap map[string]struct {
		Name        string   `json:"name"`
		Channels    []string `json:"channels"`
		Description string   `json:"description,omitempty"`
		Privacy     string   `json:"privacy,omitempty"`
	}
	if err := json.Unmarshal(data, &groupsMap); err == nil && len(groupsMap) > 0 {
		groupsArray2 := make([]struct {
			Name        string   `json:"name"`
			Channels    []string `json:"channels"`
			Description string   `json:"description,omitempty"`
			Privacy     string   `json:"privacy,omitempty"`
		}, 0, len(groupsMap))
		for _, g := range groupsMap {
			groupsArray2 = append(groupsArray2, g)
		}
		return s.migrateGroupsArray(groupsArray2, channelsJSONPath)
	}

	return 0, nil
}

func (s *Storage) migrateGroupsArray(groups []struct {
	Name        string   `json:"name"`
	Channels    []string `json:"channels"`
	Description string   `json:"description,omitempty"`
	Privacy     string   `json:"privacy,omitempty"`
}, channelsJSONPath string) (int, error) {
	// Load channels.json for metadata enrichment
	channelTitles := make(map[string]string)
	if channelsJSONPath != "" {
		if data, err := os.ReadFile(channelsJSONPath); err == nil {
			var channelData map[string]struct {
				Title string `json:"title"`
			}
			if err := json.Unmarshal(data, &channelData); err == nil {
				for id, info := range channelData {
					channelTitles[id] = info.Title
				}
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	migrated := 0
	for _, g := range groups {
		if g.Name == "" {
			continue
		}

		// Skip if group already exists in Storage (don't overwrite)
		if _, exists := s.data.Groups[g.Name]; exists {
			continue
		}

		channels := make([]Channel, 0, len(g.Channels))
		for _, chID := range g.Channels {
			title := channelTitles[chID]
			if title == "" {
				title = chID
			}
			channels = append(channels, Channel{
				ID:      chID,
				URL:     "https://www.youtube.com/channel/" + chID,
				Title:   title,
				AddedAt: time.Now(),
			})
		}

		s.data.Groups[g.Name] = &Group{
			Name:      g.Name,
			CreatedAt: time.Now(),
			Channels:  channels,
			GroupType: "upload",
		}
		migrated++
	}

	if migrated > 0 {
		if err := s.save(); err != nil {
			return migrated, err
		}
		log.Printf("✅ Migrated %d upload groups from groups.json to unified Storage", migrated)
	}

	return migrated, nil
}
