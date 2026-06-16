package youtube

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// load reads data from canonical SQLite tables (youtube_groups_v2, youtube_channels).
func (s *Storage) load() error {
	if s.store == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	groupRows, err := s.store.ListYouTubeGroupsV2()
	if err != nil {
		return fmt.Errorf("load groups: %w", err)
	}

	for _, row := range groupRows {
		name, _ := row["name"].(string)
		if name == "" {
			continue
		}
		groupType, _ := row["group_type"].(string)
		createdAt, _ := row["created_at"].(string)
		gid, _ := row["id"].(int64)

		createdAtTime := parseFlexTime(createdAt)

		s.data.Groups[name] = &Group{
			Name:      name,
			CreatedAt: createdAtTime,
			Channels:  []Channel{},
			GroupType: groupType,
		}

		if gid > 0 {
			channelIDs, err := s.store.ListGroupChannelsV2(gid)
			if err == nil {
				for _, chID := range channelIDs {
					ch, err := s.store.GetYouTubeChannel(chID)
					if err == nil && ch != nil {
						channel := channelFromCanonicalRow(ch)
						if channel != nil {
							s.data.Groups[name].Channels = append(s.data.Groups[name].Channels, *channel)
						}
					} else {
						s.data.Groups[name].Channels = append(s.data.Groups[name].Channels, Channel{ID: chID})
					}
				}
			}
		}
	}

	niches, err := s.store.ListYouTubeTrackedNiches()
	if err == nil && len(niches) > 0 {
		s.data.TrackedNiches = niches
	}

	log.Printf("[OK] Loaded %d groups from canonical tables", len(s.data.Groups))
	return nil
}

// channelFromCanonicalRow converts a canonical youtube_channels row to a Channel.
func channelFromCanonicalRow(row map[string]interface{}) *Channel {
	id, _ := row["channel_id"].(string)
	if id == "" {
		return nil
	}
	return &Channel{
		ID:        id,
		Title:     asStringField(row, "title"),
		Name:      asStringField(row, "display_name"),
		URL:       asStringField(row, "channel_url"),
		Thumbnail: asStringField(row, "thumbnail_url"),
		Language:  asStringField(row, "language"),
		ViewCount: asInt64Field(row, "view_count"),
		SubCount:  asInt64Field(row, "subscriber_count"),
	}
}

func parseFlexTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	layouts := []string{
		"2006-01-02T15:04:05.999999999Z07:00",
		"2006-01-02T15:04:05Z07:00",
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// save persists data to canonical SQLite tables.
func (s *Storage) save() error {
	if s.store == nil {
		return nil
	}

	for name, g := range s.data.Groups {
		groupType := g.GroupType
		if groupType == "" {
			groupType = "manager"
		}

		groupID, err := s.store.UpsertYouTubeGroupV2(name, groupType, "", "")
		if err != nil {
			return fmt.Errorf("save group %q: %w", name, err)
		}

		for _, ch := range g.Channels {
			addedAt := ""
			if !ch.AddedAt.IsZero() {
				addedAt = ch.AddedAt.Format(time.RFC3339)
			}
			lastSync := ""
			if !ch.LastSync.IsZero() {
				lastSync = ch.LastSync.Format(time.RFC3339)
			}
			rawMetadata, _ := json.Marshal(ch)

			if err := s.store.UpsertYouTubeChannel(
				ch.ID, ch.Title, ch.Name, ch.URL, ch.Thumbnail,
				ch.Language, ch.Notes,
				ch.ViewCount, ch.SubCount,
				addedAt, lastSync, string(rawMetadata),
			); err != nil {
				return fmt.Errorf("save channel %s: %w", safeChannelID(ch.ID), err)
			}

			if err := s.store.AddChannelToGroupV2(groupID, ch.ID); err != nil {
				return fmt.Errorf("link channel %s to group %q: %w", ch.ID[:8], name, err)
			}
		}
	}

	for _, niche := range s.data.TrackedNiches {
		if err := s.store.UpsertYouTubeTrackedNiche(niche); err != nil {
			return fmt.Errorf("save tracked niche %q: %w", niche, err)
		}
	}

	return nil
}
