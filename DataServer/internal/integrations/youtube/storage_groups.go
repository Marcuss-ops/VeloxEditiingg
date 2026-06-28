package youtube

import (
	"log"
	"time"

	"velox-server/internal/store/youtubetypes"
)

// LoadData returns a complete snapshot of the YouTube catalog in the
// legacy `*StorageData` shape expected by the business-layer service in
// `internal/services/youtube`. Returns an empty-but-non-nil
// StorageData on error so callers can iterate `nil`-safely and render
// degraded responses without nil-pointer crashes.
//
// PR-YT-REPO: this is the canonical "give me a single view of the
// catalog" entry point that replaces `storage.LoadData()` from the
// deleted *Storage facade. The output is computed on every call
// (no in-memory cache) so SQLite remains the single source of
// truth.
func (s *Service) LoadData() *StorageData {
	if s.repo == nil {
		return &StorageData{Groups: map[string]*Group{}}
	}
	out := &StorageData{Groups: map[string]*Group{}}
	rows, err := s.repo.ListYouTubeGroups()
	if err != nil {
		return out
	}
	memberships, _ := s.repo.ListAllGroupMemberships()
	groupChannels := make(map[int64][]string)
	for _, m := range memberships {
		if m.GroupID > 0 && m.ChannelID != "" {
			groupChannels[m.GroupID] = append(groupChannels[m.GroupID], m.ChannelID)
		}
	}
	for _, row := range rows {
		if row.Name == "" {
			continue
		}
		g := &Group{
			Name:      row.Name,
			CreatedAt: parseFlexTime(row.CreatedAt),
			Channels:  []Channel{},
			GroupType: row.GroupType,
		}
		for _, chID := range groupChannels[row.ID] {
			rowCh, gErr := s.repo.GetYouTubeChannel(chID)
			if gErr != nil || rowCh == nil {
				g.Channels = append(g.Channels, Channel{ID: chID})
				continue
			}
			if c := channelFromCanonicalRow(rowCh); c != nil {
				g.Channels = append(g.Channels, *c)
			} else {
				g.Channels = append(g.Channels, Channel{ID: chID})
			}
		}
		out.Groups[row.Name] = g
	}
	if niches, nErr := s.repo.ListYouTubeTrackedNiches(); nErr == nil {
		out.TrackedNiches = niches
	}
	return out
}

// channelsForGroupLocked hydrates the channels for a single group id
// from SQL.
//
// PR-YT-REPO: package helper (was a Storage method pre-PB15.4). The
// receiver is *Service because GroupChannels-for-id is invoked from
// Service.LoadData() + Service-level GetGroup orchestrations.
func (s *Service) channelsForGroupLocked(groupID int64) []Channel {
	if groupID <= 0 {
		return []Channel{}
	}
	channelIDs, err := s.repo.ListGroupChannels(groupID)
	if err != nil {
		return []Channel{}
	}
	out := make([]Channel, 0, len(channelIDs))
	for _, chID := range channelIDs {
		ch, err := s.repo.GetYouTubeChannel(chID)
		if err != nil || ch == nil {
			out = append(out, Channel{ID: chID})
			continue
		}
		if c := channelFromCanonicalRow(ch); c != nil {
			out = append(out, *c)
		} else {
			out = append(out, Channel{ID: chID})
		}
	}
	return out
}

// channelFromCanonicalRow converts a typed youtube_channels row to a
// public Channel. PR-YT-REPO: package-level helper.
func channelFromCanonicalRow(row *youtubetypes.YouTubeChannel) *Channel {
	if row == nil || row.ChannelID == "" {
		return nil
	}
	return &Channel{
		ID:        row.ChannelID,
		Title:     row.Title,
		Name:      row.DisplayName,
		URL:       row.ChannelURL,
		Thumbnail: row.ThumbnailURL,
		Language:  row.Language,
		ViewCount: row.ViewCount,
		SubCount:  row.SubscriberCount,
	}
}

// resolveGroupIDByName looks up the integer group_id for a name.
func (s *Service) resolveGroupIDByName(name string) (int64, error) {
	rows, err := s.repo.ListYouTubeGroups()
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		if row.Name != name {
			continue
		}
		return row.ID, nil
	}
	return 0, nil
}

// groupTypeForName returns the canonical group_type string for a
// group by name. Defaults to "manager" if the row is missing or has
// an empty group_type.
func (s *Service) groupTypeForName(name string) string {
	rows, err := s.repo.ListYouTubeGroups()
	if err != nil {
		return "manager"
	}
	for _, row := range rows {
		if row.Name != name {
			continue
		}
		if row.GroupType == "" {
			return "manager"
		}
		return row.GroupType
	}
	return "manager"
}

// CleanupOldData clears cached channel metadata for channels whose
// last_sync_at is older than retention. PR15.4: targeted per-channel
// UPDATEs so untouched channels are not rewritten. PR-YT-REPO: now a
// Service method (was a Storage method) reading s.repo.
func (s *Service) CleanupOldData(retention time.Duration) int {
	cutoff := time.Now().UTC().Add(-retention).Format(time.RFC3339)
	rows, listErr := s.repo.ListYouTubeChannels()
	if listErr != nil {
		log.Printf("[WARN] CleanupOldData: list channels: %v", listErr)
		return 0
	}
	removedCount := 0
	for _, row := range rows {
		if row.LastSyncAt == "" || row.LastSyncAt >= cutoff {
			continue
		}
		if row.ChannelID == "" {
			continue
		}
		// Roll forward without touching user columns.
		if err := s.repo.UpsertYouTubeChannel(
			row.ChannelID, "",
			row.DisplayName,
			row.ChannelURL,
			"",
			row.Language,
			row.Notes,
			0, 0,
			row.AddedAt,
			cutoff,
			"",
		); err != nil {
			log.Printf("[WARN] CleanupOldData: reset %s: %v", safeChannelID(row.ChannelID), err)
			continue
		}
		removedCount++
	}
	return removedCount
}

// parseFlexTime flexibly parses RFC3339 / RFC3339-nano / DB-stored
// timestamp variants. PR-YT-REPO: package helper (used by LoadData
// via groups.go) — kept here for cohesion with the other SQL-row
// shape decoders.
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
