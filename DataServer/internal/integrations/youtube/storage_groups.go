package youtube

import (
	"fmt"
	"log"
	"time"

	"velox-server/internal/store/youtubetypes"
)

// channelsForGroupLocked hydrates the channels for a single group id
// from SQL. Returns an empty slice on nil store / lookup failure (so
// callers never panic on a partial group). PR15.4: replaces the
// in-RAM `group.Channels` slice that used to live on Storage.
func (s *Storage) channelsForGroupLocked(groupID int64) []Channel {
	if s.store == nil || groupID <= 0 {
		return []Channel{}
	}
	channelIDs, err := s.store.ListGroupChannels(groupID)
	if err != nil {
		return []Channel{}
	}
	out := make([]Channel, 0, len(channelIDs))
	for _, chID := range channelIDs {
		ch, err := s.store.GetYouTubeChannel(chID)
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

// channelFromCanonicalRow converts a typed youtube_channels row to a Channel.
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

// resolveGroupIDByName looks up the integer group_id for a name. The
// pre-PR15.4 path stored GroupType on the in-RAM Group snapshot and
// used it as the lookup key. With the RAM snapshot gone, we list all
// groups and pick the first row whose name matches. Returns 0 if no
// group named `name` exists.
func (s *Storage) resolveGroupIDByName(name string) (int64, error) {
	if s.store == nil {
		return 0, ErrStoreNotConfigured
	}
	rows, err := s.store.ListYouTubeGroups()
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

// groupTypeForName returns the canonical group_type string for a group
// by name. PR15.4: previously this came from in-RAM `group.GroupType`;
// now it comes from a ListYouTubeGroups + scan. Defaults to "manager"
// if the row is missing or has an empty group_type (matches the
// pre-PR15.4 normalisation in UpsertYouTubeGroup).
func (s *Storage) groupTypeForName(name string) string {
	if s.store == nil {
		return "manager"
	}
	rows, err := s.store.ListYouTubeGroups()
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

// ListGroups returns all groups hydrated from SQL.
func (s *Storage) ListGroups() (map[string]*Group, []string) {
	if s.store == nil {
		return map[string]*Group{}, nil
	}
	data := s.LoadData()
	return data.Groups, data.TrackedNiches
}

// GetGroup returns a specific group hydrated from SQL.
func (s *Storage) GetGroup(name string) (*Group, bool) {
	if s.store == nil {
		return nil, false
	}
	rows, err := s.store.ListYouTubeGroups()
	if err != nil {
		return nil, false
	}
	for _, row := range rows {
		if row.Name != name {
			continue
		}
		g := &Group{
			Name:      row.Name,
			CreatedAt: parseFlexTime(row.CreatedAt),
			Channels:  s.channelsForGroupLocked(row.ID),
			GroupType: row.GroupType,
		}
		return g, true
	}
	return nil, false
}

// CreateGroup creates a new group with the specified type.
//
// PR15.4: restores the pre-PR15.4 ErrGroupExists semantic via an O(1)
// GetYouTubeGroupID pre-check so a duplicate "create" call returns
// ErrGroupExists instead of silently overwriting description/privacy
// via the UNIQUE-ON-CONFLICT DO UPDATE branch of UpsertYouTubeGroup.
// Without this check, callers that pre-screen before create would
// silently clobber existing groups on retry.
func (s *Storage) CreateGroup(name string, groupType string) error {
	if s.store == nil {
		return ErrStoreNotConfigured
	}
	if groupType == "" {
		groupType = "manager"
	}
	if existing, err := s.store.GetYouTubeGroupID(name, groupType); err != nil {
		return fmt.Errorf("create group %q: pre-check: %w", name, err)
	} else if existing > 0 {
		return ErrGroupExists
	}
	if _, err := s.store.UpsertYouTubeGroup(name, groupType, "", ""); err != nil {
		return fmt.Errorf("create group %q: %w", name, err)
	}
	return nil
}

// DeleteGroup removes a group by name (id resolved via O(1) lookup).
func (s *Storage) DeleteGroup(name string) error {
	if s.store == nil {
		return ErrStoreNotConfigured
	}
	gid, err := s.store.GetYouTubeGroupID(name, s.groupTypeForName(name))
	if err != nil {
		return err
	}
	if gid == 0 {
		return ErrGroupNotFound
	}
	if err := s.store.DeleteYouTubeGroupChannelsByGroupID(gid); err != nil {
		return err
	}
	return s.store.DeleteYouTubeGroup(gid)
}

// CleanupOldData clears cached channel metadata for channels whose
// last_sync_at is older than retention. PR15.4: was a destructive
// per-group diff via syncGroupLocked. Now performs targeted per-channel
// UPDATEs so untouched channels are not rewritten. Returns the number
// of channels touched.
func (s *Storage) CleanupOldData(retention time.Duration) int {
	if s.store == nil {
		return 0
	}

	cutoff := time.Now().UTC().Add(-retention).Format(time.RFC3339)
	rows, listErr := s.store.ListYouTubeChannels()
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
		if err := s.store.UpsertYouTubeChannel(
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
