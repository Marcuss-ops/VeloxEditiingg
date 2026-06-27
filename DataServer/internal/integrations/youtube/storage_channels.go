package youtube

import (
	"errors"
	"fmt"
	"time"
)

// channelURLExistsInGroup returns true if any channel in `gid` already
// has channel_url == url. PR15.4 helper used by AddChannel to preserve
// the pre-PR15.4 ErrChannelExists semantic that protected against
// URL-collision adds. Iterates ListGroupChannels + GetYouTubeChannel to
// reconcile the URL. Returns only on confirmed existence; sql errors
// fall through (no ErrChannelExists returned).
func (s *Storage) channelURLExistsInGroup(gid int64, url string) (bool, error) {
	if s.store == nil {
		return false, ErrStoreNotConfigured
	}
	ids, err := s.store.ListGroupChannels(gid)
	if err != nil {
		return false, err
	}
	for _, chID := range ids {
		if chID == "" {
			continue
		}
		ch, gErr := s.store.GetYouTubeChannel(chID)
		if gErr != nil || ch == nil {
			continue
		}
		if ch != nil && ch.ChannelURL == url {
			return true, nil
		}
	}
	return false, nil
}

// addChannelRow upserts the youtube_channels row for `channel` and
// returns the SQL channel_id (== channel.ID, used as the membership
// key).
func (s *Storage) addChannelRow(channel Channel) error {
	addedAt := ""
	if !channel.AddedAt.IsZero() {
		addedAt = channel.AddedAt.Format(time.RFC3339)
	}
	lastSync := ""
	if !channel.LastSync.IsZero() {
		lastSync = channel.LastSync.Format(time.RFC3339)
	}
	return s.store.UpsertYouTubeChannel(
		channel.ID, channel.Title, channel.Name, channel.URL, channel.Thumbnail,
		channel.Language, channel.Notes,
		channel.ViewCount, channel.SubCount,
		addedAt, lastSync, "",
	)
}

// AddChannel adds a channel to a group. PR15.4: drops the in-RAM
// write. The duplicate-URL guard still fires (preserves
// ErrChannelExists semantic from before). Persists via
// UpsertYouTubeChannel + AddChannelToGroup.
func (s *Storage) AddChannel(groupName string, channel Channel) error {
	if s.store == nil {
		return ErrStoreNotConfigured
	}
	gid, err := s.resolveGroupIDByName(groupName)
	if err != nil {
		return err
	}
	if gid == 0 {
		return ErrGroupNotFound
	}
	dup, dupErr := s.channelURLExistsInGroup(gid, channel.URL)
	if dupErr == nil && dup {
		return ErrChannelExists
	}
	if err := s.addChannelRow(channel); err != nil {
		return fmt.Errorf("upsert channel %s: %w", safeChannelID(channel.ID), err)
	}
	return s.store.AddChannelToGroup(gid, channel.ID)
}

// RemoveChannel removes a channel from a group.
func (s *Storage) RemoveChannel(groupName, channelID string) error {
	if s.store == nil {
		return ErrStoreNotConfigured
	}
	gid, err := s.resolveGroupIDByName(groupName)
	if err != nil {
		return err
	}
	if gid == 0 {
		return ErrGroupNotFound
	}
	if err := s.store.RemoveChannelFromGroup(gid, channelID); err != nil {
		return err
	}
	return nil
}

// MoveChannel moves a channel from one group to another.
//
// PR15.4: atomicity is purely DB-side. Adds the channel to the target
// group first; on source-removal failure we explicitly remove the
// just-added target membership so the DB stays coherent. No in-RAM
// rollback needed because there is no in-RAM state.
func (s *Storage) MoveChannel(sourceGroup, channelID, targetGroup string) error {
	if s.store == nil {
		return ErrStoreNotConfigured
	}
	srcID, err := s.resolveGroupIDByName(sourceGroup)
	if err != nil {
		return err
	}
	if srcID == 0 {
		return ErrGroupNotFound
	}
	tgtID, err := s.resolveGroupIDByName(targetGroup)
	if err != nil {
		return err
	}
	if tgtID == 0 {
		return ErrTargetGroupNotFound
	}

	if err := s.store.AddChannelToGroup(tgtID, channelID); err != nil {
		return fmt.Errorf("add membership %s to target %q: %w", safeChannelID(channelID), targetGroup, err)
	}
	if err := s.store.RemoveChannelFromGroup(srcID, channelID); err != nil {
		// Best-effort rollback: undo the target add so we don't strand
		// the channel in two groups. Ignore the rollback error so the
		// original error is surfaced to the caller.
		_ = s.store.RemoveChannelFromGroup(tgtID, channelID)
		return fmt.Errorf("remove membership %s from source %q: %w", safeChannelID(channelID), sourceGroup, err)
	}
	return nil
}

// UpdateChannelLanguage updates the language column for a single
// channel in a group. PR15.4: uses the targeted per-column UPDATE
// method on the store instead of the pre-PR15.4 per-group diff.
func (s *Storage) UpdateChannelLanguage(groupName, channelID, language string) (*Channel, error) {
	if s.store == nil {
		return nil, ErrStoreNotConfigured
	}
	gid, err := s.resolveGroupIDByName(groupName)
	if err != nil {
		return nil, err
	}
	if gid == 0 {
		return nil, ErrGroupNotFound
	}
	// Membership existence check: refuse if channel not in group.
	ids, listErr := s.store.ListGroupChannels(gid)
	if listErr != nil {
		return nil, listErr
	}
	found := false
	for _, id := range ids {
		if id == channelID {
			found = true
			break
		}
	}
	if !found {
		return nil, ErrChannelNotFound
	}
	// Targeted column update; rollover to UpsertYouTubeChannel would be
	// a write amplification we explicitly want to avoid.
	if err := s.store.UpdateChannelLanguage(channelID, language); err != nil {
		return nil, err
	}
	ch, gErr := s.store.GetYouTubeChannel(channelID)
	if gErr != nil || ch == nil {
		return nil, ErrChannelNotFound
	}
	if c := channelFromCanonicalRow(ch); c != nil {
		return c, nil
	}
	return nil, ErrChannelNotFound
}

// UpdateChannelMetadata updates Title, Name, and Thumbnail for a
// channel in a group. PR15.4: read-modify-write through
// UpsertYouTubeChannel so untouched columns (notes, language,
// display_name's tested companions, channel_url, view/sub counts) are
// preserved. metadata_json was retired in S7/S8 and is still a no-op.
func (s *Storage) UpdateChannelMetadata(groupName, channelID, title, name, thumbnail string) error {
	if s.store == nil {
		return ErrStoreNotConfigured
	}
	gid, err := s.resolveGroupIDByName(groupName)
	if err != nil {
		return err
	}
	if gid == 0 {
		return ErrGroupNotFound
	}
	ids, listErr := s.store.ListGroupChannels(gid)
	if listErr != nil {
		return listErr
	}
	found := false
	for _, id := range ids {
		if id == channelID {
			found = true
			break
		}
	}
	if !found {
		return ErrChannelNotFound
	}

	cur, curErr := s.store.GetYouTubeChannel(channelID)
	if curErr != nil || cur == nil {
		return ErrChannelNotFound
	}
	curTitle := cur.Title
	curName := cur.DisplayName
	curThumb := cur.ThumbnailURL
	curNotes := cur.Notes
	curLang := cur.Language
	curURL := cur.ChannelURL
	curView := cur.ViewCount
	curSub := cur.SubscriberCount
	curAdded := cur.AddedAt
	now := time.Now().UTC().Format(time.RFC3339)
	if title != "" {
		curTitle = title
	}
	if name != "" {
		curName = name
	}
	if thumbnail != "" {
		curThumb = thumbnail
	}
	if curAdded == "" {
		curAdded = now
	}
	return s.store.UpsertYouTubeChannel(
		channelID, curTitle, curName, curURL, curThumb,
		curLang, curNotes,
		curView, curSub,
		curAdded, now, "",
	)
}

// UpdateChannelStats updates the stats for a channel in a group.
// PR15.4: targeted per-column UPDATE.
func (s *Storage) UpdateChannelStats(groupName, channelID string, viewCount, subCount int64) error {
	if s.store == nil {
		return ErrStoreNotConfigured
	}
	gid, err := s.resolveGroupIDByName(groupName)
	if err != nil {
		return err
	}
	if gid == 0 {
		return ErrGroupNotFound
	}
	ids, listErr := s.store.ListGroupChannels(gid)
	if listErr != nil {
		return listErr
	}
	found := false
	for _, id := range ids {
		if id == channelID {
			found = true
			break
		}
	}
	if !found {
		return ErrChannelNotFound
	}
	return s.store.UpdateChannelStats(channelID, viewCount, subCount, time.Now().UTC().Format(time.RFC3339))
}

// GetAllChannels returns all channels across all groups hydrated from SQL.
func (s *Storage) GetAllChannels() []Channel {
	if s.store == nil {
		return nil
	}
	allGroups, mErr := s.store.ListAllGroupMemberships()
	if mErr != nil {
		return nil
	}
	seen := make(map[string]bool)
	var channels []Channel
	for _, m := range allGroups {
		if m.ChannelID == "" || seen[m.ChannelID] {
			continue
		}
		seen[m.ChannelID] = true
		ch, gErr := s.store.GetYouTubeChannel(m.ChannelID)
		if gErr != nil || ch == nil {
			continue
		}
		if c := channelFromCanonicalRow(ch); c != nil {
			channels = append(channels, *c)
		}
	}
	return channels
}

// GetGroupChannels returns channel IDs for a specific group.
func (s *Storage) GetGroupChannels(groupName string) ([]string, error) {
	if s.store == nil {
		return nil, ErrStoreNotConfigured
	}
	gid, err := s.resolveGroupIDByName(groupName)
	if err != nil {
		return nil, err
	}
	if gid == 0 {
		return nil, ErrGroupNotFound
	}
	ids, listErr := s.store.ListGroupChannels(gid)
	if listErr != nil {
		return nil, listErr
	}
	urls := make([]string, 0, len(ids))
	for _, id := range ids {
		ch, gErr := s.store.GetYouTubeChannel(id)
		if gErr != nil || ch == nil {
			continue
		}
		if ch != nil && ch.ChannelURL != "" {
			urls = append(urls, ch.ChannelURL)
		}
	}
	return urls, nil
}

// parseFlexTime flexibly parses RFC3339 / RFC3339-nano / DB-stored
// timestamp variants. PR15.4: kept here from storage_persistence.go
// so LoadData can decode created_at robustly.
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

// errAtomicityRollback is the sentinel emitted when a MoveChannel
// target-rollback succeeds but the original error still surfaces.
// Currently surfaced only via fmt.Errorf wrapping, but kept as a
// distinct type so future telemetry / audit can distinguish.
var errAtomicityRollback = errors.New("move channel rollback applied")
