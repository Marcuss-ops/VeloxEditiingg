package youtube

import (
	"fmt"
	"time"
)

// channelURLExistsInGroup returns true if any channel in `gid` already
// has channel_url == url. PR15.4 helper used by AddChannel to preserve
// the pre-PR15.4 ErrChannelExists semantic. Iterates
// ListGroupChannels + GetYouTubeChannel to reconcile the URL. Returns
// only on confirmed existence; SQL errors fall through (no
// ErrChannelExists returned).
//
// PR-YT-REPO: lifted from Storage to Service — the previous
// `if s.repo == nil` early-out is gone because Service.repo is
// REQUIRED at NewService time.
func (s *Service) channelURLExistsInGroup(gid int64, url string) (bool, error) {
	ids, err := s.repo.ListGroupChannels(gid)
	if err != nil {
		return false, err
	}
	for _, chID := range ids {
		if chID == "" {
			continue
		}
		ch, gErr := s.repo.GetYouTubeChannel(chID)
		if gErr != nil || ch == nil {
			continue
		}
		if ch.ChannelURL == url {
			return true, nil
		}
	}
	return false, nil
}

// addChannelRow upserts the youtube_channels row for `channel`.
func (s *Service) addChannelRow(channel Channel) error {
	addedAt := ""
	if !channel.AddedAt.IsZero() {
		addedAt = channel.AddedAt.Format(time.RFC3339)
	}
	lastSync := ""
	if !channel.LastSync.IsZero() {
		lastSync = channel.LastSync.Format(time.RFC3339)
	}
	return s.repo.UpsertYouTubeChannel(
		channel.ID, channel.Title, channel.Name, channel.URL, channel.Thumbnail,
		channel.Language, channel.Notes,
		channel.ViewCount, channel.SubCount,
		addedAt, lastSync, "",
	)
}

// AddChannel adds a channel to a group. PR15.4: drops the in-RAM write.
// PR-YT-REPO: the duplicate-URL guard still fires (preserves
// ErrChannelExists semantic). Persists via UpsertYouTubeChannel +
// AddChannelToGroup. The receiver changed from *Storage to *Service.
func (s *Service) AddChannel(groupName string, channel Channel) error {
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
	return s.repo.AddChannelToGroup(gid, channel.ID)
}

// RemoveChannel removes a channel from a group.
func (s *Service) RemoveChannel(groupName, channelID string) error {
	gid, err := s.resolveGroupIDByName(groupName)
	if err != nil {
		return err
	}
	if gid == 0 {
		return ErrGroupNotFound
	}
	if err := s.repo.RemoveChannelFromGroup(gid, channelID); err != nil {
		return err
	}
	return nil
}

// MoveChannel moves a channel from one group to another.
//
// PR-YT-REPO: atomicity is purely DB-side. Adds the channel to the
// target group first; on source-removal failure we explicitly remove
// the just-added target membership so the DB stays coherent. No
// in-RAM rollback is needed because there is no in-RAM state.
func (s *Service) MoveChannel(sourceGroup, channelID, targetGroup string) error {
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

	if err := s.repo.AddChannelToGroup(tgtID, channelID); err != nil {
		return fmt.Errorf("add membership %s to target %q: %w", safeChannelID(channelID), targetGroup, err)
	}
	if err := s.repo.RemoveChannelFromGroup(srcID, channelID); err != nil {
		// Best-effort rollback: undo the target add so we don't strand
		// the channel in two groups. Original error is surfaced to the caller.
		_ = s.repo.RemoveChannelFromGroup(tgtID, channelID)
		return fmt.Errorf("remove membership %s from source %q: %w", safeChannelID(channelID), sourceGroup, err)
	}
	return nil
}

// UpdateChannelLanguage updates the language column for a single
// channel in a group. PR15.4: uses the targeted per-column UPDATE
// method on the repo instead of the pre-PR15.4 per-group diff.
func (s *Service) UpdateChannelLanguage(groupName, channelID, language string) (*Channel, error) {
	gid, err := s.resolveGroupIDByName(groupName)
	if err != nil {
		return nil, err
	}
	if gid == 0 {
		return nil, ErrGroupNotFound
	}
	ids, listErr := s.repo.ListGroupChannels(gid)
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
	if err := s.repo.UpdateChannelLanguage(channelID, language); err != nil {
		return nil, err
	}
	ch, gErr := s.repo.GetYouTubeChannel(channelID)
	if gErr != nil || ch == nil {
		return nil, ErrChannelNotFound
	}
	if c := channelFromCanonicalRow(ch); c != nil {
		return c, nil
	}
	return nil, ErrChannelNotFound
}

// (UpdateChannelMetadata moved/disambiguated — see comment above.)

// UpdateChannelMetadata is intentionally NOT defined here —
// channels.go already exposes Service.UpdateChannelMetadata with the
// orchestrator-side signature `(channelID, metadata map[string]interface{})`.
// PR-YT-REPO: the previous Storage.UpdateChannelMetadata variant
// (groupName-scoped) is dropped; the orchestrator variant is the
// surviving canonical form.

// UpdateChannelStats updates the stats for a channel in a group.
// PR15.4: targeted per-column UPDATE.
func (s *Service) UpdateChannelStats(groupName, channelID string, viewCount, subCount int64) error {
	gid, err := s.resolveGroupIDByName(groupName)
	if err != nil {
		return err
	}
	if gid == 0 {
		return ErrGroupNotFound
	}
	ids, listErr := s.repo.ListGroupChannels(gid)
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
	return s.repo.UpdateChannelStats(channelID, viewCount, subCount, time.Now().UTC().Format(time.RFC3339))
}

// GetAllChannels returns all channels across all groups hydrated from SQL.
func (s *Service) GetAllChannels() []Channel {
	allGroups, mErr := s.repo.ListAllGroupMemberships()
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
		ch, gErr := s.repo.GetYouTubeChannel(m.ChannelID)
		if gErr != nil || ch == nil {
			continue
		}
		if c := channelFromCanonicalRow(ch); c != nil {
			channels = append(channels, *c)
		}
	}
	return channels
}

// GetGroupChannels returns channel URLs for a specific group.
func (s *Service) GetGroupChannels(groupName string) ([]string, error) {
	gid, err := s.resolveGroupIDByName(groupName)
	if err != nil {
		return nil, err
	}
	if gid == 0 {
		return nil, ErrGroupNotFound
	}
	ids, listErr := s.repo.ListGroupChannels(gid)
	if listErr != nil {
		return nil, listErr
	}
	urls := make([]string, 0, len(ids))
	for _, id := range ids {
		ch, gErr := s.repo.GetYouTubeChannel(id)
		if gErr != nil || ch == nil {
			continue
		}
		if ch.ChannelURL != "" {
			urls = append(urls, ch.ChannelURL)
		}
	}
	return urls, nil
}

// safeChannelID returns a prefix-truncated channel id suitable for log
// lines (PII redaction plus a stable-output-width fix). Package-level
// helper, lifted from Storage.
func safeChannelID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
