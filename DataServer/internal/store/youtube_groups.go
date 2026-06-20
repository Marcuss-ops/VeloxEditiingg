package store

import "time"

// ============================================================
// --- Canonical Groups (youtube_groups + youtube_group_channels) ---
// ============================================================
//
// PR15.4: the `V2` suffix has been dropped on every method name in this
// file. The underlying SQLite table was already renamed
// `youtube_groups_v2` → `youtube_groups` in migration 012 (S10), and
// the `V2` was historical decoration disambiguating against the legacy
// `youtube_groups` table (which carried a `channels_json` BLOB and was
// dropped by migration 009). Now that there is only one canonical
// groups table, the method names shed the suffix and become the
// single source of truth for callers.
//
// Every method here is a NON-DESTRUCTIVE single-row touch:
//   - UpsertYouTubeGroup: INSERT-or-UPDATE by (name, group_type)
//   - GetYouTubeGroupID:  read the integer row id by (name, group_type)
//   - ListYouTubeGroups:  SELECT all rows (used by loaders, feeds, stats)
//   - DeleteYouTubeGroup: DELETE row by id (also handles membership purge
//     via DeleteYouTubeGroupChannelsByGroupID for callers that opt in)
//   - AddChannelToGroup/RemoveChannelFromGroup: touch the membership
//     table by (group_id, channel_id) only
//   - ListGroupChannels: ordered channel IDs in a group
//   - ListAllGroupMemberships: cross-group dump for full state hydration

// UpsertYouTubeGroup creates or updates a group in youtube_groups.
func (s *SQLiteStore) UpsertYouTubeGroup(name, groupType, description, privacy string) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	if groupType == "" {
		groupType = "manager"
	}
	_, err := s.db.Exec(
		`INSERT INTO youtube_groups (name, group_type, description, privacy, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(name, group_type) DO UPDATE SET
		   description=excluded.description, privacy=excluded.privacy, updated_at=excluded.updated_at`,
		name, groupType, description, privacy, now, now,
	)
	if err != nil {
		return 0, err
	}
	var id int64
	err = s.db.QueryRow(`SELECT id FROM youtube_groups WHERE name=? AND group_type=?`, name, groupType).Scan(&id)
	return id, err
}

// GetYouTubeGroupID returns the group ID for a given name and type.
func (s *SQLiteStore) GetYouTubeGroupID(name, groupType string) (int64, error) {
	var id int64
	err := s.db.QueryRow(`SELECT id FROM youtube_groups WHERE name=? AND group_type=?`, name, groupType).Scan(&id)
	return id, err
}

// ListYouTubeGroups returns all groups.
func (s *SQLiteStore) ListYouTubeGroups() ([]map[string]interface{}, error) {
	rows, err := s.db.Query(`SELECT id, name, group_type, description, privacy, created_at, updated_at FROM youtube_groups ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var id int64
		var name, groupType, description, privacy, createdAt, updatedAt string
		if err := rows.Scan(&id, &name, &groupType, &description, &privacy, &createdAt, &updatedAt); err != nil {
			continue
		}
		result = append(result, map[string]interface{}{
			"id": id, "name": name, "group_type": groupType,
			"description": description, "privacy": privacy,
			"created_at": createdAt, "updated_at": updatedAt,
		})
	}
	return result, rows.Err()
}

// DeleteYouTubeGroup deletes a group by ID.
func (s *SQLiteStore) DeleteYouTubeGroup(id int64) error {
	_, err := s.db.Exec(`DELETE FROM youtube_groups WHERE id=?`, id)
	return err
}

// DeleteYouTubeGroupChannelsByGroupID removes all memberships for a group.
func (s *SQLiteStore) DeleteYouTubeGroupChannelsByGroupID(groupID int64) error {
	_, err := s.db.Exec(`DELETE FROM youtube_group_channels WHERE group_id=?`, groupID)
	return err
}

// DeleteYouTubeGroupChannelsByChannelID removes a channel from all groups.
func (s *SQLiteStore) DeleteYouTubeGroupChannelsByChannelID(channelID string) error {
	_, err := s.db.Exec(`DELETE FROM youtube_group_channels WHERE channel_id=?`, channelID)
	return err
}

// AddChannelToGroup adds a channel membership with position.
func (s *SQLiteStore) AddChannelToGroup(groupID int64, channelID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`INSERT INTO youtube_group_channels (group_id, channel_id, position, added_at)
		 VALUES (?, ?, (SELECT COALESCE(MAX(position), -1) + 1 FROM youtube_group_channels WHERE group_id=?), ?)
		 ON CONFLICT(group_id, channel_id) DO NOTHING`,
		groupID, channelID, groupID, now,
	)
	return err
}

// RemoveChannelFromGroup removes a channel membership.
func (s *SQLiteStore) RemoveChannelFromGroup(groupID int64, channelID string) error {
	_, err := s.db.Exec(`DELETE FROM youtube_group_channels WHERE group_id=? AND channel_id=?`, groupID, channelID)
	return err
}

// ListGroupChannels returns channel IDs for a group.
func (s *SQLiteStore) ListGroupChannels(groupID int64) ([]string, error) {
	rows, err := s.db.Query(`SELECT channel_id FROM youtube_group_channels WHERE group_id=? ORDER BY position`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ListAllGroupMemberships returns all group-channel memberships (for loading full state).
func (s *SQLiteStore) ListAllGroupMemberships() ([]map[string]interface{}, error) {
	rows, err := s.db.Query(`SELECT gc.group_id, gc.channel_id, gc.position, g.name as group_name, g.group_type
		FROM youtube_group_channels gc
		JOIN youtube_groups g ON g.id = gc.group_id
		ORDER BY g.name, gc.position`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var groupID int64
		var channelID, groupName, groupType string
		var position int
		if err := rows.Scan(&groupID, &channelID, &position, &groupName, &groupType); err != nil {
			continue
		}
		result = append(result, map[string]interface{}{
			"group_id":   groupID,
			"channel_id": channelID,
			"position":   position,
			"group_name": groupName,
			"group_type": groupType,
		})
	}
	return result, rows.Err()
}
