package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// --- YouTube Channel Metadata ---

func (s *SQLiteStore) UpsertYouTubeChannelMetadata(channelID, title, tokenPath, language, addedDate, lastUsed, rawJSON string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`INSERT INTO youtube_channel_metadata (channel_id, title, token_path, language, added_date, last_used, raw_json, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(channel_id) DO UPDATE SET title=excluded.title, token_path=excluded.token_path,
		   language=excluded.language, added_date=excluded.added_date, last_used=excluded.last_used,
		   raw_json=excluded.raw_json, updated_at=excluded.updated_at`,
		channelID, title, tokenPath, language, addedDate, lastUsed, rawJSON, now,
	)
	return err
}

func (s *SQLiteStore) ListYouTubeChannelMetadata() (map[string]map[string]interface{}, error) {
	rows, err := s.db.Query(`SELECT channel_id, title, token_path, language, added_date, last_used, raw_json FROM youtube_channel_metadata`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]map[string]interface{})
	for rows.Next() {
		var channelID, title, tokenPath, language, addedDate, lastUsed, rawJSON string
		if err := rows.Scan(&channelID, &title, &tokenPath, &language, &addedDate, &lastUsed, &rawJSON); err != nil {
			continue
		}
		var raw map[string]interface{}
		if err := json.Unmarshal([]byte(rawJSON), &raw); err != nil {
			raw = make(map[string]interface{})
		}
		raw["title"] = title
		raw["token_path"] = tokenPath
		raw["language"] = language
		raw["added_date"] = addedDate
		raw["last_used"] = lastUsed
		result[channelID] = raw
	}
	return result, rows.Err()
}

func (s *SQLiteStore) MigrateYouTubeChannelMetadata(channels map[string]interface{}) (int, error) {
	if len(channels) == 0 {
		return 0, nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339)
	stmt, err := tx.Prepare(
		`INSERT INTO youtube_channel_metadata (channel_id, title, token_path, language, added_date, last_used, raw_json, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(channel_id) DO UPDATE SET title=excluded.title, token_path=excluded.token_path,
		   language=excluded.language, added_date=excluded.added_date, last_used=excluded.last_used,
		   raw_json=excluded.raw_json, updated_at=excluded.updated_at`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	count := 0
	for id, ch := range channels {
		chMap, _ := ch.(map[string]interface{})
		if chMap == nil {
			continue
		}
		raw, _ := json.Marshal(ch)
		title, _ := chMap["title"].(string)
		tokenPath, _ := chMap["token_path"].(string)
		language, _ := chMap["language"].(string)
		addedDate, _ := chMap["added_date"].(string)
		lastUsed, _ := chMap["last_used"].(string)

		if _, err := stmt.Exec(id, title, tokenPath, language, addedDate, lastUsed, string(raw), now); err != nil {
			fmt.Printf("[WARN] migrate youtube channel metadata: skip %s: %v\n", id, err)
			continue
		}
		count++
	}
	return count, tx.Commit()
}

// --- YouTube Groups ---

func (s *SQLiteStore) UpsertYouTubeGroup(name, description, privacy string, channels []string, rawJSON string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	chJSON, _ := json.Marshal(channels)
	_, err := s.db.Exec(
		`INSERT INTO youtube_groups (name, description, privacy, channels_json, updated_at) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(name) DO UPDATE SET description=excluded.description, privacy=excluded.privacy,
		   channels_json=excluded.channels_json, updated_at=excluded.updated_at`,
		name, description, privacy, string(chJSON), now,
	)
	return err
}

func (s *SQLiteStore) DeleteYouTubeGroup(name string) error {
	_, err := s.db.Exec(`DELETE FROM youtube_groups WHERE name=?`, name)
	return err
}

func (s *SQLiteStore) ListYouTubeGroups() ([]map[string]interface{}, error) {
	rows, err := s.db.Query(`SELECT name, description, privacy, channels_json FROM youtube_groups`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var name, description, privacy, channelsJSON string
		if err := rows.Scan(&name, &description, &privacy, &channelsJSON); err != nil {
			continue
		}
		var channels []string
		json.Unmarshal([]byte(channelsJSON), &channels)
		result = append(result, map[string]interface{}{
			"name":        name,
			"description": description,
			"privacy":     privacy,
			"channels":    channels,
		})
	}
	return result, rows.Err()
}

func (s *SQLiteStore) MigrateYouTubeGroups(groups []map[string]interface{}) (int, error) {
	if len(groups) == 0 {
		return 0, nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339)
	stmt, err := tx.Prepare(
		`INSERT INTO youtube_groups (name, description, privacy, channels_json, updated_at) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(name) DO UPDATE SET description=excluded.description, privacy=excluded.privacy,
		   channels_json=excluded.channels_json, updated_at=excluded.updated_at`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	count := 0
	for _, g := range groups {
		name, _ := g["name"].(string)
		if name == "" {
			continue
		}
		desc, _ := g["description"].(string)
		privacy, _ := g["privacy"].(string)
		channelsRaw, _ := json.Marshal(g["channels"])

		if _, err := stmt.Exec(name, desc, privacy, string(channelsRaw), now); err != nil {
			fmt.Printf("[WARN] migrate youtube group: skip %s: %v\n", name, err)
			continue
		}
		count++
	}
	return count, tx.Commit()
}

// ============================================================
// --- Canonical YouTube Catalog (Migration 003) ---
// ============================================================

// UpsertYouTubeChannel canonical: creates or updates a channel in youtube_channels.
// If addedAt is empty on INSERT, it is auto-set to now().
// If addedAt is empty on UPDATE, the existing value is preserved.
func (s *SQLiteStore) UpsertYouTubeChannel(channelID, title, displayName, channelURL, thumbnailURL, language, notes string, viewCount, subCount int64, addedAt, lastSyncAt, metadataJSON string) error {
	now := time.Now().UTC().Format(time.RFC3339)

	// On INSERT, set addedAt to now if empty.
	// On UPDATE, leave empty so the ON CONFLICT SET clause preserves the existing value.
	var isNew bool
	err := s.db.QueryRow(`SELECT 1 FROM youtube_channels WHERE channel_id=?`, channelID).Scan(new(int))
	if err != nil {
		// Channel does not exist — this is an INSERT
		isNew = true
	}

	if isNew && addedAt == "" {
		addedAt = now
	}

	_, err = s.db.Exec(
		`INSERT INTO youtube_channels
		 (channel_id, title, display_name, channel_url, thumbnail_url, language, notes,
		  view_count, subscriber_count, added_at, last_sync_at, metadata_json, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(channel_id) DO UPDATE SET
		   title=excluded.title, display_name=excluded.display_name, channel_url=excluded.channel_url,
		   thumbnail_url=excluded.thumbnail_url, language=excluded.language, notes=excluded.notes,
		   view_count=excluded.view_count, subscriber_count=excluded.subscriber_count,
		   added_at=COALESCE(NULLIF(excluded.added_at, ''), youtube_channels.added_at),
		   last_sync_at=excluded.last_sync_at,
		   metadata_json=excluded.metadata_json, updated_at=excluded.updated_at`,
		channelID, title, displayName, channelURL, thumbnailURL, language, notes,
		viewCount, subCount, addedAt, lastSyncAt, metadataJSON, now, now,
	)
	return err
}

// ListYouTubeChannels returns all canonical channels.
func (s *SQLiteStore) ListYouTubeChannels() ([]map[string]interface{}, error) {
	rows, err := s.db.Query(`SELECT channel_id, title, display_name, channel_url, thumbnail_url, language, notes, view_count, subscriber_count, added_at, last_sync_at, metadata_json FROM youtube_channels ORDER BY title`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var channelID, title, displayName, channelURL, thumbnailURL, language, notes, addedAt, lastSyncAt, metadataJSON string
		var viewCount, subCount int64
		if err := rows.Scan(&channelID, &title, &displayName, &channelURL, &thumbnailURL, &language, &notes, &viewCount, &subCount, &addedAt, &lastSyncAt, &metadataJSON); err != nil {
			continue
		}
		result = append(result, map[string]interface{}{
			"channel_id":       channelID,
			"title":            title,
			"display_name":     displayName,
			"channel_url":      channelURL,
			"thumbnail_url":    thumbnailURL,
			"language":         language,
			"notes":            notes,
			"view_count":       viewCount,
			"subscriber_count": subCount,
			"added_at":         addedAt,
			"last_sync_at":     lastSyncAt,
			"metadata_json":    metadataJSON,
		})
	}
	return result, rows.Err()
}

// GetYouTubeChannel returns a single canonical channel.
func (s *SQLiteStore) GetYouTubeChannel(channelID string) (map[string]interface{}, error) {
	row := s.db.QueryRow(`SELECT channel_id, title, display_name, channel_url, thumbnail_url, language, notes, view_count, subscriber_count, added_at, last_sync_at, metadata_json FROM youtube_channels WHERE channel_id=?`, channelID)
	var cid, title, displayName, channelURL, thumbnailURL, language, notes, addedAt, lastSyncAt, metadataJSON string
	var viewCount, subCount int64
	if err := row.Scan(&cid, &title, &displayName, &channelURL, &thumbnailURL, &language, &notes, &viewCount, &subCount, &addedAt, &lastSyncAt, &metadataJSON); err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"channel_id":       cid,
		"title":            title,
		"display_name":     displayName,
		"channel_url":      channelURL,
		"thumbnail_url":    thumbnailURL,
		"language":         language,
		"notes":            notes,
		"view_count":       viewCount,
		"subscriber_count": subCount,
		"added_at":         addedAt,
		"last_sync_at":     lastSyncAt,
		"metadata_json":    metadataJSON,
	}, nil
}

// DeleteYouTubeChannel removes a canonical channel.
func (s *SQLiteStore) DeleteYouTubeChannel(channelID string) error {
	_, err := s.db.Exec(`DELETE FROM youtube_channels WHERE channel_id=?`, channelID)
	return err
}

// --- Canonical Groups V2 ---

// UpsertYouTubeGroupV2 creates or updates a group in youtube_groups_v2.
func (s *SQLiteStore) UpsertYouTubeGroupV2(name, groupType, description, privacy string) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	if groupType == "" {
		groupType = "manager"
	}
	// Use INSERT OR IGNORE + UPDATE to handle the UNIQUE(name, group_type) constraint
	_, err := s.db.Exec(
		`INSERT INTO youtube_groups_v2 (name, group_type, description, privacy, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(name, group_type) DO UPDATE SET
		   description=excluded.description, privacy=excluded.privacy, updated_at=excluded.updated_at`,
		name, groupType, description, privacy, now, now,
	)
	if err != nil {
		return 0, err
	}
	// Return the group ID
	var id int64
	err = s.db.QueryRow(`SELECT id FROM youtube_groups_v2 WHERE name=? AND group_type=?`, name, groupType).Scan(&id)
	return id, err
}

// GetYouTubeGroupV2ID returns the group ID for a given name and type.
func (s *SQLiteStore) GetYouTubeGroupV2ID(name, groupType string) (int64, error) {
	var id int64
	err := s.db.QueryRow(`SELECT id FROM youtube_groups_v2 WHERE name=? AND group_type=?`, name, groupType).Scan(&id)
	return id, err
}

// ListYouTubeGroupsV2 returns all groups.
func (s *SQLiteStore) ListYouTubeGroupsV2() ([]map[string]interface{}, error) {
	rows, err := s.db.Query(`SELECT id, name, group_type, description, privacy, created_at, updated_at FROM youtube_groups_v2 ORDER BY name`)
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

// DeleteYouTubeGroupV2 deletes a group by ID.
func (s *SQLiteStore) DeleteYouTubeGroupV2(id int64) error {
	_, err := s.db.Exec(`DELETE FROM youtube_groups_v2 WHERE id=?`, id)
	return err
}

// --- Group-Channel Memberships ---

// AddChannelToGroupV2 adds a channel membership with position.
func (s *SQLiteStore) AddChannelToGroupV2(groupID int64, channelID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`INSERT INTO youtube_group_channels (group_id, channel_id, position, added_at)
		 VALUES (?, ?, (SELECT COALESCE(MAX(position), -1) + 1 FROM youtube_group_channels WHERE group_id=?), ?)
		 ON CONFLICT(group_id, channel_id) DO NOTHING`,
		groupID, channelID, groupID, now,
	)
	return err
}

// RemoveChannelFromGroupV2 removes a channel membership.
func (s *SQLiteStore) RemoveChannelFromGroupV2(groupID int64, channelID string) error {
	_, err := s.db.Exec(`DELETE FROM youtube_group_channels WHERE group_id=? AND channel_id=?`, groupID, channelID)
	return err
}

// ListGroupChannelsV2 returns channel IDs for a group.
func (s *SQLiteStore) ListGroupChannelsV2(groupID int64) ([]string, error) {
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

// ListAllGroupMembershipsV2 returns all group-channel memberships (for loading full state).
func (s *SQLiteStore) ListAllGroupMembershipsV2() ([]map[string]interface{}, error) {
	rows, err := s.db.Query(`SELECT gc.group_id, gc.channel_id, gc.position, g.name as group_name, g.group_type
		FROM youtube_group_channels gc
		JOIN youtube_groups_v2 g ON g.id = gc.group_id
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
			"group_id":    groupID,
			"channel_id":  channelID,
			"position":    position,
			"group_name":  groupName,
			"group_type":  groupType,
		})
	}
	return result, rows.Err()
}

// --- Tracked Niches ---

func (s *SQLiteStore) UpsertYouTubeTrackedNiche(niche string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(`INSERT INTO youtube_tracked_niches (niche, created_at) VALUES (?, ?) ON CONFLICT(niche) DO NOTHING`, niche, now)
	return err
}

func (s *SQLiteStore) DeleteYouTubeTrackedNiche(niche string) error {
	_, err := s.db.Exec(`DELETE FROM youtube_tracked_niches WHERE niche=?`, niche)
	return err
}

func (s *SQLiteStore) ListYouTubeTrackedNiches() ([]string, error) {
	rows, err := s.db.Query(`SELECT niche FROM youtube_tracked_niches ORDER BY niche`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var niches []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			continue
		}
		niches = append(niches, n)
	}
	return niches, rows.Err()
}

// --- YouTube API Cache ---

func (s *SQLiteStore) SetYouTubeCache(key string, timestamp int64, dataJSON string) error {
	_, err := s.db.Exec(
		`INSERT INTO youtube_api_cache (cache_key, timestamp, data_json) VALUES (?, ?, ?)
		 ON CONFLICT(cache_key) DO UPDATE SET timestamp=excluded.timestamp, data_json=excluded.data_json`,
		key, timestamp, dataJSON,
	)
	return err
}

func (s *SQLiteStore) GetYouTubeCache(key string) (int64, string, error) {
	var timestamp int64
	var dataJSON string
	err := s.db.QueryRow(`SELECT timestamp, data_json FROM youtube_api_cache WHERE cache_key=?`, key).Scan(&timestamp, &dataJSON)
	if err == sql.ErrNoRows {
		return 0, "", nil
	}
	return timestamp, dataJSON, err
}

func (s *SQLiteStore) CleanupYouTubeCache(maxAge int64) (int64, error) {
	cutoff := time.Now().Unix() - maxAge
	result, err := s.db.Exec(`DELETE FROM youtube_api_cache WHERE timestamp < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *SQLiteStore) ClearYouTubeCache() error {
	_, err := s.db.Exec(`DELETE FROM youtube_api_cache`)
	return err
}

func (s *SQLiteStore) MigrateYouTubeCache(entries map[string]struct {
	Timestamp int64       `json:"timestamp"`
	Data      interface{} `json:"data"`
}) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(
		`INSERT INTO youtube_api_cache (cache_key, timestamp, data_json) VALUES (?, ?, ?)
		 ON CONFLICT(cache_key) DO UPDATE SET timestamp=excluded.timestamp, data_json=excluded.data_json`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	count := 0
	for key, entry := range entries {
		dataJSON, _ := json.Marshal(entry.Data)
		if _, err := stmt.Exec(key, entry.Timestamp, string(dataJSON)); err != nil {
			continue
		}
		count++
	}
	return count, tx.Commit()
}

// --- YouTube Manager Channels ---

func (s *SQLiteStore) UpsertYouTubeManagerChannel(channelID, groupName, url, title, name, thumbnail, notes, language string, keywords []string, addedAt, lastSync string, viewCount, subCount int64, rawJSON string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	kwJSON, _ := json.Marshal(keywords)
	_, err := s.db.Exec(
		`INSERT INTO youtube_manager_channels (channel_id, group_name, url, title, name, thumbnail, notes, language, keywords_json, added_at, last_sync, view_count, sub_count, raw_json, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(channel_id) DO UPDATE SET group_name=excluded.group_name, url=excluded.url,
		   title=excluded.title, name=excluded.name, thumbnail=excluded.thumbnail, notes=excluded.notes,
		   language=excluded.language, keywords_json=excluded.keywords_json, added_at=excluded.added_at,
		   last_sync=excluded.last_sync, view_count=excluded.view_count, sub_count=excluded.sub_count,
		   raw_json=excluded.raw_json, updated_at=excluded.updated_at`,
		channelID, groupName, url, title, name, thumbnail, notes, language,
		string(kwJSON), addedAt, lastSync, viewCount, subCount, rawJSON, now,
	)
	return err
}

func (s *SQLiteStore) DeleteYouTubeManagerChannel(channelID string) error {
	_, err := s.db.Exec(`DELETE FROM youtube_manager_channels WHERE channel_id=?`, channelID)
	return err
}

func (s *SQLiteStore) GetYouTubeManagerChannel(channelID string) (string, error) {
	var rawJSON string
	err := s.db.QueryRow(`SELECT raw_json FROM youtube_manager_channels WHERE channel_id=?`, channelID).Scan(&rawJSON)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return rawJSON, err
}

func (s *SQLiteStore) ListYouTubeManagerChannels() ([]map[string]interface{}, error) {
	rows, err := s.db.Query(`SELECT channel_id, group_name, url, title, name, thumbnail, notes, language, keywords_json, added_at, last_sync, view_count, sub_count, raw_json FROM youtube_manager_channels`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var channelID, groupName, url, title, name, thumbnail, notes, language, keywordsJSON, addedAt, lastSync, rawJSON string
		var viewCount, subCount int64
		if err := rows.Scan(&channelID, &groupName, &url, &title, &name, &thumbnail, &notes, &language, &keywordsJSON, &addedAt, &lastSync, &viewCount, &subCount, &rawJSON); err != nil {
			continue
		}
		result = append(result, map[string]interface{}{
			"channel_id": channelID, "group_name": groupName, "url": url,
			"title": title, "name": name, "thumbnail": thumbnail, "notes": notes,
			"language": language, "added_at": addedAt, "last_sync": lastSync,
			"view_count": viewCount, "sub_count": subCount, "raw_json": rawJSON,
		})
	}
	return result, rows.Err()
}

// --- YouTube Manager Groups ---

func (s *SQLiteStore) UpsertYouTubeManagerGroup(name, createdAt, groupType string, trackedNiches []string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	nichesJSON, _ := json.Marshal(trackedNiches)
	_, err := s.db.Exec(
		`INSERT INTO youtube_manager_groups (name, created_at, group_type, tracked_niches_json, updated_at) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(name) DO UPDATE SET created_at=excluded.created_at, group_type=excluded.group_type,
		   tracked_niches_json=excluded.tracked_niches_json, updated_at=excluded.updated_at`,
		name, createdAt, groupType, string(nichesJSON), now,
	)
	return err
}

func (s *SQLiteStore) DeleteYouTubeManagerGroup(name string) error {
	_, err := s.db.Exec(`DELETE FROM youtube_manager_groups WHERE name=?`, name)
	return err
}

func (s *SQLiteStore) ListYouTubeManagerGroups() ([]map[string]interface{}, error) {
	rows, err := s.db.Query(`SELECT name, created_at, group_type, tracked_niches_json FROM youtube_manager_groups`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var name, createdAt, groupType, nichesJSON string
		if err := rows.Scan(&name, &createdAt, &groupType, &nichesJSON); err != nil {
			continue
		}
		result = append(result, map[string]interface{}{
			"name": name, "created_at": createdAt, "group_type": groupType, "tracked_niches": nichesJSON,
		})
	}
	return result, rows.Err()
}
