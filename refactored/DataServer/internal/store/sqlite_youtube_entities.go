package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"velox-server/internal/store/youtubetypes"
)

// ============================================================
// --- Canonical YouTube Catalog (Migration 003) ---
// ============================================================

// UpsertYouTubeChannel canonical: creates or updates a channel in youtube_channels.
// If addedAt is empty on INSERT, it is auto-set to now().
// If addedAt is empty on UPDATE, the existing value is preserved.
//
// `metadataJSON` was retired in S7/S8 of the verdict plan: the column was
// DROPPED by migration 014. There is no typed column to back it, so the
// blob (which historically held `token_path` from the now-deleted
// `saveChannelToken` JSON writer) is simply gone. The parameter is
// retained on the signature for interface conformance with
// StorageStore.UpsertYouTubeChannel and YouTubeStore.UpsertYouTubeChannel
// so that callers and mock-store test fixtures do not need a second
// variant — the value is accepted but never persisted. New writes should
// use the typed columns for any operator-readable metadata.
func (s *SQLiteStore) UpsertYouTubeChannel(channelID, title, displayName, channelURL, thumbnailURL, language, notes string, viewCount, subCount int64, addedAt, lastSyncAt, metadataJSON string) error {
	_ = metadataJSON // accepted for interface conformance; column dropped by migration 014.
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
		  view_count, subscriber_count, added_at, last_sync_at, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(channel_id) DO UPDATE SET
		   title=excluded.title, display_name=excluded.display_name, channel_url=excluded.channel_url,
		   thumbnail_url=excluded.thumbnail_url, language=excluded.language, notes=excluded.notes,
		   view_count=excluded.view_count, subscriber_count=excluded.subscriber_count,
		   added_at=COALESCE(NULLIF(excluded.added_at, ''), youtube_channels.added_at),
		   last_sync_at=excluded.last_sync_at,
		   updated_at=excluded.updated_at`,
		channelID, title, displayName, channelURL, thumbnailURL, language, notes,
		viewCount, subCount, addedAt, lastSyncAt, now, now,
	)
	return err
}

// ListYouTubeChannels returns all canonical channels.
func (s *SQLiteStore) ListYouTubeChannels() ([]map[string]interface{}, error) {
	rows, err := s.db.Query(`SELECT channel_id, title, display_name, channel_url, thumbnail_url, language, notes, view_count, subscriber_count, added_at, last_sync_at, created_at, updated_at FROM youtube_channels ORDER BY title`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var channelID, title, displayName, channelURL, thumbnailURL, language, notes, addedAt, lastSyncAt, createdAt, updatedAt string
		var viewCount, subCount int64
		if err := rows.Scan(&channelID, &title, &displayName, &channelURL, &thumbnailURL, &language, &notes, &viewCount, &subCount, &addedAt, &lastSyncAt, &createdAt, &updatedAt); err != nil {
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
			"created_at":       createdAt,
			"updated_at":       updatedAt,
		})
	}
	return result, rows.Err()
}

// GetYouTubeChannel returns a single canonical channel.
func (s *SQLiteStore) GetYouTubeChannel(channelID string) (map[string]interface{}, error) {
	row := s.db.QueryRow(`SELECT channel_id, title, display_name, channel_url, thumbnail_url, language, notes, view_count, subscriber_count, added_at, last_sync_at, created_at, updated_at FROM youtube_channels WHERE channel_id=?`, channelID)
	var cid, title, displayName, channelURL, thumbnailURL, language, notes, addedAt, lastSyncAt, createdAt, updatedAt string
	var viewCount, subCount int64
	if err := row.Scan(&cid, &title, &displayName, &channelURL, &thumbnailURL, &language, &notes, &viewCount, &subCount, &addedAt, &lastSyncAt, &createdAt, &updatedAt); err != nil {
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
		"created_at":       createdAt,
		"updated_at":       updatedAt,
	}, nil
}

// DeleteYouTubeChannel removes a canonical channel.
func (s *SQLiteStore) DeleteYouTubeChannel(channelID string) error {
	_, err := s.db.Exec(`DELETE FROM youtube_channels WHERE channel_id=?`, channelID)
	return err
}// UpdateYouTubeChannelMetadata persists a YouTube-API metadata refresh into
// the canonical youtube_channels row. Only the columns the refresh can
// actually change from a YouTube channels.list response are written:
// title, thumbnail_url, last_sync_at, updated_at.
//
// Every other column is intentionally left alone: refresh is the system
// source of truth for title and thumbnail only. Display name, language,
// view/sub counts, notes, channel_url are owned by the initial AddChannel
// path (or by user edits afterwards) and MUST NOT be silently wiped by an
// API roundtrip — otherwise a single refresh would erase user-set notes
// and language.
//
// Use this explicitly on metadata refresh paths. Use UpsertYouTubeChannel
// for initial channel ingest where every column needs to be seeded.
func (s *SQLiteStore) UpdateYouTubeChannelMetadata(channelID, title, thumbnailURL string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`UPDATE youtube_channels
	 SET title = ?, thumbnail_url = ?, last_sync_at = ?, updated_at = ?
	 WHERE channel_id = ?`,
		title, thumbnailURL, now, now, channelID,
	)
	return err
}

// UpdateChannelTitle sets ONLY the title column of a single channel row.
// Repeatedly preserves every other column — including display_name,
// language, notes, view_count, subscriber_count, channel_url, added_at.
// This is the typed update for the S11 spec: an operator-driven title
// edit MUST not silently wipe the user-set notes/language that the
// previous mega-upsert could clobber via empty/zero fill. Errors are
// surfaced (not logged-and-swallowed) so the caller can abort before
// mutating the in-RAM copy (DB-first ordering).
func (s *SQLiteStore) UpdateChannelTitle(channelID, title string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`UPDATE youtube_channels SET title = ?, updated_at = ? WHERE channel_id = ?`,
		title, now, channelID,
	)
	return err
}

// UpdateChannelLanguage sets ONLY the language column of a single
// channel row. Distinct from the refresh path (which deliberately
// doesn't touch language) and from the wide upsert (which can wipe
// language via empty-fill).
func (s *SQLiteStore) UpdateChannelLanguage(channelID, language string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`UPDATE youtube_channels SET language = ?, updated_at = ? WHERE channel_id = ?`,
		language, now, channelID,
	)
	return err
}

// UpdateChannelNotes sets ONLY the notes column of a single channel row.
// Notes are operator-curated free text; no API path ever touches them.
// Refreshing the channel from YouTube MUST NOT clobber notes (the
// previous UpsertYouTubeChannel path could — this typed method is the
// fix).
func (s *SQLiteStore) UpdateChannelNotes(channelID, notes string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`UPDATE youtube_channels SET notes = ?, updated_at = ? WHERE channel_id = ?`,
		notes, now, channelID,
	)
	return err
}

// UpdateChannelStats sets ONLY the view_count + subscriber_count +
// last_sync_at columns of a single channel row. Refreshing from
// YouTube writes through this path; user-edited typed columns (notes,
// language, display_name, channel_url) remain untouched.
func (s *SQLiteStore) UpdateChannelStats(channelID string, viewCount, subCount int64, lastSyncAt string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`UPDATE youtube_channels SET view_count = ?, subscriber_count = ?, last_sync_at = ?, updated_at = ? WHERE channel_id = ?`,
		viewCount, subCount, lastSyncAt, now, channelID,
	)
	return err
}

// --- Canonical Groups ---
//
// NOTE: the V2 suffix on the method names is intentional and STAYS even
// after migration 012 renamed the table from `youtube_groups_v2` to
// `youtube_groups` (S10 of the verdict plan). Reasons:
//   1. The old `youtube_groups` table (with its `channels_json` BLOB) is
//      what the suffix used to disambiguate against. The legacy table
//      is gone (migration 009). The suffix is now decorative only.
//   2. Keeping the V2 suffix on the *method* names keeps the rename
//      a pure SQL-only change, avoiding a propagation storm across the
//      ~20 callsites in service.go / storage.go / storage_*.go.
//   3. A future cleanup pass (post-S11) can drop the suffix cleanly.

// UpsertYouTubeGroupV2 creates or updates a group in youtube_groups.
func (s *SQLiteStore) UpsertYouTubeGroupV2(name, groupType, description, privacy string) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339)
	if groupType == "" {
		groupType = "manager"
	}
	// Use INSERT OR IGNORE + UPDATE to handle the UNIQUE(name, group_type) constraint
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
	// Return the group ID
	var id int64
	err = s.db.QueryRow(`SELECT id FROM youtube_groups WHERE name=? AND group_type=?`, name, groupType).Scan(&id)
	return id, err
}

// GetYouTubeGroupV2ID returns the group ID for a given name and type.
func (s *SQLiteStore) GetYouTubeGroupV2ID(name, groupType string) (int64, error) {
	var id int64
	err := s.db.QueryRow(`SELECT id FROM youtube_groups WHERE name=? AND group_type=?`, name, groupType).Scan(&id)
	return id, err
}

// ListYouTubeGroups returns all groups (legacy wrapper for backward compat).
func (s *SQLiteStore) ListYouTubeGroups() ([]map[string]interface{}, error) {
	return s.ListYouTubeGroupsV2()
}

// UpsertYouTubeManagerChannel is a legacy wrapper that maps to the canonical
// UpsertYouTubeChannel. The legacy youtube_channel_metadata table was dropped
// by migration 014; this method persists through the canonical youtube_channels table.
func (s *SQLiteStore) UpsertYouTubeManagerChannel(channelID, groupName, url, title, name, thumbnail, notes, language string, keywords []string, addedAt, lastSync string, viewCount, subCount int64, rawJSON string) error {
	return s.UpsertYouTubeChannel(channelID, title, name, url, thumbnail, language, notes, viewCount, subCount, addedAt, lastSync, rawJSON)
}

// ListYouTubeGroupsV2 returns all groups.
func (s *SQLiteStore) ListYouTubeGroupsV2() ([]map[string]interface{}, error) {
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

// DeleteYouTubeGroupV2 deletes a group by ID.
func (s *SQLiteStore) DeleteYouTubeGroupV2(id int64) error {
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

// --- Group-Channel Memberships ---
//
// Membership table is `youtube_group_channels`. Its FK to groups points at
// the renamed `youtube_groups` (S10). ON DELETE CASCADE keeps
// removal atomic. The V2 suffix on the methods is decorative (see note
// on the Groups section above); renaming these methods is post-S11.

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

// ============================================================
// --- Canonical YouTube OAuth Tokens (Migration 011) ---
// ============================================================
//
// All three methods on this block accept / return ALREADY-ENCRYPTED BLOB
// values. The encryption-decision policy lives in the service layer (which
// holds the AES-GCM cipher resolved from env vars via internal/secrets/aesgcm).
// Keeping the store free of crypto concerns means a future cipher rotation
// only touches the encryptor package + maybe a per-row re-encryption
// migration — the SQL contract stays unchanged.

// UpsertYouTubeOAuthToken stores or replaces the OAuth credentials for one
// channel. Arguments are:
//   - channelID: the YouTube channel ID; PK + FK to youtube_channels
//   - accessTokenEnc: AES-GCM encrypted access token bytes (NOT NULL)
//   - refreshTokenEnc: AES-GCM encrypted refresh token bytes (NULL when the
//     grant flow did not issue one)
//   - tokenType: usually "Bearer"
//   - expiry: RFC3339 timestamp; empty when the token never expires
//   - scopes: space-separated OAuth scope list
//   - keyVersion: the cipher key rotation stamp; persisted so future
//     rotation can detect old rows that still need migration
//
// Conflict policy: ON CONFLICT(channel_id) DO UPDATE replaces the row
// atomically. The `revoked_at` column is intentionally NOT updated by
// this method: revoking is a separate, audit-logged action via
// MarkYouTubeOAuthTokenRevoked (we don't want a token refresh or
// re-grant silently wiping a revocation that another operator set).
func (s *SQLiteStore) UpsertYouTubeOAuthToken(channelID string, accessTokenEnc, refreshTokenEnc []byte, tokenType, expiry, scopes string, keyVersion int) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`INSERT INTO youtube_oauth_tokens
		 (channel_id, access_token_encrypted, refresh_token_encrypted, token_type, expiry, scopes, key_version, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(channel_id) DO UPDATE SET
		   access_token_encrypted  = excluded.access_token_encrypted,
		   refresh_token_encrypted = excluded.refresh_token_encrypted,
		   token_type              = excluded.token_type,
		   expiry                  = excluded.expiry,
		   scopes                  = excluded.scopes,
		   key_version             = excluded.key_version,
		   updated_at              = excluded.updated_at`,
		channelID, accessTokenEnc, refreshTokenEnc, tokenType, expiry, scopes, keyVersion, now, now,
	)
	return err
}

// GetYouTubeOAuthToken returns the row for channelID, or (nil, nil) when
// the channel has no OAuth entry. BLOB columns surface as []byte. A nil
// Encryptor at the call site is responsible for translating these bytes
// back into plaintext; a non-nil key_version lets the caller decide
// whether the row needs re-encryption on the next write.
//
// revoked_at is nullable (the column is only set after MarkYouTubeOAuthTokenRevoked)
// so it is scanned into sql.NullString and surfaced as "" when unset — the
// map[string]interface{} shape stays simple so callers don't have to import
// database/sql just to read the rows.
func (s *SQLiteStore) GetYouTubeOAuthToken(channelID string) (map[string]interface{}, error) {
	row := s.db.QueryRow(
		`SELECT channel_id, access_token_encrypted, refresh_token_encrypted, token_type, expiry, scopes, key_version, revoked_at, created_at, updated_at
		 FROM youtube_oauth_tokens WHERE channel_id = ?`,
		channelID,
	)
	var cid, tokenType, scopes, expiry, createdAt, updatedAt string
	var accessBlob, refreshBlob []byte
	var keyVersion int64
	var revokedAt sql.NullString
	if err := row.Scan(&cid, &accessBlob, &refreshBlob, &tokenType, &expiry, &scopes, &keyVersion, &revokedAt, &createdAt, &updatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	revokedAtStr := ""
	if revokedAt.Valid {
		revokedAtStr = revokedAt.String
	}
	return map[string]interface{}{
		"channel_id":              cid,
		"access_token_encrypted":  accessBlob,
		"refresh_token_encrypted": refreshBlob,
		"token_type":              tokenType,
		"expiry":                  expiry,
		"scopes":                  scopes,
		"key_version":             keyVersion,
		"revoked_at":              revokedAtStr,
		"created_at":              createdAt,
		"updated_at":              updatedAt,
	}, nil
}

// ListActiveYouTubeOAuthTokens enumerates every non-revoked OAuth credential
// row for startup hydration. The boot path uses this to rehydrate the in-RAM
// AuthChannel cache without ever touching the JSON token directory. Returns
// a slice of the same row shape produced by GetYouTubeOAuthToken (BLOBs
// surface as []byte; the caller decrypts via a matching aesgcm.Encryptor).
//
// "Active" semantics: revoked_at IS NULL. Revoked rows are deliberately
// omitted so a stale revoked credential cannot silently re-enter the runtime
// cache after a server restart.
func (s *SQLiteStore) ListActiveYouTubeOAuthTokens() ([]map[string]interface{}, error) {
	rows, err := s.db.Query(
		`SELECT channel_id, access_token_encrypted, refresh_token_encrypted, token_type, expiry, scopes, key_version, revoked_at, created_at, updated_at
		 FROM youtube_oauth_tokens
		 WHERE revoked_at IS NULL`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]interface{}
	for rows.Next() {
		var cid, tokenType, scopes, expiry, createdAt, updatedAt string
		var accessBlob, refreshBlob []byte
		var keyVersion int64
		var revokedAt sql.NullString
		if err := rows.Scan(&cid, &accessBlob, &refreshBlob, &tokenType, &expiry, &scopes, &keyVersion, &revokedAt, &createdAt, &updatedAt); err != nil {
			return nil, err
		}
		revokedAtStr := ""
		if revokedAt.Valid {
			revokedAtStr = revokedAt.String
		}
		result = append(result, map[string]interface{}{
			"channel_id":              cid,
			"access_token_encrypted":  accessBlob,
			"refresh_token_encrypted": refreshBlob,
			"token_type":              tokenType,
			"expiry":                  expiry,
			"scopes":                  scopes,
			"key_version":             keyVersion,
			"revoked_at":              revokedAtStr,
			"created_at":              createdAt,
			"updated_at":              updatedAt,
		})
	}
	return result, rows.Err()
}

// ConnectChannelAtomic creates (or upserts) a youtube_channels row and the
// matching youtube_oauth_tokens row in ONE SQLite transaction. Returns a
// typed error if either leg of the transaction fails so the operator sees
// a single failure rather than half-persisted state.
//
// This is the canonical entry point for both "first-time connect" and
// "explicit re-auth" (a user redoing OAuth on a previously-revoked channel).
// The previous HandleOAuthCallback path performed two separate
// non-transactional calls (UpsertYouTubeOAuthToken alone) which would fail
// with a FK violation when the OAuth row tried to insert into
// youtube_oauth_tokens before any youtube_channels row existed.
// ConnectChannelAtomic fixes that.
//
// On the OAuth leg's UPDATE branch, revoked_at is reset to NULL. This is
// the explicit new-auth semantic: a user who revoked a channel and then
// chose to reconnect MUST be reactivated by the new grant, otherwise
// ListActiveYouTubeOAuthTokens (the boot hydrator and the validate-all
// route) would silently filter the row out after every restart and the
// operator would be stuck in "channels look active but tokens don't
// load" limbo. A normal token refresh does NOT call this method; it goes
// through UpsertYouTubeOAuthToken which preserves revoked_at verbatim.
//
// Both legs run before any RAM update so a partial failure leaves the DB
// consistent with the operator-visible error.
func (s *SQLiteStore) ConnectChannelAtomic(channel *youtubetypes.YouTubeChannelSeed, accessTokenEnc, refreshTokenEnc []byte, tokenType, expiry, scopes string, keyVersion int) error {
	if channel == nil {
		return fmt.Errorf("connect atomic: nil channel seed")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("connect atomic: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	addedAt := channel.AddedAt
	if addedAt == "" {
		addedAt = time.Now().UTC().Format(time.RFC3339)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	// Channel leg: the UPDATE branch touches ONLY seed-owned columns
	// (title, thumbnail_url, last_sync_at, updated_at). User-edited
	// typed columns — notes, language, view_count, subscriber_count,
	// display_name, channel_url — are preserved verbatim across re-auth.
	// added_at / created_at are also preserved because they are not in
	// the SET clause at all.
	if _, err := tx.Exec(
		`INSERT INTO youtube_channels
		 (channel_id, title, display_name, channel_url, thumbnail_url, language, notes,
		  view_count, subscriber_count, added_at, last_sync_at, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(channel_id) DO UPDATE SET
		   title         = excluded.title,
		   thumbnail_url = excluded.thumbnail_url,
		   last_sync_at  = excluded.last_sync_at,
		   updated_at    = excluded.updated_at`,
		channel.ChannelID, channel.Title, channel.DisplayName, channel.ChannelURL, channel.ThumbnailURL,
		channel.Language, channel.Notes, channel.ViewCount, channel.SubCount,
		addedAt, channel.LastSyncAt, now, now,
	); err != nil {
		return fmt.Errorf("connect atomic: upsert channel: %w", err)
	}	// Always fire the OAuth leg in the same transaction. A re-auth flow
	// (user redoing OAuth on an existing channel) also enters through
	// here: the channel upsert is a no-op when the row already exists,
	// and the OAuth leg's UPDATE branch below resets revoked_at so the
	// channel is reactivated on the next boot hydrator pass.
	if _, err := tx.Exec(
		`INSERT INTO youtube_oauth_tokens
	 (channel_id, access_token_encrypted, refresh_token_encrypted, token_type, expiry, scopes, key_version, created_at, updated_at)
	 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	 ON CONFLICT(channel_id) DO UPDATE SET
	   access_token_encrypted=excluded.access_token_encrypted,
	   refresh_token_encrypted=excluded.refresh_token_encrypted,
	   token_type=excluded.token_type,
	   expiry=excluded.expiry,
	   scopes=excluded.scopes,
	   key_version=excluded.key_version,
	   -- Explicit re-auth resets revocation (see doc comment above).
	   -- Cannot be silently wiped by UpsertYouTubeOAuthToken (the
	   -- refresh path) because that method never touches revoked_at.
	   revoked_at=NULL,
	   updated_at=excluded.updated_at`,
		channel.ChannelID, accessTokenEnc, refreshTokenEnc, tokenType, expiry, scopes, keyVersion, now, now,
	); err != nil {
		return fmt.Errorf("connect atomic: upsert oauth token: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("connect atomic: commit: %w", err)
	}
	return nil
}

// AuditYouTubeOAuthTokenOrphans returns the channel_ids present in
// youtube_oauth_tokens but missing from youtube_channels. The caller is
// expected to log these on boot so post-bootstrap operators know whether the
// canonical set is fully consistent.
func (s *SQLiteStore) AuditYouTubeOAuthTokenOrphans() ([]youtubetypes.YouTubeTokenOrphan, error) {
	rows, err := s.db.Query(
		`SELECT t.channel_id, t.updated_at
		 FROM youtube_oauth_tokens t
		 LEFT JOIN youtube_channels c ON c.channel_id = t.channel_id
		 WHERE c.channel_id IS NULL`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var orphans []youtubetypes.YouTubeTokenOrphan
	for rows.Next() {
		var o youtubetypes.YouTubeTokenOrphan
		if err := rows.Scan(&o.ChannelID, &o.UpdatedAt); err != nil {
			return nil, err
		}
		orphans = append(orphans, o)
	}
	return orphans, rows.Err()
}

// MarkYouTubeOAuthTokenRevoked records a revocation timestamp on the OAuth
// row. Idempotent: WHERE revoked_at IS NULL means a second call is a no-op
// and the original timestamp stays intact (audit-friendly). This method
// does NOT delete the row \u2014 the existing Service.DeleteChannel remains the
// single deletion entry point so cascades behave consistently. Revoke =
// disable; Delete = remove.
func (s *SQLiteStore) MarkYouTubeOAuthTokenRevoked(channelID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`UPDATE youtube_oauth_tokens SET revoked_at = ?, updated_at = ?
		 WHERE channel_id = ? AND revoked_at IS NULL`,
		now, now, channelID,
	)
	return err
}

// DeleteChannelAtomic atomically removes a youtube_channels row + its
// youtube_group_channels memberships + (FK-cascade) the matching
// youtube_oauth_tokens row in a single SQLite transaction. Returns the
// number of group memberships cleared for telemetry.
//
// Used by Service.DeleteChannel so the deactivation is consistently
// atomic: a mid-txn failure leaves NO partial state in the canonical
// tables — either the channel is fully gone or untouched. Pairs with
// RevokeToken (which marks revoked_at on the oauth row but keeps the
// channel entry) to give the operator two distinct semantics.
//
// Note: we explicitly DELETE from youtube_group_channels before the
// parent youtube_channels row even though FK CASCADE would handle it,
// because doing so lets us return the membership count for the audit
// endpoint and protects against a misconfigured FK pragma at startup
// (foreign_keys=OFF on legacy DBs).
func (s *SQLiteStore) DeleteChannelAtomic(channelID string) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("delete atomic: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // safe even after explicit Commit

	res, err := tx.Exec(`DELETE FROM youtube_group_channels WHERE channel_id = ?`, channelID)
	if err != nil {
		return 0, fmt.Errorf("delete atomic: memberships: %w", err)
	}
	membershipsDeleted, _ := res.RowsAffected()

	if _, err := tx.Exec(`DELETE FROM youtube_channels WHERE channel_id = ?`, channelID); err != nil {
		return 0, fmt.Errorf("delete atomic: channel row: %w", err)
	}
	// youtube_oauth_tokens row is wiped by FK CASCADE on the channel row.

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("delete atomic: commit: %w", err)
	}
	return membershipsDeleted, nil
}

// ============================================================
// --- Legacy stubs (tables dropped by migrations 008/014) ---
// ============================================================
// These methods satisfy the YouTubeStore and StorageStore interfaces
// for backward compatibility. The legacy tables they once queried have
// been dropped, so they return empty/no-op results.

// UpsertYouTubeChannelMetadata persists a legacy youtube_channel_metadata row
// into the canonical youtube_channels table (the legacy table was dropped by
// migration 008). `tokenPath` is now derived from the canonical
// youtube_oauth_tokens table.
func (s *SQLiteStore) UpsertYouTubeChannelMetadata(channelID, title, tokenPath, language, addedDate, lastUsed, rawJSON string) error {
	if channelID == "" {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`INSERT INTO youtube_channels
		 (channel_id, title, language, added_at, last_sync_at, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(channel_id) DO UPDATE SET
		   title=COALESCE(NULLIF(excluded.title,''), youtube_channels.title),
		   language=COALESCE(NULLIF(excluded.language,''), youtube_channels.language),
		   last_sync_at=excluded.last_sync_at,
		   updated_at=excluded.updated_at`,
		channelID, title, language, addedDate, now, now, now,
	)
	return err
}

// ListYouTubeChannelMetadata returns an empty map (legacy table dropped by migration 014).
func (s *SQLiteStore) ListYouTubeChannelMetadata() (map[string]map[string]interface{}, error) {
	return make(map[string]map[string]interface{}), nil
}

// UpsertYouTubeGroup is a legacy wrapper that routes to the canonical UpsertYouTubeGroupV2.
func (s *SQLiteStore) UpsertYouTubeGroup(name, description, privacy string, channels []string, rawJSON string) error {
	if name == "" {
		return nil
	}
	groupID, err := s.UpsertYouTubeGroupV2(name, "manager", description, privacy)
	if err != nil {
		return err
	}
	for _, ch := range channels {
		_ = s.AddChannelToGroupV2(groupID, ch)
	}
	return nil
}

// GetYouTubeManagerChannel is a legacy no-op (legacy table dropped by migration 008).
func (s *SQLiteStore) GetYouTubeManagerChannel(channelID string) (string, error) {
	return "", nil
}

// ListYouTubeManagerChannels returns nil (legacy table dropped by migration 008).
func (s *SQLiteStore) ListYouTubeManagerChannels() ([]map[string]interface{}, error) {
	return nil, nil
}

// UpsertYouTubeManagerGroup is a legacy no-op (legacy table dropped by migration 008).
func (s *SQLiteStore) UpsertYouTubeManagerGroup(name, createdAt, groupType string, trackedNiches []string) error {
	return nil
}

// DeleteYouTubeManagerGroup is a legacy no-op (legacy table dropped by migration 008).
func (s *SQLiteStore) DeleteYouTubeManagerGroup(name string) error {
	return nil
}

// ListYouTubeManagerGroups returns nil (legacy table dropped by migration 008).
func (s *SQLiteStore) ListYouTubeManagerGroups() ([]map[string]interface{}, error) {
	return nil, nil
}

// DeleteYouTubeManagerChannel routes through the atomic DeleteChannelAtomic.
func (s *SQLiteStore) DeleteYouTubeManagerChannel(channelID string) error {
	if channelID == "" {
		return nil
	}
	_, err := s.DeleteChannelAtomic(channelID)
	return err
}


// DeleteYouTubeGroup is a thin name-based convenience wrapper for tests
// and HTTP shims that historically used the legacy signature. Production
// callers should prefer DeleteYouTubeGroupV2(id).
func (s *SQLiteStore) DeleteYouTubeGroup(name string) error {
	_, err := s.db.Exec(`DELETE FROM youtube_groups WHERE name = ?`, name)
	return err
}
