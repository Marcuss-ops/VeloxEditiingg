package store

import (
	"database/sql"
	"encoding/json"
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

// ListYouTubeChannels returns all typed canonical channels.
func (s *SQLiteStore) ListYouTubeChannels() ([]youtubetypes.YouTubeChannel, error) {
	rows, err := s.db.Query(`SELECT channel_id, title, display_name, channel_url, thumbnail_url, language, notes, view_count, subscriber_count, added_at, last_sync_at, created_at, updated_at FROM youtube_channels ORDER BY title`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []youtubetypes.YouTubeChannel
	for rows.Next() {
		var ch youtubetypes.YouTubeChannel
		if err := rows.Scan(&ch.ChannelID, &ch.Title, &ch.DisplayName, &ch.ChannelURL, &ch.ThumbnailURL, &ch.Language, &ch.Notes, &ch.ViewCount, &ch.SubscriberCount, &ch.AddedAt, &ch.LastSyncAt, &ch.CreatedAt, &ch.UpdatedAt); err != nil {
			continue
		}
		result = append(result, ch)
	}
	return result, rows.Err()
}

// GetYouTubeChannel returns a typed canonical channel.
func (s *SQLiteStore) GetYouTubeChannel(channelID string) (*youtubetypes.YouTubeChannel, error) {
	row := s.db.QueryRow(`SELECT channel_id, title, display_name, channel_url, thumbnail_url, language, notes, view_count, subscriber_count, added_at, last_sync_at, created_at, updated_at FROM youtube_channels WHERE channel_id=?`, channelID)
	var ch youtubetypes.YouTubeChannel
	if err := row.Scan(&ch.ChannelID, &ch.Title, &ch.DisplayName, &ch.ChannelURL, &ch.ThumbnailURL, &ch.Language, &ch.Notes, &ch.ViewCount, &ch.SubscriberCount, &ch.AddedAt, &ch.LastSyncAt, &ch.CreatedAt, &ch.UpdatedAt); err != nil {
		return nil, err
	}
	return &ch, nil
}

// DeleteYouTubeChannel removes a canonical channel.
func (s *SQLiteStore) DeleteYouTubeChannel(channelID string) error {
	_, err := s.db.Exec(`DELETE FROM youtube_channels WHERE channel_id=?`, channelID)
	return err
} // UpdateYouTubeChannelMetadata persists a YouTube-API metadata refresh into
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

// UpdateChannelDisplayName sets ONLY the display_name column of a single
// channel row. PR15.4 added this method so Storage can call a targeted
// per-column UPDATE for display-name updates without falling back to the
// read-modify-write UpsertYouTubeChannel path (which would risk clobbering
// user-edited language/notes on the way through).
func (s *SQLiteStore) UpdateChannelDisplayName(channelID, displayName string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`UPDATE youtube_channels SET display_name = ?, updated_at = ? WHERE channel_id = ?`,
		displayName, now, channelID,
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
