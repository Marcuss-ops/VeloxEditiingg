package store

import (
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/store/youtubetypes"
)

// ============================================================
// --- Canonical YouTube OAuth Tokens (Migration 011) ---
// ============================================================

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

// GetYouTubeOAuthToken returns a typed oauth token row, or (nil, nil) if missing.
func (s *SQLiteStore) GetYouTubeOAuthToken(channelID string) (*youtubetypes.YouTubeOAuthToken, error) {
	row := s.db.QueryRow(
		`SELECT channel_id, access_token_encrypted, refresh_token_encrypted, token_type, expiry, scopes, key_version, revoked_at, created_at, updated_at
		 FROM youtube_oauth_tokens WHERE channel_id = ?`,
		channelID,
	)
	var tok youtubetypes.YouTubeOAuthToken
	var revokedAt sql.NullString
	if err := row.Scan(&tok.ChannelID, &tok.AccessTokenEncrypted, &tok.RefreshTokenEncrypted,
		&tok.TokenType, &tok.Expiry, &tok.Scopes, &tok.KeyVersion, &revokedAt,
		&tok.CreatedAt, &tok.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if revokedAt.Valid {
		tok.RevokedAt = revokedAt.String
	}
	return &tok, nil
}

// ListActiveYouTubeOAuthTokens returns all typed non-revoked oauth token rows.
func (s *SQLiteStore) ListActiveYouTubeOAuthTokens() ([]youtubetypes.YouTubeOAuthToken, error) {
	rows, err := s.db.Query(
		`SELECT channel_id, access_token_encrypted, refresh_token_encrypted, token_type, expiry, scopes, key_version, revoked_at, created_at, updated_at
		 FROM youtube_oauth_tokens
		 WHERE revoked_at IS NULL`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []youtubetypes.YouTubeOAuthToken
	for rows.Next() {
		var tok youtubetypes.YouTubeOAuthToken
		var revokedAt sql.NullString
		if err := rows.Scan(&tok.ChannelID, &tok.AccessTokenEncrypted, &tok.RefreshTokenEncrypted,
			&tok.TokenType, &tok.Expiry, &tok.Scopes, &tok.KeyVersion, &revokedAt,
			&tok.CreatedAt, &tok.UpdatedAt); err != nil {
			return nil, err
		}
		if revokedAt.Valid {
			tok.RevokedAt = revokedAt.String
		}
		result = append(result, tok)
	}
	return result, rows.Err()
}

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
	}
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

func (s *SQLiteStore) MarkYouTubeOAuthTokenRevoked(channelID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`UPDATE youtube_oauth_tokens SET revoked_at = ?, updated_at = ?
		 WHERE channel_id = ? AND revoked_at IS NULL`,
		now, now, channelID,
	)
	return err
}

func (s *SQLiteStore) DeleteChannelAtomic(channelID string) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("delete atomic: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.Exec(`DELETE FROM youtube_group_channels WHERE channel_id = ?`, channelID)
	if err != nil {
		return 0, fmt.Errorf("delete atomic: memberships: %w", err)
	}
	membershipsDeleted, _ := res.RowsAffected()

	if _, err := tx.Exec(`DELETE FROM youtube_channels WHERE channel_id = ?`, channelID); err != nil {
		return 0, fmt.Errorf("delete atomic: channel row: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("delete atomic: commit: %w", err)
	}
	return membershipsDeleted, nil
}
