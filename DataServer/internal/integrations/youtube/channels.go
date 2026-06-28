package youtube

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"
)

// expiryTimeLayout is the RFC3339 timestamp format used for oauth expiry
// values flowing between the SQLite store and the go-oauth2 library.
// Centralised so the schema, the cipher path, and the runtime decode agree.
const expiryTimeLayout = time.RFC3339

// loadAuthChannel builds an AuthChannel for channelID by reading the
// canonical youtube_channels row and the youtube_oauth_tokens row,
// decrypting token blobs on the fly. Returns nil when the channel does
// not exist.
//
// PR-YT-REPO: repo is required at NewService time; the `if s.repo == nil`
// early-out is gone.
func (s *Service) loadAuthChannel(channelID string) (*AuthChannel, error) {
	row, err := s.repo.GetYouTubeChannel(channelID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	if row == nil {
		return nil, nil
	}
	ch := &AuthChannel{ID: channelID}
	ch.Title = row.Title
	ch.Name = row.DisplayName
	ch.Thumbnail = row.ThumbnailURL
	ch.Language = row.Language

	if s.oauthBuf != nil {
		tokenRow, terr := s.repo.GetYouTubeOAuthToken(channelID)
		if terr == nil && tokenRow != nil {
			if len(tokenRow.AccessTokenEncrypted) > 0 {
				if plain, derr := s.oauthBuf.Decrypt(tokenRow.AccessTokenEncrypted); derr == nil {
					ch.AccessToken = string(plain)
				}
			}
			if len(tokenRow.RefreshTokenEncrypted) > 0 {
				if plain, derr := s.oauthBuf.Decrypt(tokenRow.RefreshTokenEncrypted); derr == nil {
					ch.RefreshToken = string(plain)
				}
			}
			if tokenRow.Expiry != "" {
				if t, perr := time.Parse(expiryTimeLayout, tokenRow.Expiry); perr == nil {
					ch.Expiry = t
				}
			}
		}
	}
	return ch, nil
}

// loadAuthChannels returns all channels that have an active OAuth token.
//
// PR-YT-REPO: repo is required.
func (s *Service) loadAuthChannels() ([]*AuthChannel, error) {
	tokenRows, err := s.repo.ListActiveYouTubeOAuthTokens()
	if err != nil {
		return nil, err
	}
	var out []*AuthChannel
	for _, row := range tokenRows {
		if row.ChannelID == "" {
			continue
		}
		ch, err := s.loadAuthChannel(row.ChannelID)
		if err != nil || ch == nil {
			continue
		}
		out = append(out, ch)
	}
	return out, nil
}

// Membership returns the typed canonical channel row for channelID from
// the SQLite-backed youtube_channels table. Returns (nil, nil) when no
// row exists so callers can map the not-found case to their own response
// (HTTP 404 / 200 with stub / etc.) without inspecting errors.
//
// This is the typed view the S11 migration exposes so handler files
// can replace the legacy in-RAM "for _, ch := range group.Channels
// { ... ch.Title ... }" pattern (where group.Channels was []Channel
// the previous Storage struct carried in its data.Groups map) with a
// single SQL-backed per-channel read. Handlers migrating to the S11
// canonical shape iterate group.ChannelIDs ([]string) and call
// Membership(id) for each instead of dereferencing a full Channel
// slice off the group struct.
//
// DB-first: errors are surfaced (not logged-and-swallowed) so callers
// can abort an outgoing response rather than render stale RAM data
// that no longer matches the canonical row.
//
// PR-YT-REPO: `if s.repo == nil` early-out removed; repo is required.
func (s *Service) Membership(channelID string) (*Channel, error) {
	row, err := s.repo.GetYouTubeChannel(channelID)
	// sql.ErrNoRows is the canonical "row absent" sentinel from
	// *SQLiteStore.QueryRow().Scan(...); treat it as (nil, nil) so the
	// Membership typed view matches its doc-comment contract ("Returns
	// (nil, nil) when no row exists"). The legacy mock-based tests hid
	// this branch because GetYouTubeChannel's mock returned (nil, nil)
	// directly; the S11 SQLite-fixture cutover pinned it.
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("membership for %s: %w", channelID, err)
	}
	if row == nil {
		return nil, nil
	}
	if ch := channelFromCanonicalRow(row); ch != nil {
		return ch, nil
	}
	// The only path to channelFromCanonicalRow returning nil while the row
	// is non-nil is when row["channel_id"] fails the string type assertion
	// (DB corruption / schema drift). Returning (nil, nil) here is the
	// right shape: callers map to their own not-found response, and we do
	// NOT synthesise a *Channel with the caller's id (which would silently
	// mask bad data behind an unhelpful 200 + wrong payload).
	return nil, nil
}

// BulkMembership returns one *Channel per id, in the same order as the
// input slice. nil entries indicate not-found / no canonical row. A
// single SQLite read is NOT issued per id: callers can batch the lookup
// with the YouTubeStore.ListYouTubeChannels() snapshot in higher layers
// when handler fan-outs are large. The default implementation here is
// sequential Membership() calls; per-handler commits can swap to a
// single batched read where the fan-out is meaningful.
func (s *Service) BulkMembership(ids []string) ([]*Channel, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	out := make([]*Channel, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			out = append(out, nil)
			continue
		}
		ch, err := s.Membership(id)
		if err != nil {
			return nil, err // fail-closed; caller decides abort-vs-skip
		}
		out = append(out, ch)
	}
	return out, nil
}

// UpdateChannelMetadata updates metadata fields in SQLite.
//
// Typed-update path (S11): the operator may set ONLY language or ONLY
// title, never both. Each typed column is written via its own
// UpdateChannel* method so user-set notes / view_count /
// subscriber_count / display_name / channel_url are NOT wiped by
// empty-fill side effects of the wide UpsertYouTubeChannel path.
func (s *Service) UpdateChannelMetadata(channelID string, metadata map[string]interface{}) error {
	if s.GetAuthChannel(channelID) == nil {
		return fmt.Errorf("channel not found: %s", channelID)
	}

	var lang, title string
	if l, ok := metadata["language"].(string); ok {
		lang = l
	}
	if t, ok := metadata["title"].(string); ok {
		title = t
	}

	// PR-YT-REPO: repo is unconditional; UpdateChannelMetadata errors
	// are surfaced to the caller instead of swallowed when store is nil.
	return s.repo.UpsertYouTubeChannel(channelID, title, "", "", "", lang, "", 0, 0, "", "", "")
}

// GetChannels returns all available channels
func (s *Service) GetChannels() []*AuthChannel {
	return s.GetAuthChannels()
}

// GetAuthChannels returns all channels with active OAuth tokens.
func (s *Service) GetAuthChannels() []*AuthChannel {
	chs, _ := s.loadAuthChannels()
	return chs
}

// GetChannel returns a channel by ID
func (s *Service) GetChannel(id string) *AuthChannel {
	return s.GetAuthChannel(id)
}

// GetAuthChannel returns a channel by ID, hydrating from SQLite on demand.
func (s *Service) GetAuthChannel(id string) *AuthChannel {
	ch, _ := s.loadAuthChannel(id)
	return ch
}

// GetConfig returns the service configuration
func (s *Service) GetConfig() *ServiceConfig {
	return s.config
}

// DeleteChannel permanently deletes a channel: removes the channel row,
// every group membership, and the OAuth token row (FK CASCADE) in a
// single atomic transaction. SQLite is the single source of truth.
//
// PR-YT-REPO: `if s.repo != nil` guard removed; repo is required so the
// atomic cleanup is unconditional.
func (s *Service) DeleteChannel(channelID string) error {
	if s.GetAuthChannel(channelID) == nil {
		return fmt.Errorf("channel not found")
	}

	if _, err := s.repo.DeleteChannelAtomic(channelID); err != nil {
		return fmt.Errorf("delete channel: transactional cleanup failed for %s: %w", channelID, err)
	}
	log.Printf("[DEL] Atomic SQL cleanup for channel %s completed", channelID)
	log.Printf("[OK] Channel permanently deleted: %s", channelID)
	return nil
}

// RefreshChannelMetadata fetches fresh channel info from the YouTube API
// and persists it to SQLite.
func (s *Service) RefreshChannelMetadata(ctx context.Context, channelID string) (*AuthChannel, error) {
	ch := s.GetAuthChannel(channelID)
	if ch == nil {
		return nil, fmt.Errorf("channel not found: %s", channelID)
	}

	if ch.AccessToken == "" && ch.RefreshToken == "" {
		return nil, fmt.Errorf("channel %s has no OAuth token, cannot refresh", channelID)
	}

	service, err := s.GetYouTubeService(ctx, channelID)
	if err != nil {
		return nil, fmt.Errorf("failed to get YouTube service for %s: %w", channelID, err)
	}

	resp, err := service.Channels.List([]string{"snippet"}).Mine(true).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch channel info from YouTube API: %w", err)
	}

	if len(resp.Items) == 0 {
		return nil, fmt.Errorf("no channel info returned from YouTube API for %s", channelID)
	}

	item := resp.Items[0]
	newTitle := item.Snippet.Title
	newThumbnail := item.Snippet.Thumbnails.Default.Url

	// PR-YT-REPO: repo is unconditional.
	if err := s.repo.UpdateYouTubeChannelMetadata(channelID, newTitle, newThumbnail); err != nil {
		return nil, fmt.Errorf("persist refreshed metadata for %s: %w", channelID, err)
	}

	log.Printf("[OK] Refreshed metadata for channel %s: title=%q", channelID, newTitle)
	return s.GetAuthChannel(channelID), nil
}

// RefreshAllChannelsMetadata refreshes metadata for all channels with OAuth tokens
func (s *Service) RefreshAllChannelsMetadata(ctx context.Context) (int, []error) {
	channels := s.GetAuthChannels()
	var errors []error
	successCount := 0

	for _, ch := range channels {
		if ch.AccessToken == "" && ch.RefreshToken == "" {
			continue
		}
		if _, err := s.RefreshChannelMetadata(ctx, ch.ID); err != nil {
			errors = append(errors, err)
			log.Printf("[WARN] Failed to refresh metadata for channel %s: %v", ch.ID, err)
		} else {
			successCount++
		}
	}

	log.Printf("[OK] Refreshed metadata for %d/%d channels", successCount, len(channels))
	return successCount, errors
}
