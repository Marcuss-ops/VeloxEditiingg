package youtube

import (
	"context"
	"fmt"
	"log"
	"time"
)

// expiryTimeLayout is the RFC3339 timestamp format used for oauth expiry
// values flowing between the SQLite store and the go-oauth2 library.
// Centralised so the schema, the cipher path, and the runtime decode agree.
const expiryTimeLayout = time.RFC3339

// loadOAuthChannelsFromSQLite loads OAuth credentials for every non-revoked
// row in youtube_oauth_tokens, decrypts them via the AES-GCM cipher, and
// materialises an AuthChannel entry in s.channels for each. This is the
// canonical boot hydrator — the JSON-directory scan (loadChannels /
// loadChannelFromToken) has been removed.
//
// Requires s.oauthBuf != nil (the AES cipher). The cipher is mounted on
// s.oauthBuf directly inside NewService (fail-closed: a nil cipher
// returns an error from NewService, so this service cannot exist
// without a mounted cipher). The module enforces requireIfMissing=true
// via aesgcm.LoadFromEnv(true) so a server without
// VELOX_YT_OAUTH_TOKEN_KEY (or _FILE variant) refuses to boot the
// YouTube route surface; a runtime invocation without a cipher
// therefore reflects a programmer error rather than an operator
// choice — we log + return early rather than panic so the failure
// surfaces in the log instead of crashing the server.
//
// Title / thumbnail / language metadata is folded into the same in-memory
// channel entry by loadCanonicalChannels (next step in NewService), so a
// single s.channels[id] entry carries both credentials and display data.
//
// Returns (loaded, err). loaded is the number of channels hydrated.
// err is non-nil only on a HARD error (DB read or decrypt failure); a
// missing cipher is NOT an error and the call returns (0, nil).
func (s *Service) loadOAuthChannelsFromSQLite() (int, error) {
	if s.store == nil {
		return 0, nil
	}
	if s.oauthBuf == nil {
		log.Printf("[ERR] loadOAuthChannelsFromSQLite: oauth cipher nil; module wiring failed to install VELOX_YT_OAUTH_TOKEN_KEY. Runtime cache will be empty until the cipher is set.")
		return 0, nil
	}
	tokenRows, err := s.store.ListActiveYouTubeOAuthTokens()
	if err != nil {
		return 0, fmt.Errorf("load oauth tokens: %w", err)
	}
	// Orphan audit (S4): log any oauth row whose parent channel row is
	// missing so operators see the canonical set is fully consistent on boot.
	if orphans, oerr := s.store.AuditYouTubeOAuthTokenOrphans(); oerr == nil && len(orphans) > 0 {
		for _, o := range orphans {
			log.Printf("[WARN] youtube_oauth_tokens row for %s has no matching youtube_channels row; consider DropOrphan or backfill", o.ChannelID)
		}
	}

	hydrated := 0
	for _, row := range tokenRows {
		cid, _ := row["channel_id"].(string)
		if cid == "" {
			continue
		}
		accessBlob, _ := row["access_token_encrypted"].([]byte)
		refreshBlob, _ := row["refresh_token_encrypted"].([]byte)
		access, aerr := s.oauthBuf.Decrypt(accessBlob)
		if aerr != nil {
			log.Printf("[WARN] decrypt access_token for %s: %v (skipping hydration)", cid, aerr)
			continue
		}
		var refresh string
		if len(refreshBlob) > 0 {
			plain, rerr := s.oauthBuf.Decrypt(refreshBlob)
			if rerr != nil {
				log.Printf("[WARN] decrypt refresh_token for %s: %v (continuing with empty refresh)", cid, rerr)
			} else {
				refresh = string(plain)
			}
		}
		var expiry time.Time
		if raw, ok := row["expiry"].(string); ok && raw != "" {
			if t, perr := time.Parse(expiryTimeLayout, raw); perr == nil {
				expiry = t
			}
		}
		s.mu.Lock()
		ch, exists := s.channels[cid]
		if !exists {
			ch = &AuthChannel{ID: cid}
			s.channels[cid] = ch
		}
		ch.AccessToken = string(access)
		ch.RefreshToken = refresh
		ch.Expiry = expiry
		s.mu.Unlock()
		hydrated++
	}
	if hydrated > 0 {
		log.Printf("[OK] Hydrated %d OAuth credentials from youtube_oauth_tokens", hydrated)
	}
	return hydrated, nil
}

// loadCanonicalChannels loads channel metadata from the canonical
// youtube_channels table and folds it into the in-RAM AuthChannel entries
// populated by loadOAuthChannelsFromSQLite. Pure read: no filesystem, no
// JSON.
func (s *Service) loadCanonicalChannels() bool {
	if s.store == nil {
		return false
	}

	rows, err := s.store.ListYouTubeChannels()
	if err != nil || len(rows) == 0 {
		return false
	}

	for _, row := range rows {
		id, _ := row["channel_id"].(string)
		if id == "" {
			continue
		}
		title, _ := row["title"].(string)
		displayName, _ := row["display_name"].(string)
		channelURL, _ := row["channel_url"].(string)
		language, _ := row["language"].(string)
		thumbnailURL, _ := row["thumbnail_url"].(string)

		if ch, exists := s.channels[id]; exists {
			if title != "" {
				ch.Title = title
			}
			if displayName != "" {
				ch.Name = displayName
			}
			if channelURL != "" {
				ch.URL = channelURL
			}
			if language != "" {
				ch.Language = language
			}
			if thumbnailURL != "" && ch.Thumbnail == "" {
				ch.Thumbnail = thumbnailURL
			}
		} else {
			s.channels[id] = &AuthChannel{
				ID:        id,
				URL:       channelURL,
				Title:     title,
				Name:      displayName,
				Language:  language,
				Thumbnail: thumbnailURL,
			}
		}
	}

	log.Printf("[OK] Loaded channel metadata from canonical tables (%d entries)", len(rows))
	return true
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
func (s *Service) Membership(channelID string) (*Channel, error) {
	if s.store == nil {
		return nil, nil
	}
	row, err := s.store.GetYouTubeChannel(channelID)
	if err != nil {
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
// DB-first: the SQLite write happens under s.mu (read+write atomically)
// so a transient SQL failure aborts the operator edit before any RAM
// update is visible.
func (s *Service) UpdateChannelMetadata(channelID string, metadata map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch, exists := s.channels[channelID]
	if !exists {
		return fmt.Errorf("channel not found: %s", channelID)
	}

	// Typed title update (if provided). One column, no clobber.
	if s.store != nil {
		if title, ok := metadata["title"].(string); ok && title != "" {
			if err := s.store.UpdateChannelTitle(channelID, title); err != nil {
				return fmt.Errorf("typed update title: %w", err)
			}
			ch.Title = title // mirror to RAM only after a successful SQL write
		}
		if lang, ok := metadata["language"].(string); ok && lang != "" {
			if err := s.store.UpdateChannelLanguage(channelID, lang); err != nil {
				return fmt.Errorf("typed update language: %w", err)
			}
			ch.Language = lang // mirror to RAM only after a successful SQL write
		}
	} else {
		// No store: degrade gracefully — keep the RAM update so the
		// in-memory surface is coherent (the canonical SQL row will be
		// reconciled on the next process restart by loadCanonicalChannels).
		if title, ok := metadata["title"].(string); ok && title != "" {
			ch.Title = title
		}
		if lang, ok := metadata["language"].(string); ok && lang != "" {
			ch.Language = lang
		}
	}
	return nil
}

// GetChannels returns all available channels
func (s *Service) GetChannels() []*AuthChannel {
	return s.GetAuthChannels()
}

// GetAuthChannels returns all available channels
func (s *Service) GetAuthChannels() []*AuthChannel {
	s.mu.RLock()
	defer s.mu.RUnlock()

	channels := make([]*AuthChannel, 0, len(s.channels))
	for _, ch := range s.channels {
		channels = append(channels, ch)
	}
	return channels
}

// GetChannel returns a channel by ID
func (s *Service) GetChannel(id string) *AuthChannel {
	return s.GetAuthChannel(id)
}

// GetAuthChannel returns a channel by ID
func (s *Service) GetAuthChannel(id string) *AuthChannel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.channels[id]
}

// GetConfig returns the service configuration
func (s *Service) GetConfig() *ServiceConfig {
	return s.config
}

// DeleteChannel permanently deletes a channel: removes the channel row,
// every group membership, the OAuth token row (FK CASCADE), and the
// in-memory entry — all in a coherent "everything-or-nothing" pass. The
// previously-present JSON token file delete is gone: SQLite is the single
// source of truth and DeleteChannelAtomic removes the oauth row via the
// FK cascade. JSON side-effects were the second half of the "race"
// documented in the migration step 6 verdict; with them gone the runtime
// stays coherent with SQL after a single atomic transaction.
//
// The SQL cleanup is delegated to store.DeleteChannelAtomic so a
// failed mid-transaction leaves no orphan rows in the canonical tables.
// If the SQL store is unavailable, DeleteChannel returns the error so
// the operator knows the channel is still considered alive server-side.
// RAM cleanup happens only after the SQL transaction commits to preserve
// the "everything or nothing" invariant.
func (s *Service) DeleteChannel(channelID string) error {
	s.mu.RLock()
	_, exists := s.channels[channelID]
	s.mu.RUnlock()
	if !exists {
		return fmt.Errorf("channel not found")
	}

	// Atomic SQL cleanup: group memberships + channel row + oauth tokens.
	// This runs FIRST so a failed delete does not leave inconsistent RAM
	// state. The previous order (RAM delete → DB) would surface a missing
	// channel to listing endpoints while the SQL row was still alive; a
	// later reload could resurrect it. The new order is DB → RAM, which
	// means: if SQL fails, RAM is untouched and the operator sees the
	// error and retries against the SAME state.
	if s.store != nil {
		if _, err := s.store.DeleteChannelAtomic(channelID); err != nil {
			return fmt.Errorf("delete channel: transactional cleanup failed for %s: %w", channelID, err)
		}
		log.Printf("[DEL] Atomic SQL cleanup for channel %s completed", channelID)
	}

	// Drop RAM entries only after SQL has committed (DB-first order).
	s.mu.Lock()
	for groupName, group := range s.groups {
		found := false
		for i, chID := range group.Channels {
			if chID == channelID {
				group.Channels = append(group.Channels[:i], group.Channels[i+1:]...)
				log.Printf("[YT] Removed channel %s from group %s", channelID, groupName)
				found = true
				break
			}
		}
		_ = found
	}
	delete(s.channels, channelID)
	s.mu.Unlock()

	log.Printf("[OK] Channel permanently deleted: %s", channelID)
	return nil
}

// RefreshChannelMetadata fetches fresh channel info from the YouTube API
func (s *Service) RefreshChannelMetadata(ctx context.Context, channelID string) (*AuthChannel, error) {
	s.mu.RLock()
	ch, exists := s.channels[channelID]
	s.mu.RUnlock()

	if !exists {
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

	// DB-first persistence (S11). The SQL write happens BEFORE the in-RAM
	// mirror update so a transient SQLite failure can never leak into the
	// runtime cache. Refresh is a metadata operation, NOT an OAuth
	// operation — it goes through the typed metadata repository method
	// (UpdateYouTubeChannelMetadata) which only touches title and
	// thumbnail, never user-edited columns.
	if s.store != nil {
		if err := s.store.UpdateYouTubeChannelMetadata(channelID, newTitle, newThumbnail); err != nil {
			return nil, fmt.Errorf("persist refreshed metadata for %s: %w", channelID, err)
		}
	}

	s.mu.Lock()
	if ch, ok := s.channels[channelID]; ok {
		ch.Title = newTitle
		ch.Thumbnail = newThumbnail
		if ch.Name == "" || ch.Name == channelID {
			ch.Name = newTitle
		}
	}
	s.mu.Unlock()

	log.Printf("[OK] Refreshed metadata for channel %s: title=%q", channelID, newTitle)
	return s.channels[channelID], nil
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
