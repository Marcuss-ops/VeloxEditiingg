package youtube

import (
	"context"
	"fmt"
	"log"
	"time"

	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	"google.golang.org/api/youtube/v3"

	"velox-server/internal/store/youtubetypes"
)

// HandleOAuthCallback handles OAuth callback and persists the channel +
// credentials in ONE SQLite transaction via ConnectChannelAtomic.
//
// Pre-fix: UpsertYouTubeOAuthToken was called directly, which the FK from
// youtube_oauth_tokens.channel_id → youtube_channels.channel_id turned into
// a FK violation when the channel had never been inserted before (very
// first connect). ConnectChannelAtomic fixes that by upserting the channel
// row first, then the oauth row, in one transaction.
//
// Fail-closed: if the cipher or store is missing, the callback returns an
// error BEFORE any RAM update, so the operator sees a coherent error and
// no half-persisted state. The module enforces a non-nil cipher at wire-up
// time (requireIfMissing=true in module.go).
func (am *AuthManager) HandleOAuthCallback(ctx context.Context, code string, channelName string, redirectURL string) (*Channel, error) {
	if am.oauthConfig == nil {
		return nil, fmt.Errorf("OAuth not configured")
	}

	cfg := *am.oauthConfig
	if redirectURL != "" {
		cfg.RedirectURL = redirectURL
	}

	token, err := cfg.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("failed to exchange token: %w", err)
	}

	client := cfg.Client(ctx, token)
	service, err := youtube.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("failed to create YouTube service: %w", err)
	}

	channelsResponse, err := service.Channels.List([]string{"snippet"}).Mine(true).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to get channel info: %w", err)
	}

	var channelInfo *youtube.Channel
	if len(channelsResponse.Items) > 0 {
		channelInfo = channelsResponse.Items[0]
	}

	channel := &AuthChannel{
		ID:           channelName,
		Name:         channelName,
		AccessToken:  token.AccessToken,
		RefreshToken: token.RefreshToken,
		Expiry:       token.Expiry,
	}

	if channelInfo != nil {
		channel.Title = channelInfo.Snippet.Title
		channel.Thumbnail = channelInfo.Snippet.Thumbnails.Default.Url
		channel.ID = channelInfo.Id
	}

	// Fail-closed on missing cipher/store: refuse rather than degrade to
	// JSON-only persistence (which is the old dual-write path that
	// produced drift). The module wiring guarantees a cipher is present
	// when OAuth is configured.
	if am.service.store == nil || am.service.oauthBuf == nil {
		return nil, fmt.Errorf("oauth callback: cipher or store not configured; refusing to persist %s without AES encryption", channel.ID)
	}

	accessEnc, err := am.service.oauthBuf.Encrypt([]byte(token.AccessToken))
	if err != nil {
		return nil, fmt.Errorf("oauth callback: encrypt access token: %w", err)
	}
	var refreshEnc []byte
	if token.RefreshToken != "" {
		r, rerr := am.service.oauthBuf.Encrypt([]byte(token.RefreshToken))
		if rerr != nil {
			return nil, fmt.Errorf("oauth callback: encrypt refresh token: %w", rerr)
		}
		refreshEnc = r
	}
	var expiry string
	if !token.Expiry.IsZero() {
		expiry = token.Expiry.Format(time.RFC3339)
	}

	// Single-transaction upsert (channel + oauth token). On the channel
	// leg's UPDATE branch, only seed-owned columns change — user-edited
	// notes/language/view/sub are preserved by the SQLiteStore
	// implementation.
	seed := &youtubetypes.YouTubeChannelSeed{
		ChannelID:    channel.ID,
		Title:        channel.Title,
		DisplayName:  channel.Name,
		ChannelURL:   channel.URL,
		ThumbnailURL: channel.Thumbnail,
		Language:     channel.Language,
	}
	if err := am.service.store.ConnectChannelAtomic(seed, accessEnc, refreshEnc, "Bearer", expiry, "", am.service.oauthBuf.KeyVersion()); err != nil {
		return nil, fmt.Errorf("oauth callback: persist to sqlite: %w", err)
	}

	// Only commit RAM after SQLite has persisted. No JSON dual-write.
	am.service.mu.Lock()
	am.service.channels[channel.ID] = channel
	am.service.mu.Unlock()

	log.Printf("[OK] New YouTube channel added: %s", channel.Title)
	return AuthChannelToChannel(channel), nil
}

// ValidateToken validates a channel's OAuth token and returns detailed
// status. When a refresh round-trip succeeds, the new credentials are
// persisted via Service.persistRefreshedToken (single-write to SQLite, no
// JSON dual-write) — the previous "JSON compat fallback" path has been
// removed because module.go now requires the cipher.
func (am *AuthManager) ValidateToken(ctx context.Context, channelID string) (map[string]interface{}, error) {
	channel := am.service.GetChannel(channelID)
	if channel == nil {
		return map[string]interface{}{
			"ok":    false,
			"error": "Channel not found",
			"valid": false,
		}, nil
	}

	if am.oauthConfig == nil {
		return map[string]interface{}{
			"ok":    false,
			"error": "OAuth not configured",
			"valid": false,
		}, nil
	}

	token := &oauth2.Token{
		AccessToken:  channel.AccessToken,
		RefreshToken: channel.RefreshToken,
		TokenType:    "Bearer",
		Expiry:       channel.Expiry,
	}

	now := time.Now()
	isExpired := !channel.Expiry.IsZero() && channel.Expiry.Before(now)

	result := map[string]interface{}{
		"channel_id":        channelID,
		"channel_name":      channel.Name,
		"channel_title":     channel.Title,
		"expiry":            channel.Expiry,
		"is_expired":        isExpired,
		"has_refresh_token": channel.RefreshToken != "",
	}

	service, err := am.service.GetYouTubeService(ctx, channelID)
	if err != nil {
		result["ok"] = false
		result["valid"] = false
		result["error"] = err.Error()
		return result, nil //nolint:nilerr // status endpoint embeds error in result map
	}

	resp, err := service.Channels.List([]string{"snippet", "statistics"}).Mine(true).Do()
	if err != nil {
		if channel.RefreshToken != "" {
			newToken, refreshErr := am.oauthConfig.TokenSource(ctx, token).Token()
			if refreshErr != nil {
				result["ok"] = false
				result["valid"] = false
				result["error"] = fmt.Sprintf("Token refresh failed: %v", refreshErr)
				return result, nil
			}

			am.service.mu.Lock()
			channel.AccessToken = newToken.AccessToken
			channel.Expiry = newToken.Expiry
			am.service.mu.Unlock()

			// Single SQLite write via the shared persistence primitive.
			// If the SQLite write fails, return the error so the caller
			// sees the refresh was NOT persistently successful — the
			// previously-present "JSON compat trail" is gone. Both
			// previous fail-soft paths (warn-and-continue) are removed:
			// either SQLite wrote the new token or the operator is told.
			if am.service.store == nil || am.service.oauthBuf == nil {
				result["ok"] = false
				result["valid"] = false
				result["error"] = "refresh not persisted: cipher or store missing"
				return result, nil
			}
			if perr := am.service.persistRefreshedToken(channelID, newToken); perr != nil {
				result["ok"] = false
				result["valid"] = false
				result["error"] = fmt.Sprintf("Token refresh not persisted: %v", perr)
				return result, nil
			}

			result["ok"] = true
			result["valid"] = true
			result["refreshed"] = true
			result["message"] = "Token refreshed successfully"
			return result, nil
		}

		result["ok"] = false
		result["valid"] = false
		result["error"] = fmt.Sprintf("API call failed: %v", err)
		return result, nil
	}

	if len(resp.Items) > 0 {
		ch := resp.Items[0]
		result["ok"] = true
		result["valid"] = true
		result["youtube_channel_id"] = ch.Id
		result["channel_title"] = ch.Snippet.Title
		result["subscriber_count"] = ch.Statistics.SubscriberCount
		result["video_count"] = ch.Statistics.VideoCount
		result["view_count"] = ch.Statistics.ViewCount
		result["thumbnail"] = ch.Snippet.Thumbnails.Default.Url
	}

	return result, nil
}
