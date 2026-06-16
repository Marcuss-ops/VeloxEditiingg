package youtube

import (
	"context"
	"fmt"
	"log"
	"time"

	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	"google.golang.org/api/youtube/v3"
)

// HandleOAuthCallback handles OAuth callback and saves token
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

	// Persist the OAuth credentials encrypted at-rest into the canonical
	// youtube_oauth_tokens row. From this commit forward, SQLite is the
	// SINGLE source of truth for tokens: the JSON write below is kept only
	// as a deprecation trail so installs that boot without
	// VELOX_YT_OAUTH_TOKEN_KEY still have a working OAuth flow. Once the
	// key is widely deployed the JSON write is removed (Fix C). A nil
	// oauthBuf is a degraded mode: no SQLite row, JSON only.
	//
	// Note: we encrypt FIRST and bail out before any RAM update if the
	// cipher fails — that way a partially-persisted state (JSON written but
	// SQLite not) is never observed for the SAME callback.
	if am.service.store != nil && am.service.oauthBuf != nil {
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
		if err := am.service.store.UpsertYouTubeOAuthToken(channel.ID, accessEnc, refreshEnc, "Bearer", expiry, "", am.service.oauthBuf.KeyVersion()); err != nil {
			return nil, fmt.Errorf("oauth callback: persist to sqlite: %w", err)
		}
	} else {
		log.Printf("[WARN] OAuth secret cipher unavailable: persisting channel %s to JSON only, youtube_oauth_tokens will not be populated", channel.ID)
	}

	// Existing JSON write kept for backward compat (one release).
	if err := am.saveChannelToken(channel); err != nil {
		log.Printf("[WARN] Failed to save token (JSON compat): %v", err)
	}

	am.service.mu.Lock()
	am.service.channels[channel.ID] = channel
	am.service.mu.Unlock()

	log.Printf("[OK] New YouTube channel added: %s", channel.Title)
	return AuthChannelToChannel(channel), nil
}

// ValidateToken validates a channel's OAuth token and returns detailed status
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

			// Extend Fix B's encrypted-SoT pattern to the ValidateToken refresh
			// path so a crash mid-refresh leaves SQLite consistent with the
			// in-RAM copy. The JSON write path is kept as a one-release-compat
			// trail matching HandleOAuthCallback; it will be removed when the
			// AES-GCM key is everywhere (see migration plan step S6).
			if am.service.store != nil && am.service.oauthBuf != nil {
				if perr := am.service.persistRefreshedToken(channelID, newToken); perr != nil {
					log.Printf("[WARN] validateToken refresh: persist to sqlite: %v", perr)
				}
			} else {
				log.Printf("[WARN] validateToken refresh: oauthBuf or store nil for %s; refresh not persisted", channelID)
			}
			if err := am.saveChannelToken(channel); err != nil {
				log.Printf("[WARN] validateToken refresh: save JSON compat trail failed: %v", err)
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
