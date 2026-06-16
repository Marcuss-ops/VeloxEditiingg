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
func (am *AuthManager) HandleOAuthCallback(ctx context.Context, code string, channelName string) (*Channel, error) {
	if am.oauthConfig == nil {
		return nil, fmt.Errorf("OAuth not configured")
	}

	token, err := am.oauthConfig.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("failed to exchange token: %w", err)
	}

	client := am.oauthConfig.Client(ctx, token)
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

	if err := am.saveChannelToken(channel); err != nil {
		log.Printf("[WARN] Failed to save token: %v", err)
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

			am.saveChannelToken(channel)

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
