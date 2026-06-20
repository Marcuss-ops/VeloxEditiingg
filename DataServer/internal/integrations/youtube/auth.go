// Package youtube provides YouTube API integration for the Velox server.
// This file contains OAuth and authentication functionality.
package youtube

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/youtube/v3"
)

// oauthCredentials holds parsed OAuth client credentials.
type oauthCredentials struct {
	ClientID     string
	ClientSecret string
	RedirectURI  string
}

// parseOAuthCredentialsFile parses a client_secret.json file and returns credentials.
func parseOAuthCredentialsFile(data []byte) (*oauthCredentials, error) {
	var clientSecret struct {
		Installed struct {
			ClientID     string   `json:"client_id"`
			ClientSecret string   `json:"client_secret"`
			RedirectUris []string `json:"redirect_uris"`
		} `json:"installed"`
		Web struct {
			ClientID     string   `json:"client_id"`
			ClientSecret string   `json:"client_secret"`
			RedirectUris []string `json:"redirect_uris"`
		} `json:"web"`
	}

	if err := json.Unmarshal(data, &clientSecret); err != nil {
		return nil, fmt.Errorf("parse client_secret.json: %w", err)
	}

	creds := &oauthCredentials{
		RedirectURI: "http://localhost:8080/oauth2callback",
	}

	// Try installed first, then web
	if clientSecret.Installed.ClientID != "" {
		creds.ClientID = clientSecret.Installed.ClientID
		creds.ClientSecret = clientSecret.Installed.ClientSecret
		if len(clientSecret.Installed.RedirectUris) > 0 {
			creds.RedirectURI = clientSecret.Installed.RedirectUris[0]
		}
	} else if clientSecret.Web.ClientID != "" {
		creds.ClientID = clientSecret.Web.ClientID
		creds.ClientSecret = clientSecret.Web.ClientSecret
		if len(clientSecret.Web.RedirectUris) > 0 {
			creds.RedirectURI = clientSecret.Web.RedirectUris[0]
		}
	} else {
		return nil, fmt.Errorf("no valid OAuth credentials found")
	}

	return creds, nil
}

// findOAuthSecretFile searches for client_secret.json in the configured CredentialsDir only.
func findOAuthSecretFile(cfg *ServiceConfig) (string, []byte, error) {
	if cfg.CredentialsDir == "" {
		return "", nil, fmt.Errorf("YouTube CredentialsDir not configured")
	}
	secretPaths := []string{
		filepath.Join(cfg.CredentialsDir, "client_secret.json"),
		filepath.Join(cfg.CredentialsDir, "credentials.json"),
	}

	for _, path := range secretPaths {
		data, err := os.ReadFile(path)
		if err == nil {
			return path, data, nil
		}
	}
	return "", nil, fmt.Errorf("client_secret.json not found in %s", cfg.CredentialsDir)
}

// AuthManager handles OAuth authentication and token management for YouTube channels
type AuthManager struct {
	service     *Service
	oauthConfig *oauth2.Config
	tokenCache  map[string]*oauth2.Token
}

// NewAuthManager creates a new AuthManager
func NewAuthManager(s *Service) *AuthManager {
	return &AuthManager{
		service:    s,
		tokenCache: make(map[string]*oauth2.Token),
	}
}

// LoadOAuthConfig loads OAuth2 configuration from client_secret.json
func (am *AuthManager) LoadOAuthConfig() error {
	cfg := am.service.config

	secretPath, secretData, err := findOAuthSecretFile(cfg)
	if err != nil {
		return err
	}

	creds, err := parseOAuthCredentialsFile(secretData)
	if err != nil {
		return fmt.Errorf("load OAuth config: %w", err)
	}

	// Override with config values if provided
	if cfg.ClientID != "" {
		creds.ClientID = cfg.ClientID
	}
	if cfg.ClientSecret != "" {
		creds.ClientSecret = cfg.ClientSecret
	}
	if cfg.RedirectURL != "" {
		creds.RedirectURI = cfg.RedirectURL
	}

	am.oauthConfig = &oauth2.Config{
		ClientID:     creds.ClientID,
		ClientSecret: creds.ClientSecret,
		RedirectURL:  creds.RedirectURI,
		Scopes: []string{
			"https://www.googleapis.com/auth/youtube",
			"https://www.googleapis.com/auth/youtube.upload",
			"https://www.googleapis.com/auth/youtube.readonly",
			"https://www.googleapis.com/auth/yt-analytics.readonly",
			"https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
		},
		Endpoint: google.Endpoint,
	}

	log.Printf("[OK] YouTube OAuth config loaded from %s", secretPath)
	return nil
}

// GetOAuthStartURL returns the URL to start OAuth flow
func (am *AuthManager) GetOAuthStartURL(channelName string) string {
	if am.oauthConfig == nil {
		return ""
	}

	// Add channel name to state for tracking
	state := fmt.Sprintf("youtube_%s_%d", channelName, time.Now().Unix())
	// Force account picker + refresh token issuance (when possible).
	return am.oauthConfig.AuthCodeURL(
		state,
		oauth2.AccessTypeOffline,
		oauth2.SetAuthURLParam("prompt", "consent select_account"),
	)
}

// HandleOAuthCallback handles OAuth callback and saves token
func (am *AuthManager) HandleOAuthCallback(ctx context.Context, code string, channelName string) (*Channel, error) {
	if am.oauthConfig == nil {
		return nil, fmt.Errorf("OAuth not configured")
	}

	// Exchange code for token
	token, err := am.oauthConfig.Exchange(ctx, code)
	if err != nil {
		return nil, fmt.Errorf("failed to exchange token: %w", err)
	}

	// Create YouTube service to get channel info
	client := am.oauthConfig.Client(ctx, token)
	service, err := youtube.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("failed to create YouTube service: %w", err)
	}

	// Get channel info
	channelsResponse, err := service.Channels.List([]string{"snippet"}).Mine(true).Do()
	if err != nil {
		return nil, fmt.Errorf("failed to get channel info: %w", err)
	}

	var channelInfo *youtube.Channel
	if len(channelsResponse.Items) > 0 {
		channelInfo = channelsResponse.Items[0]
	}

	// Create internal channel object with tokens
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
		// Use YouTube channel ID if available
		channel.ID = channelInfo.Id
	}

	// Save token to file
	if err := am.saveChannelToken(channel); err != nil {
		log.Printf("[WARN] Failed to save token: %v", err)
	}

	// Add to channels map
	am.service.mu.Lock()
	am.service.channels[channel.ID] = channel
	am.service.mu.Unlock()

	log.Printf("[OK] New YouTube channel added: %s", channel.Title)
	// Return public Channel (without sensitive tokens)
	return AuthChannelToChannel(channel), nil
}

// ValidateStoredYouTubeCredentials validates a channel's stored OAuth token
// and returns detailed status. Renamed from ValidateToken to eliminate ambiguity
// with the worker token validator and the OAuth access token validator.
func (am *AuthManager) ValidateStoredYouTubeCredentials(ctx context.Context, channelID string) (map[string]interface{}, error) {
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

	// Create token from stored credentials
	token := &oauth2.Token{
		AccessToken:  channel.AccessToken,
		RefreshToken: channel.RefreshToken,
		TokenType:    "Bearer",
		Expiry:       channel.Expiry,
	}

	// Check if token is expired
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

	// Try to use the token
	service, err := am.service.GetYouTubeService(ctx, channelID)
	if err != nil {
		result["ok"] = false
		result["valid"] = false
		result["error"] = err.Error()
		return result, nil
	}

	// Try to get channel info to verify token works
	resp, err := service.Channels.List([]string{"snippet", "statistics"}).Mine(true).Do()
	if err != nil {
		// Token might be invalid, try to refresh
		if channel.RefreshToken != "" {
			// Attempt token refresh
			newToken, refreshErr := am.oauthConfig.TokenSource(ctx, token).Token()
			if refreshErr != nil {
				result["ok"] = false
				result["valid"] = false
				result["error"] = fmt.Sprintf("Token refresh failed: %v", refreshErr)
				return result, nil
			}

			// Update channel with new token
			am.service.mu.Lock()
			channel.AccessToken = newToken.AccessToken
			channel.Expiry = newToken.Expiry
			am.service.mu.Unlock()

			// Save updated token
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

// RevokeToken revokes a channel's OAuth token
func (am *AuthManager) RevokeToken(ctx context.Context, channelID string) error {
	channel := am.service.GetChannel(channelID)
	if channel == nil {
		return fmt.Errorf("channel not found: %s", channelID)
	}

	// OAuth2 token revocation endpoint
	revokeURL := "https://oauth2.googleapis.com/revoke"

	// Create revocation request
	req, err := http.NewRequestWithContext(ctx, "POST", revokeURL, strings.NewReader("token="+channel.AccessToken))
	if err != nil {
		return fmt.Errorf("failed to create revocation request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[WARN] Token revocation request failed: %v", err)
		// Continue to remove local token anyway
	} else {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			log.Printf("[OK] Token revoked successfully for channel: %s", channelID)
		} else {
			log.Printf("[WARN] Token revocation returned status: %d", resp.StatusCode)
		}
	}

	// Remove token file
	if channel.TokenPath != "" {
		if err := os.Remove(channel.TokenPath); err != nil {
			log.Printf("[WARN] Failed to remove token file: %v", err)
		}
	}

	// Remove from channels map
	am.service.mu.Lock()
	delete(am.service.channels, channelID)
	am.service.mu.Unlock()

	log.Printf("[OK] Channel removed: %s", channelID)
	return nil
}

// saveChannelToken saves an AuthChannel's token to file
func (am *AuthManager) saveChannelToken(channel *AuthChannel) error {
	cfg := am.service.config

	if channel.TokenPath == "" {
		channel.TokenPath = filepath.Join(cfg.TokensDir, fmt.Sprintf("account_%s.json", channel.ID))
	}

	tokenData := map[string]interface{}{
		"token":         channel.AccessToken,
		"refresh_token": channel.RefreshToken,
		"token_uri":     "https://oauth2.googleapis.com/token",
		"expiry":        channel.Expiry.Format(time.RFC3339),
		"channel_title": channel.Title,
		"channel_id":    channel.ID,
		"label":         channel.Name,
		"thumbnail_url": channel.Thumbnail,
	}

	tokenJSON, err := json.MarshalIndent(tokenData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal token: %w", err)
	}

	if err := os.WriteFile(channel.TokenPath, tokenJSON, 0600); err != nil {
		return fmt.Errorf("failed to save token: %w", err)
	}

	log.Printf("[OK] Token saved for channel: %s", channel.ID)
	return nil
}

// GetOAuthConfig returns the OAuth configuration
func (am *AuthManager) GetOAuthConfig() *oauth2.Config {
	return am.oauthConfig
}
