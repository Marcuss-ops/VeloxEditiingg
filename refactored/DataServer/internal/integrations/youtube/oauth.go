package youtube

import (
	"context"
	"fmt"
	"log"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/youtube/v3"
)

// loadOAuthConfig loads OAuth2 configuration from client_secret.json
func (s *Service) loadOAuthConfig() error {
	secretPath, secretData, err := findOAuthSecretFile(s.config)
	if err != nil {
		return err
	}

	creds, err := parseOAuthCredentialsFile(secretData)
	if err != nil {
		return fmt.Errorf("load OAuth config: %w", err)
	}

	if s.config.ClientID != "" {
		creds.ClientID = s.config.ClientID
	}
	if s.config.ClientSecret != "" {
		creds.ClientSecret = s.config.ClientSecret
	}
	if s.config.RedirectURL != "" {
		creds.RedirectURI = s.config.RedirectURL
	}

	s.oauthConfig = &oauth2.Config{
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

	if s.authManager != nil {
		s.authManager.oauthConfig = s.oauthConfig
	}

	log.Printf("[OK] YouTube OAuth config loaded from %s", secretPath)
	return nil
}

// PersistedTokenSource wraps an oauth2.TokenSource and saves refreshed tokens to disk.
type PersistedTokenSource struct {
	source oauth2.TokenSource
	save   func(*oauth2.Token) error
}

func (pts *PersistedTokenSource) Token() (*oauth2.Token, error) {
	t, err := pts.source.Token()
	if err != nil {
		return nil, err
	}
	if err := pts.save(t); err != nil {
		log.Printf("[WARN] YouTube: Failed to save refreshed token: %v", err)
	}
	return t, nil
}

// GetYouTubeService returns a YouTube service for a channel
func (s *Service) GetYouTubeService(ctx context.Context, channelID string) (*youtube.Service, error) {
	channel := s.GetChannel(channelID)
	if channel == nil {
		return nil, fmt.Errorf("channel not found: %s", channelID)
	}

	if s.oauthConfig == nil {
		return nil, fmt.Errorf("OAuth not configured")
	}

	token := &oauth2.Token{
		AccessToken:  channel.AccessToken,
		RefreshToken: channel.RefreshToken,
		TokenType:    "Bearer",
		Expiry:       channel.Expiry,
	}

	baseSource := s.oauthConfig.TokenSource(ctx, token)
	pts := &PersistedTokenSource{
		source: baseSource,
		save: func(newToken *oauth2.Token) error {
			if newToken.AccessToken != channel.AccessToken {
				s.mu.Lock()
				channel.AccessToken = newToken.AccessToken
				channel.Expiry = newToken.Expiry
				if newToken.RefreshToken != "" {
					channel.RefreshToken = newToken.RefreshToken
				}
				s.mu.Unlock()

				if err := s.authManager.saveChannelToken(channel); err != nil {
					return err
				}
				log.Printf("[OK] YouTube token auto-refreshed and saved for channel: %s", channel.ID)
			}
			return nil
		},
	}

	client := oauth2.NewClient(ctx, pts)

	service, err := youtube.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("failed to create YouTube service: %w", err)
	}

	return service, nil
}

// GetQuotaManager returns the quota manager
func (s *Service) GetQuotaManager() *QuotaManager {
	return s.quotaManager
}

// HealthCheck checks the health of YouTube API connection
func (s *Service) HealthCheck(ctx context.Context, channelID string) (map[string]interface{}, error) {
	service, err := s.GetYouTubeService(ctx, channelID)
	if err != nil {
		return map[string]interface{}{
			"ok":    false,
			"error": err.Error(),
		}, nil //nolint:nilerr // status endpoint embeds error in result map
	}

	_, err = service.Channels.List([]string{"snippet"}).Mine(true).Do()
	if err != nil {
		return map[string]interface{}{
			"ok":    false,
			"error": fmt.Sprintf("API call failed: %v", err),
		}, nil
	}

	return map[string]interface{}{
		"ok":      true,
		"message": "YouTube API connection healthy",
	}, nil
}
