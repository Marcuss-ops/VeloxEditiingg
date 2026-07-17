// OAuth facade methods for the YouTube Service.
//
// PR-YT-SVC-SPLIT: this file hosts the OAuth facade methods that were
// previously declared inline in service.go under the
// "Public API: OAuth (Delegated to AuthManager)" section. They are
// pure delegators to s.authManager (defined in auth.go) and are
// extracted only so service.go can stay focused on its construction
// concerns. No behaviour change.
package youtube

import (
	"context"
)

// --- Public API: OAuth (Delegated to AuthManager) ---

func (s *Service) GetOAuthStartURL(channelName string) string {
	return s.authManager.GetOAuthStartURL(channelName)
}

func (s *Service) HandleOAuthCallback(ctx context.Context, code string, channelName string) (*Channel, error) {
	return s.authManager.HandleOAuthCallback(ctx, code, channelName)
}

// ValidateOAuthAccessToken validates a channel's OAuth access token
// against the live YouTube API.
func (s *Service) ValidateOAuthAccessToken(ctx context.Context, channelID string) (map[string]interface{}, error) {
	return s.authManager.ValidateStoredYouTubeCredentials(ctx, channelID)
}

func (s *Service) RevokeToken(ctx context.Context, channelID string) error {
	return s.authManager.RevokeToken(ctx, channelID)
}
