package youtube

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
)

// RevokeToken revokes a channel's OAuth token
func (am *AuthManager) RevokeToken(ctx context.Context, channelID string) error {
	channel := am.service.GetChannel(channelID)
	if channel == nil {
		return fmt.Errorf("channel not found: %s", channelID)
	}

	revokeURL := "https://oauth2.googleapis.com/revoke"

	req, err := http.NewRequestWithContext(ctx, "POST", revokeURL, strings.NewReader("token="+channel.AccessToken))
	if err != nil {
		return fmt.Errorf("failed to create revocation request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[WARN] Token revocation request failed: %v", err)
	} else {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			log.Printf("[OK] Token revoked successfully for channel: %s", channelID)
		} else {
			log.Printf("[WARN] Token revocation returned status: %d", resp.StatusCode)
		}
	}

	if channel.TokenPath != "" {
		if err := os.Remove(channel.TokenPath); err != nil {
			log.Printf("[WARN] Failed to remove token file: %v", err)
		}
	}

	am.service.mu.Lock()
	delete(am.service.channels, channelID)
	am.service.mu.Unlock()

	log.Printf("[OK] Channel removed: %s", channelID)
	return nil
}

// saveChannelToken persists an AuthChannel's OAuth credentials. Delegates
// to Service.saveChannelToken so the two save paths stay byte-for-byte
// identical (canonical path, JSON-secrets-only payload). Logged via the
// service path so operators see one consistent message source.
func (am *AuthManager) saveChannelToken(channel *AuthChannel) error {
	if am == nil || am.service == nil {
		return fmt.Errorf("saveChannelToken: auth manager not wired to service")
	}
	return am.service.saveChannelToken(channel)
}
