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
