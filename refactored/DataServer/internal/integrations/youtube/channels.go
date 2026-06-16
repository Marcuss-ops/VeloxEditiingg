package youtube

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// OAuth-secret payload literals. Centralised so the JSON write path
// (Service.saveChannelToken) and the JSON read path
// (Service.loadChannelFromToken) agree on the token-URI endpoint and the
// expiry-time layout forever. Without these, a future second writer (or
// a copy/paste mistake) could pick a different endpoint or RFC string and
// silently break token round-tripping.
//
// Kept unexported on purpose: nothing outside the youtube integration
// package needs these, and surfacing them risks two callers disagreeing
// about which endpoint "the YouTube OAuth flow" should use.
const (
	oauthTokenRefreshURL = "https://oauth2.googleapis.com/token"
	expiryTimeLayout     = time.RFC3339
)

// loadChannels loads available channels from token files
func (s *Service) loadChannels() {
	tokensDir := s.config.TokensDir
	if tokensDir != "" {
		if err := os.MkdirAll(tokensDir, 0755); err != nil {
			log.Printf("youtube: MkdirAll failed for tokens dir %s: %v", tokensDir, err)
		}
		entries, err := os.ReadDir(tokensDir)
		if err == nil {
			s.loadChannelsFromDir(tokensDir, entries)
		} else {
			log.Printf("[WARN] Failed to read tokens directory: %v", err)
		}
	}
	if len(s.channels) > 0 {
		log.Printf("[OK] Loaded %d YouTube channels", len(s.channels))
	}
}

func (s *Service) loadChannelsFromDir(dir string, entries []os.DirEntry) {
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "account_") || strings.Contains(name, "drive") {
			continue
		}
		if strings.Contains(name, "_token") {
			continue
		}
		tokenPath := filepath.Join(dir, name)
		channel := s.loadChannelFromToken(tokenPath)
		if channel != nil {
			if channel.ID == "" {
				channel.ID = strings.TrimPrefix(name, "account_")
				channel.ID = strings.TrimSuffix(channel.ID, ".json")
			}
			channel.TokenPath = tokenPath
			s.mu.Lock()
			s.channels[channel.ID] = channel
			s.mu.Unlock()
		}
	}
}

// loadChannelFromToken loads channel info from a token file
func (s *Service) loadChannelFromToken(tokenPath string) *AuthChannel {
	data, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil
	}

	// Read ONLY OAuth secret fields. The legacy schema included
	// channel_title, label, thumbnail_url, thumbnail, email — those fields
	// are still accepted by the JSON decoder (encoding/json silently
	// ignores extra fields) so on-disk token files written by older
	// releases still parse cleanly, but we DROP them here: the canonical
	// source for channel Title/Name/Thumbnail/Language/Email is the
	// SQLite youtube_channels table, loaded afterward by
	// loadCanonicalChannels. Reading stale JSON-derived values would
	// create a transient window where an orphan OAuth channel (no
	// SQLite row yet) carries metadata that drifts from SQLite until
	// the next RefreshChannelMetadata call.
	var tokenSecrets struct {
		Token        string `json:"token"`
		RefreshToken string `json:"refresh_token"`
		TokenURI     string `json:"token_uri"`
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
		Scopes       string `json:"scopes"`
		Expiry       string `json:"expiry"`
		ChannelID    string `json:"channel_id"`
		AccessToken  string `json:"access_token"`
	}

	if err := json.Unmarshal(data, &tokenSecrets); err != nil {
		return nil
	}

	accessToken := tokenSecrets.Token
	if accessToken == "" {
		accessToken = tokenSecrets.AccessToken
	}

	channel := &AuthChannel{
		ID:           tokenSecrets.ChannelID,
		AccessToken:  accessToken,
		RefreshToken: tokenSecrets.RefreshToken,
	}

	// OAuth-token expiry is a SECRET VALIDITY metadata, not domain data.
	// It belongs in JSON so token refresh / ValidateToken can reason about
	// the credential's own lifetime without going through SQLite.
	if tokenSecrets.Expiry != "" {
		if t, err := time.Parse(expiryTimeLayout, tokenSecrets.Expiry); err == nil {
			channel.Expiry = t
		}
	}

	return channel
}

// loadCanonicalChannels loads channel metadata from the canonical youtube_channels table.
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

// UpdateChannelMetadata updates metadata fields in SQLite.
func (s *Service) UpdateChannelMetadata(channelID string, metadata map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch, exists := s.channels[channelID]
	if !exists {
		return fmt.Errorf("channel not found: %s", channelID)
	}

	if lang, ok := metadata["language"].(string); ok {
		ch.Language = lang
	}
	if title, ok := metadata["title"].(string); ok {
		ch.Title = title
	}

	// Persist to canonical youtube_channels
	if s.store != nil {
		rawMetadata, _ := json.Marshal(map[string]string{
			"token_path": ch.TokenPath,
		})
		return s.store.UpsertYouTubeChannel(ch.ID, ch.Title, ch.Name, ch.URL, ch.Thumbnail, ch.Language, "", 0, 0, "", "", string(rawMetadata))
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

// DeleteChannel permanently deletes a channel
func (s *Service) DeleteChannel(channelID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	channel, exists := s.channels[channelID]
	if !exists {
		return fmt.Errorf("channel not found")
	}

	for groupName, group := range s.groups {
		for i, chID := range group.Channels {
			if chID == channelID {
				group.Channels = append(group.Channels[:i], group.Channels[i+1:]...)
				log.Printf("[YT] Removed channel %s from group %s", channelID, groupName)
				break
			}
		}
	}

	if s.store != nil {
		if err := s.store.DeleteYouTubeGroupChannelsByChannelID(channelID); err != nil {
			log.Printf("[WARN] Failed to remove DB memberships for channel %s: %v", channelID, err)
		}
		if err := s.store.DeleteYouTubeChannel(channelID); err != nil {
			log.Printf("[WARN] Failed to delete canonical channel %s: %v", channelID, err)
		}
	}

	if channel.TokenPath != "" {
		if err := os.Remove(channel.TokenPath); err != nil {
			log.Printf("[WARN] Failed to remove token file: %v", err)
		} else {
			log.Printf("[DEL] Deleted token file: %s", channel.TokenPath)
		}
	}

	delete(s.channels, channelID)
	// Persisted in-place above via DeleteYouTubeGroupChannelsByChannelID +
	// DeleteYouTubeChannel. No need to call saveGroups() (which would
	// destructively rewrite every group in DB).

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

	s.mu.Lock()
	if ch, ok := s.channels[channelID]; ok {
		ch.Title = newTitle
		ch.Thumbnail = newThumbnail
		if ch.Name == "" || ch.Name == channelID {
			ch.Name = newTitle
		}
	}
	s.mu.Unlock()

	// Persist the refreshed title+thumbnail to SQLite. Refresh is a metadata
	// operation, NOT an OAuth operation, so it MUST go through the metadata
	// repository path — not saveChannelToken, which only writes OAuth
	// secrets (token, refresh_token, token_uri, expiry, channel_id) to the
	// local JSON file. Calling saveChannelToken here would silently drop
	// title and thumbnail: the JSON schema has nowhere to store them, so
	// the refresh would appear successful in RAM but never reach SQLite,
	// and loadCanonicalChannels would then overwrite the in-memory update
	// with the still-stale SQLite row on the next restart — exactly the
	// ghost-channel class of bug we want to eliminate.
	if s.store != nil {
		if err := s.store.UpdateYouTubeChannelMetadata(channelID, newTitle, newThumbnail); err != nil {
			log.Printf("[WARN] Failed to persist refreshed metadata for %s: %v", channelID, err)
		}
	}

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

// saveChannelToken saves an AuthChannel's token to the canonical OAuth
// directory (dataDir/secrets/youtube/tokens/). Always forces the canonical
// path; any pre-existing channel.TokenPath that points to a legacy
// location is overwritten so callers cannot accidentally split source-of-truth.
func (s *Service) saveChannelToken(channel *AuthChannel) error {
	if channel == nil {
		return fmt.Errorf("saveChannelToken: nil channel")
	}
	canonical := CanonicalOAuthTokenPath(s.config.DataDir, channel.ID)
	if canonical == "" {
		return fmt.Errorf("saveChannelToken: cannot resolve canonical path (dataDir=%q, channelID=%q)", s.config.DataDir, channel.ID)
	}
	channel.TokenPath = canonical

	// JSON token file holds OAuth SECRETS ONLY — domain metadata is owned
	// by SQLite (youtube_channels table). Writing Title/Name/Thumbnail to
	// JSON here would create a dual source-of-truth that drifts on edits.
	tokenData := map[string]interface{}{
		"token":         channel.AccessToken,
		"refresh_token": channel.RefreshToken,
		"token_uri":     oauthTokenRefreshURL,
		"expiry":        channel.Expiry.Format(expiryTimeLayout),
		"channel_id":    channel.ID,
	}

	tokenJSON, err := json.MarshalIndent(tokenData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal token: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(canonical), 0755); err != nil {
		return fmt.Errorf("failed to create token directory: %w", err)
	}
	if err := os.WriteFile(canonical, tokenJSON, 0600); err != nil {
		return fmt.Errorf("failed to save token: %w", err)
	}

	log.Printf("[OK] Token saved for channel: %s at %s", channel.ID, canonical)
	return nil
}
