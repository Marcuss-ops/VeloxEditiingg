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

	var tokenData struct {
		Token        string   `json:"token"`
		RefreshToken string   `json:"refresh_token"`
		TokenURI     string   `json:"token_uri"`
		ClientID     string   `json:"client_id"`
		ClientSecret string   `json:"client_secret"`
		Scopes       []string `json:"scopes"`
		Expiry       string   `json:"expiry"`
		ChannelTitle string   `json:"channel_title"`
		ChannelID    string   `json:"channel_id"`
		Label        string   `json:"label"`
		ThumbnailURL string   `json:"thumbnail_url"`
		AccessToken  string   `json:"access_token"`
		Email        string   `json:"email"`
		Thumbnail    string   `json:"thumbnail"`
	}

	if err := json.Unmarshal(data, &tokenData); err != nil {
		return nil
	}

	accessToken := tokenData.Token
	if accessToken == "" {
		accessToken = tokenData.AccessToken
	}

	thumbnail := tokenData.ThumbnailURL
	if thumbnail == "" {
		thumbnail = tokenData.Thumbnail
	}

	channel := &AuthChannel{
		ID:           tokenData.ChannelID,
		Name:         tokenData.Label,
		Title:        tokenData.ChannelTitle,
		Thumbnail:    thumbnail,
		AccessToken:  accessToken,
		RefreshToken: tokenData.RefreshToken,
		Email:        tokenData.Email,
	}

	if tokenData.Label != "" && channel.Name == "" {
		channel.Name = tokenData.Label
	}

	if tokenData.Expiry != "" {
		if t, err := time.Parse(time.RFC3339, tokenData.Expiry); err == nil {
			channel.Expiry = t
		}
	}

	return channel
}

// loadChannelsJSON loads channel details from SQLite (legacy path).
func (s *Service) loadChannelsJSON() {
	if s.store != nil {
		s.loadChannelsFromSQLite()
	}
}

// loadChannelsFromSQLite loads channel metadata from legacy youtube_channel_metadata.
func (s *Service) loadChannelsFromSQLite() bool {
	rows, err := s.store.ListYouTubeChannelMetadata()
	if err != nil || len(rows) == 0 {
		return false
	}

	for id, info := range rows {
		if ch, exists := s.channels[id]; exists {
			if title, ok := info["title"].(string); ok && title != "" {
				ch.Title = title
			}
			if lang, ok := info["language"].(string); ok {
				ch.Language = lang
			}
		} else {
			title, _ := info["title"].(string)
			lang, _ := info["language"].(string)
			s.channels[id] = &AuthChannel{
				ID:       id,
				Title:    title,
				Name:     title,
				Language: lang,
			}
		}
	}

	log.Printf("[OK] Loaded channel metadata from legacy SQLite (%d entries)", len(rows))
	return true
}

// loadCanonicalChannels loads channel metadata from the canonical youtube_channels table.
func (s *Service) loadCanonicalChannels() bool {
	if s.store == nil {
		// Fall back to legacy path
		return s.loadChannelsFromSQLite()
	}

	rows, err := s.store.ListYouTubeChannels()
	if err != nil || len(rows) == 0 {
		// Fall back to legacy if canonical is empty
		return s.loadChannelsFromSQLite()
	}

	for _, row := range rows {
		id, _ := row["channel_id"].(string)
		if id == "" {
			continue
		}
		title, _ := row["title"].(string)
		displayName, _ := row["display_name"].(string)
		language, _ := row["language"].(string)
		thumbnailURL, _ := row["thumbnail_url"].(string)

		if ch, exists := s.channels[id]; exists {
			if title != "" {
				ch.Title = title
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
		return s.store.UpsertYouTubeChannel(ch.ID, ch.Title, ch.Name, "", ch.Thumbnail, ch.Language, "", 0, 0, "", "", string(rawMetadata))
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

	if channel.TokenPath != "" {
		if err := os.Remove(channel.TokenPath); err != nil {
			log.Printf("[WARN] Failed to remove token file: %v", err)
		} else {
			log.Printf("[DEL] Deleted token file: %s", channel.TokenPath)
		}
	}

	delete(s.channels, channelID)
	s.saveGroups()

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

	if err := s.saveChannelToken(s.channels[channelID]); err != nil {
		log.Printf("[WARN] Failed to save updated token for %s: %v", channelID, err)
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

// saveChannelToken saves an AuthChannel's token to file
func (s *Service) saveChannelToken(channel *AuthChannel) error {
	if channel.TokenPath == "" {
		channel.TokenPath = filepath.Join(s.config.TokensDir, fmt.Sprintf("account_%s.json", channel.ID))
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
