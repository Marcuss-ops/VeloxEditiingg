// Package youtube provides YouTube API integration for the Velox server.
package youtube

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/youtube/v3"
	ytanalytics "google.golang.org/api/youtubeanalytics/v2"
)

// Service provides YouTube API functionality
// This is the main orchestrator that coordinates auth, upload, metadata, and quota modules
type Service struct {
	config      *ServiceConfig
	oauthConfig *oauth2.Config
	channels    map[string]*AuthChannel
	groups      map[string]*ChannelGroup
	mu          sync.RWMutex
	cache       *Cache

	// Module managers (extracted from service.go)
	authManager  *AuthManager
	uploader     *Uploader
	videoManager *VideoManager
	quotaManager *QuotaManager
}

// NewService creates a new YouTube service
func NewService(cfg *ServiceConfig) (*Service, error) {
	if cfg.TokensDir == "" {
		if env := os.Getenv("VELOX_YOUTUBE_TOKENS_DIR"); env != "" {
			cfg.TokensDir = env
		} else {
			for _, candidate := range []string{
				filepath.Join(cfg.DataDir, "secrets", "youtube", "tokens"),
				filepath.Join(cfg.DataDir, "youtube", "tokens"),
				filepath.Join("DataServer", "data", "secrets", "youtube", "tokens"),
				filepath.Join("DataServer", "data", "youtube", "tokens"),
			} {
				if info, err := os.Stat(candidate); err == nil && info.IsDir() {
					cfg.TokensDir = candidate
					break
				}
			}
			if cfg.TokensDir == "" {
				cfg.TokensDir = filepath.Join(cfg.DataDir, "secrets", "youtube", "tokens")
			}
		}
	}
	if cfg.YoutubePostingPath == "" {
		// Try environment variable first, then relative path, then legacy fallback
		if env := os.Getenv("VELOX_YOUTUBE_POSTING_PATH"); env != "" {
			cfg.YoutubePostingPath = env
		} else {
			// Default to relative path from working directory
			cfg.YoutubePostingPath = "YoutubePosting"
		}
	}

	s := &Service{
		config:   cfg,
		channels: make(map[string]*AuthChannel),
		groups:   make(map[string]*ChannelGroup),
		cache:    NewCache(cfg.DataDir, 12*time.Hour),
	}

	// Initialize module managers
	s.authManager = NewAuthManager(s)
	s.uploader = NewUploader(s)
	s.videoManager = NewVideoManager(s)
	s.quotaManager = NewQuotaManager(s)

	// Load OAuth config from client_secret.json if available
	if err := s.loadOAuthConfig(); err != nil {
		log.Printf("⚠️ YouTube OAuth config not loaded: %v", err)
	}

	// Load channels and groups
	s.loadChannels()
	s.loadChannelsJSON() // enrich from DataDir/youtube/channels/channels.json or YoutubePostingPath/Modules
	s.loadGroups()

	return s, nil
}

// --- Module Getters ---

// AuthManager returns the auth manager
func (s *Service) AuthManager() *AuthManager {
	return s.authManager
}

// Uploader returns the uploader
func (s *Service) Uploader() *Uploader {
	return s.uploader
}

// VideoManager returns the video manager
func (s *Service) VideoManager() *VideoManager {
	return s.videoManager
}

// QuotaManager returns the quota manager
func (s *Service) QuotaManager() *QuotaManager {
	return s.quotaManager
}

// SyncAllAnalytics fetches analytics for all authorized channels
func (s *Service) SyncAllAnalytics(ctx context.Context) error {
	channels := s.GetAuthChannels()
	if len(channels) == 0 {
		return nil
	}

	log.Printf("📊 YouTube: Syncing analytics for %d channels...", len(channels))
	successCount := 0

	for _, ch := range channels {
		// 1. Fetch channel-level last 30 days
		data, err := s.quotaManager.FetchAnalytics(ctx, ch.ID, 30)
		if err != nil {
			log.Printf("⚠️ YouTube: Failed to fetch channel analytics for %s: %v", ch.ID, err)
			continue
		}

		if err := s.quotaManager.UpdateAnalyticsCache(ctx, ch.ID, 30, data); err != nil {
			log.Printf("⚠️ YouTube: Failed to update channel cache for %s: %v", ch.ID, err)
		}

		// 2. Fetch video-level performance
		videoData, err := s.quotaManager.FetchVideoAnalytics(ctx, ch.ID, 30)
		if err != nil {
			log.Printf("⚠️ YouTube: Failed to fetch video analytics for %s: %v", ch.ID, err)
		} else {
			if err := s.quotaManager.UpdateVideoAnalyticsCache(ctx, ch.ID, videoData); err != nil {
				log.Printf("⚠️ YouTube: Failed to update video cache for %s: %v", ch.ID, err)
			}
		}

		successCount++
	}

	log.Printf("✅ YouTube: Analytics sync complete (%d/%d successful)", successCount, len(channels))
	return nil
}

// --- Channel Management ---

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
			log.Printf("⚠️ Failed to read tokens directory: %v", err)
		}
	}
	// Tokens are loaded from TokensDir (secrets/youtube/tokens/)
	if len(s.channels) > 0 {
		log.Printf("✅ Loaded %d YouTube channels", len(s.channels))
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
		Token        string   `json:"token"` // Access token (new format)
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
		// Legacy fields
		AccessToken string `json:"access_token"`
		Email       string `json:"email"`
		Thumbnail   string `json:"thumbnail"`
	}

	if err := json.Unmarshal(data, &tokenData); err != nil {
		return nil
	}

	// Use new token format or fall back to legacy
	accessToken := tokenData.Token
	if accessToken == "" {
		accessToken = tokenData.AccessToken
	}

	thumbnail := tokenData.ThumbnailURL
	if thumbnail == "" {
		thumbnail = tokenData.Thumbnail
	}

	channel := &AuthChannel{
		ID:           tokenData.ChannelID, // Use YouTube channel ID from token
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

// loadChannelsJSON loads channel details from channels.json
func (s *Service) loadChannelsJSON() {
	candidates := []string{
		filepath.Join(s.config.DataDir, "youtube", "channels", "channels.json"),
		filepath.Join(s.config.YoutubePostingPath, "Modules", "channels.json"),
		filepath.Join("DataServer", "data", "youtube", "channels", "channels.json"),
	}

	var data []byte
	var err error
	for _, path := range candidates {
		data, err = os.ReadFile(path)
		if err == nil {
			break
		}
	}
	if err != nil || data == nil {
		return
	}

	// channels.json is a map of channel_id -> channel_info
	var channelsData map[string]struct {
		Title     string `json:"title"`
		Token     string `json:"token"`
		ClientID  string `json:"client_id"`
		AddedDate string `json:"added_date"`
		LastUsed  string `json:"last_used"`
		Language  string `json:"language,omitempty"`
	}

	if err := json.Unmarshal(data, &channelsData); err != nil {
		log.Printf("⚠️ Failed to parse channels.json: %v", err)
		return
	}

	// Update channel info with data from channels.json
	for id, info := range channelsData {
		if ch, exists := s.channels[id]; exists {
			if info.Title != "" {
				ch.Title = info.Title
			}
			ch.Language = info.Language
		} else {
			// Channel not in tokens, create from channels.json
			s.channels[id] = &AuthChannel{
				ID:       id,
				Title:    info.Title,
				Name:     info.Title,
				Language: info.Language,
			}
		}
	}

	log.Printf("✅ Loaded channel details from channels.json (%d entries)", len(channelsData))
}

// UpdateChannelMetadata updates metadata fields (language, token_status, etc.) in channels.json
func (s *Service) UpdateChannelMetadata(channelID string, metadata map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update in-memory channel (only known fields)
	if ch, exists := s.channels[channelID]; exists {
		if lang, ok := metadata["language"].(string); ok {
			ch.Language = lang
		}
	}

	// Update channels.json on disk
	channelsPath := ""
	for _, candidate := range []string{
		filepath.Join(s.config.DataDir, "youtube", "channels", "channels.json"),
		filepath.Join(s.config.YoutubePostingPath, "Modules", "channels.json"),
		filepath.Join("DataServer", "data", "youtube", "channels", "channels.json"),
	} {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			channelsPath = candidate
			break
		}
	}
	if channelsPath == "" {
		channelsPath = filepath.Join(s.config.DataDir, "youtube", "channels", "channels.json")
	}

	data, err := os.ReadFile(channelsPath)
	if err != nil {
		// File doesn't exist yet, create it
		chMap := map[string]interface{}{
			channelID: metadata,
		}
		return s.writeChannelsJSON(channelsPath, chMap)
	}

	var channelsData map[string]interface{}
	if err := json.Unmarshal(data, &channelsData); err != nil {
		return fmt.Errorf("failed to parse channels.json: %w", err)
	}

	if channelsData == nil {
		channelsData = make(map[string]interface{})
	}

	entry, exists := channelsData[channelID]
	if !exists {
		channelsData[channelID] = metadata
	} else {
		if entryMap, ok := entry.(map[string]interface{}); ok {
			for k, v := range metadata {
				entryMap[k] = v
			}
		} else {
			channelsData[channelID] = metadata
		}
	}

	return s.writeChannelsJSON(channelsPath, channelsData)
}

func (s *Service) writeChannelsJSON(path string, data interface{}) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0644)
}

// --- Public API: Channels ---

// GetChannels returns all available channels (alias for GetAuthChannels for backward compatibility)
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

// GetChannel returns a channel by ID (alias for GetAuthChannel for backward compatibility)
func (s *Service) GetChannel(id string) *AuthChannel {
	return s.GetAuthChannel(id)
}

// GetConfig returns the service configuration
func (s *Service) GetConfig() *ServiceConfig {
	return s.config
}

// GetAuthChannel returns a channel by ID
func (s *Service) GetAuthChannel(id string) *AuthChannel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.channels[id]
}

// --- Public API: OAuth (Delegated to AuthManager) ---

// GetOAuthStartURL returns the URL to start OAuth flow
func (s *Service) GetOAuthStartURL(channelName string) string {
	return s.authManager.GetOAuthStartURL(channelName)
}

// HandleOAuthCallback handles OAuth callback and saves token
func (s *Service) HandleOAuthCallback(ctx context.Context, code string, channelName string) (*Channel, error) {
	return s.authManager.HandleOAuthCallback(ctx, code, channelName)
}

// ValidateToken validates a channel's OAuth token and returns detailed status
func (s *Service) ValidateToken(ctx context.Context, channelID string) (map[string]interface{}, error) {
	return s.authManager.ValidateToken(ctx, channelID)
}

// RevokeToken revokes a channel's OAuth token
func (s *Service) RevokeToken(ctx context.Context, channelID string) error {
	return s.authManager.RevokeToken(ctx, channelID)
}

// --- Public API: Upload (Delegated to Uploader) ---

// UploadVideo uploads a video to YouTube
func (s *Service) UploadVideo(ctx context.Context, channelID string, videoPath string, config UploadConfig) (*UploadResult, error) {
	return s.uploader.UploadVideo(ctx, channelID, videoPath, config)
}

// SetThumbnail sets the thumbnail for a YouTube video
func (s *Service) SetThumbnail(ctx context.Context, channelID string, videoID string, thumbnailPath string) (string, error) {
	return s.uploader.SetThumbnail(ctx, channelID, videoID, thumbnailPath)
}

// --- Public API: Video Metadata (Delegated to VideoManager) ---

// UpdateVideoMetadata updates a video's metadata
func (s *Service) UpdateVideoMetadata(ctx context.Context, channelID string, videoID string, config UploadConfig) error {
	return s.videoManager.UpdateVideoMetadata(ctx, channelID, videoID, config)
}

// DeleteVideo deletes a video from YouTube
func (s *Service) DeleteVideo(ctx context.Context, channelID string, videoID string) error {
	return s.videoManager.DeleteVideo(ctx, channelID, videoID)
}

// ListVideos lists videos for a channel
func (s *Service) ListVideos(ctx context.Context, channelID string, maxResults int64) ([]*youtube.Video, error) {
	return s.videoManager.ListVideos(ctx, channelID, maxResults)
}

// --- Public API: Quota/Analytics (Delegated to QuotaManager) ---

// GetQuotaUsage returns quota usage information
func (s *Service) GetQuotaUsage(ctx context.Context) map[string]interface{} {
	return s.quotaManager.GetQuotaUsage(ctx)
}

// GetAnalyticsService creates a YouTube Analytics API service for a channel
func (s *Service) GetAnalyticsService(ctx context.Context, channelID string) (*ytanalytics.Service, error) {
	return s.quotaManager.GetAnalyticsService(ctx, channelID)
}

// FetchAnalytics fetches analytics data for a channel
func (s *Service) FetchAnalytics(ctx context.Context, channelID string, days int) (map[string]interface{}, error) {
	return s.quotaManager.FetchAnalytics(ctx, channelID, days)
}

// UpdateAnalyticsCache processes raw analytics data and updates the shared cache
func (s *Service) UpdateAnalyticsCache(ctx context.Context, channelID string, days int, data map[string]interface{}) error {
	return s.quotaManager.UpdateAnalyticsCache(ctx, channelID, days, data)
}

// --- Health Check ---

// HealthCheck checks the health of YouTube API connection
func (s *Service) HealthCheck(ctx context.Context, channelID string) (map[string]interface{}, error) {
	service, err := s.GetYouTubeService(ctx, channelID)
	if err != nil {
		return map[string]interface{}{
			"ok":    false,
			"error": err.Error(),
		}, nil
	}

	// Try to get channel info
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

// --- OAuth Config (Internal) ---

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

	// Override with config values if provided
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

	// Keep AuthManager in sync
	if s.authManager != nil {
		s.authManager.oauthConfig = s.oauthConfig
	}

	log.Printf("✅ YouTube OAuth config loaded from %s", secretPath)
	return nil
}

// DetectChannelLanguage attempts to detect the language of a YouTube channel.
// Strategy:
//  1. Check if language is already stored in AuthChannel
//  2. Try YouTube API channel snippet defaultLanguage field
//  3. Fall back to Unicode/heuristic name-based detection
func (s *Service) DetectChannelLanguage(ctx context.Context, channelID string, channelName string) string {
	// Step 1: Check existing AuthChannel data
	if ch := s.GetAuthChannel(channelID); ch != nil && ch.Language != "" {
		return ch.Language
	}

	// Step 2: Try YouTube API snippet
	tryAPIDetect := func() string {
		ytService, err := s.GetYouTubeService(ctx, channelID)
		if err != nil {
			return ""
		}
		resp, err := ytService.Channels.List([]string{"snippet"}).Id(channelID).Do()
		if err != nil || len(resp.Items) == 0 {
			return ""
		}
		// Check defaultLanguage from snippet (often empty for most channels)
		lang := resp.Items[0].Snippet.DefaultLanguage
		if lang != "" && isValidLanguageCode(lang) {
			return lang
		}
		// Try country as fallback
		country := resp.Items[0].Snippet.Country
		if country != "" {
			if code := countryToLanguage(country); code != "" {
				return code
			}
		}
		return ""
	}

	if lang := tryAPIDetect(); lang != "" {
		return lang
	}

	// Step 3: Name-based detection
	return DetectLanguageFromName(channelName)
}

// isValidLanguageCode checks if the given string is a known ISO 639-1 language code
func isValidLanguageCode(code string) bool {
	knownCodes := map[string]bool{
		"en": true, "es": true, "fr": true, "de": true, "it": true,
		"pt": true, "ru": true, "ja": true, "ko": true, "zh": true,
		"ar": true, "hi": true, "nl": true, "pl": true, "tr": true,
		"sv": true, "da": true, "fi": true, "no": true, "cs": true,
		"hu": true, "ro": true, "th": true, "vi": true, "el": true,
		"he": true, "id": true, "ms": true, "tl": true, "uk": true,
	}
	return knownCodes[code]
}

// countryToLanguage maps a country code to a common language code
func countryToLanguage(country string) string {
	mapping := map[string]string{
		"US": "en", "GB": "en", "AU": "en", "CA": "en",
		"IT": "it", "FR": "fr", "DE": "de", "ES": "es",
		"PT": "pt", "BR": "pt", "RU": "ru", "JP": "ja",
		"KR": "ko", "CN": "zh", "TW": "zh", "SA": "ar",
		"IN": "hi", "NL": "nl", "PL": "pl", "TR": "tr",
		"SE": "sv", "DK": "da", "FI": "fi", "NO": "no",
		"TH": "th", "VN": "vi", "GR": "el", "IL": "he",
		"ID": "id", "UA": "uk",
	}
	return mapping[country]
}

// Helper: checks if a keyword is in the text with boundary rules for 2-letter codes to avoid false positives.
func hasWord(text, word string) bool {
	if len(word) > 2 {
		return strings.Contains(text, word)
	}
	pattern := `(?i)(^|[\s_\-\.\/])` + regexp.QuoteMeta(word) + `($|[\s_\-\.\/])`
	matched, _ := regexp.MatchString(pattern, text)
	return matched
}

// DetectLanguageFromName attempts to detect language from channel name/title using Unicode ranges and keywords
func DetectLanguageFromName(name string) string {
	if name == "" {
		return "en"
	}

	// Do not run keyword detection on raw Channel IDs to avoid random letters matching keywords (e.g. "it")
	if strings.HasPrefix(name, "UC") && len(name) == 24 {
		return "unknown"
	}

	// Check for non-Latin scripts using Unicode ranges
	hasCyrillic := false
	hasJapanese := false
	hasChinese := false
	hasKorean := false
	hasArabic := false
	hasHindi := false

	for _, r := range name {
		switch {
		case r >= 0x0400 && r <= 0x04FF:
			hasCyrillic = true
		case r >= 0x3040 && r <= 0x309F, r >= 0x30A0 && r <= 0x30FF:
			hasJapanese = true
		case r >= 0x4E00 && r <= 0x9FFF:
			hasChinese = true
		case r >= 0xAC00 && r <= 0xD7AF:
			hasKorean = true
		case r >= 0x0600 && r <= 0x06FF:
			hasArabic = true
		case r >= 0x0900 && r <= 0x097F:
			hasHindi = true
		}
	}

	if hasJapanese {
		return "ja"
	}
	if hasKorean {
		return "ko"
	}
	if hasChinese {
		return "zh"
	}
	if hasCyrillic {
		return "ru"
	}
	if hasArabic {
		return "ar"
	}
	if hasHindi {
		return "hi"
	}

	// For Latin-script names, use keyword matching
	lower := strings.ToLower(name)

	// Italian keywords
	italianKeywords := []string{"it", "italia", "italiano", "pizza", "mamma", "ciao", "buongiorno", "canale", "video", "ufficiale"}
	for _, kw := range italianKeywords {
		if hasWord(lower, kw) {
			return "it"
		}
	}

	// French keywords
	frenchKeywords := []string{"fr", "france", "français", "francaise", "bonjour", "chaîne", "officiel", "paris"}
	for _, kw := range frenchKeywords {
		if hasWord(lower, kw) {
			return "fr"
		}
	}

	// German keywords
	germanKeywords := []string{"de", "deutsch", "german", "kanal", "offiziell", "berlin"}
	for _, kw := range germanKeywords {
		if hasWord(lower, kw) {
			return "de"
		}
	}

	// Spanish keywords
	spanishKeywords := []string{"es", "españa", "espana", "espanol", "español", "canal", "oficial", "madrid"}
	for _, kw := range spanishKeywords {
		if hasWord(lower, kw) {
			return "es"
		}
	}

	// Portuguese keywords
	portugueseKeywords := []string{"pt", "portugal", "português", "portugues", "brasil", "canal"}
	for _, kw := range portugueseKeywords {
		if hasWord(lower, kw) {
			return "pt"
		}
	}

	// Polish keywords
	polishKeywords := []string{"pl", "polska", "polski", "kanał", "oficjalny"}
	for _, kw := range polishKeywords {
		if hasWord(lower, kw) {
			return "pl"
		}
	}

	// Turkish keywords
	turkishKeywords := []string{"tr", "türk", "turk", "türkiye", "kanal", "resmi"}
	for _, kw := range turkishKeywords {
		if hasWord(lower, kw) {
			return "tr"
		}
	}

	// Default to English for Latin-script names without specific keywords
	return "en"
}

// --- Groups Management ---

// loadGroups loads channel groups from groups.json (array or map format)
func (s *Service) loadGroups() {
	var data []byte
	var err error
	// Prefer DataDir/youtube if set, then YoutubePostingPath
	paths := []string{
		filepath.Join(s.config.YoutubePostingPath, "Modules", "groups.json"),
	}
	if s.config.DataDir != "" {
		paths = append([]string{filepath.Join(s.config.DataDir, "youtube", "groups.json")}, paths...)
	}
	for _, groupsPath := range paths {
		data, err = os.ReadFile(groupsPath)
		if err == nil {
			break
		}
	}
	if err != nil || data == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Try array format first (common export format)
	var groupsArray []ChannelGroup
	if err := json.Unmarshal(data, &groupsArray); err == nil && len(groupsArray) > 0 {
		for i := range groupsArray {
			g := &groupsArray[i]
			if g.Name != "" {
				s.groups[g.Name] = g
			}
		}
		log.Printf("✅ Loaded %d YouTube groups from array", len(s.groups))
		return
	}

	// Then map format
	var groupsData map[string]ChannelGroup
	if err := json.Unmarshal(data, &groupsData); err != nil {
		return
	}
	for name, group := range groupsData {
		groupCopy := group
		s.groups[name] = &groupCopy
	}
	log.Printf("✅ Loaded %d YouTube groups", len(s.groups))
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
		log.Printf("⚠️ YouTube: Failed to save refreshed token: %v", err)
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

	// Create token from stored credentials
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
			// Only save if the access token changed
			if newToken.AccessToken != channel.AccessToken {
				s.mu.Lock()
				channel.AccessToken = newToken.AccessToken
				channel.Expiry = newToken.Expiry
				if newToken.RefreshToken != "" {
					channel.RefreshToken = newToken.RefreshToken
				}
				s.mu.Unlock()

				// Save to disk
				if err := s.authManager.saveChannelToken(channel); err != nil {
					return err
				}
				log.Printf("✅ YouTube token auto-refreshed and saved for channel: %s", channel.ID)
			}
			return nil
		},
	}

	// Create HTTP client with our custom persisted token source
	client := oauth2.NewClient(ctx, pts)

	// Create YouTube service
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

// CreateGroup creates a new channel group and persists it
func (s *Service) CreateGroup(name, description string, channelIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.groups[name]; exists {
		return fmt.Errorf("group '%s' already exists", name)
	}

	s.groups[name] = &ChannelGroup{
		Name:        name,
		Description: description,
		Channels:    channelIDs,
	}

	s.saveGroups()
	return nil
}

// DeleteGroup deletes a channel group and persists the change
func (s *Service) DeleteGroup(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.groups[name]; !exists {
		return fmt.Errorf("group '%s' not found", name)
	}

	delete(s.groups, name)
	s.saveGroups()
	return nil
}

// AddChannelToGroup adds a channel to a group and persists the change
func (s *Service) AddChannelToGroup(groupName, channelID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupName]
	if !exists {
		return fmt.Errorf("group '%s' not found", groupName)
	}

	// Check for duplicates
	for _, chID := range group.Channels {
		if chID == channelID {
			return fmt.Errorf("channel '%s' already in group '%s'", channelID, groupName)
		}
	}

	group.Channels = append(group.Channels, channelID)
	s.saveGroups()
	return nil
}

// RemoveChannelFromGroup removes a channel from a group and persists the change
func (s *Service) RemoveChannelFromGroup(groupName, channelID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	group, exists := s.groups[groupName]
	if !exists {
		return fmt.Errorf("group '%s' not found", groupName)
	}

	for i, chID := range group.Channels {
		if chID == channelID {
			group.Channels = append(group.Channels[:i], group.Channels[i+1:]...)
			s.saveGroups()
			return nil
		}
	}

	return fmt.Errorf("channel '%s' not found in group '%s'", channelID, groupName)
}

// GetGroups returns all channel groups
func (s *Service) GetGroups() map[string]*ChannelGroup {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups := make(map[string]*ChannelGroup, len(s.groups))
	for name, group := range s.groups {
		groupCopy := *group
		groups[name] = &groupCopy
	}
	return groups
}

// GetGroup returns a specific group by name
func (s *Service) GetGroup(name string) *ChannelGroup {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.groups[name]
}

// AuthChannelToChannel converts an AuthChannel to a public Channel (without sensitive tokens)
func AuthChannelToChannel(ac *AuthChannel) *Channel {
	if ac == nil {
		return nil
	}
	return &Channel{
		ID:        ac.ID,
		Title:     ac.Title,
		Thumbnail: ac.Thumbnail,
		Notes:     ac.Name, // Use Name as Notes for compatibility
	}
}

// ChannelGroupToGroup converts a ChannelGroup to a public Group with full channel details
func (s *Service) ChannelGroupToGroup(cg *ChannelGroup) *Group {
	if cg == nil {
		return nil
	}
	group := &Group{
		Name:      cg.Name,
		CreatedAt: time.Now(),
		Channels:  make([]Channel, 0, len(cg.Channels)),
	}
	s.mu.RLock()
	for _, chID := range cg.Channels {
		if ac, exists := s.channels[chID]; exists {
			group.Channels = append(group.Channels, *AuthChannelToChannel(ac))
		} else {
			// Channel ID without full details
			group.Channels = append(group.Channels, Channel{ID: chID})
		}
	}
	s.mu.RUnlock()
	return group
}

// GetGroupsWithChannels returns groups with full channel details
func (s *Service) GetGroupsWithChannels() []map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]map[string]interface{}, 0, len(s.groups))

	for _, g := range s.groups {
		groupData := map[string]interface{}{
			"name":        g.Name,
			"description": g.Description,
			"privacy":     g.Privacy,
			"channels":    make([]map[string]interface{}, 0, len(g.Channels)),
			"count":       len(g.Channels),
		}

		// Add channel details
		for _, chID := range g.Channels {
			if ch, exists := s.channels[chID]; exists {
				groupData["channels"] = append(groupData["channels"].([]map[string]interface{}), map[string]interface{}{
					"id":        ch.ID,
					"title":     ch.Title,
					"name":      ch.Name,
					"thumbnail": ch.Thumbnail,
					"language":  ch.Language,
				})
			} else {
				// Channel in group but not in tokens
				groupData["channels"] = append(groupData["channels"].([]map[string]interface{}), map[string]interface{}{
					"id":    chID,
					"title": "Unknown",
					"name":  chID,
				})
			}
		}

		result = append(result, groupData)
	}

	return result
}

// GetUndefinedChannels returns channels not assigned to any group (public API)
func (s *Service) GetUndefinedChannels() []*Channel {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Build set of all assigned channel IDs
	assigned := make(map[string]bool)
	for _, cg := range s.groups {
		for _, chID := range cg.Channels {
			assigned[chID] = true
		}
	}

	// Find channels not in any group
	var undefined []*Channel
	for id, ac := range s.channels {
		if !assigned[id] {
			undefined = append(undefined, AuthChannelToChannel(ac))
		}
	}

	return undefined
}

// DeleteChannel permanently deletes a channel (removes from groups and deletes token)
func (s *Service) DeleteChannel(channelID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	channel, exists := s.channels[channelID]
	if !exists {
		return fmt.Errorf("channel not found")
	}

	// Remove channel from all groups
	for groupName, group := range s.groups {
		for i, chID := range group.Channels {
			if chID == channelID {
				group.Channels = append(group.Channels[:i], group.Channels[i+1:]...)
				log.Printf("📦 Removed channel %s from group %s", channelID, groupName)
				break
			}
		}
	}

	// Delete token file
	if channel.TokenPath != "" {
		if err := os.Remove(channel.TokenPath); err != nil {
			log.Printf("⚠️ Failed to remove token file: %v", err)
		} else {
			log.Printf("🗑️ Deleted token file: %s", channel.TokenPath)
		}
	}

	// Remove from channels map
	delete(s.channels, channelID)

	// Save updated groups
	s.saveGroups()

	log.Printf("✅ Channel permanently deleted: %s", channelID)
	return nil
}

// RefreshChannelMetadata fetches fresh channel info from the YouTube API using the OAuth token
// and updates the stored title, thumbnail, and name in both memory and the token file on disk.
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

	// Update the in-memory channel
	s.mu.Lock()
	if ch, ok := s.channels[channelID]; ok {
		ch.Title = newTitle
		ch.Thumbnail = newThumbnail
		if ch.Name == "" || ch.Name == channelID {
			ch.Name = newTitle
		}
	}
	s.mu.Unlock()

	// Update the token file on disk
	if err := s.saveChannelToken(s.channels[channelID]); err != nil {
		log.Printf("⚠️ Failed to save updated token for %s: %v", channelID, err)
	}

	log.Printf("✅ Refreshed metadata for channel %s: title=%q", channelID, newTitle)
	return s.channels[channelID], nil
}

// RefreshAllChannelsMetadata refreshes metadata for all channels with OAuth tokens.
// Returns the count of successfully refreshed channels and any errors encountered.
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
			log.Printf("⚠️ Failed to refresh metadata for channel %s: %v", ch.ID, err)
		} else {
			successCount++
		}
	}

	log.Printf("✅ Refreshed metadata for %d/%d channels", successCount, len(channels))
	return successCount, errors
}

// saveGroups saves groups to groups.json
func (s *Service) saveGroups() {
	var groupsPath string
	if s.config.DataDir != "" {
		groupsPath = filepath.Join(s.config.DataDir, "youtube", "groups.json")
	} else {
		groupsPath = filepath.Join(s.config.YoutubePostingPath, "Modules", "groups.json")
	}

	// Convert to array format for export
	groupsArray := make([]ChannelGroup, 0, len(s.groups))
	for _, g := range s.groups {
		groupsArray = append(groupsArray, *g)
	}

	data, err := json.MarshalIndent(groupsArray, "", "  ")
	if err != nil {
		log.Printf("⚠️ Failed to marshal groups: %v", err)
		return
	}

	if err := os.WriteFile(groupsPath, data, 0644); err != nil {
		log.Printf("⚠️ Failed to save groups: %v", err)
		return
	}

	log.Printf("✅ Groups saved to %s", groupsPath)
}

// saveChannelToken saves an AuthChannel's token to file (internal method)
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

	log.Printf("✅ Token saved for channel: %s", channel.ID)
	return nil
}
