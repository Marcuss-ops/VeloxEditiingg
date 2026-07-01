// Package youtube provides YouTube Data API integration and management functionality.
package youtube

import (
	"time"
)

// Group represents a collection of YouTube channels
type Group struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	Channels  []Channel `json:"channels"`
	GroupType string    `json:"group_type,omitempty"` // "upload" or "manager" (empty = manager for backward compat)
}

// Channel represents a tracked YouTube channel
type Channel struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Title     string    `json:"title"`
	Name      string    `json:"name,omitempty"` // Internal name/label for the channel
	Thumbnail string    `json:"thumbnail"`
	Notes     string    `json:"notes,omitempty"`
	AddedAt   time.Time `json:"added_at"`
	Keywords  []string  `json:"keywords,omitempty"`
	ViewCount int64     `json:"view_count,omitempty"`
	SubCount  int64     `json:"subscriber_count,omitempty"`
	Language  string    `json:"language,omitempty"`
	LastSync  time.Time `json:"last_sync,omitempty"`
}

// Video represents a YouTube video with metadata
type Video struct {
	ID            string  `json:"id"`
	Title         string  `json:"title"`
	URL           string  `json:"url"`
	Thumbnail     string  `json:"thumbnail"`
	ChannelID     string  `json:"channel_id,omitempty"`
	ChannelURL    string  `json:"channel_url,omitempty"`
	ChannelTitle  string  `json:"channel_title,omitempty"`
	Uploader      string  `json:"uploader,omitempty"`
	ViewCount     int64   `json:"view_count"`
	UploadDate    string  `json:"upload_date,omitempty"`
	Duration      string  `json:"duration,omitempty"`
	DurationSecs  int     `json:"duration_seconds,omitempty"`
	Velocity      float64 `json:"velocity"`
	DaysOld       int     `json:"days_old"`
	RelativeDate  string  `json:"relative_date"`
	FormattedDate string  `json:"formatted_date"`
	SourceChannel string  `json:"source_channel,omitempty"`
	GroupName     string  `json:"group_name,omitempty"`
	Source        string  `json:"source,omitempty"`
}

// ChannelInfo represents resolved channel information
type ChannelInfo struct {
	URL         string `json:"url"`
	Title       string `json:"title"`
	Thumbnail   string `json:"thumbnail"`
	Description string `json:"description,omitempty"`
	ChannelID   string `json:"channel_id,omitempty"`
}

// StorageData represents the in-memory snapshot mirrored to SQLite.
type StorageData struct {
	Groups        map[string]*Group `json:"groups"`
	TrackedNiches []string          `json:"tracked_niches,omitempty"`
}

// --- Request DTOs ---

// CreateGroupRequest represents a request to create a new group
type CreateGroupRequest struct {
	Name string `json:"name" binding:"required"`
}

// AddChannelRequest represents a request to add a channel to a group
type AddChannelRequest struct {
	URL       string `json:"url" binding:"required"`
	ChannelID string `json:"channel_id,omitempty"`
	Title     string `json:"title,omitempty"`
	Thumbnail string `json:"thumbnail,omitempty"`
	Notes     string `json:"notes,omitempty"`
}

// MoveChannelRequest represents a request to move a channel between groups
type MoveChannelRequest struct {
	TargetGroup string `json:"target_group" binding:"required"`
}

// ScrapeRequest represents a request to scrape video metadata
type ScrapeRequest struct {
	URL string `json:"url" binding:"required"`
}

// ViralSearchRequest represents a request for viral video search
type ViralSearchRequest struct {
	Query       string  `json:"query"`
	FilterDate  string  `json:"filter_date"`
	SortBy      string  `json:"sort_by"`
	Limit       int     `json:"limit"`
	MinViews    int64   `json:"min_views"`
	MinVelocity float64 `json:"min_velocity"`
	HideShorts  bool    `json:"hide_shorts"`
}

// SimilarChannelsRequest represents a request to find similar channels
type SimilarChannelsRequest struct {
	URL string `json:"url" binding:"required"`
}

// --- Response DTOs ---

// APIResponse is a generic API response wrapper
type APIResponse struct {
	OK      bool        `json:"ok"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// GroupsListResponse represents the response for listing groups
type GroupsListResponse struct {
	OK            bool              `json:"ok"`
	Groups        map[string]*Group `json:"groups"`
	TrackedNiches []string          `json:"tracked_niches,omitempty"`
}

// FeedResponse represents the response for video feed
type FeedResponse struct {
	OK     bool    `json:"ok"`
	Group  string  `json:"group,omitempty"`
	Videos []Video `json:"videos"`
	Count  int     `json:"count"`
}

// UploadsListResponse represents the response for listing uploads
type UploadsListResponse struct {
	OK    bool                     `json:"ok"`
	Group string                   `json:"group,omitempty"`
	Items []map[string]interface{} `json:"items"`
	Count int                      `json:"count"`
}

// SimilarChannelsResponse represents the response for similar channels
type SimilarChannelsResponse struct {
	OK           bool                `json:"ok"`
	Channels     []SimilarChannelHit `json:"channels,omitempty"`
	Tracked      int                 `json:"tracked_channels,omitempty"`
	Message      string              `json:"message,omitempty"`
	KeywordsUsed []string            `json:"keywords_used,omitempty"`
}

// SimilarChannelHit represents a similar channel result
type SimilarChannelHit struct {
	Title     string  `json:"title"`
	URL       string  `json:"url"`
	Thumbnail string  `json:"thumbnail"`
	ViewCount int64   `json:"view_count"`
	Velocity  float64 `json:"velocity"`
	Reason    string  `json:"reason,omitempty"`
}

// DiscoveryResponse represents the response for discovery queries
type DiscoveryResponse struct {
	OK     bool    `json:"ok"`
	Videos []Video `json:"videos,omitempty"`
	Error  string  `json:"error,omitempty"`
}

// TrendsResponse represents the response for niche trends
type TrendsResponse struct {
	OK     bool         `json:"ok"`
	Trends []TrendTopic `json:"trends,omitempty"`
}

// TrendTopic represents a trending topic
type TrendTopic struct {
	Title     string `json:"title"`
	URL       string `json:"url"`
	Views     string `json:"views"`
	Thumbnail string `json:"thumbnail"`
}

// ScriptResponse represents the response for script generation
type ScriptResponse struct {
	OK     bool   `json:"ok"`
	Script string `json:"script,omitempty"`
	Error  string `json:"error,omitempty"`
}

// ThumbnailResponse represents thumbnail URLs
type ThumbnailResponse struct {
	OK         bool              `json:"ok"`
	Thumbnails map[string]string `json:"thumbnails,omitempty"`
	Error      string            `json:"error,omitempty"`
}

// VideoInfoResponse represents video metadata
type VideoInfoResponse struct {
	OK    bool                   `json:"ok"`
	Info  map[string]interface{} `json:"info,omitempty"`
	Error string                 `json:"error,omitempty"`
}

// --- OAuth and Upload Types ---

// AuthChannel represents a YouTube channel with its OAuth token (for upload/auth operations)
type AuthChannel struct {
	ID           string    `json:"id"`
	URL          string    `json:"url,omitempty"`
	Name         string    `json:"name"`
	Title        string    `json:"title,omitempty"`
	Thumbnail    string    `json:"thumbnail,omitempty"`
	Language     string    `json:"language,omitempty"`
	TokenPath    string    `json:"token_path,omitempty"`
	AccessToken  string    `json:"-"`
	RefreshToken string    `json:"-"`
	Expiry       time.Time `json:"expiry,omitempty"`
	Email        string    `json:"email,omitempty"`
}

// ChannelGroup represents a YouTube channel group (for grouping channels by ID)
type ChannelGroup struct {
	Name        string   `json:"name"`
	Channels    []string `json:"channels"`
	Description string   `json:"description,omitempty"`
	Privacy     string   `json:"privacy,omitempty"`
	// GroupType discriminates between manager groups ("manager") and upload
	// groups ("upload"). Promoted from the legacy *Group struct so the SPA
	// /manager/* endpoints keep receiving the discriminator field. The
	// canonical row in youtube_groups supplies it via loadCanonicalGroups.
	// Stored as the same stringly-typed value the previous Storage used so
	// no DB migration is required.
	GroupType string `json:"group_type,omitempty"`
}

// ServiceConfig holds configuration for the YouTube service
type ServiceConfig struct {
	TokensDir          string
	CredentialsDir     string // Directory for OAuth client secrets
	ClientID           string
	ClientSecret       string
	RedirectURL        string
	YoutubePostingPath string
	DataDir            string // Data root (e.g. DataServer/data) for youtube/group/* tokens and groups
	NVIDIAAPIKey       string // Optional NVIDIA API key used for image generation
	NVIDIATextURL      string // Optional chat endpoint for translation / copy helpers
}

// UploadConfig represents the configuration for a YouTube upload
type UploadConfig struct {
	Title            string   `json:"title"`
	Description      string   `json:"description"`
	Tags             []string `json:"tags"`
	CategoryID       string   `json:"category_id"`
	PrivacyStatus    string   `json:"privacy_status"` // private, unlisted, public
	ThumbnailPath    string   `json:"thumbnail_path"`
	ChannelID        string   `json:"channel_id"`
	ChannelName      string   `json:"channel_name"`
	IdempotencyToken string   `json:"idempotency_token,omitempty"`
}

// UploadResult represents the result of a YouTube upload
type UploadResult struct {
	ID           string `json:"id"`
	VideoID      string `json:"video_id"`
	Status       string `json:"status"`
	YouTubeURL   string `json:"youtube_url,omitempty"`
	ThumbnailURL string `json:"thumbnail_url,omitempty"`
	Error        string `json:"error,omitempty"`
}

// TokenChannelInfo represents detailed channel info from channels.json (for token management)
type TokenChannelInfo struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Token     string `json:"token"`
	ClientID  string `json:"client_id"`
	AddedDate string `json:"added_date"`
	LastUsed  string `json:"last_used"`
}

// --- New Advanced Features Models ---

// ABTestRecord represents an A/B test entry for metadata changes
type ABTestRecord struct {
	VideoID     string    `json:"video_id"`
	TestType    string    `json:"test_type"` // "title" or "thumbnail"
	OldValue    string    `json:"old_value"`
	NewValue    string    `json:"new_value"`
	ChangeDate  time.Time `json:"change_date"`
	BeforeStats Stats     `json:"before_stats"`
	AfterStats  Stats     `json:"after_stats"`
}

// Stats represents generic video statistics
type Stats struct {
	Views                 int64   `json:"views"`
	EstimatedRevenue      float64 `json:"estimated_revenue"`
	EstimatedMinutes      float64 `json:"estimated_minutes_watched"`
	AverageViewDuration   int     `json:"average_view_duration"`
	AverageViewPercentage float64 `json:"average_view_percentage"`
}

// CommunityPost represents a post for the YouTube Community tab
type CommunityPost struct {
	Type     string   `json:"type"` // "text", "image", "poll"
	Text     string   `json:"text"`
	ImageURL string   `json:"image_url,omitempty"`
	PollOpts []string `json:"poll_options,omitempty"`
}

// CopyrightStatus represents the copyright status of a video
type CopyrightStatus struct {
	VideoID       string   `json:"video_id"`
	HasClaims     bool     `json:"has_claims"`
	Claims        []Claim  `json:"claims,omitempty"`
	PrivacyStatus string   `json:"privacy_status"`
	Allowed       []string `json:"allowed_regions,omitempty"`
	Blocked       []string `json:"blocked_regions,omitempty"`
}

// Claim represents a copyright claim detail
type Claim struct {
	AssetTitle string `json:"asset_title"`
	ClaimType  string `json:"claim_type"` // "visual", "audio"
	Policy     string `json:"policy"`     // "monetize", "track", "block"
}
