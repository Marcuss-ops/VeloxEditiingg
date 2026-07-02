// sql-allowlist: youtube QuotaManager writes analytics_cache + youtube_revenue_metrics + youtube_video_metrics from runtime YouTube Analytics API fetches. Legacy direct access — tracked for refactor into internal/store typed repos.

// Package youtube provides YouTube API integration for the Velox server.
// This file contains quota and analytics functionality.
package youtube

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	ytanalytics "google.golang.org/api/youtubeanalytics/v2"
)

const (
	MaxDailyQuota      = 10000
	QuotaThreshold     = 0.9 // 90%
	CostSearch         = 100
	CostVideoList      = 1
	CostUpload         = 1600
	CostAnalyticsQuery = 0 // Analytics API has separate quota, usually very generous
)

// QuotaManager handles quota tracking and analytics for YouTube channels
type QuotaManager struct {
	service *Service
	db      *sql.DB // Primary store connection (set via SetDB)
	store   interface {
		TrackQuotaUsage(units int) error
		GetDailyQuotaUsage() (int, error)
	}
}

// NewQuotaManager creates a new QuotaManager
func NewQuotaManager(s *Service) *QuotaManager {
	return &QuotaManager{
		service: s,
	}
}

// SetDB sets the primary store database connection.
func (qm *QuotaManager) SetDB(db *sql.DB) {
	qm.db = db
}

// SetStore sets the store for quota tracking
func (qm *QuotaManager) SetStore(store interface {
	TrackQuotaUsage(units int) error
	GetDailyQuotaUsage() (int, error)
}) {
	qm.store = store
}

// TrackUsage records quota consumption
func (qm *QuotaManager) TrackUsage(units int) {
	if qm.store != nil {
		if err := qm.store.TrackQuotaUsage(units); err != nil {
			log.Printf("youtube/quota: TrackQuotaUsage failed: %v", err)
		}
	}
}

// CheckQuota returns true if we are within safe limits
func (qm *QuotaManager) CheckQuota() bool {
	if qm.store == nil {
		return true
	}
	used, _ := qm.store.GetDailyQuotaUsage()
	return float64(used) < float64(MaxDailyQuota)*QuotaThreshold
}

// GetQuotaUsage returns quota usage information
func (qm *QuotaManager) GetQuotaUsage(ctx context.Context) map[string]interface{} {
	used := 0
	if qm.store != nil {
		used, _ = qm.store.GetDailyQuotaUsage()
	}
	return map[string]interface{}{
		"daily_quota":         MaxDailyQuota,
		"estimated_used":      used,
		"estimated_remaining": MaxDailyQuota - used,
		"percent_used":        fmt.Sprintf("%.1f%%", float64(used)/float64(MaxDailyQuota)*100),
		"is_safe":             float64(used) < float64(MaxDailyQuota)*QuotaThreshold,
		"reset_time":          "midnight Pacific Time",
	}
}

// GetAnalyticsService creates a YouTube Analytics API service for a channel
func (qm *QuotaManager) GetAnalyticsService(ctx context.Context, channelID string) (*ytanalytics.Service, error) {
	channel := qm.service.GetChannel(channelID)
	if channel == nil {
		return nil, fmt.Errorf("channel not found: %s", channelID)
	}

	// Get OAuth config from service
	oauthConfig := qm.service.oauthConfig
	if oauthConfig == nil {
		return nil, fmt.Errorf("OAuth not configured")
	}

	// Create token from stored credentials
	token := &oauth2.Token{
		AccessToken:  channel.AccessToken,
		RefreshToken: channel.RefreshToken,
		TokenType:    "Bearer",
		Expiry:       channel.Expiry,
	}

	// Create HTTP client with token source
	client := oauthConfig.Client(ctx, token)

	// Create Analytics service
	service, err := ytanalytics.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, fmt.Errorf("failed to create Analytics service: %w", err)
	}

	return service, nil
}

// FetchAnalytics fetches analytics data for a channel
func (qm *QuotaManager) FetchAnalytics(ctx context.Context, channelID string, days int) (map[string]interface{}, error) {
	service, err := qm.GetAnalyticsService(ctx, channelID)
	if err != nil {
		return nil, err
	}

	// Calculate date range
	now := time.Now()
	endDate := now.AddDate(0, 0, -1).Format("2006-01-02")
	startDate := now.AddDate(0, 0, -days).Format("2006-01-02")

	// Query analytics
	// Note: dimensions=day, metrics=views,estimatedMinutesWatched,estimatedRevenue
	call := service.Reports.Query().
		Ids("channel==MINE").
		StartDate(startDate).
		EndDate(endDate).
		Metrics("views,estimatedMinutesWatched,estimatedRevenue,averageViewDuration,averageViewPercentage,subscribersGained").
		Dimensions("day")

	resp, err := call.Do()
	if err != nil {
		return nil, fmt.Errorf("analytics query failed: %w", err)
	}

	// Transform response into a usable format
	// rows is a slice of rows, each row is a slice of values corresponding to dimension/metric columns
	result := map[string]interface{}{
		"channel_id": channelID,
		"days":       days,
		"start_date": startDate,
		"end_date":   endDate,
		"rows":       resp.Rows,
		"headers":    resp.ColumnHeaders,
	}

	return result, nil
}

// UpdateAnalyticsCache processes raw analytics data and updates the shared cache (JSON and SQLite)
func (qm *QuotaManager) UpdateAnalyticsCache(ctx context.Context, channelID string, days int, data map[string]interface{}) error {
	rows, ok := data["rows"].([][]interface{})
	if !ok || len(rows) == 0 {
		return nil
	}

	totalViews := 0.0
	totalRevenue := 0.0
	dailyStats := make([]map[string]interface{}, 0, len(rows))

	for _, row := range rows {
		if len(row) < 4 {
			continue
		}
		date := fmt.Sprintf("%v", row[0])
		views := toFloat(row[1])
		revenue := toFloat(row[3]) // Index 3 is estimatedRevenue if using the Metrics list in FetchAnalytics

		totalViews += views
		totalRevenue += revenue

		dailyStats = append(dailyStats, map[string]interface{}{
			"date":    date,
			"views":   views,
			"revenue": revenue,
		})
	}

	// Prepare dashboard format
	cacheEntry := map[string]interface{}{
		"totals": map[string]interface{}{
			"views":   totalViews,
			"revenue": totalRevenue,
		},
		"channels": []map[string]interface{}{
			{
				"id":      channelID,
				"name":    channelID,
				"views":   totalViews,
				"revenue": totalRevenue,
			},
		},
		"daily_stats": dailyStats,
	}

	period := strconv.Itoa(days)

	// 1. Update SQLite via primary store connection
	if qm.db != nil {
		jsonData, _ := json.Marshal(cacheEntry)
		_, err := qm.db.Exec("INSERT OR REPLACE INTO analytics_cache (cache_key, ts, data_json, migrated_at) VALUES (?, ?, ?, ?)",
			period, float64(time.Now().Unix()), string(jsonData), time.Now().UTC().Format(time.RFC3339))
		if err != nil {
			log.Printf("[WARN] SQLite analytics update failed: %v", err)
		} else {
			log.Printf("[OK] SQLite analytics updated for period %s", period)
		}

		// ALSO: Save historical metrics to structured tables
		for _, ds := range dailyStats {
			dateStr := fmt.Sprintf("%v", ds["date"])
			views := int64(ds["views"].(float64))
			revenue := ds["revenue"].(float64)

			_, err = qm.db.Exec(
				`INSERT INTO youtube_revenue_metrics (channel_id, date, estimated_revenue, currency, views)
				 VALUES (?, ?, ?, ?, ?)
				 ON CONFLICT(channel_id, date) DO UPDATE SET
				   estimated_revenue=excluded.estimated_revenue,
				   views=excluded.views`,
				channelID, dateStr, revenue, "USD", views,
			)
			if err != nil {
				log.Printf("[WARN] Failed to save historical metric for %s on %s: %v", channelID, dateStr, err)
			}
		}
		log.Printf("[OK] YouTube historical metrics saved to SQLite for channel %s", channelID)
	} else {
		return fmt.Errorf("quota: database connection not set — analytics not persisted")
	}

	return nil
}

// FetchVideoAnalytics fetches performance data for top videos of a channel
func (qm *QuotaManager) FetchVideoAnalytics(ctx context.Context, channelID string, days int) (map[string]interface{}, error) {
	service, err := qm.GetAnalyticsService(ctx, channelID)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	endDate := now.AddDate(0, 0, -1).Format("2006-01-02")
	startDate := now.AddDate(0, 0, -days).Format("2006-01-02")

	// Metrics: views, estimatedRevenue
	// Dimensions: video
	// Sort by views descending
	call := service.Reports.Query().
		Ids("channel==MINE").
		StartDate(startDate).
		EndDate(endDate).
		Metrics("views,estimatedRevenue").
		Dimensions("video").
		Sort("-views").
		MaxResults(20)

	resp, err := call.Do()
	if err != nil {
		return nil, fmt.Errorf("video analytics query failed: %w", err)
	}

	return map[string]interface{}{
		"channel_id": channelID,
		"rows":       resp.Rows,
	}, nil
}

// UpdateVideoAnalyticsCache saves video performance snapshots to SQLite and fetches titles/thumbnails in batches
func (qm *QuotaManager) UpdateVideoAnalyticsCache(ctx context.Context, channelID string, data map[string]interface{}) error {
	rows, ok := data["rows"].([][]interface{})
	if !ok || len(rows) == 0 {
		return nil
	}

	// 1. Collect video IDs to fetch metadata (titles, thumbnails) in batch
	videoIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		if len(row) > 0 {
			videoIDs = append(videoIDs, fmt.Sprintf("%v", row[0]))
		}
	}

	// Fetch metadata in batches of 50 (Quota optimized)
	metadataMap := make(map[string]struct{ Title, Thumbnail string })
	yt, err := qm.service.GetYouTubeService(ctx, channelID)
	if err == nil {
		for i := 0; i < len(videoIDs); i += 50 {
			end := i + 50
			if end > len(videoIDs) {
				end = len(videoIDs)
			}
			batch := videoIDs[i:end]
			resp, err := yt.Videos.List([]string{"snippet"}).Id(strings.Join(batch, ",")).Do()
			if err == nil {
				for _, item := range resp.Items {
					metadataMap[item.Id] = struct{ Title, Thumbnail string }{
						Title:     item.Snippet.Title,
						Thumbnail: item.Snippet.Thumbnails.Default.Url,
					}
				}
			}
		}
	}

	// 2. Save to structured SQLite table via primary store connection
	if qm.db == nil {
		return fmt.Errorf("quota: database connection not set")
	}

	today := time.Now().Format("2006-01-02")
	for _, row := range rows {
		if len(row) < 3 {
			continue
		}
		videoID := fmt.Sprintf("%v", row[0])
		views := int64(toFloat(row[1]))
		revenue := toFloat(row[2])

		title := ""
		thumbnail := ""
		if m, ok := metadataMap[videoID]; ok {
			title = m.Title
			thumbnail = m.Thumbnail
		}

		_, err = qm.db.Exec(
			`INSERT INTO youtube_video_metrics (video_id, channel_id, date, title, thumbnail_url, views, revenue)
			 VALUES (?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(video_id, date) DO UPDATE SET
			   views=excluded.views,
			   revenue=excluded.revenue,
			   title=excluded.title,
			   thumbnail_url=excluded.thumbnail_url`,
			videoID, channelID, today, title, thumbnail, views, revenue,
		)
	}

	return nil
}

// toFloat converts various types to float64
func toFloat(v interface{}) float64 {
	if v == nil {
		return 0
	}
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case string:
		f, _ := strconv.ParseFloat(t, 64)
		return f
	case json.Number:
		f, _ := t.Float64()
		return f
	default:
		return 0
	}
}
