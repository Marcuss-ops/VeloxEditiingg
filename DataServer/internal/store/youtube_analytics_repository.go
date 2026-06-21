package store

import (
	"context"
	"fmt"
)

// AnalyticsCacheEntry is one row for the analytics_cache table.
type AnalyticsCacheEntry struct {
	CacheKey   string
	TS         float64
	DataJSON   string
	MigratedAt string
}

// RevenueMetric is one row for the youtube_revenue_metrics table.
type RevenueMetric struct {
	ChannelID string
	Date      string
	Revenue   float64
	Currency  string
	Views     int64
}

// VideoMetric is one row for the youtube_video_metrics table.
type VideoMetric struct {
	VideoID      string
	ChannelID    string
	Date         string
	Title        string
	ThumbnailURL string
	Views        int64
	Revenue      float64
}

// YouTubeAnalyticsRepository is the persistence contract for YouTube
// analytics data (quota cache, revenue metrics, video metrics).
type YouTubeAnalyticsRepository interface {
	SaveAnalyticsCache(ctx context.Context, entry AnalyticsCacheEntry) error
	SaveRevenueMetrics(ctx context.Context, metrics []RevenueMetric) error
	SaveVideoMetrics(ctx context.Context, metrics []VideoMetric) error
}

// SQLiteYouTubeAnalyticsRepository implements YouTubeAnalyticsRepository
// against a SQLiteStore.
type SQLiteYouTubeAnalyticsRepository struct {
	store *SQLiteStore
}

// NewSQLiteYouTubeAnalyticsRepository creates a repository backed by store.
func NewSQLiteYouTubeAnalyticsRepository(store *SQLiteStore) *SQLiteYouTubeAnalyticsRepository {
	return &SQLiteYouTubeAnalyticsRepository{store: store}
}

// Compile-time check.
var _ YouTubeAnalyticsRepository = (*SQLiteYouTubeAnalyticsRepository)(nil)

// SaveAnalyticsCache inserts or replaces one analytics_cache row.
func (r *SQLiteYouTubeAnalyticsRepository) SaveAnalyticsCache(ctx context.Context, entry AnalyticsCacheEntry) error {
	_, err := r.store.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO analytics_cache (cache_key, ts, data_json, migrated_at)
		 VALUES (?, ?, ?, ?)`,
		entry.CacheKey, entry.TS, entry.DataJSON, entry.MigratedAt)
	if err != nil {
		return fmt.Errorf("youtube analytics: SaveAnalyticsCache: %w", err)
	}
	return nil
}

// SaveRevenueMetrics upserts revenue metrics (one Exec per row).
func (r *SQLiteYouTubeAnalyticsRepository) SaveRevenueMetrics(ctx context.Context, metrics []RevenueMetric) error {
	for _, m := range metrics {
		_, err := r.store.db.ExecContext(ctx,
			`INSERT INTO youtube_revenue_metrics (channel_id, date, estimated_revenue, currency, views)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(channel_id, date) DO UPDATE SET
			   estimated_revenue=excluded.estimated_revenue,
			   views=excluded.views`,
			m.ChannelID, m.Date, m.Revenue, m.Currency, m.Views)
		if err != nil {
			return fmt.Errorf("youtube analytics: SaveRevenueMetrics: %w", err)
		}
	}
	return nil
}

// SaveVideoMetrics upserts video metrics (one Exec per row).
func (r *SQLiteYouTubeAnalyticsRepository) SaveVideoMetrics(ctx context.Context, metrics []VideoMetric) error {
	for _, m := range metrics {
		_, err := r.store.db.ExecContext(ctx,
			`INSERT INTO youtube_video_metrics (video_id, channel_id, date, title, thumbnail_url, views, revenue)
			 VALUES (?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(video_id, date) DO UPDATE SET
			   views=excluded.views,
			   revenue=excluded.revenue,
			   title=excluded.title,
			   thumbnail_url=excluded.thumbnail_url`,
			m.VideoID, m.ChannelID, m.Date, m.Title, m.ThumbnailURL, m.Views, m.Revenue)
		if err != nil {
			return fmt.Errorf("youtube analytics: SaveVideoMetrics: %w", err)
		}
	}
	return nil
}
