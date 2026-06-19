package store

import (
	"database/sql"
	"time"
)

// YouTubeChannelMetric represents a snapshot of channel statistics
type YouTubeChannelMetric struct {
	ChannelID       string    `json:"channel_id"`
	Timestamp       time.Time `json:"timestamp"`
	SubscriberCount int64     `json:"subscriber_count"`
	ViewCount       int64     `json:"view_count"`
	VideoCount      int64     `json:"video_count"`
}

// YouTubeRevenueMetric represents daily revenue for a channel
type YouTubeRevenueMetric struct {
	ChannelID        string    `json:"channel_id"`
	Date             time.Time `json:"date"`
	EstimatedRevenue float64   `json:"estimated_revenue"`
	Currency         string    `json:"currency"`
	Views            int64     `json:"views"`
}

// YouTubeVideoMetric represents a snapshot of video performance
type YouTubeVideoMetric struct {
	VideoID      string  `json:"video_id"`
	ChannelID    string  `json:"channel_id"`
	Date         string  `json:"date"` // YYYY-MM-DD
	Title        string  `json:"title"`
	ThumbnailURL string  `json:"thumbnail_url"`
	Views        int64   `json:"views"`
	Revenue      float64 `json:"revenue"`
}

// initYouTubeSchema has been migrated to migrations/001_initial.sql.
// DDL is no longer inlined here — schema is managed by the migration runner.

// TrackQuotaUsage increments the units used for today
func (s *SQLiteStore) TrackQuotaUsage(units int) error {
	today := time.Now().Format("2006-01-02")
	_, err := s.db.Exec(
		`INSERT INTO youtube_quota_usage (date, units_used)
		 VALUES (?, ?)
		 ON CONFLICT(date) DO UPDATE SET units_used = units_used + excluded.units_used`,
		today, units,
	)
	return err
}

// GetDailyQuotaUsage returns units used today
func (s *SQLiteStore) GetDailyQuotaUsage() (int, error) {
	today := time.Now().Format("2006-01-02")
	var units int
	err := s.db.QueryRow("SELECT units_used FROM youtube_quota_usage WHERE date = ?", today).Scan(&units)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return units, err
}

// SaveYouTubeChannelMetric saves a channel's statistics snapshot
func (s *SQLiteStore) SaveYouTubeChannelMetric(metric YouTubeChannelMetric) error {
	ts := metric.Timestamp.UTC().Format(time.RFC3339)
	_, err := s.db.Exec(
		`INSERT INTO youtube_channel_metrics (channel_id, ts, subscriber_count, view_count, video_count)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(channel_id, ts) DO UPDATE SET
		   subscriber_count=excluded.subscriber_count,
		   view_count=excluded.view_count,
		   video_count=excluded.video_count`,
		metric.ChannelID, ts, metric.SubscriberCount, metric.ViewCount, metric.VideoCount,
	)
	return err
}

// SaveYouTubeRevenueMetric saves a channel's daily revenue
func (s *SQLiteStore) SaveYouTubeRevenueMetric(metric YouTubeRevenueMetric) error {
	dateStr := metric.Date.Format("2006-01-02")
	_, err := s.db.Exec(
		`INSERT INTO youtube_revenue_metrics (channel_id, date, estimated_revenue, currency, views)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(channel_id, date) DO UPDATE SET
		   estimated_revenue=excluded.estimated_revenue,
		   currency=excluded.currency,
		   views=excluded.views`,
		metric.ChannelID, dateStr, metric.EstimatedRevenue, metric.Currency, metric.Views,
	)
	return err
}

// SaveYouTubeVideoMetric saves a video performance snapshot
func (s *SQLiteStore) SaveYouTubeVideoMetric(m YouTubeVideoMetric) error {
	_, err := s.db.Exec(
		`INSERT INTO youtube_video_metrics (video_id, channel_id, date, title, thumbnail_url, views, revenue)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(video_id, date) DO UPDATE SET
		   views=excluded.views,
		   revenue=excluded.revenue,
		   title=excluded.title,
		   thumbnail_url=excluded.thumbnail_url`,
		m.VideoID, m.ChannelID, m.Date, m.Title, m.ThumbnailURL, m.Views, m.Revenue,
	)
	return err
}

// GetTopVideosFromDB returns top videos based on views in a period
func (s *SQLiteStore) GetTopVideosFromDB(days int, limit int) ([]VideoStat, error) {
	cutoff := time.Now().AddDate(0, 0, -days).Format("2006-01-02")
	rows, err := s.db.Query(
		`SELECT video_id, title, thumbnail_url, SUM(views) as total_views, SUM(revenue) as total_revenue
		 FROM youtube_video_metrics
		 WHERE date >= ?
		 GROUP BY video_id
		 ORDER BY total_views DESC
		 LIMIT ?`,
		cutoff, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var videos []VideoStat
	for rows.Next() {
		var v VideoStat
		if err := rows.Scan(&v.VideoID, &v.Title, &v.ThumbnailURL, &v.Views30d, &v.Revenue); err != nil {
			continue
		}
		videos = append(videos, v)
	}
	return videos, nil
}

// GetYouTubeMoMStats calculates MoM performance
func (s *SQLiteStore) GetYouTubeMoMStats() (currentMonth, prevMonth DailyStat, err error) {
	now := time.Now()
	currentMonthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
	prevMonthStart := time.Date(now.Year(), now.Month()-1, 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
	prevMonthEnd := time.Date(now.Year(), now.Month(), 0, 23, 59, 59, 0, time.UTC).Format("2006-01-02")

	// Current Month
	err = s.db.QueryRow(
		`SELECT SUM(views), SUM(estimated_revenue) FROM youtube_revenue_metrics WHERE date >= ?`,
		currentMonthStart,
	).Scan(&currentMonth.Views, &currentMonth.Revenue)
	if err != nil && err != sql.ErrNoRows {
		return
	}

	// Previous Month
	err = s.db.QueryRow(
		`SELECT SUM(views), SUM(estimated_revenue) FROM youtube_revenue_metrics WHERE date >= ? AND date <= ?`,
		prevMonthStart, prevMonthEnd,
	).Scan(&prevMonth.Views, &prevMonth.Revenue)
	if err == sql.ErrNoRows {
		err = nil
	}
	return
}

// GetYouTubeHistoricalStats returns daily stats for a period
func (s *SQLiteStore) GetYouTubeHistoricalStats(days int) ([]DailyStat, error) {
	cutoff := time.Now().AddDate(0, 0, -days).Format("2006-01-02")
	rows, err := s.db.Query(
		`SELECT date, SUM(views), SUM(estimated_revenue)
		 FROM youtube_revenue_metrics
		 WHERE date >= ?
		 GROUP BY date
		 ORDER BY date ASC`,
		cutoff,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats []DailyStat
	for rows.Next() {
		var ds DailyStat
		if err := rows.Scan(&ds.Date, &ds.Views, &ds.Revenue); err != nil {
			continue
		}
		stats = append(stats, ds)
	}
	return stats, nil
}
