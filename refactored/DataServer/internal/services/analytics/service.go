package analytics

import (
	"context"

	"velox-server/internal/store"
)

// AnalyticsService defines the interface for analytics business logic
type AnalyticsService interface {
	// GetJobCounts returns job statistics
	GetJobCounts(ctx context.Context) (map[string]int64, error)

	// GetAnalyticsTotals returns aggregated analytics for a period
	GetAnalyticsTotals(period string) (map[string]any, error)

	// GetAnalyticsCache returns cached analytics data
	GetAnalyticsCache(cacheKey string) (map[string]any, error)

	// GetTopVideos returns top performing videos
	GetTopVideos(limit int, period string) ([]store.VideoStat, error)

	// GetTopChannels returns top performing channels
	GetTopChannels(limit int, period string) ([]store.ChannelStat, error)

	// GetDailyStats returns daily statistics
	GetDailyStats(days int) ([]store.DailyStat, error)
}

// analyticsService implements AnalyticsService
type analyticsService struct {
	store *store.SQLiteStore
}

// NewAnalyticsService creates a new analytics service
func NewAnalyticsService(store *store.SQLiteStore) AnalyticsService {
	return &analyticsService{
		store: store,
	}
}

// GetJobCounts returns job statistics from the store
func (s *analyticsService) GetJobCounts(ctx context.Context) (map[string]int64, error) {
	if s.store == nil {
		return map[string]int64{
			"total":      0,
			"pending":    0,
			"processing": 0,
			"completed":  0,
			"error":      0,
		}, nil
	}
	return s.store.JobCounts(ctx)
}

// GetAnalyticsTotals returns aggregated analytics for a period
func (s *analyticsService) GetAnalyticsTotals(period string) (map[string]any, error) {
	if s.store == nil {
		return map[string]any{
			"views":    0,
			"revenue":  0.0,
			"videos":   0,
			"channels": 0,
		}, nil
	}
	return s.store.GetAnalyticsTotals(period)
}

// GetAnalyticsCache returns cached analytics data
func (s *analyticsService) GetAnalyticsCache(cacheKey string) (map[string]any, error) {
	if s.store == nil {
		return make(map[string]any), nil
	}
	return s.store.GetAnalyticsCache(cacheKey)
}

// GetTopVideos returns top performing videos
func (s *analyticsService) GetTopVideos(limit int, period string) ([]store.VideoStat, error) {
	if s.store == nil {
		return []store.VideoStat{}, nil
	}
	return s.store.GetTopVideos(limit, period)
}

// GetTopChannels returns top performing channels
func (s *analyticsService) GetTopChannels(limit int, period string) ([]store.ChannelStat, error) {
	if s.store == nil {
		return []store.ChannelStat{}, nil
	}
	return s.store.GetTopChannels(limit, period)
}

// GetDailyStats returns daily statistics
func (s *analyticsService) GetDailyStats(days int) ([]store.DailyStat, error) {
	if s.store == nil {
		return []store.DailyStat{}, nil
	}
	return s.store.GetDailyStats(days)
}
