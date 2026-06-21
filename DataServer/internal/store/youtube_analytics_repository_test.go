package store

import (
	"context"
	"testing"
)

func TestYouTubeAnalyticsRepository_SaveAnalyticsCache(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		entry   AnalyticsCacheEntry
		wantErr bool
	}{
		{
			name: "insert new cache entry",
			entry: AnalyticsCacheEntry{
				CacheKey:   "quota:UC123",
				TS:         1234567890,
				DataJSON:   `{"quota_remaining": 100}`,
				MigratedAt: "2024-01-01T00:00:00Z",
			},
		},
		{
			name: "upsert existing cache entry updates ts and data",
			entry: AnalyticsCacheEntry{
				CacheKey:   "quota:UC123",
				TS:         9999999999,
				DataJSON:   `{"quota_remaining": 50}`,
				MigratedAt: "2024-06-01T00:00:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestDB(t)
			defer s.Close()
			repo := NewSQLiteYouTubeAnalyticsRepository(s)

			err := repo.SaveAnalyticsCache(ctx, tt.entry)
			if (err != nil) != tt.wantErr {
				t.Fatalf("SaveAnalyticsCache error = %v, wantErr %v", err, tt.wantErr)
			}

			var ts float64
			var dataJSON, migratedAt string
			row := s.db.QueryRowContext(ctx,
				`SELECT ts, data_json, migrated_at FROM analytics_cache WHERE cache_key = ?`,
				tt.entry.CacheKey)
			if err := row.Scan(&ts, &dataJSON, &migratedAt); err != nil {
				t.Fatalf("read back: %v", err)
			}
			if ts != tt.entry.TS {
				t.Errorf("ts = %v, want %v", ts, tt.entry.TS)
			}
			if dataJSON != tt.entry.DataJSON {
				t.Errorf("dataJSON = %q, want %q", dataJSON, tt.entry.DataJSON)
			}
			if migratedAt != tt.entry.MigratedAt {
				t.Errorf("migratedAt = %q, want %q", migratedAt, tt.entry.MigratedAt)
			}
		})
	}
}

func TestYouTubeAnalyticsRepository_SaveRevenueMetrics(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		metrics []RevenueMetric
		wantErr bool
	}{
		{
			name: "insert single metric",
			metrics: []RevenueMetric{
				{ChannelID: "UC123", Date: "2024-01-01", Revenue: 123.45, Currency: "USD", Views: 1000},
			},
		},
		{
			name: "upsert same key updates values",
			metrics: []RevenueMetric{
				{ChannelID: "UC123", Date: "2024-01-01", Revenue: 200.00, Currency: "USD", Views: 2000},
			},
		},
		{
			name: "insert multiple metrics",
			metrics: []RevenueMetric{
				{ChannelID: "UC123", Date: "2024-01-02", Revenue: 50.00, Currency: "USD", Views: 500},
				{ChannelID: "UC456", Date: "2024-01-01", Revenue: 75.00, Currency: "EUR", Views: 750},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestDB(t)
			defer s.Close()
			repo := NewSQLiteYouTubeAnalyticsRepository(s)

			err := repo.SaveRevenueMetrics(ctx, tt.metrics)
			if (err != nil) != tt.wantErr {
				t.Fatalf("SaveRevenueMetrics error = %v, wantErr %v", err, tt.wantErr)
			}

			last := tt.metrics[len(tt.metrics)-1]
			var revenue float64
			var currency string
			var views int64
			row := s.db.QueryRowContext(ctx,
				`SELECT estimated_revenue, currency, views FROM youtube_revenue_metrics WHERE channel_id = ? AND date = ?`,
				last.ChannelID, last.Date)
			if err := row.Scan(&revenue, &currency, &views); err != nil {
				t.Fatalf("read back: %v", err)
			}
			if revenue != last.Revenue {
				t.Errorf("revenue = %v, want %v", revenue, last.Revenue)
			}
			if currency != last.Currency {
				t.Errorf("currency = %q, want %q", currency, last.Currency)
			}
			if views != last.Views {
				t.Errorf("views = %v, want %v", views, last.Views)
			}
		})
	}
}

func TestYouTubeAnalyticsRepository_SaveVideoMetrics(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		metrics []VideoMetric
		wantErr bool
	}{
		{
			name: "insert single metric",
			metrics: []VideoMetric{
				{VideoID: "vid1", ChannelID: "UC123", Date: "2024-01-01", Title: "Test", ThumbnailURL: "https://img/1.jpg", Views: 100, Revenue: 10.50},
			},
		},
		{
			name: "upsert updates views and revenue and title",
			metrics: []VideoMetric{
				{VideoID: "vid1", ChannelID: "UC123", Date: "2024-01-01", Title: "Test Updated", ThumbnailURL: "https://img/2.jpg", Views: 200, Revenue: 20.00},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestDB(t)
			defer s.Close()
			repo := NewSQLiteYouTubeAnalyticsRepository(s)

			err := repo.SaveVideoMetrics(ctx, tt.metrics)
			if (err != nil) != tt.wantErr {
				t.Fatalf("SaveVideoMetrics error = %v, wantErr %v", err, tt.wantErr)
			}

			last := tt.metrics[len(tt.metrics)-1]
			var views int64
			var revenue float64
			var title string
			row := s.db.QueryRowContext(ctx,
				`SELECT views, revenue, title FROM youtube_video_metrics WHERE video_id = ? AND date = ?`,
				last.VideoID, last.Date)
			if err := row.Scan(&views, &revenue, &title); err != nil {
				t.Fatalf("read back: %v", err)
			}
			if views != last.Views {
				t.Errorf("views = %v, want %v", views, last.Views)
			}
			if revenue != last.Revenue {
				t.Errorf("revenue = %v, want %v", revenue, last.Revenue)
			}
			if title != last.Title {
				t.Errorf("title = %q, want %q", title, last.Title)
			}
		})
	}
}
