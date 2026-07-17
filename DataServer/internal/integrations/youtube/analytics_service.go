// Quota / Analytics facade methods for the YouTube Service.
//
// PR-YT-SVC-SPLIT: this file hosts the Quota / Analytics facade
// methods that were previously declared inline in service.go under
// the "Public API: Quota/Analytics (Delegated to QuotaManager)"
// section. They are pure delegators to s.quotaManager (defined in
// quota.go / analytics.go) and are extracted only so service.go can
// stay focused on its construction concerns. No behaviour change.
//
// ytanalytics alias matches the original service.go so existing
// call-sites / future debug references to *ytanalytics.Service keep
// resolving identically.
package youtube

import (
	"context"

	ytanalytics "google.golang.org/api/youtubeanalytics/v2"
)

// --- Public API: Quota/Analytics (Delegated to QuotaManager) ---

func (s *Service) GetQuotaUsage(ctx context.Context) map[string]interface{} {
	return s.quotaManager.GetQuotaUsage(ctx)
}

func (s *Service) GetAnalyticsService(ctx context.Context, channelID string) (*ytanalytics.Service, error) {
	return s.quotaManager.GetAnalyticsService(ctx, channelID)
}

func (s *Service) FetchAnalytics(ctx context.Context, channelID string, days int) (map[string]interface{}, error) {
	return s.quotaManager.FetchAnalytics(ctx, channelID, days)
}

func (s *Service) UpdateAnalyticsCache(ctx context.Context, channelID string, days int, data map[string]interface{}) error {
	return s.quotaManager.UpdateAnalyticsCache(ctx, channelID, days, data)
}
