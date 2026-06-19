package youtube

import (
	"context"
	"log"
)

// SyncAllAnalytics fetches analytics for all authorized channels
func (s *Service) SyncAllAnalytics(ctx context.Context) error {
	channels := s.GetAuthChannels()
	if len(channels) == 0 {
		return nil
	}

	log.Printf("[YT] YouTube: Syncing analytics for %d channels...", len(channels))
	successCount := 0

	for _, ch := range channels {
		data, err := s.quotaManager.FetchAnalytics(ctx, ch.ID, 30)
		if err != nil {
			log.Printf("[WARN] YouTube: Failed to fetch channel analytics for %s: %v", ch.ID, err)
			continue
		}

		if err := s.quotaManager.UpdateAnalyticsCache(ctx, ch.ID, 30, data); err != nil {
			log.Printf("[WARN] YouTube: Failed to update channel cache for %s: %v", ch.ID, err)
		}

		videoData, err := s.quotaManager.FetchVideoAnalytics(ctx, ch.ID, 30)
		if err != nil {
			log.Printf("[WARN] YouTube: Failed to fetch video analytics for %s: %v", ch.ID, err)
		} else {
			if err := s.quotaManager.UpdateVideoAnalyticsCache(ctx, ch.ID, videoData); err != nil {
				log.Printf("[WARN] YouTube: Failed to update video cache for %s: %v", ch.ID, err)
			}
		}

		successCount++
	}

	log.Printf("[OK] YouTube: Analytics sync complete (%d/%d successful)", successCount, len(channels))
	return nil
}
