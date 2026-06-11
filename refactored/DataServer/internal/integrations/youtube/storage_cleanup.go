package youtube

import "time"

// CleanupOldData removes cached channel metadata that is older than the retention period.
// This is required to comply with YouTube's data retention policies (max 13 days).
// Purges ALL YouTube API-derived fields: Title, Thumbnail, ViewCount, SubCount, Keywords.
func (s *Storage) CleanupOldData(retention time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	removedCount := 0

	// We don't remove the channel itself from the group (to keep the tracking link),
	// but we purge ALL "YouTube API Content" fields if not synced recently.
	for _, group := range s.data.Groups {
		for i := range group.Channels {
			ch := &group.Channels[i]
			if !ch.LastSync.IsZero() && now.Sub(ch.LastSync) > retention {
				// Purge ALL YouTube API-derived content to comply with data retention policies
				ch.Title = ""
				ch.Thumbnail = ""
				ch.ViewCount = 0
				ch.SubCount = 0
				ch.Keywords = nil
				removedCount++
			}
		}
	}

	if removedCount > 0 {
		s.save()
	}
	return removedCount
}

// ClearCache invalidates any cached data (for compatibility with Python)
func (s *Storage) ClearCache() {
	// No-op for now since we don't cache reads
	// In future, this could clear feed cache
}
