// Video Metadata facade methods for the YouTube Service.
//
// PR-YT-SVC-SPLIT: this file hosts the Video Metadata facade methods
// that were previously declared inline in service.go under the
// "Public API: Video Metadata (Delegated to VideoManager)" section.
// They are pure delegators to s.videoManager (defined in video.go) and
// are extracted only so service.go can stay focused on its
// construction concerns. No behaviour change.
//
// Note on the import alias: this file is `package youtube`, but
// references `*youtube.Video` to mean the v3 API type from
// "google.golang.org/api/youtube/v3" (whose default package name is
// also `youtube`). Go resolves `youtube.X` in this file's body to the
// imported v3 package, not to the local package — which is standard
// Go behaviour and matches the original service.go.
package youtube

import (
	"context"

	"google.golang.org/api/youtube/v3"
)

// --- Public API: Video Metadata (Delegated to VideoManager) ---

func (s *Service) UpdateVideoMetadata(ctx context.Context, channelID string, videoID string, config UploadConfig) error {
	return s.videoManager.UpdateVideoMetadata(ctx, channelID, videoID, config)
}

func (s *Service) DeleteVideo(ctx context.Context, channelID string, videoID string) error {
	return s.videoManager.DeleteVideo(ctx, channelID, videoID)
}

func (s *Service) ListVideos(ctx context.Context, channelID string, maxResults int64) ([]*youtube.Video, error) {
	return s.videoManager.ListVideos(ctx, channelID, maxResults)
}
