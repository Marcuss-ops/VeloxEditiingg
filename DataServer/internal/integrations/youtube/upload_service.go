// Upload facade methods for the YouTube Service.
//
// PR-YT-SVC-SPLIT: this file hosts the Upload facade methods that were
// previously declared inline in service.go under the
// "Public API: Upload (Delegated to Uploader)" section. They are pure
// delegators to s.uploader (defined in upload.go) and are extracted
// only so service.go can stay focused on its construction concerns.
// No behaviour change.
package youtube

import (
	"context"
)

// --- Public API: Upload (Delegated to Uploader) ---

func (s *Service) UploadVideo(ctx context.Context, channelID string, videoPath string, config UploadConfig) (*UploadResult, error) {
	return s.uploader.UploadVideo(ctx, channelID, videoPath, config)
}

func (s *Service) SetThumbnail(ctx context.Context, channelID string, videoID string, thumbnailPath string) (string, error) {
	return s.uploader.SetThumbnail(ctx, channelID, videoID, thumbnailPath)
}
