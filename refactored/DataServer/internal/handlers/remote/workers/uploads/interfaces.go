package uploads

import (
	"context"

	driveapi "velox-server/internal/integrations/drive"
	ytservice "velox-server/internal/integrations/youtube"
)

// YouTubeAutoUploader captures the subset of the YouTube service used by the
// completed-video upload flow. Keeping this local to the uploads package makes
// the final handoff testable without real Google credentials.
type YouTubeAutoUploader interface {
	ResolveChannelByLanguage(groupName, language string) (*ytservice.AuthChannel, error)
	HealthCheck(ctx context.Context, channelID string) (map[string]interface{}, error)
	UploadVideo(ctx context.Context, channelID string, videoPath string, config ytservice.UploadConfig) (*ytservice.UploadResult, error)
}

// DriveAutoUploader captures the subset of the Drive service used by the
// completed-video upload flow.
type DriveAutoUploader interface {
	UploadVideo(ctx context.Context, filePath string, projectName string, parentFolderID string) (*driveapi.UploadResult, error)
}
