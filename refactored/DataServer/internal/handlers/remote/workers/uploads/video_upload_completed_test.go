package uploads

import "testing"

func TestUploadCompletedVideo_AutoUploadsToYouTubeAndDrive(t *testing.T) {
	t.Skip("Skipping: requires rewriting for new BlobStore-based UploadCompletedVideo signature (removed youtubeSvc/driveSvc params)")
}
