package worker

import (
	"testing"

	"velox-worker-agent/pkg/api"
)

func TestShouldUploadCompletedVideoSkipsHealthCheck(t *testing.T) {
	job := &api.Job{JobType: "health_check"}
	if shouldUploadCompletedVideo(job, map[string]interface{}{"status": "healthy"}) {
		t.Fatal("expected health_check jobs to skip upload")
	}
}

func TestShouldUploadCompletedVideoRequiresPathForVideoJobs(t *testing.T) {
	job := &api.Job{JobType: "process_video"}
	if shouldUploadCompletedVideo(job, map[string]interface{}{"status": "completed"}) {
		t.Fatal("expected video jobs without output path to skip upload")
	}

	if !shouldUploadCompletedVideo(job, map[string]interface{}{"output_path": "/tmp/out.mp4"}) {
		t.Fatal("expected video jobs with output path to upload")
	}
}
