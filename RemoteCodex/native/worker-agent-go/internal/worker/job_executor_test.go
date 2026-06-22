package worker

import (
	"context"
	"testing"

	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
)

func TestShouldUploadCompletedVideoRequiresPathForVideoJobs(t *testing.T) {
	job := &api.Job{JobType: "process_video"}
	if shouldUploadCompletedVideo(job, map[string]interface{}{"status": "completed"}) {
		t.Fatal("expected video jobs without output path to skip upload")
	}

	if !shouldUploadCompletedVideo(job, map[string]interface{}{"output_path": "/tmp/out.mp4"}) {
		t.Fatal("expected video jobs with output path to upload")
	}
}

func TestShouldUploadCompletedVideoSkipsNilJob(t *testing.T) {
	if shouldUploadCompletedVideo(nil, nil) {
		t.Fatal("expected nil job to skip upload")
	}
}

func TestShouldUploadCompletedVideoSkipsRenderJobsWithoutPath(t *testing.T) {
	job := &api.Job{JobType: "render"}
	if shouldUploadCompletedVideo(job, map[string]interface{}{"status": "completed"}) {
		t.Fatal("expected render jobs without output path to skip upload")
	}
}

func TestShouldUploadCompletedVideoSkipsAudioJobs(t *testing.T) {
	job := &api.Job{JobType: "process_audio"}
	if shouldUploadCompletedVideo(job, map[string]interface{}{"status": "completed"}) {
		t.Fatal("expected audio jobs without output path to skip upload")
	}

	if !shouldUploadCompletedVideo(job, map[string]interface{}{"output_path": "/tmp/out.mp3"}) {
		t.Fatal("expected audio jobs with output path to upload")
	}
}

func TestRunJobTaskUnknownType(t *testing.T) {
	w := &Worker{
		config: &config.WorkerConfig{WorkerID: "w1"},
		logger: logger.New(logger.InfoLevel, nil),
	}
	job := &api.Job{JobID: "j1", JobType: "bogus_type"}
	_, err := w.runJobTask(context.Background(), job)
	if err == nil {
		t.Fatal("expected error for unknown job type")
	}
}
