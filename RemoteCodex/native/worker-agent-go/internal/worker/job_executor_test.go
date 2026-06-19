package worker

import (
	"context"
	"testing"

	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/config"
	"velox-worker-agent/pkg/logger"
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

func TestHealthCheckJobTypeConstant(t *testing.T) {
	job := &api.Job{JobType: "health_check"}
	if job.JobType != "health_check" {
		t.Fatalf("expected job type health_check, got %s", job.JobType)
	}
}

func TestRunJobTaskHealthCheck(t *testing.T) {
	w := &Worker{
		config: &config.WorkerConfig{
			WorkerID:   "test-worker-001",
			WorkerName: "test-worker",
		},
		logger: logger.New(logger.InfoLevel, nil),
	}

	job := &api.Job{
		JobID:    "job-hc-001",
		JobType:  "health_check",
		Priority: 10,
	}

	result, err := w.runJobTask(context.Background(), job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if status, ok := result["status"].(string); !ok || status != "healthy" {
		t.Fatalf("expected status=healthy, got %v", result["status"])
	}
	if wid, ok := result["worker_id"].(string); !ok || wid != "test-worker-001" {
		t.Fatalf("expected worker_id=test-worker-001, got %v", result["worker_id"])
	}
}

func TestRunJobTaskHealthCheckNoUpload(t *testing.T) {
	job := &api.Job{JobType: "health_check"}
	output := map[string]interface{}{"status": "healthy", "worker_id": "w1"}
	if shouldUploadCompletedVideo(job, output) {
		t.Fatal("health_check should never trigger upload")
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
