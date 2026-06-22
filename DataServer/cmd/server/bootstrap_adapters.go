package main

import (
	"context"
	"encoding/json"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
)

// writerAdapter wraps a jobs.Writer to satisfy enqueue.JobQueue.
//
// PR-04.5: SubmitJob gains a costmodel.JobRequirements parameter and
// forwards it onto jobs.Job.Requirements so the canonical jobs.Writer.Create
// path can persist them in dedicated columns + the request_json._requirements
// sub-object. Bootstrap callers that have not yet decided on Requirements
// pass costmodel.DefaultRequirements() (preserves today's FIFO routing).
type writerAdapter struct {
	w jobs.Writer
}

func (a *writerAdapter) SubmitJob(ctx context.Context, jobID string, payload map[string]interface{}, req costmodel.JobRequirements) error {
	var videoName, projectID, runID string
	if s, ok := payload["video_name"].(string); ok {
		videoName = s
	}
	if s, ok := payload["project_id"].(string); ok {
		projectID = s
	}
	if s, ok := payload["job_run_id"].(string); ok && s != "" {
		runID = s
	} else if s, ok := payload["run_id"].(string); ok && s != "" {
		runID = s
	}
	raw, _ := json.Marshal(payload)
	job := &jobs.Job{
		ID:           jobID,
		Status:       jobs.StatusPending,
		VideoName:    videoName,
		ProjectID:    projectID,
		RunID:        runID,
		MaxRetries:   3,
		Payload:      string(raw),
		Requirements: req,
	}
	return a.w.Create(ctx, job)
}
