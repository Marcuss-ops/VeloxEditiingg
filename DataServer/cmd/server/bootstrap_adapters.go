package main

import (
	"context"
	"encoding/json"

	"velox-server/internal/jobs"
	"velox-server/internal/outbox"
	"velox-server/internal/workflow"
)

// writerAdapter wraps a jobs.Writer to satisfy enqueue.JobQueue.
type writerAdapter struct {
	w jobs.Writer
}

func (a *writerAdapter) SubmitJob(ctx context.Context, jobID string, payload map[string]interface{}) error {
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
		ID:         jobID,
		Status:     jobs.StatusPending,
		VideoName:  videoName,
		ProjectID:  projectID,
		RunID:      runID,
		MaxRetries: 3,
		Payload:    string(raw),
	}
	return a.w.Create(ctx, job)
}

// outboxWorkflowAdapter adapts *outbox.Store to the workflow.OutboxWriter interface.
type outboxWorkflowAdapter struct {
	store *outbox.Store
}

func (a *outboxWorkflowAdapter) Enqueue(ctx context.Context, ev workflow.WorkflowOutboxEvent) error {
	_, err := a.store.Insert(ctx, nil, outbox.InsertParams{
		AggregateType: "workflow",
		AggregateID:   ev.AggregateID,
		EventType:     ev.EventType,
		Payload:       ev.Payload,
	})
	return err
}
