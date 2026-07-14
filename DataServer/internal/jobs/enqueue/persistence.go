// Package enqueue — persistence.go (Stage 3 of the enqueue split).
//
// compileSceneVideoJob is the third phase of the Enqueuer's pipeline:
// it converts a *normalized* scene-video payload into the canonical
// (*jobs.Job, *taskgraph.TaskSpec, priority) triple the orchestrator
// (enqueue.go) hands to the atomic-creation path. The caller still
// owns the insert; this file just owns canonical-entity construction.
//
// Filename note: "persistence.go" is the Stage-3 routing label per the
// §15.6 + §16.7 forward-map re-routing. It is NOT a claim that
// compileSceneVideoJob performs I/O — see docs/metrics/loc-baseline.md
// §17 for the R2 delta. Pairs with the §15.7 "smaller atomic step" rule.
//
// Imports mirror the EXACT set compileSceneVideoJob references, derived
// from the canonical pre-extraction body (git HEAD's lines 234-278 of
// enqueue.go). The import block is also a defence against drift: if the
// body ever grows new symbol references, the build will surface an
// unimported-symbol error before the import block silently rots.
package enqueue

import (
	"encoding/json"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
	"velox-shared/payload"
)

// compileSceneVideoJob builds a canonical *jobs.Job and *taskgraph.TaskSpec
// from a normalized scene-video payload. The caller owns the atomic creation.
func compileSceneVideoJob(normalized map[string]interface{}, req costmodel.JobRequirements) (*jobs.Job, *taskgraph.TaskSpec, int) {
	jobID, _ := normalized["job_id"].(string)
	videoName, _ := normalized["video_name"].(string)
	projectID, _ := normalized["project_id"].(string)
	jobRunID, _ := normalized["job_run_id"].(string)
	if jobRunID == "" {
		jobRunID, _ = normalized["run_id"].(string)
	}
	jobType, _ := normalized["job_type"].(string)
	if jobType == "" {
		jobType = "process_video"
	}
	priority := payload.EnsureInt(normalized["priority"], 5)

	raw, _ := json.Marshal(normalized)

	job := &jobs.Job{
		ID:        jobID,
		Type:      jobType,
		Status:    jobs.StatusPending,
		VideoName: videoName,
		ProjectID: projectID,
		RunID:     jobRunID,
		// MaxRetries is set by extractPlanMaxRetry (single writer on
		// the insert path). Left at 0 here so the owner is explicit.
		MaxRetries:   0,
		Payload:      string(raw),
		Requirements: req,
	}

	executorID := "scene.composite.v1"
	if resolved := resolveInternalExecutorID(normalized); resolved != "" {
		executorID = resolved
	}

	spec := &taskgraph.TaskSpec{
		Version:              taskgraph.SpecVersion,
		JobID:                jobID,
		ExecutorID:           executorID,
		Payload:              normalized,
		RequiredCapabilities: resolveRequiredCapabilities(executorID),
	}

	return job, spec, priority
}
