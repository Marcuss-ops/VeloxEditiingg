// COMPATIBILITY:
// Owner:        PR #8 (workflow package removed; this adapter uses taskgraph/jobs only)
// Remove after: 2026-12-31
// Read-only:    yes (POST writes go to taskgraph/jobs/tasks/task_specs only) — see OWNERSHIP.md "Legacy Workflow v2 state (DECOMMISSIONING)"
//
// orchestrator_legacy_adapter.go
//
// This file hosts the /api/v1/orchestrator/* HTTP handlers during the
// PR-operation 01 cutover from `workflow.Repository` to `creatorflow` +
// `taskgraph.Repository`. The split is intentional: the file is self-contained,
// so Fase 8 (package removal) can delete it in a single commit without
// touching router.go or bootstrap.go.
//
// POST /api/v1/orchestrator/jobs :
//   - accepts a JSON body shaped as creatorflow.RenderPlan (typed at the edge)
//   - delegates to creatorflow.CreateJobWithPlan, which writes Job + Task
//     + TaskSpec atomically via store.AtomicJobTaskCreator
//   - does NOT touch workflow_runs / workflow_steps / workflow_dependencies
//     / workflow_events any more — see Fase 3 runbook Criteri di Completamento.
//
// GET /api/v1/orchestrator/jobs/:id :
//   - read-only adapter that fetches jobs.Job + taskgraph.Task and projects
//     them into the legacy orchestratorv1 DTO shape so existing frontend
//     clients keep working
//   - emits RFC 8594 Deprecation header.
//
// GET /api/v1/orchestrator/jobs :
//   - read-only adapter that lists recent jobs via jobs.Writer and projects
//     each entry into orchestratorv1.LegacyRunResponse.
//
// GET /api/v1/orchestrator/stats :
//   - read-only adapter that bins jobs by status (the legacy StatsReport
//     shape) so dashboards don't break, but the underlying counts come from
//     jobs.Writer, NOT from workflow_runs).

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"velox-server/internal/costmodel"
	"velox-server/internal/creatorflow"
	"velox-server/internal/handlers/server/orchestratorv1"
	"velox-server/internal/jobs"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
)

// orchestratorLegacyAdapter wires the POST cutover entry point and the
// 3 GET read-only projections. It is constructed once per request from
// serverDeps in newRouter.
//
// jobsRepo is jobs.Reader (NOT jobs.Writer) — POST writes go through
// store.AtomicJobTaskCreator; the adapter only needs the read surface
// (Get/List/Counts) plus an idempotency pre-check inside
// creatorflow.CreateJobWithPlan. jobs.Reader is the minimal
// canonical contract for both. Reusing jobs.Reader keeps the adapter
// from introducing a third, parallel interface that would drift from
// the canonical Writer/Reader split the rest of the codebase already
// honours.
type orchestratorLegacyAdapter struct {
	atomicPlanWriter *store.AtomicJobTaskCreator
	jobsRepo         jobs.Reader
	tasksRepo        taskgraph.Reader
}

// newOrchestratorLegacyAdapter constructs the adapter from serverDeps. It
// returns an error if any of the Fase 3 wiring pieces is nil, so the router
// can choose to skip the registrar block vs returning a 500 to the client.
func newOrchestratorLegacyAdapter(d *serverDeps) (*orchestratorLegacyAdapter, error) {
	if d == nil {
		return nil, fmt.Errorf("orchestratorLegacyAdapter: nil serverDeps")
	}
	if d.atomicPlanWriter == nil {
		return nil, fmt.Errorf("orchestratorLegacyAdapter: nil atomicPlanWriter")
	}
	if d.jobsRepo == nil {
		return nil, fmt.Errorf("orchestratorLegacyAdapter: nil jobsRepo")
	}
	if d.tasksRepo == nil {
		return nil, fmt.Errorf("orchestratorLegacyAdapter: nil tasksRepo")
	}
	return &orchestratorLegacyAdapter{
		atomicPlanWriter: d.atomicPlanWriter,
		jobsRepo:         d.jobsRepo,
		tasksRepo:        d.tasksRepo,
	}, nil
}

// orchestratorJobReq is the HTTP-edge JSON shape for POST
// /api/v1/orchestrator/jobs. It mirrors creatorflow.RenderPlan's fields
// directly so the body is fully typed at the edge — no map[string]interface{}
// survives past this struct.
type orchestratorJobReq struct {
	VideoName      string                 `json:"video_name"`
	ProjectID      string                 `json:"project_id"`
	ExecutorID     string                 `json:"executor_id"`
	RunID          string                 `json:"run_id"`
	IdempotencyKey string                 `json:"idempotency_key"`
	MaxRetries     int                    `json:"max_retries"`
	Priority       int                    `json:"priority"`
	Payload        map[string]interface{} `json:"payload"`
}

// postJob is the canonical POST handler. It compiles the typed
// orchestratorJobReq into a creatorflow.RenderPlan and delegates to
// creatorflow.CreateJobWithPlan — which is the ONLY writer-side path
// the runbook permits during the cutover (Fase 3 §"Risultato finale").
//
// Error codes are stable:
//
//   - 400 — input validation failure (missing idempotency_key, RenderPlan
//     validation: empty video_name / executor_id / nil Payload / negative max_retries)
//   - 409 — atomic creator refused the insert (UNIQUE violation, task
//     insert failure with rollback, etc.) OR idempotency-key replay
//     (the canonical job already exists for this idempotency_key —
//     client should pivot to the existing job_id returned in body).
//   - 201 — created (job_id + minimal payload for client UX)
//
// Critical: plan.Validate() is called BEFORE CreateJobWithPlan so that
// validation errors return 400 at the edge, NOT 409. CreateJobWithPlan
// also calls Validate() internally; we re-validate here to keep the HTTP
// error contract stable. The cost is one extra Validate() call —
// RenderPlan validation is sub-microsecond.
//
// Idempotency-replay semantics: CreateJobWithPlan returns
// (jobID, created=false, err=nil) when the idempotency_key already maps
// to an existing Job row. The handler surfaces this as 409 Conflict with
// the existing job_id echoed, so clients can converge on the resource
// without a follow-up GET. This is REST-conventional: an idempotent POST
// that did NOT create a new resource is conflict-class, not success-class.
//
// The "phase" field documents the post-cutover lineage so clients can
// grep their logs.
func (a *orchestratorLegacyAdapter) postJob(c *gin.Context) {
	var req orchestratorJobReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid JSON: " + err.Error()})
		return
	}
	if req.IdempotencyKey == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "idempotency_key required"})
		return
	}
	// Best-effort: if VideoName is empty but the payload carries
	// video_name/title, lift it out. This keeps small client mistakes
	// from dropping the whole submission, while keeping validation
	// strict (missing both fields still errors below).
	if req.VideoName == "" && req.Payload != nil {
		if v, ok := req.Payload["video_name"].(string); ok && v != "" {
			req.VideoName = v
		} else if v, ok := req.Payload["title"].(string); ok && v != "" {
			req.VideoName = v
		}
	}
	plan := creatorflow.RenderPlan{
		VideoName:      req.VideoName,
		ProjectID:      req.ProjectID,
		ExecutorID:     req.ExecutorID,
		RunID:          req.RunID,
		IdempotencyKey: req.IdempotencyKey,
		MaxRetries:     req.MaxRetries,
		Priority:       req.Priority,
		Payload:        req.Payload,
	}
	// Edge-level validation: keep RenderPlan contract failures on 400,
	// atomic-insert conflicts on 409. Without this re-check, partial bad
	// bodies (empty video_name with no payload) would alias into 409
	// at the handler — confusing for HTTP clients who expect 4xx-class
	// semantics for validation, 4xx-conflict only for resource races.
	if err := plan.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid RenderPlan: " + err.Error()})
		return
	}
	jobID, created, err := creatorflow.CreateJobWithPlan(
		c.Request.Context(),
		a.atomicPlanWriter,
		a.jobsRepo,
		plan,
		costmodel.DefaultRequirements(),
	)
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	if !created {
		// Idempotency replay: the canonical writer found an existing
		// Job row for this idempotency_key and short-circuited the
		// atomic insert. 409 + echoed job_id lets clients converge on
		// the canonical resource without a follow-up GET.
		c.JSON(http.StatusConflict, gin.H{
			"error":           "idempotency_key replay",
			"job_id":          jobID,
			"idempotency_key": plan.IdempotencyKey,
			"phase":           "taskgraph-canonical",
			"replay":          true,
		})
		return
	}
	c.JSON(http.StatusCreated, gin.H{
		"job_id":          jobID,
		"workflow_type":   plan.ExecutorID,
		"status":          string(jobs.StatusPending),
		"idempotency_key": plan.IdempotencyKey,
		"created_at":      time.Now().UTC().Format(time.RFC3339),
		"phase":           "taskgraph-canonical",
		"replay":          false,
	})
}

// getJob is the read-only projection of /api/v1/orchestrator/jobs/:id.
// It emits the legacy {run, steps[]} shape via projectRun + projectStep.
func (a *orchestratorLegacyAdapter) getJob(c *gin.Context) {
	c.Header("Deprecation", "true")
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id required"})
		return
	}
	job, err := a.jobsRepo.Get(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if job == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	task, taskErr := a.tasksRepo.GetByJobID(c.Request.Context(), id)
	steps := []orchestratorv1.LegacyStepResponse{}
	if taskErr == nil && task != nil {
		steps = []orchestratorv1.LegacyStepResponse{projectStep(task, id)}
	}
	c.JSON(http.StatusOK, gin.H{
		"run":        projectRun(job),
		"steps":      steps,
		"deprecated": true,
	})
}

// listJobs is the read-only projection of /api/v1/orchestrator/jobs (list).
// Reads from jobs.Reader (NOT workflow_runs) and projects each entry to
// orchestratorv1.LegacyRunResponse shape.
func (a *orchestratorLegacyAdapter) listJobs(c *gin.Context) {
	c.Header("Deprecation", "true")
	limit := 100
	if v := c.Query("limit"); v != "" {
		if _, scanErr := fmt.Sscanf(v, "%d", &limit); scanErr != nil || limit <= 0 {
			limit = 100
		}
	}
	js, err := a.jobsRepo.List(c.Request.Context(), jobs.Filter{Limit: limit})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	runs := make([]orchestratorv1.LegacyRunResponse, 0, len(js))
	for i := range js {
		runs = append(runs, projectRun(&js[i]))
	}
	c.JSON(http.StatusOK, gin.H{
		"jobs":       runs,
		"total":      len(runs),
		"deprecated": true,
	})
}

// allRunStatuses is the canonical 5-key RunStatus enum surface, used by
// getStats to seed the StatsReport with all keys = 0 even when no jobs
// have run yet.
var allRunStatuses = []orchestratorv1.LegacyRunStatus{
	orchestratorv1.LegacyRunStatusPending,
	orchestratorv1.LegacyRunStatusRunning,
	orchestratorv1.LegacyRunStatusSucceeded,
	orchestratorv1.LegacyRunStatusFailed,
	orchestratorv1.LegacyRunStatusCancelled,
}

// allStepStatuses is the canonical 5-key StepStatus enum surface.
var allStepStatuses = []orchestratorv1.LegacyStepStatus{
	orchestratorv1.LegacyStepStatusBlocked,
	orchestratorv1.LegacyStepStatusReady,
	orchestratorv1.LegacyStepStatusRunning,
	orchestratorv1.LegacyStepStatusSucceeded,
	orchestratorv1.LegacyStepStatusFailed,
}

// stepStatsCap is the per-request ceiling on tasksRepo.List inside
// getStats(). Package-level var so tests can lower it to exercise the
// truncation path without seeding 10k rows.
var stepStatsCap = 10000

// warnStepCap is invoked when the stats request hit the per-request cap
// and the step bucket counts may be truncated. Package-level var so a
// test can swap the recorder without polluting log.SetOutput globally.
var warnStepCap = func(capArg, actual int) {
	log.Printf("[ORCHESTRATOR] stats: step count may be truncated at %d (got %d) — backlog exceeded the per-request cap", capArg, actual)
}

// getStats is the read-only projection of /api/v1/orchestrator/stats.
// Counts come from jobs.Reader.Counts() (Status-based), which is the
// canonical post-cutover source.
func (a *orchestratorLegacyAdapter) getStats(c *gin.Context) {
	c.Header("Deprecation", "true")
	c.Header("Sunset", "Sat, 31 Dec 2026 23:59:59 GMT")
	counts, err := a.jobsRepo.Counts(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	totalRuns := 0
	for _, n := range counts {
		totalRuns += int(n)
	}
	runsByStatus := make(map[orchestratorv1.LegacyRunStatus]int, len(allRunStatuses))
	for _, st := range allRunStatuses {
		runsByStatus[st] = 0
	}
	for st, n := range counts {
		runsByStatus[orchestratorv1.LegacyRunStatus(st)] = int(n)
	}
	stepsByStatus := make(map[orchestratorv1.LegacyStepStatus]int, len(allStepStatuses))
	for _, st := range allStepStatuses {
		stepsByStatus[st] = 0
	}
	out := orchestratorv1.LegacyStatsReport{
		TotalRuns:     totalRuns,
		RunsByStatus:  runsByStatus,
		TotalSteps:    0,
		StepsByStatus: stepsByStatus,
	}
	tasks, err := a.tasksRepo.List(c.Request.Context(), taskgraph.Filter{Limit: stepStatsCap})
	if err == nil {
		out.TotalSteps = len(tasks)
		if len(tasks) >= stepStatsCap {
			warnStepCap(stepStatsCap, len(tasks))
		}
		for _, t := range tasks {
			out.StepsByStatus[mapTaskStatusToStep(t.Status)]++
		}
	}
	c.JSON(http.StatusOK, out)
}

// projectRun maps jobs.Job → orchestratorv1.LegacyRunResponse.
func projectRun(j *jobs.Job) orchestratorv1.LegacyRunResponse {
	if j == nil {
		return orchestratorv1.LegacyRunResponse{}
	}
	out := orchestratorv1.LegacyRunResponse{
		RunID:        j.ID,
		WorkflowType: j.Type,
		Status:       orchestratorv1.LegacyRunStatus(j.Status),
		Revision:     int64(j.Revision),
		Output:       map[string]any{},
	}
	if j.Payload != "" {
		out.Input = readJSONMap(j.Payload)
	}
	if !j.StartedAt.IsZero() {
		t := j.StartedAt
		out.StartedAt = &t
	}
	if !j.CompletedAt.IsZero() {
		t := j.CompletedAt
		out.CompletedAt = &t
	}
	return out
}

// projectStep maps taskgraph.Task + runID → orchestratorv1.LegacyStepResponse.
func projectStep(t *taskgraph.Task, runID string) orchestratorv1.LegacyStepResponse {
	out := orchestratorv1.LegacyStepResponse{
		StepID:      t.ID,
		RunID:       runID,
		StepKey:     t.ExecutorID,
		JobID:       &t.JobID,
		Revision:    int64(t.Revision),
		Attempt:     t.AttemptCount,
		MaxAttempts: 1, // single-task model has no per-task retry budget
		Input:       map[string]any{},
		Output:      map[string]any{},
	}
	switch t.Status {
	case taskgraph.StatusPending:
		out.Status = orchestratorv1.LegacyStepStatusBlocked
	default:
		out.Status = orchestratorv1.LegacyStepStatus(t.Status)
	}
	if t.StartedAt != nil && !t.StartedAt.IsZero() {
		out.StartedAt = t.StartedAt
	}
	if t.CompletedAt != nil && !t.CompletedAt.IsZero() {
		out.CompletedAt = t.CompletedAt
	}
	return out
}

// mapTaskStatusToStep reconciles taskgraph.Status → orchestratorv1.LegacyStepStatus.
func mapTaskStatusToStep(s taskgraph.Status) orchestratorv1.LegacyStepStatus {
	switch s {
	case taskgraph.StatusPending:
		return orchestratorv1.LegacyStepStatusBlocked
	case taskgraph.StatusReady:
		return orchestratorv1.LegacyStepStatusReady
	case taskgraph.StatusLeased, taskgraph.StatusRunning:
		return orchestratorv1.LegacyStepStatusRunning
	case taskgraph.StatusSucceeded:
		return orchestratorv1.LegacyStepStatusSucceeded
	case taskgraph.StatusFailed:
		return orchestratorv1.LegacyStepStatusFailed
	case taskgraph.StatusCancelled:
		return orchestratorv1.LegacyStepStatusFailed
	}
	return orchestratorv1.LegacyStepStatusBlocked
}

// readJSONMap is a tiny inline JSON parser for jobs.Payload (string column)
// → LegacyRunResponse.Input.
func readJSONMap(s string) map[string]any {
	out := map[string]any{}
	if s == "" {
		return out
	}
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return out
	}
	return out
}
