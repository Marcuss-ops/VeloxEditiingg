// PR-operation 01 / Fase 3 — HTTP smoke tests for the orchestrator legacy
// adapter that fronts POST /api/v1/orchestrator/jobs (canonical cutover
// path) and the 3 read-only GET projections.
//
// These tests build a real persistenceDeps + jobs.Repository +
// taskgraph.Repository + orchestratorLegacyAdapter stack against an
// in-process ephemeral SQLite database. Following the pattern already
// proven in cmd/server/bootstrap_test.go, the test fixture is built
// from buildPersistence + buildJobs + buildTasks so we exercise the
// real atomic-creator wiring that production uses (no mocks/fakes).
//
// The tests assert the cutover contract documented in
// docs/operations/01-workflow-taskgraph-cutover.md (Fase 3 §Criteri di
// Completamento): POST writes do NOT touch workflow_runs/steps/events
// (verified by the absence of the legacy CreateRun path AND by the
// canonical job+task being recoverable through jobs.Reader and
// taskgraph.Reader on the GET side).

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/handlers/server/orchestratorv1"
	"velox-server/internal/jobs"
)

const orchestratorTestPath = "/api/v1"

// orchestratorTestStack bundles the wired adapter + a fresh gin engine
// that mounts only the 4 orchestrator routes (no admin auth middleware,
// no global routing — we drive the routes directly via httptest).
// The Deps pointer is exposed so individual tests can poke jobs.Reader /
// taskgraph.Reader for ground-truth assertions.
type orchestratorTestStack struct {
	Adapter *orchestratorLegacyAdapter
	Engine  *gin.Engine
	Deps    *serverDeps
}

// newOrchestratorTestStack constructs a full real-stack fixture:
//   - ephemeral SQLite in t.TempDir() (auto-cleanup by testing.T)
//   - jobs.Repository backed by SQLiteJobRepository
//   - taskgraph.Repository backed by SQLiteTaskRepository
//   - canonical AtomicJobTaskCreator (the one production uses)
//   - orchestratorLegacyAdapter wiring exactly as runServer wires it
//   - a gin engine under /api/v1 exclusively with the 4 orchestrator routes
//   - t.Cleanup closes SQLite so subsequent tests don't see residual
//     file locks on POSIX.
func newOrchestratorTestStack(t *testing.T) *orchestratorTestStack {
	t.Helper()
	gin.SetMode(gin.TestMode)

	tmpDir := t.TempDir()
	cfg := &config.Config{
		Database: config.DatabaseConfig{DBPath: filepath.Join(tmpDir, "velox.db")},
		Runtime:  config.RuntimeConfig{DataDir: tmpDir},
		Workers: config.WorkersConfig{
			MaxJobAttempts:   3,
			AllowedWorkerIDs: []string{"test-worker-1"},
		},
	}

	p, err := buildPersistence(cfg)
	if err != nil {
		t.Fatalf("buildPersistence: %v", err)
	}
	j, err := buildJobs(p)
	if err != nil {
		t.Fatalf("buildJobs: %v", err)
	}
	tk, err := buildTasks(p)
	if err != nil {
		t.Fatalf("buildTasks: %v", err)
	}

	// Minimal serverDeps — only the 3 slots that
	// newOrchestratorLegacyAdapter peeks at. We deliberately skip
	// buildServerDeps / buildWorkers / buildAssets because the adapter
	// only needs the 3 reader/writer bindings (PR-operation 01 / Fase 3
	// composition-root discipline — keep the test stack shallow).
	deps := &serverDeps{
		paths:            &serverPaths{dataDir: tmpDir},
		sqliteStore:      p.SQLite,
		atomicPlanWriter: tk.AtomicCreator,
		jobsRepo:         j.Repository,
		tasksRepo:        tk.TaskRepository,
	}

	adapter, err := newOrchestratorLegacyAdapter(deps)
	if err != nil {
		t.Fatalf("newOrchestratorLegacyAdapter: %v", err)
	}

	r := gin.New()
	v1Admin := r.Group(orchestratorTestPath)
	registerOrchestratorRoutes(v1Admin, adapter)

	t.Cleanup(func() {
		if deps.sqliteStore != nil {
			_ = deps.sqliteStore.Close()
		}
	})

	return &orchestratorTestStack{
		Adapter: adapter,
		Engine:  r,
		Deps:    deps,
	}
}

// validRenderPlan returns a creatorflow.RenderPlan that passes
// RenderPlan.Validate(). Each test mints its own idempotency_key so
// the canonical writer's deterministic SHA-256 mapping produces
// distinct job_ids across tests.
func validRenderPlan(idempotencyKey string) creatorflow.RenderPlan {
	return creatorflow.RenderPlan{
		VideoName:      "orchestrator-smoke-test",
		ExecutorID:     "process_video",
		IdempotencyKey: idempotencyKey,
		MaxRetries:     3,
		Payload: map[string]interface{}{
			"video_name": "orchestrator-smoke-test",
			"smoke":      true,
		},
	}
}

// postJobPayload marshals the orchestratorJobReq edge shape for the
// given RenderPlan. The conversion is intentionally explicit (rather
// than reusing postJob's internal lift-from-payload logic) so the test
// sees exactly the wire shape it would send.
func postJobPayload(plan creatorflow.RenderPlan) []byte {
	body, _ := json.Marshal(orchestratorJobReq{
		VideoName:      plan.VideoName,
		ExecutorID:     plan.ExecutorID,
		IdempotencyKey: plan.IdempotencyKey,
		MaxRetries:     plan.MaxRetries,
		Payload:        plan.Payload,
	})
	return body
}

// doRequest is a tiny playwright helper that drives the gin engine and
// returns the recorder. We set Content-Type only when a non-empty body
// is present.
func doRequest(stack *orchestratorTestStack, method, path string, body []byte) *httptest.ResponseRecorder {
	var rdr *bytes.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	stack.Engine.ServeHTTP(w, req)
	return w
}

// ── POST /orchestrator/jobs ────────────────────────────────────────────

// TestOrchestratorPost_CreatesCanonicalJob verifies the canonical
// cutover path:
//   - POST returns 201 with a non-empty job_id
//   - response carries phase=taskgraph-canonical (semantic fingerprint)
//   - the canonical Job row is recoverable through jobs.Reader.Get
//     (this is the strongest "no CreateRun path" assertion — a real
//     SQLite post-cutover write, not a workflow.repository stub)
//   - the canonical Task row is recoverable through taskgraph.Reader.GetByJobID
//     (single-task / single-job invariant)
func TestOrchestratorPost_CreatesCanonicalJob(t *testing.T) {
	stack := newOrchestratorTestStack(t)

	w := doRequest(stack, http.MethodPost, orchestratorTestPath+"/orchestrator/jobs",
		postJobPayload(validRenderPlan("post-creates-canonical-1")))
	if w.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d body=%s", w.Code, w.Body.String())
	}

	var resp struct {
		JobID          string `json:"job_id"`
		WorkflowType   string `json:"workflow_type"`
		Status         string `json:"status"`
		IdempotencyKey string `json:"idempotency_key"`
		Phase          string `json:"phase"`
		Replay         bool   `json:"replay"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v body=%s", err, w.Body.String())
	}
	if resp.JobID == "" {
		t.Fatal("response.job_id is empty")
	}
	if !stringsHasPrefix(resp.JobID, "job_") {
		t.Errorf("response.job_id=%q: missing canonical 'job_' prefix (createJobWithPlan deriveJobID contract)", resp.JobID)
	}
	if resp.WorkflowType != "process_video" {
		t.Errorf("response.workflow_type=%q, want process_video", resp.WorkflowType)
	}
	if resp.Status != "PENDING" {
		t.Errorf("response.status=%q, want PENDING", resp.Status)
	}
	if resp.Phase != "taskgraph-canonical" {
		t.Errorf("response.phase=%q, want taskgraph-canonical", resp.Phase)
	}
	if resp.Replay {
		t.Errorf("response.replay=true on first submission, want false")
	}

	// Ground truth: jobs.Reader sees the Job.
	ctx := context.Background()
	gotJob, err := stack.Deps.jobsRepo.Get(ctx, resp.JobID)
	if err != nil {
		t.Fatalf("jobsRepo.Get(%s) err=%v — canonical write did not persist", resp.JobID, err)
	}
	if gotJob == nil {
		t.Fatalf("jobsRepo.Get(%s) returned nil — canonical write did not persist", resp.JobID)
	}
	if gotJob.VideoName != "orchestrator-smoke-test" {
		t.Errorf("persisted VideoName=%q, want orchestrator-smoke-test", gotJob.VideoName)
	}
	if gotJob.Type != "process_video" {
		t.Errorf("persisted Type=%q, want process_video", gotJob.Type)
	}
	if gotJob.RunID == "" {
		t.Errorf("persisted RunID is empty (deriveJobID must stamp deterministic job_id as RunID)")
	}

	// Ground truth: taskgraph.Reader sees the matching Task.
	gotTask, err := stack.Deps.tasksRepo.GetByJobID(ctx, resp.JobID)
	if err != nil {
		t.Fatalf("tasksRepo.GetByJobID(%s) err=%v — atomic insert did not pair the Task", resp.JobID, err)
	}
	if gotTask == nil {
		t.Fatalf("tasksRepo.GetByJobID(%s) returned nil — atomic insert did not pair the Task", resp.JobID)
	}
	if gotTask.JobID != resp.JobID {
		t.Errorf("task.JobID=%q, want %q", gotTask.JobID, resp.JobID)
	}
	if gotTask.ExecutorID != "process_video" {
		t.Errorf("task.ExecutorID=%q, want process_video", gotTask.ExecutorID)
	}
}

// TestOrchestratorPost_InvalidPlan_400 verifies the validation edge:
// bad bodies return 400 BEFORE the canonical writer is invoked, so
// 4xx-validation is strictly distinct from 4xx-conflict semantics.
func TestOrchestratorPost_InvalidPlan_400(t *testing.T) {
	stack := newOrchestratorTestStack(t)

	cases := []struct {
		name string
		body []byte
	}{
		{
			name: "missing idempotency_key",
			body: []byte(`{"video_name":"x","executor_id":"process_video"}`),
		},
		{
			name: "missing video_name and payload has neither",
			body: []byte(`{"executor_id":"process_video","idempotency_key":"k-1"}`),
		},
		{
			name: "missing executor_id",
			body: []byte(`{"video_name":"x","idempotency_key":"k-2"}`),
		},
		{
			name: "negative max_retries",
			body: []byte(`{"video_name":"x","executor_id":"process_video","idempotency_key":"k-3","max_retries":-1}`),
		},
		{
			name: "invalid JSON",
			body: []byte(`not-json-at-all`),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := doRequest(stack, http.MethodPost, orchestratorTestPath+"/orchestrator/jobs", tc.body)
			if w.Code != http.StatusBadRequest {
				t.Errorf("want 400, got %d body=%s", w.Code, w.Body.String())
			}
			// Verify the canonical writer was NOT invoked — jobsRepo.Counts
			// stays 0 (no Job row was attempted).
			counts, err := stack.Deps.jobsRepo.Counts(context.Background())
			if err != nil {
				t.Fatalf("jobsRepo.Counts: %v", err)
			}
			var total int64
			for _, n := range counts {
				total += n
			}
			if total != 0 {
				t.Errorf("after 400 rejection: jobs.Counts.total=%d, want 0 — canonical writer was incorrectly invoked", total)
			}
		})
	}
}

// TestOrchestratorPost_IdempotencyReplay_409 verifies the runbook
// contract for idempotent POSTs: a second submission with the same
// idempotency_key returns 409 Conflict and echoes the canonical
// job_id, so clients can converge on the resource without an extra
// GET. This is the wire-shape change introduced in this PR.
func TestOrchestratorPost_IdempotencyReplay_409(t *testing.T) {
	stack := newOrchestratorTestStack(t)

	body := postJobPayload(validRenderPlan("replay-409-1"))

	// First submission: 201 + canonical created=true.
	w1 := doRequest(stack, http.MethodPost, orchestratorTestPath+"/orchestrator/jobs", body)
	if w1.Code != http.StatusCreated {
		t.Fatalf("first submit: want 201, got %d body=%s", w1.Code, w1.Body.String())
	}
	var first struct {
		JobID  string `json:"job_id"`
		Replay bool   `json:"replay"`
	}
	if err := json.Unmarshal(w1.Body.Bytes(), &first); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	if first.JobID == "" {
		t.Fatal("first response.job_id is empty")
	}
	if first.Replay {
		t.Fatal("first response.replay=true on canonical first submit")
	}

	// Second submission with the same idempotency_key: 409 + replay=true
	// + same job_id echoed.
	w2 := doRequest(stack, http.MethodPost, orchestratorTestPath+"/orchestrator/jobs", body)
	if w2.Code != http.StatusConflict {
		t.Fatalf("idempotency replay: want 409, got %d body=%s", w2.Code, w2.Body.String())
	}
	var second struct {
		Error          string `json:"error"`
		JobID          string `json:"job_id"`
		Replay         bool   `json:"replay"`
		IdempotencyKey string `json:"idempotency_key"`
	}
	if err := json.Unmarshal(w2.Body.Bytes(), &second); err != nil {
		t.Fatalf("decode second response: %v body=%s", err, w2.Body.String())
	}
	if second.JobID != first.JobID {
		t.Errorf("echoed job_id=%s, want existing=%s", second.JobID, first.JobID)
	}
	if !second.Replay {
		t.Errorf("response.replay=false, want true on idempotency replay")
	}
	if second.Error == "" {
		t.Errorf("response.error is empty on 409 — clients have no signal to distinguish replay vs. atomic conflict")
	}
	if second.IdempotencyKey != "replay-409-1" {
		t.Errorf("response.idempotency_key=%q, want replay-409-1", second.IdempotencyKey)
	}

	// Ground truth: exactly one Job row was created across both calls.
	counts, err := stack.Deps.jobsRepo.Counts(context.Background())
	if err != nil {
		t.Fatalf("jobsRepo.Counts: %v", err)
	}
	var total int64
	for _, n := range counts {
		total += n
	}
	if total != 1 {
		t.Errorf("after replay: total=%d, want 1 — replay must not double-write", total)
	}
}

// ── GET /orchestrator/jobs ─────────────────────────────────────────────

// TestOrchestratorGetList_DeprecationHeader verifies the read-only
// adapter contract: GET emits the RFC 8594 Deprecation header + body
// field, returns the canonical wire shape (jobs[] + total +
// deprecated=true), and the list size matches the underlying
// jobs.Reader count.
func TestOrchestratorGetList_DeprecationHeader(t *testing.T) {
	stack := newOrchestratorTestStack(t)

	const seedN = 3
	for i := 0; i < seedN; i++ {
		w := doRequest(stack, http.MethodPost, orchestratorTestPath+"/orchestrator/jobs",
			postJobPayload(validRenderPlan(fmt.Sprintf("getlist-seed-%d", i))))
		if w.Code != http.StatusCreated {
			t.Fatalf("seed %d: want 201, got %d body=%s", i, w.Code, w.Body.String())
		}
	}

	w := doRequest(stack, http.MethodGet, orchestratorTestPath+"/orchestrator/jobs", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Deprecation"); got != "true" {
		t.Errorf("response header Deprecation=%q, want 'true'", got)
	}
	if got := w.Header().Get("Sunset"); got == "" {
		t.Errorf("response header Sunset is empty — RFC 8594 deprecation contract missing the removal date")
	}

	var listResp struct {
		Jobs       []orchestratorv1.LegacyRunResponse `json:"jobs"`
		Total      int                                `json:"total"`
		Deprecated bool                               `json:"deprecated"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("decode: %v body=%s", err, w.Body.String())
	}
	if !listResp.Deprecated {
		t.Errorf("response.deprecated=false, want true (adapter flag)")
	}
	if listResp.Total != seedN {
		t.Errorf("response.total=%d, want %d", listResp.Total, seedN)
	}
	if len(listResp.Jobs) != seedN {
		t.Fatalf("response.jobs len=%d, want %d", len(listResp.Jobs), seedN)
	}
	for i, run := range listResp.Jobs {
		if run.RunID == "" {
			t.Errorf("jobs[%d].run_id is empty", i)
		}
		if run.WorkflowType != "process_video" {
			t.Errorf("jobs[%d].workflow_type=%q, want process_video (Job.Type → LegacyRunResponse.WorkflowType)", i, run.WorkflowType)
		}
		if run.Status != orchestratorv1.LegacyRunStatus(jobs.StatusPending) {
			t.Errorf("jobs[%d].status=%q, want PENDING (Job.Status → LegacyRunStatus)", i, run.Status)
		}
	}
}

// ── GET /orchestrator/jobs/:id ─────────────────────────────────────────

// TestOrchestratorGetJob_ProjectsRunShape verifies the single-task
// projection: GET :id returns {run, steps[], deprecated:true}; the
// run shape mirrors the canonical Job columns, and the steps[0]
// carries the JobID pointer back to the canonical Job.
func TestOrchestratorGetJob_ProjectsRunShape(t *testing.T) {
	stack := newOrchestratorTestStack(t)

	// Seed: POST and capture the canonical job_id.
	wpost := doRequest(stack, http.MethodPost, orchestratorTestPath+"/orchestrator/jobs",
		postJobPayload(validRenderPlan("get-job-projects-1")))
	if wpost.Code != http.StatusCreated {
		t.Fatalf("seed: want 201, got %d body=%s", wpost.Code, wpost.Body.String())
	}
	var seed struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(wpost.Body.Bytes(), &seed); err != nil {
		t.Fatalf("decode seed: %v", err)
	}
	if seed.JobID == "" {
		t.Fatal("seed.JobID is empty")
	}

	w := doRequest(stack, http.MethodGet, orchestratorTestPath+"/orchestrator/jobs/"+seed.JobID, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Deprecation"); got != "true" {
		t.Errorf("response header Deprecation=%q, want 'true'", got)
	}

	var resp struct {
		Run        orchestratorv1.LegacyRunResponse    `json:"run"`
		Steps      []orchestratorv1.LegacyStepResponse `json:"steps"`
		Deprecated bool                                `json:"deprecated"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v body=%s", err, w.Body.String())
	}
	if !resp.Deprecated {
		t.Errorf("response.deprecated=false, want true")
	}
	if resp.Run.RunID != seed.JobID {
		t.Errorf("response.run.run_id=%q, want %q (Job.ID → LegacyRunResponse.RunID projection)", resp.Run.RunID, seed.JobID)
	}
	if resp.Run.WorkflowType != "process_video" {
		t.Errorf("response.run.workflow_type=%q, want process_video", resp.Run.WorkflowType)
	}
	if resp.Run.Status != orchestratorv1.LegacyRunStatus(jobs.StatusPending) {
		t.Errorf("response.run.status=%q, want PENDING", resp.Run.Status)
	}
	if len(resp.Steps) != 1 {
		t.Fatalf("response.steps len=%d, want 1 (single-task model invariant)", len(resp.Steps))
	}
	if resp.Steps[0].JobID == nil {
		t.Fatalf("response.steps[0].job_id is nil — canonical Job ↔ Task link lost")
	}
	if *resp.Steps[0].JobID != seed.JobID {
		t.Errorf("response.steps[0].job_id=%q, want %q", *resp.Steps[0].JobID, seed.JobID)
	}
	if resp.Steps[0].StepKey != "process_video" {
		t.Errorf("response.steps[0].step_key=%q, want process_video (Task.ExecutorID → LegacyStepResponse.StepKey)", resp.Steps[0].StepKey)
	}
	// Single-task model: StatusPending maps to StepStatusBlocked per
	// mapTaskStatusToStep (legacy convention kept for wire shape compat).
	if resp.Steps[0].Status != orchestratorv1.LegacyStepStatusBlocked {
		t.Errorf("response.steps[0].status=%q, want BLOCKED (StatusPending → StepStatusBlocked projection)", resp.Steps[0].Status)
	}
}

// TestOrchestratorGetJob_NotFound_404 verifies that GET on an
// unknown id returns 404 instead of 500, so dashboards can
// distinguish missing resources from adapter bugs.
func TestOrchestratorGetJob_NotFound_404(t *testing.T) {
	stack := newOrchestratorTestStack(t)

	w := doRequest(stack, http.MethodGet, orchestratorTestPath+"/orchestrator/jobs/job_does_not_exist", nil)
	if w.Code != http.StatusNotFound {
		t.Errorf("want 404, got %d body=%s", w.Code, w.Body.String())
	}
}

// ── GET /orchestrator/stats ────────────────────────────────────────────

// TestOrchestratorGetStats_SeededShape verifies the read-only stats
// adapter contract: every RunStatus / StepStatus enum key is seeded
// with 0 even on an empty database, so dashboard fan-outs over the
// enum see a stable shape (no missing keys when zero jobs are present).
// This is the regression guard for allRunStatuses + allStepStatuses
// declared in orchestrator_legacy_adapter.go.
func TestOrchestratorGetStats_SeededShape(t *testing.T) {
	stack := newOrchestratorTestStack(t)

	w := doRequest(stack, http.MethodGet, orchestratorTestPath+"/orchestrator/stats", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Deprecation"); got != "true" {
		t.Errorf("response header Deprecation=%q, want 'true'", got)
	}
	if got := w.Header().Get("Sunset"); got == "" {
		t.Errorf("response header Sunset is empty — Fase 3 Compliance header missing")
	}

	var stats orchestratorv1.LegacyStatsReport
	if err := json.Unmarshal(w.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode: %v body=%s", err, w.Body.String())
	}

	expectedRuns := []orchestratorv1.LegacyRunStatus{
		orchestratorv1.LegacyRunStatusPending,
		orchestratorv1.LegacyRunStatusRunning,
		orchestratorv1.LegacyRunStatusSucceeded,
		orchestratorv1.LegacyRunStatusFailed,
		orchestratorv1.LegacyRunStatusCancelled,
	}
	if len(stats.RunsByStatus) != len(expectedRuns) {
		t.Errorf("RunsByStatus len=%d, want %d (allRunStatuses seed)",
			len(stats.RunsByStatus), len(expectedRuns))
	}
	for _, st := range expectedRuns {
		if _, present := stats.RunsByStatus[st]; !present {
			t.Errorf("RunsByStatus missing key %q (allRunStatuses seed contract violation)", st)
		}
	}
	if v := stats.RunsByStatus[orchestratorv1.LegacyRunStatusPending]; v != 0 {
		t.Errorf("empty-DB RunsByStatus[PENDING]=%d, want 0", v)
	}

	expectedSteps := []orchestratorv1.LegacyStepStatus{
		orchestratorv1.LegacyStepStatusBlocked,
		orchestratorv1.LegacyStepStatusReady,
		orchestratorv1.LegacyStepStatusRunning,
		orchestratorv1.LegacyStepStatusSucceeded,
		orchestratorv1.LegacyStepStatusFailed,
	}
	if len(stats.StepsByStatus) != len(expectedSteps) {
		t.Errorf("StepsByStatus len=%d, want %d (allStepStatuses seed)",
			len(stats.StepsByStatus), len(expectedSteps))
	}
	for _, st := range expectedSteps {
		if _, present := stats.StepsByStatus[st]; !present {
			t.Errorf("StepsByStatus missing key %q (allStepStatuses seed contract violation)", st)
		}
	}
	if stats.TotalRuns != 0 {
		t.Errorf("empty-DB TotalRuns=%d, want 0", stats.TotalRuns)
	}
	if stats.TotalSteps != 0 {
		t.Errorf("empty-DB TotalSteps=%d, want 0", stats.TotalSteps)
	}
}

// TestOrchestratorGetStats_Populated counts populated counts from the
// canonical post-cutover source (jobs.Reader.Counts + taskgraph.Reader.List)
// by inspecting a single seed Job. This is the second half of the
// stats contract — when N jobs exist, TotalRuns >= 1 and the relevant
// status bucket counts.
func TestOrchestratorGetStats_Populated(t *testing.T) {
	stack := newOrchestratorTestStack(t)

	if w := doRequest(stack, http.MethodPost, orchestratorTestPath+"/orchestrator/jobs",
		postJobPayload(validRenderPlan("stats-populated-1"))); w.Code != http.StatusCreated {
		t.Fatalf("seed: want 201, got %d", w.Code)
	}

	w := doRequest(stack, http.MethodGet, orchestratorTestPath+"/orchestrator/stats", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}

	var stats orchestratorv1.LegacyStatsReport
	if err := json.Unmarshal(w.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if stats.TotalRuns != 1 {
		t.Errorf("TotalRuns=%d, want 1 after seeding one Job", stats.TotalRuns)
	}
	if v := stats.RunsByStatus[orchestratorv1.LegacyRunStatusPending]; v != 1 {
		t.Errorf("RunsByStatus[PENDING]=%d, want 1 (newly-created Job is PENDING)", v)
	}
	if stats.TotalSteps != 1 {
		t.Errorf("TotalSteps=%d, want 1 (single-task model: each Job has one Task)", stats.TotalSteps)
	}
	// mapTaskStatusToStep(StatusPending) → StepStatusBlocked per the
	// adapter's enum reconciliation table.
	if v := stats.StepsByStatus[orchestratorv1.LegacyStepStatusBlocked]; v != 1 {
		t.Errorf("StepsByStatus[BLOCKED]=%d, want 1 (PENDING task → BLOCKED step projection)", v)
	}
}

// ── helpers ───────────────────────────────────────────────────────────

// TestOrchestratorGetStats_TruncationCap verifies the per-request
// cap code path in getStats: when tasksRepo.List returns >= the
// configured cap, warnStepCap is invoked exactly once with (cap, actual)
// so operators notice the truncation in their logs. The cap is lowered
// to 3 for the test so the path is exercised without seeding 10k rows;
// warnStepCap is swapped for a recorder that captures capArg + actual so
// the assertion is deterministic and parallel-safe (no global log state).
func TestOrchestratorGetStats_TruncationCap(t *testing.T) {
	stack := newOrchestratorTestStack(t)

	origCap, origSink := stepStatsCap, warnStepCap
	t.Cleanup(func() {
		stepStatsCap = origCap
		warnStepCap = origSink
	})

	const testCap = 3
	var captured [2]int
	var capturedCalls int
	warnStepCap = func(capArg, actual int) {
		capturedCalls++
		captured[0] = capArg
		captured[1] = actual
	}
	stepStatsCap = testCap

	// Seed `testCap` jobs: each POST creates exactly one Job + one Task
	// (atomic insert), so tasksRepo.List returns exactly testCap rows.
	for i := 0; i < testCap; i++ {
		w := doRequest(stack, http.MethodPost, orchestratorTestPath+"/orchestrator/jobs",
			postJobPayload(validRenderPlan(fmt.Sprintf("truncation-cap-%d", i))))
		if w.Code != http.StatusCreated {
			t.Fatalf("seed %d: want 201, got %d body=%s", i, w.Code, w.Body.String())
		}
	}

	w := doRequest(stack, http.MethodGet, orchestratorTestPath+"/orchestrator/stats", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("first GET stats: want 200, got %d body=%s", w.Code, w.Body.String())
	}
	if capturedCalls != 1 {
		t.Fatalf("warnStepCap called %d times at-cap, want 1", capturedCalls)
	}
	if captured[0] != testCap {
		t.Errorf("warnStepCap capArg=%d, want %d", captured[0], testCap)
	}
	if captured[1] != testCap {
		t.Errorf("warnStepCap actual=%d, want %d", captured[1], testCap)
	}

	// Negative case: bump the cap well above the seeded task count so
	// warnStepCap must NOT fire. Cross-check that the threshold is
	// strictly `>=` cap (not `>`, not `<`).
	stepStatsCap = testCap * 100
	capturedCalls = 0
	w2 := doRequest(stack, http.MethodGet, orchestratorTestPath+"/orchestrator/stats", nil)
	if w2.Code != http.StatusOK {
		t.Fatalf("second GET stats: want 200, got %d", w2.Code)
	}
	if capturedCalls != 0 {
		t.Errorf("warnStepCap called %d times below cap, want 0 (cross-check)", capturedCalls)
	}
}

func stringsHasPrefix(s, prefix string) bool {
	if len(prefix) > len(s) {
		return false
	}
	return s[:len(prefix)] == prefix
}
