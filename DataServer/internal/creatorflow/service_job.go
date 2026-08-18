package creatorflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
)

// service_job.go owns the PR-operation 01 / Fase 2 canonical Job+Task
// creation surface (RenderPlan + CreateJobWithPlan + deriveJobID). The
// remote-creator stage (Service / New / StartOrPersistForwarding) lives
// in service.go; the atomic 3-table INSERT is delegated to an injected
// jobTaskCreator (implemented by store.AtomicJobTaskCreator).

// =============================================================================
// PR-operation 01 / Fase 2 — CreateService canonico
// =============================================================================
//
// PR #8: workflow package removed. CreateService validates a RenderPlan,
// derives the canonical TaskSpec, and delegates to the injected jobTaskCreator
// for atomic Job+Task insertion. One writer.
//
// Idempotency: the (RenderPlan.IdempotencyKey) is the canonical dedupe token. Two
// calls with the same plan yield ONE Job row. The job_id is a deterministic
// SHA-256 truncation of the idempotency_key, so the SQLite UNIQUE(job_id)
// constraint enforced inside AtomicJobTaskCreator.CreateJobWithTask is what
// makes the dedup race-safe (the pre-check is an optimisation, not the
// authoritative guard).

// jobGetterForIdempotency is the minimal surface CreateJobWithPlan uses to
// perform the optimistic pre-check. jobs.Writer and jobs.Reader both satisfy
// it (the canonical store.SQLiteJobRepository satisfies jobs.Writer and has
// Get(ctx,id)(*Job,error)). Decoupling from jobs.Writer keeps the test
// surface narrow and removes the dependency on the writer-side Commit path.
type jobGetterForIdempotency interface {
	Get(ctx context.Context, id string) (*jobs.Job, error)
}

// jobTaskCreator is the narrow atomic Job+Task creation contract
// CreateJobWithPlan needs at persist time. store.AtomicJobTaskCreator
// implements it; defining the contract here (consumer-side) removes
// creatorflow's import edge on the concrete store package, mirroring
// enqueue.AtomicJobTaskCreator.
type jobTaskCreator interface {
	CreateJobWithTask(ctx context.Context, job *jobs.Job, spec *taskgraph.TaskSpec, priority int) error
}

// RenderPlan is the validated, typed input shape for CreateJobWithPlan.
// It is local to creatorflow because (a) the canonical RenderPlan contract
// lives in the worker-agent Go module (cross-module import is not viable from
// this side), and (b) the runbook requires the validator to live with the
// service that owns the Job creation. Future PRs (Fase 5 dispatch) will pass
// plan.Payload into the TaskSpec that flows to the worker.
type RenderPlan struct {
	// VideoName is the human-readable asset label. Mirrors jobs.Job.VideoName.
	VideoName string
	// ProjectID is the owning project. Mirrors jobs.Job.ProjectID. Empty is OK.
	ProjectID string
	// ExecutorID selects the worker-side executor that will run the Task.
	// Required: every RenderPlan must name an executor (taskgraph.TaskSpec
	// embeds the ID and the worker uses it to route the task to a registered
	// capability).
	ExecutorID string
	// RunID is the workflow run identifier the job is part of. Optional.
	RunID string
	// IdempotencyKey is REQUIRED. Same key + same plan ⇒ one Job row.
	// Two calls with the same key return the same job_id and do not duplicate
	// the rows. SHA-256(key)[:16] is the deterministic job_id.
	IdempotencyKey string
	// MaxRetries is the per-job retry budget. 0 means default (3).
	MaxRetries int
	// Priority is the master-side dispatch priority. 0 means default (5).
	Priority int
	// Payload is the typed TaskSpec payload. Will be embedded in
	// taskgraph.TaskSpec.Payload verbatim.
	Payload map[string]interface{}
}

// Validate enforces the structural invariants. Phase 2 / runbook §Test:
// "dipendenze inesistenti vengono rifiutate" — Fase 4 dispatch will validate
// the dependency graph; Fase 2 only enforces the SHAPE of one initial Task.
func (p *RenderPlan) Validate() error {
	if p == nil {
		return fmt.Errorf("creatorflow.RenderPlan: nil")
	}
	if strings.TrimSpace(p.VideoName) == "" {
		return fmt.Errorf("creatorflow.RenderPlan: video_name required")
	}
	if strings.TrimSpace(p.ExecutorID) == "" {
		return fmt.Errorf("creatorflow.RenderPlan: executor_id required")
	}
	if strings.TrimSpace(p.IdempotencyKey) == "" {
		return fmt.Errorf("creatorflow.RenderPlan: idempotency_key required")
	}
	if p.MaxRetries < 0 {
		return fmt.Errorf("creatorflow.RenderPlan: max_retries must be >= 0")
	}
	return nil
}

// deriveJobID maps an idempotency key to a deterministic, UUID-shaped job ID.
// SHA-256 truncated to 16 hex chars gives 64-bit collision space; the UNIQUE
// constraint on jobs.job_id is the authoritative dedup at the storage layer.
func deriveJobID(idempotencyKey string) string {
	sum := sha256.Sum256([]byte(idempotencyKey))
	return "job_" + hex.EncodeToString(sum[:8])
}

// CreateJobWithPlan is the PR-operation 01 / Fase 2 canonical entry point
// for Job+Task creation. It replaces ad-hoc CreateRun calls in handlers and
// is the only path that may write to (jobs, tasks, task_specs) on this side
// of the cutover. The body is the canonical sequence:
//
//  1. Validate the RenderPlan shape (cheap, in-memory).
//  2. Optimistic pre-check via repo.Get(jobID) — if a previous call with the
//     same IdempotencyKey already inserted, return it (created=false).
//  3. Build the canonical *jobs.Job (status=PENDING).
//  4. Build the canonical *taskgraph.TaskSpec (version=SpecVersion).
//  5. Validate the TaskSpec (Version>0, JobID set).
//  6. Delegate to the injected jobTaskCreator.CreateJobWithTask, which
//     performs the 3-table INSERT inside a single SQLite tx with
//     `defer Rollback`.
//
// Errors at any step propagate without side effects. If step 6 returns an
// error, the tx is rolled back so the Job row does not orphan (per runbook
// "errore nella creazione di una Task esegue rollback del Job").
//
// The free-function form (vs. a method on Service) is intentional:
// CreateJobWithPlan is part of the PR-operation 01 / Fase 2 cutover, which
// owns the (jobs, tasks, task_specs) writer surface via the narrow
// jobTaskCreator contract. Keeping it off the Service struct isolates the
// dependency graph (atomic creator + jobs repo) from the Service's runtime
// topology (enqueuer + remoteengine client + dbStore). Both wiring paths
// reach the same composition root under cmd/server/bootstrap.go.
func CreateJobWithPlan(
	ctx context.Context,
	atomic jobTaskCreator,
	repo jobGetterForIdempotency,
	plan RenderPlan,
	req costmodel.JobRequirements,
) (jobID string, created bool, err error) {
	if err := plan.Validate(); err != nil {
		return "", false, fmt.Errorf("creatorflow.CreateJobWithPlan: invalid plan: %w", err)
	}
	if atomic == nil {
		return "", false, fmt.Errorf("creatorflow.CreateJobWithPlan: nil atomic creator")
	}
	if repo == nil {
		return "", false, fmt.Errorf("creatorflow.CreateJobWithPlan: nil jobs repo")
	}

	jobID = deriveJobID(plan.IdempotencyKey)

	// Optimistic idempotency pre-check. The SQLite UNIQUE(job_id) inside the
	// atomic insert is the authoritative dedup; this pre-check is just to spare
	// a transactional roll-forward when we're confident the row already exists.
	if existing, getErr := repo.Get(ctx, jobID); getErr == nil && existing != nil && existing.ID == jobID {
		return jobID, false, nil
	}
	// repo.Get returning (nil, nil) is the canonical "not found" idiom in this
	// codebase, so any non-nil error is treated as "proceed to insert" rather
	// than "fail loudly" — the UNIQUE constraint is the truth, not the pre-check.

	priority := plan.Priority
	if priority <= 0 {
		priority = 5
	}

	runID := plan.RunID
	if runID == "" {
		// When the caller does not supply a RunID, stamp the derived
		// job_id as the canonical run identifier so every GET/list
		// projection has a non-empty workflow scope.
		runID = jobID
	}
	job := &jobs.Job{
		ID:           jobID,
		Type:         plan.ExecutorID,
		Status:       jobs.StatusPending,
		VideoName:    plan.VideoName,
		ProjectID:    plan.ProjectID,
		RunID:        runID,
		MaxRetries:   plan.MaxRetries,
		Requirements: req,
	}

	spec := &taskgraph.TaskSpec{
		Version:    taskgraph.SpecVersion,
		JobID:      jobID,
		ExecutorID: plan.ExecutorID,
		Payload:    plan.Payload,
	}
	if spec.Payload == nil {
		spec.Payload = map[string]interface{}{}
	}
	if err := spec.Validate(); err != nil {
		return "", false, fmt.Errorf("creatorflow.CreateJobWithPlan: invalid task spec: %w", err)
	}

	if err := atomic.CreateJobWithTask(ctx, job, spec, priority); err != nil {
		return "", false, fmt.Errorf("creatorflow.CreateJobWithPlan: atomic insert: %w", err)
	}
	return jobID, true, nil
}
