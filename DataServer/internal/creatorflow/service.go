package creatorflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"

	"github.com/google/uuid"

	"velox-server/internal/config"
	"velox-server/internal/costmodel"
	remoteansible "velox-server/internal/handlers/remote/ansible"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/remoteengine"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
)

// Service encapsulates the optional "creator" stage so multiple endpoints can
// reuse the same remote-engine -> worker handoff path without duplicating it.
//
// PR15.7a: `queue` was removed. The *enqueue.Enqueuer
// holds the JobQueue reference.
// at the composition root if they need the concrete type. This collapses
// two parallel fields that always pointed to the same underlying queue.
//
// PR-forwarding-runner: `dbStore` was added so Forward can persist a
// durable creator_forwardings row (PENDING) instead of spawning a
// volatile in-memory goroutine. The CreatorForwardingRunner picks up
// the row on its next tick and handles polling + forwarding durably.
type Service struct {
	enqueuer *enqueue.Enqueuer
	client   *remoteengine.Client
	dbStore  *store.SQLiteStore
	dataDir  string
	videosDir string
	masterURL string
}

// New creates a creator-flow service from runtime config.
// enqueuer is mandatory (PR15.7a): it owns the voiceover rewrite.
// dbStore is mandatory (PR-forwarding-runner): used to persist
// PENDING creator_forwardings rows for durable polling.
func New(cfg *config.Config, enqueuer *enqueue.Enqueuer, dbStore *store.SQLiteStore) *Service {
	if cfg == nil || enqueuer == nil || dbStore == nil {
		return nil
	}
	if strings.TrimSpace(cfg.Render.RemoteEngineURL) == "" {
		return nil
	}

	return &Service{
		enqueuer: enqueuer,
		client: remoteengine.NewClient(remoteengine.Config{
			URL:       cfg.Render.RemoteEngineURL,
			Token:     cfg.Render.RemoteEngineToken,
			TimeoutMS: cfg.Render.RemoteEngineTimeoutMS,
			Retries:   cfg.Render.RemoteEngineRetries,
		}),
		dbStore:   dbStore,
		dataDir:   strings.TrimSpace(cfg.Runtime.DataDir),
		videosDir: strings.TrimSpace(cfg.Runtime.VideosDir),
		masterURL: resolvePublicMasterURL(cfg),
	}
}

// Forward tries the creator stage and, if it returns a complete payload,
// forwards the resulting job to the worker queue.
//
// It returns:
// - response: queue response enriched with creator metadata
// - used: true only when the creator stage fully handled the request
// - error: fatal creator/queue errors that should surface to callers
func (s *Service) Forward(ctx context.Context, rawPayload map[string]interface{}) (map[string]interface{}, bool, error) {
	if s == nil || s.client == nil || !s.client.IsConfigured() {
		return nil, false, nil
	}

	creatorResult, err := s.client.StartPipeline(ctx, rawPayload)
	if err != nil {
		return nil, false, err
	}

	if enqueue.ShouldForwardPipelineResult(creatorResult) {
		workerResponse, err := s.forwardCompletedResult(ctx, creatorResult)
		if err != nil {
			return nil, false, err
		}

		response := make(map[string]interface{}, len(workerResponse)+4)
		for k, v := range workerResponse {
			response[k] = v
		}
		response["creator_stage"] = "remote_engine"
		response["creator_job_id"] = firstString(creatorResult, "job_id", "trace_id", "id")
		response["creator_status"] = creatorResult["status"]
		response["creator_response"] = creatorResult

		return response, true, nil
	}

	creatorJobID := firstString(creatorResult, "job_id", "trace_id", "id")
	if creatorJobID == "" {
		log.Printf("[CREATOR] remote result incomplete and missing job id, keeping local fallback")
		return nil, false, nil
	}

	// PR-forwarding-runner: persist a durable forwarding record instead of
	// spawning a volatile goroutine. The CreatorForwardingRunner picks up
	// PENDING rows on its next tick and handles polling + forwarding.
	targetExecutorID := firstString(creatorResult, "executor_id", "pipeline_id")
	if targetExecutorID == "" {
		targetExecutorID = "scene.composite.v1"
	}
	forwardingID := "cf_" + uuid.NewString()
	now := time.Now().UTC().Format(time.RFC3339)
	if err := s.dbStore.InsertCreatorForwarding(&store.CreatorForwarding{
		ForwardingID:     forwardingID,
		SourceProvider:   "remote_engine",
		SourceJobID:      creatorJobID,
		TargetExecutorID: targetExecutorID,
		Status:           string(store.CFStatusPending),
		CreatedAt:        now,
		UpdatedAt:        now,
	}); err != nil {
		log.Printf("[CREATOR] failed to insert forwarding row for job_id=%s: %v", creatorJobID, err)
		return nil, false, fmt.Errorf("insert creator forwarding: %w", err)
	}

	return map[string]interface{}{
		"ok":               true,
		"creator_stage":    "remote_engine",
		"creator_job_id":   creatorJobID,
		"creator_status":   creatorResult["status"],
		"creator_polling":  true,
		"creator_response": creatorResult,
	}, true, nil
}

func firstString(m map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if v, ok := m[key]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

// ForwardCompletedResult converts a completed creator payload into a worker job
// and enqueues it for the remote worker pool.
//
// PR #3: takes *enqueue.Enqueuer (which now owns AtomicJobTaskCreator + jobs.Reader
// instead of Queue) for atomic Job+Task creation.
func ForwardCompletedResult(ctx context.Context, enqueuer *enqueue.Enqueuer, result map[string]interface{}) (map[string]interface{}, error) {
	if enqueuer == nil || enqueuer.Creator == nil {
		return nil, fmt.Errorf("creator unavailable")
	}
	if !enqueue.ShouldForwardPipelineResult(result) {
		return nil, nil
	}

	workerPayload, err := enqueue.BuildPipelinePayload(result)
	if err != nil {
		return nil, err
	}

	// PR-04.5: legacy creator-flow callers do not publish per-job
	// JobRequirements — pass the permissive default so today's FIFO
	// queue routing is preserved. Future slices that decide on
	// concrete requirements can plumb them through here.
	return enqueuer.Enqueue(ctx, workerPayload, costmodel.DefaultRequirements())
}

func (s *Service) forwardCompletedResult(ctx context.Context, result map[string]interface{}) (map[string]interface{}, error) {
	if s == nil {
		return nil, fmt.Errorf("service unavailable")
	}
	if s.enqueuer == nil || s.enqueuer.Creator == nil {
		return nil, fmt.Errorf("creator unavailable")
	}
	if !enqueue.ShouldForwardPipelineResult(result) {
		return nil, nil
	}

	workerPayload, err := enqueue.BuildPipelinePayload(result)
	if err != nil {
		return nil, err
	}

	masterURL := strings.TrimSpace(s.masterURL)
	if masterURL == "" || remoteansible.IsLocalhostURL(masterURL) {
		masterURL = detectPublicMasterURL()
	}
	if s.dataDir != "" && masterURL != "" {
		workerPayload, err = enqueue.BuildSceneImagePayloadForMaster(workerPayload, s.dataDir, s.videosDir, masterURL)
		if err != nil {
			return nil, err
		}
	}

	// PR-04.5: see ForwardCompletedResult comment above.
	return s.enqueuer.Enqueue(ctx, workerPayload, costmodel.DefaultRequirements())
}



func resolvePublicMasterURL(cfg *config.Config) string {
	if cfg != nil {
		if v := strings.TrimSpace(cfg.Workers.MasterURL); v != "" {
			return v
		}
	}
	if v := strings.TrimSpace(config.GetMasterURL()); v != "" {
		return v
	}
	return detectPublicMasterURL()
}

func detectPublicMasterURL() string {
	out, err := exec.Command("hostname", "-I").Output()
	if err == nil {
		fields := strings.Fields(string(out))
		if len(fields) > 0 {
			ip := strings.TrimSpace(fields[0])
			if ip != "" && !remoteansible.IsLocalhostURL(ip) {
				return "http://" + ip + ":8000"
			}
		}
	}
	return remoteansible.DetectLocalMasterURL()
}

// =============================================================================
// PR-operation 01 / Fase 2 — CreateService canonico
// =============================================================================
//
// PR #8: workflow package removed. CreateService validates a RenderPlan,
// derives the canonical TaskSpec, and delegates to store.AtomicJobTaskCreator
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
//  6. Delegate to store.AtomicJobTaskCreator.CreateJobWithTask, which performs
//     the 3-table INSERT inside a single SQLite tx with `defer Rollback`.
//
// Errors at any step propagate without side effects. If step 6 returns an
// error, the tx is rolled back so the Job row does not orphan (per runbook
// "errore nella creazione di una Task esegue rollback del Job").
//
// The free function form (vs. a method on Service) matches the pattern of
// ForwardCompletedResult, and avoids touching the existing Service struct,
// which has its own dependencies wired through New(cfg, enqueuer) at the
// composition root. Atomic creator + jobs repo are wired alongside inside
// buildTasks / buildJobs (see cmd/server/bootstrap_tasks.go).
func CreateJobWithPlan(
	ctx context.Context,
	atomic *store.AtomicJobTaskCreator,
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
