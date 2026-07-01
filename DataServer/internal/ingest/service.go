// Package ingest implements TaskReportIngestionService — the audit-mandated
// canonical entry point for worker TaskResult messages.
//
// fix/task-native-artifact-bridge — this is the renames home of the
// previously-named `taskingestion` package. The name change reflects the
// package's actual responsibility (typed + identity-validated RESULT
// ingestion) rather than the cross-package import-cycle reason the old
// name was originally chosen for in PR-06. The package stays in its
// own import subtree to preserve the cycle-break against taskattempts ↔
// taskgraph; the cycle-break applies regardless of the package name.
//
// Audit §P1.4 / PR-06 / feat/task-report-ingestion (re-opened in the
// current cutover since the prior PR-11 reconciliation left two real
// gaps unguarded: registering worker-declared output_artifacts and the
// Job AWAITING_ARTIFACT transition depend on handler logic outside the
// ingestion sequence).
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"velox-server/internal/jobs"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-server/internal/taskoutput_artifacts"
)

// IngestCommand is the typed input for TaskReportIngestionService.IngestTaskResult.
// Mirrors the audit-mandated TaskResult identity tuple (PR-03) plus the
// declaration fields. Output artifacts are worker-claimed descriptors;
// this service persists them so the artifact upload pipeline can later
// verify the bytes uploaded match these declarations.
type IngestCommand struct {
	TaskID    string
	AttemptID string
	LeaseID   string
	WorkerID  string
	JobID     string // optional but required for the Job roll-up step (4)

	// AttemptNumber is the canonical attempt number stamped at Claim time
	// (PR-2 / fix/canonical-attempt-identity). Authoritatively-derived
	// ValidateIdentityTuple strict-compares the wire attempt_number against
	// the canonical task_attempts.attempt_number for the matched tuple.
	AttemptNumber int32

	// Status is "succeeded" or "failed". The handler maps any other value
	// to "failed" defensively.
	Status string

	// Error fields. Populated when Status == "failed"; ignored otherwise.
	ErrorCode   string
	ErrorDetail string

	// OutputArtifacts is the worker's map of declared artifacts. Each
	// entry is converted to OutputArtifact via metadata JSON; declared_path
	// and declared_sha256 are worker-supplied hints (NOT authoritative;
	// the artifact upload pipeline's FinalizeVerified recomputes both).
	OutputArtifacts []DeclaredArtifact

	// Scorecard v1 / F1 — typed execution metrics hoisted from the
	// pb.TaskExecutionMetrics wire payload by the gRPC handler via
	// executionMetricsToAttemptMetrics (handler_jobs_metrics.go).
	// Persisted by IngestTaskResult under the per-task mutex immediately
	// after the atomic close-write so the typed metrics commit together
	// with the terminal status flip — guaranteeing serializable scorecard
	// ingest with NO observable window where a task is SUCCEEDED on
	// task_attempts but missing on task_attempt_metrics.
	TypedMetrics taskattempts.AttemptMetrics
	CacheStats   taskattempts.AttemptCacheStats
	CostBasis    taskattempts.AttemptCostBasis
}

// DeclaredArtifact is one worker-claimed artifact. Mirrors the proto
// TaskResult.OutputArtifacts[].Item Struct shape.
type DeclaredArtifact struct {
	ArtifactID   string
	ArtifactType string
	Path         string // worker-supplied hint; not authoritative
	Size         int64
	SHA256       string // worker-supplied hint; verified by master during upload
	Metadata     map[string]interface{}
}

// IngestResult reports what IngestTaskResult did. Counters let callers
// (handler, observability) emit structured logs without re-querying
// the database.
//
// fix/atomic-ingestion: ArtifactsSkips is always 0 — duplicate detection
// now happens inside IngestTaskResultAtomic's SQL transaction (UNIQUE
// constraint skip), so the ingest service no longer distinguishes new
// vs duplicate declarations.
type IngestResult struct {
	TaskID          string
	AttemptID       string
	JobID           string
	AttemptClosed   bool // true iff the atomic actually flipped an attempt
	ArtifactsNew    int  // number of artifact declarations sent (all registered or skipped as duplicates)
	ArtifactsSkips  int  // always 0 under atomic ingestion; kept for API compatibility
	JobTransitioned bool // true iff Ingest transitioned the Job to AWAITING_ARTIFACT or FAILED
	JobNewStatus    string
}

// TaskReportIngestionService is the canonical ingestion entry point for
// worker TaskResult messages. Wired in cmd/server/bootstrap.go and called
// from grpcserver.handleTaskResult (one-line delegate).
//
// fix/atomic-ingestion: outputArtRepo is no longer called directly from
// IngestTaskResult (artifact registration now happens inside the
// taskRepo.IngestTaskResultAtomic transaction). The field is kept for
// API compatibility and may be used by future methods.
//
// Concurrency: handleTaskResult calls IngestTaskResult synchronously
// (no goroutine fan-out). Cross-session concurrency is serialized by
// IngestTaskResultAtomic's database-level CAS — the caller that
// wins the CAS commits everything atomically; the loser gets
// ErrTransitionConflict and the tx rolls back entirely.
// No in-process lock is needed.
type TaskReportIngestionService struct {
	taskRepo      taskgraph.Repository
	jobsRepo      jobs.Repository
	attemptRepo   taskattempts.Repository
	outputArtRepo taskoutput_artifacts.Repository
	logger        *log.Logger
}

// NewTaskReportIngestionService constructs the ingest service. ALL
// four deps are REQUIRED.
//
//   - taskRepo      : task-side atomic transitions + listing (canonical
//     taskgraph.Repository).
//   - jobsRepo      : job-side roll-up target (canonical jobs.Repository).
//   - attemptRepo   : wire-fallback identity tuple validation. The
//     (task_id, worker_id, lease_id) tuple on the wire
//     must map to a non-terminal attempt at ingestion
//     time (PR-02 / canonical attempt identity). A nil
//     attemptRepo is rejected so the contract cannot be
//     silently weakened by a future bootstrap mistake.
//   - outputArtRepo : persistent target for worker-declared artifacts.
//     Registered in step (3) of the audit sequence; the
//     artifact upload pipeline's FinalizeVerified later
//     joins to these declarations to validate that
//     bytes uploaded match what the worker promised.
func NewTaskReportIngestionService(
	taskRepo taskgraph.Repository,
	jobsRepo jobs.Repository,
	attemptRepo taskattempts.Repository,
	outputArtRepo taskoutput_artifacts.Repository,
) (*TaskReportIngestionService, error) {
	if taskRepo == nil {
		return nil, fmt.Errorf("ingest.NewTaskReportIngestionService: taskRepo is required")
	}
	if jobsRepo == nil {
		return nil, fmt.Errorf("ingest.NewTaskReportIngestionService: jobsRepo is required")
	}
	if attemptRepo == nil {
		return nil, fmt.Errorf("ingest.NewTaskReportIngestionService: attemptRepo is required (wire-fallback identity tuple validation needs it)")
	}
	if outputArtRepo == nil {
		return nil, fmt.Errorf("ingest.NewTaskReportIngestionService: outputArtRepo is required")
	}
	return &TaskReportIngestionService{
		taskRepo:      taskRepo,
		jobsRepo:      jobsRepo,
		attemptRepo:   attemptRepo,
		outputArtRepo: outputArtRepo,
		logger:        log.Default(),
	}, nil
}

// SetLogger overrides the default logger (test-friendly).
func (s *TaskReportIngestionService) SetLogger(l *log.Logger) {
	if l != nil {
		s.logger = l
	}
}

// validateIdentityTuple runs the audit-mandated wire-tuple gate that
// precedes any state-changing write in IngestTaskResult. Returns
// taskattempts.ErrIdentityMismatch (wrapped) when ANY of the canonical
// identity values on the wire mismatches the authoritatively-derived
// row from task_attempts.
//
// Three layers of defense:
//  1. Cheap field presence checks
//     (TaskID/AttemptID/LeaseID/WorkerID + JobID + AttemptNumber>0).
//  2. attemptRepo.GetByTaskIDAndWorkerAndLease lookup — the canonical
//     PR-02 wire-fallback path. If the lookup returns nil AND the task
//     has zero attempts at all, the message is rejected as an
//     impersonation attempt. If it returns nil AND non-terminal attempts
//     exist for the task with a different worker/lease, the message is
//     rejected as lease-revoked stale-worker retry.
//  3. STRICT-COMPARE the wire (attempt_id, attempt_number, job_id)
//     against the canonical row derived from GetByTaskIDAndWorkerAndLease.
//     Any mismatch surfaces as ErrIdentityMismatch — the message cannot
//     be trusted and is DROPPED upstream by handleTaskResult.
//
// The function is exported so tests + non-gRPC callers can drive the
// gate without the close-write + artifact-register side-effects.
func (s *TaskReportIngestionService) ValidateIdentityTuple(ctx context.Context, cmd IngestCommand) error {
	if cmd.TaskID == "" {
		return fmt.Errorf("ingest.ValidateIdentityTuple: TaskID is required")
	}
	if cmd.AttemptID == "" {
		return fmt.Errorf("ingest.ValidateIdentityTuple: AttemptID is required")
	}
	if cmd.LeaseID == "" {
		return fmt.Errorf("ingest.ValidateIdentityTuple: LeaseID is required")
	}
	if cmd.WorkerID == "" {
		return fmt.Errorf("ingest.ValidateIdentityTuple: WorkerID is required")
	}
	if cmd.JobID == "" {
		return fmt.Errorf("ingest.ValidateIdentityTuple: JobID is required (full-tuple strict-compare, PR-2)")
	}
	if cmd.AttemptNumber <= 0 {
		return fmt.Errorf("ingest.ValidateIdentityTuple: AttemptNumber must be >0 (got %d)", cmd.AttemptNumber)
	}

	att, err := s.attemptRepo.GetByTaskIDAndWorkerAndLease(ctx, cmd.TaskID, cmd.WorkerID, cmd.LeaseID)
	if err != nil {
		return fmt.Errorf("ingest.ValidateIdentityTuple: lookup attempt (%s, %s, %s): %w",
			cmd.TaskID, cmd.WorkerID, cmd.LeaseID, err)
	}
	if att == nil {
		return fmt.Errorf("ingest.ValidateIdentityTuple: tuple (%s, %s, %s) not found: %w",
			cmd.TaskID, cmd.WorkerID, cmd.LeaseID, taskattempts.ErrIdentityMismatch)
	}

	// PR-2 strict-compare the FULL wire tuple against the canonical row.
	// Any mismatch is an impersonation / wire-drift attempt and is dropped.
	if att.ID != cmd.AttemptID {
		return fmt.Errorf("ingest.ValidateIdentityTuple: attempt_id mismatch (wire=%s db=%s task=%s): %w",
			cmd.AttemptID, att.ID, cmd.TaskID, taskattempts.ErrIdentityMismatch)
	}
	if att.AttemptNumber != int(cmd.AttemptNumber) {
		return fmt.Errorf("ingest.ValidateIdentityTuple: attempt_number mismatch (wire=%d db=%d task=%s): %w",
			cmd.AttemptNumber, att.AttemptNumber, cmd.TaskID, taskattempts.ErrIdentityMismatch)
	}
	if att.JobID != cmd.JobID {
		return fmt.Errorf("ingest.ValidateIdentityTuple: job_id mismatch (wire=%s db=%s task=%s): %w",
			cmd.JobID, att.JobID, cmd.TaskID, taskattempts.ErrIdentityMismatch)
	}
	return nil
}

// IngestTaskResult executes the audit-mandated sequence for a single TaskResult:
//
//  1. Validate wire identity tuple (TaskID + AttemptID + LeaseID + WorkerID
//     non-empty AND canonical attempt lookup via GetByTaskIDAndWorkerAndLease).
//  2. Call IngestTaskResultAtomic — one database transaction that transitions
//     Task + Attempt to terminal AND persists typed metrics, cache stats,
//     cost basis, AND registers output artifact declarations atomically.
//     fix/atomic-ingestion: replaces the former 3-step sequence.
//  3. Roll up Job transition when all sibling tasks are terminal.
//  4. Return IngestResult counters.
//
// Errors are surfaced (no silent swallowing); the handler logs and
// continues — the per-row error does not stop subsequent best-effort
// writes that already committed at step (2).
func (s *TaskReportIngestionService) IngestTaskResult(ctx context.Context, cmd IngestCommand) (IngestResult, error) {
	res := IngestResult{TaskID: cmd.TaskID, AttemptID: cmd.AttemptID, JobID: cmd.JobID}

	// Step 1: identity tuple validation. The handler pre-validates
	// the cheap field checks, but defending here (and adding the
	// store-side wire-fallback check) makes the service composable with
	// non-gRPC callers and prevents a misconfigured bootstrap from
	// letting impersonation attempts bypass the gate.
	if err := s.ValidateIdentityTuple(ctx, cmd); err != nil {
		return res, err
	}

	// Step 2: atomic ingestion — Task CAS + Attempt CAS + metrics +
	// cache + cost + artifact registration in ONE database transaction.
	// fix/atomic-ingestion: replaces TransitionTaskToTerminalAtomic +
	// PersistMetrics + PersistCacheStats + PersistCostBasis +
	// per-artifact Register with a single atomic call.
	var (
		taskStatus    taskgraph.Status
		attemptStatus taskattempts.AttemptStatus
	)
	status := cmd.Status
	if status != "succeeded" && status != "failed" {
		status = "failed"
	}
	if status == "succeeded" {
		taskStatus = taskgraph.StatusSucceeded
		attemptStatus = taskattempts.AttemptStatusSucceeded
	} else {
		taskStatus = taskgraph.StatusFailed
		attemptStatus = taskattempts.AttemptStatusFailed
	}

	// Build typed artifacts from declared artifacts.
	var typedArtifacts []taskoutput_artifacts.OutputArtifact
	artifactCount := 0
	for _, decl := range cmd.OutputArtifacts {
		if decl.ArtifactID == "" {
			continue
		}
		metadataJSON := "{}"
		if decl.Metadata != nil {
			if buf, mErr := json.Marshal(decl.Metadata); mErr == nil {
				metadataJSON = string(buf)
			}
		}
		typedArtifacts = append(typedArtifacts, taskoutput_artifacts.OutputArtifact{
			TaskID:         cmd.TaskID,
			AttemptID:      cmd.AttemptID,
			ArtifactID:     decl.ArtifactID,
			ArtifactType:   decl.ArtifactType,
			DeclaredPath:   decl.Path,
			DeclaredSize:   decl.Size,
			DeclaredSHA256: decl.SHA256,
			MetadataJSON:   metadataJSON,
		})
		artifactCount++
	}

	// Ensure metrics/cache/cost have attempt_id stamped.
	metrics := cmd.TypedMetrics
	if metrics.AttemptID == "" {
		metrics.AttemptID = cmd.AttemptID
	}
	cs := cmd.CacheStats
	if cs.AttemptID == "" {
		cs.AttemptID = cmd.AttemptID
	}
	cb := cmd.CostBasis
	if cb.AttemptID == "" {
		cb.AttemptID = cmd.AttemptID
	}

	ingestErr := s.taskRepo.IngestTaskResultAtomic(ctx, taskgraph.IngestResultCommand{
		TaskID:        cmd.TaskID,
		WorkerID:      cmd.WorkerID,
		LeaseID:       cmd.LeaseID,
		AttemptID:     cmd.AttemptID,
		TaskStatus:    taskStatus,
		AttemptStatus: attemptStatus,
		ErrorCode:     cmd.ErrorCode,
		ErrorMsg:      cmd.ErrorDetail,
		Metrics:       metrics,
		CacheStats:    cs,
		CostBasis:     cb,
		Artifacts:     typedArtifacts,
	})

	// fix/atomic-ingestion: IngestTaskResultAtomic succeeded — the Task +
	// Attempt transition committed atomically together with metrics,
	// cache stats, cost basis, and artifact declarations.
	res.AttemptClosed = true
	res.ArtifactsNew = artifactCount

	if ingestErr != nil {
		// fix/cas-conflict-noop: ErrTransitionConflict on a stale Task
		// means someone else already closed it (replay, sibling result
		// arrived first, OR lease was revoked and reassigned). With
		// atomic ingestion, the ENTIRE transaction rolled back — no
		// metrics, no cache stats, no cost basis, no artifacts were
		// written. We must NOT proceed to job roll-up either: the
		// report that WON the CAS race will trigger the correct Job
		// transition when IT lands. A stale report triggering a
		// spurious job roll-up would produce a ghost transition
		// (e.g. AWAITING_ARTIFACT before all tasks are truly terminal)
		// and mask the true audit trail.
		if !errors.Is(ingestErr, taskgraph.ErrTransitionConflict) {
			return res, fmt.Errorf("ingest.IngestTaskResult: atomic ingest %s: %w", cmd.TaskID, ingestErr)
		}
		// CAS miss: Task was already closed by another report. We must NOT
		// report AttemptClosed=true — someone else won the race.
		res.AttemptClosed = false
		res.ArtifactsNew = 0
		s.logger.Printf(
			"[INGEST] Task %s CAS miss (stale/replay/lease-revoked) reporter=%s lease=%s — complete no-op, skipping job roll-up",
			cmd.TaskID, cmd.WorkerID, cmd.LeaseID,
		)
		return res, nil
	}

	// Step 3: Job roll-up. Skip when the worker did not declare a
	// job_id (defensive — task-native dispatch always sets it; legacy
	// wires may not).
	if cmd.JobID == "" {
		// Breadcrumb for traceback continuity: a malformed TaskResult
		// arriving without a job_id leaves no audit trail otherwise.
		// Task+Attempt close + artifact register still committed above,
		// so the job-side state machine just stays at the previously
		// observed status (recoverable from the next sibling report).
		s.logger.Printf(
			"[INGEST] received TaskResult without job_id task=%s worker=%s lease=%s — skipping job roll-up",
			cmd.TaskID, cmd.WorkerID, cmd.LeaseID,
		)
		return res, nil
	}

	transitioned, newStatus, jobErr := s.maybeTransitionJob(ctx, cmd.JobID, status == "succeeded")
	if jobErr != nil {
		s.logger.Printf("[INGEST] job roll-up for %s failed: %v", cmd.JobID, jobErr)
		// Don't bubble: the Task+Attempt close has already committed; a
		// stale job aggregate is recoverable from the next sibling
		// result.
	} else {
		res.JobTransitioned = transitioned
		res.JobNewStatus = newStatus
	}

	// Step 5: explicit forward-to-finalization signal + observability
	// breadcrumbs. Gate once on JobTransitioned=true (an idempotent
	// re-read sees transitioned=false and emits nothing, so each log
	// line maps to exactly one Job SetStatus write).
	//
	//   * AWAITING_ARTIFACT — the audit contract binds verified-finalization
	//     (`artifacts/sqlite_finalization_repository.go`) downstream;
	//     a "forward-to-finalization" emission lets operators grep
	//     Job arrivals at the verified-finalization gate.
	//   * FAILED            — observability breadcrumb ONLY. The stuck-STAGING
	//     cleanup contract is owned independently by verified-finalization's
	//     `stuck-STAGING` rule (audit §P2 cleanup); this log is purely an
	//     observability breadcrumb and does NOT imply ingest owns the cleanup.
	//   * default           — defensive WARN log so a future maintainer who
	//     adds StatusRetryWait / StatusCancelled writes through this
	//     helper surfaces loud rather than vanishing silently.
	switch {
	case !res.JobTransitioned:
		// Idempotent re-read or already-terminal no-op: emit nothing.
	case res.JobNewStatus == string(jobs.StatusAwaitingArtifact):
		s.logger.Printf(
			"[INGEST] forward-to-finalization job=%s task=%s artifacts_new=%d artifacts_dup=%d",
			cmd.JobID, cmd.TaskID, res.ArtifactsNew, res.ArtifactsSkips,
		)
	case res.JobNewStatus == string(jobs.StatusFailed):
		s.logger.Printf(
			"[INGEST] forward-to-observe-failed job=%s task=%s artifacts_new=%d artifacts_dup=%d — verified-finalization owns stuck-STAGING cleanup independently (this line is observability-only)",
			cmd.JobID, cmd.TaskID, res.ArtifactsNew, res.ArtifactsSkips,
		)
	default:
		s.logger.Printf(
			"[INGEST] warn unexpected Job→%s roll-up via ingest job=%s task=%s — neither finalization nor cleanup emitted; downstream contract unclear",
			res.JobNewStatus, cmd.JobID, cmd.TaskID,
		)
	}

	return res, nil
}

// maybeTransitionJob mirrors the helpers introduced in PR-4 + #5 with
// the Phase 2.8 gating: when all sibling tasks are terminal AND each
// succeeded task has an attempt_commits row with status='COMMITTED',
// flip the Job to AWAITING_ARTIFACT. If any task failed, the Job
// moves to FAILED. PR-02 / Phase 2.5: SUCCEEDED on the Job itself is
// reserved for Coordinator.CommitAttempt and we still do NOT write it
// here.
//
// Returns (transitioned, newStatus, err):
//
//	transitioned=true when a SetStatus write really fired;
//	newStatus is the post-state (also populated on idempotency no-op
//	so the handler can report "already at AWAITING_ARTIFACT" honestly).
func (s *TaskReportIngestionService) maybeTransitionJob(ctx context.Context, jobID string, allSucceeded bool) (bool, string, error) {
	job, err := s.jobsRepo.Get(ctx, jobID)
	if err != nil || job == nil {
		return false, "", err
	}
	if job.Status.IsTerminal() {
		// Already terminal — report the current status so the handler
		// logs "Job is in terminal state, no transition needed".
		return false, string(job.Status), nil
	}

	tasks, err := s.taskRepo.List(ctx, taskgraph.Filter{JobIDs: []string{jobID}})
	if err != nil {
		return false, "", fmt.Errorf("list tasks for job %s: %w", jobID, err)
	}
	if len(tasks) == 0 {
		return false, string(job.Status), nil
	}

	allTerminal := true
	anyFailed := false
	allSucceededAndCommitted := true
	for _, t := range tasks {
		if !t.Status.IsTerminal() {
			allTerminal = false
			break
		}
		if t.Status == taskgraph.StatusFailed || t.Status == taskgraph.StatusCancelled {
			anyFailed = true
			allSucceededAndCommitted = false
		}
	}
	if !allTerminal {
		return false, string(job.Status), nil
	}

	// Phase 2.8 guard: a Task is "succeeded-and-committed" only when
	// status='SUCCEEDED' AND an attempt_commits row exists for it
	// with status='COMMITTED'. RUNNING+winning_attempt_terminal_pending
	// = FALSE (the Ingest path left it there temporarily); the commit
	// protocol must ratify it before the Job promotes. Until that
	// happens, the Job stays at RUNNING.
	if allSucceeded && !anyFailed {
		allSucceededAndCommitted, err = s.allTasksCommitted(ctx, tasks)
		if err != nil {
			return false, string(job.Status), fmt.Errorf("check task commits for job %s: %w", jobID, err)
		}
	} else {
		allSucceededAndCommitted = false
	}

	var newStatus jobs.Status
	if allSucceededAndCommitted {
		newStatus = jobs.StatusAwaitingArtifact
	} else if anyFailed {
		newStatus = jobs.StatusFailed
	} else {
		// allSucceeded AND !anyFailed but the commit-protocol gate
		// block suceeded-only-by-terminal_pending. Stay RUNNING until
		// CommitAttempt ratifies.
		return false, string(job.Status), nil
	}

	// PR-02 idempotency: skip a spurious re-write. We return
	// (transitioned=false, observed_status) on this branch so the
	// handler does not double-log "transitioned Job X" when goroutine
	// B unblocks AFTER goroutine A already wrote AWAITING_ARTIFACT.
	if job.Status == newStatus {
		return false, string(job.Status), nil
	}

	if setErr := s.jobsRepo.SetStatus(ctx, jobID, job.Status, newStatus); setErr != nil {
		return false, string(job.Status), fmt.Errorf("SetStatus %s→%s: %w", job.Status, newStatus, setErr)
	}
	s.logger.Printf("[INGEST] job %s transitioned %s → %s (all sibling tasks terminal)", jobID, job.Status, newStatus)
	return true, string(newStatus), nil
}

// allTasksCommitted returns true iff every Task in `tasks` has an
// attempt_commits row with status='COMMITTED'. Phase 2.8: this is the
// gating condition for AWAITING_ARTIFACT roll-up — pre-Phase-2 the
// roll-up fired as soon as TaskStatus='SUCCEEDED', which produced the
// "Task SUCCEEDED, Job AWAITING_ARTIFACT, no artifact READY"
// impossible state the closure-gate preserves.
func (s *TaskReportIngestionService) allTasksCommitted(ctx context.Context, tasks []taskgraph.Task) (bool, error) {
	if len(tasks) == 0 {
		return false, nil
	}
	taskRepo, ok := s.taskRepo.(interface {
		IsAllAttemptCommitsCommittedForTasks(ctx context.Context, taskIDs []string) (bool, error)
	})
	if !ok {
		return false, fmt.Errorf("ingest.allTasksCommitted: taskRepo %T does not expose commit-presence check (Phase 2.8 wiring)", s.taskRepo)
	}
	ids := make([]string, 0, len(tasks))
	for _, t := range tasks {
		if t.Status == taskgraph.StatusSucceeded {
			ids = append(ids, t.ID)
		}
	}
	if len(ids) == 0 {
		return false, nil
	}
	return taskRepo.IsAllAttemptCommitsCommittedForTasks(ctx, ids)
}
