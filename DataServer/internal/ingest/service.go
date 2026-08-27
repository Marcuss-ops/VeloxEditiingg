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
//
// File layout (per-concern split):
//
//	service.go        — service type + constructor + IngestTaskResult.
//	types.go          — IngestCommand, DeclaredArtifact, IngestResult.
//	identity.go       — ValidateIdentityTuple (wire-tuple gate).
//	timing.go         — canonicalizePhaseTimingIdentity (master-owned stamps).
//	job_transitions.go — maybeTransitionJob + allTasksCommitted (Job roll-up).
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"velox-server/internal/credentials"
	"velox-server/internal/jobs"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-server/internal/taskoutput_artifacts"
)

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
	taskRepo       taskgraph.Repository
	jobsRepo       jobs.Repository
	jobTransitions *jobs.TransitionService
	attemptRepo    taskattempts.Repository
	outputArtRepo  taskoutput_artifacts.Repository
	logger         *log.Logger
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
	jobTransitions, err := jobs.NewTransitionService(jobsRepo, jobArtifactContractReader{jobs: jobsRepo})
	if err != nil {
		return nil, fmt.Errorf("ingest.NewTaskReportIngestionService: job transition service: %w", err)
	}
	return &TaskReportIngestionService{
		taskRepo:       taskRepo,
		jobsRepo:       jobsRepo,
		jobTransitions: jobTransitions,
		attemptRepo:    attemptRepo,
		outputArtRepo:  outputArtRepo,
		logger:         log.Default(),
	}, nil
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

	// Normalize detailed-event identity from master-owned rows before any
	// event reaches the atomic persistence boundary. Worker echoes are
	// telemetry only and are never authoritative.
	if len(cmd.PhaseTimings) > 0 || len(cmd.PartialPhaseMetrics) > 0 {
		if err := s.canonicalizePhaseTimingIdentity(ctx, &cmd); err != nil {
			return res, err
		}
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
	if status != "succeeded" && status != "failed" && status != "cancelled" {
		status = "failed"
	}
	if status == "succeeded" {
		taskStatus = taskgraph.StatusSucceeded
		attemptStatus = taskattempts.AttemptStatusSucceeded
	} else if status == "cancelled" {
		taskStatus = taskgraph.StatusCancelled
		attemptStatus = taskattempts.AttemptStatusCancelled
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

	rawReportJSON := cmd.RawReportJSON
	if rawReportJSON != "" {
		rawReportJSON = credentials.JSON(rawReportJSON)
	}

	// Determinism chain closure (Fase D tail, migration 148): stamp the
	// report-time render identity. RendererVersion mirrors the worker
	// engine version (the session-derived value persisted by the handler);
	// ArtifactSHA256 is the first non-empty worker-declared SHA in
	// declaration order (typically the final video). The authoritative
	// master-computed SHA is joined from the artifacts table after
	// finalization — the chain column here is the report-time correlation.
	rendererVersion := cmd.EngineVersion
	artifactSHA := ""
	for _, decl := range cmd.OutputArtifacts {
		if strings.TrimSpace(decl.SHA256) != "" {
			artifactSHA = strings.TrimSpace(decl.SHA256)
			break
		}
	}
	ingestErr := s.taskRepo.IngestTaskResultAtomic(ctx, taskgraph.IngestResultCommand{
		TaskID:        cmd.TaskID,
		JobID:         cmd.JobID,
		WorkerID:      cmd.WorkerID,
		LeaseID:       cmd.LeaseID,
		AttemptID:     cmd.AttemptID,
		TaskStatus:    taskStatus,
		AttemptStatus: attemptStatus,
		ErrorCode:     cmd.ErrorCode,
		ErrorMsg:      cmd.ErrorDetail,
		FailureClass:  cmd.FailureClass,
		Metrics:       metrics,
		CacheStats:    cs,
		CostBasis:     cb,
		Artifacts:     typedArtifacts,
		// Scorecard v2 / Step 8: versioning.
		GitSHA:            cmd.GitSHA,
		WorkerVersion:     cmd.WorkerVersion,
		EngineVersion:     cmd.EngineVersion,
		FFmpegVersion:     cmd.FFmpegVersion,
		ConfigHash:        cmd.ConfigHash,
		DockerImageDigest: cmd.DockerImageDigest,
		// Determinism chain closure (Fase D tail).
		RendererVersion:   rendererVersion,
		ArtifactSHA256:    artifactSHA,
		RenderFingerprint: cmd.RenderFingerprint,
		// Scorecard v2 / Step 15: tracing.
		TraceID: cmd.TraceID,
		SpanID:  cmd.SpanID,
		// Step 16: raw worker report payload for audit/replay.
		RawReportJSON:       rawReportJSON,
		RawReportReceivedAt: cmd.RawReportReceivedAt,
		// PerformanceReport metadata supplied by the worker.
		ReportSchemaVersion:    cmd.ReportSchemaVersion,
		ReportVersion:          cmd.ReportVersion,
		ReportHash:             cmd.ReportHash,
		TelemetrySchemaVersion: cmd.TelemetrySchemaVersion,
		// Scorecard v2 / Step 17: per-segment C++ sidecar timings.
		SegmentTimings: cmd.SegmentTimings,
		// Scorecard v2 / Step 18: phase timeline for successful and failed attempts.
		PartialPhaseMetrics: cmd.PartialPhaseMetrics,
		PhaseTimings:        cmd.PhaseTimings,
		EnvelopeSentAt:      cmd.EnvelopeSentAt,
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
	//     (`artifacts/sqlite_finalize_writer.go`) downstream;
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
