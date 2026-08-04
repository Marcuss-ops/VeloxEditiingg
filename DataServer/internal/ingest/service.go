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
//	service.go        — types + constructor + SetLogger + IngestTaskResult.
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
	"time"

	"velox-server/internal/credentials"
	"velox-server/internal/jobs"
	"velox-server/internal/renderfingerprint"
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

	// Executor identity is master-owned. The handler may populate these
	// values from the canonical task row, but canonicalizePhaseTimingIdentity
	// resolves and overwrites them again before persistence.
	ExecutorID      string
	ExecutorVersion int

	// AttemptNumber is the canonical attempt number stamped at Claim time
	// (PR-2 / fix/canonical-attempt-identity). Authoritatively-derived
	// ValidateIdentityTuple strict-compares the wire attempt_number against
	// the canonical task_attempts.attempt_number for the matched tuple.
	AttemptNumber int32

	// Status is "succeeded" or "failed". The handler maps any other value
	// to "failed" defensively.
	Status string

	// Error fields. Populated when Status == "failed"; ignored otherwise.
	ErrorCode    string
	ErrorDetail  string
	FailureClass string
	// RenderFingerprint is supplied by a trusted compiler/worker adapter and
	// persisted atomically with the terminal attempt report.
	RenderFingerprint *renderfingerprint.Fingerprint

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

	// Scorecard v2 / Step 8: software versioning from the worker report.
	GitSHA            string
	WorkerVersion     string
	EngineVersion     string
	FFmpegVersion     string
	ConfigHash        string
	DockerImageDigest string
	// Scorecard v2 / Step 15: tracing correlation from gRPC metadata.
	TraceID string
	SpanID  string
	// Step 16: raw worker report payload (JSON) for audit and replay.
	RawReportJSON       string
	RawReportReceivedAt time.Time
	// PerformanceReport metadata supplied by the worker for idempotency
	// and conflict detection in task_attempt_reports.
	ReportSchemaVersion    int32
	ReportVersion          int32
	ReportHash             string
	TelemetrySchemaVersion int32
	// Scorecard v2 / Step 17: per-segment C++ sidecar timings.
	SegmentTimings []taskattempts.SegmentTiming
	// Scorecard v2 / Step 18: partial phase metrics captured when an
	// attempt fails before all phases complete.
	PartialPhaseMetrics []taskattempts.PhaseTimingDetailed
	// PhaseTimings is the complete append-only event timeline. It is
	// persisted atomically with the terminal result; the legacy partial
	// field remains a fallback for older workers. Identity fields inside
	// these entries are always overwritten from master-owned task/attempt
	// rows before persistence.
	PhaseTimings []taskattempts.PhaseTimingDetailed
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
