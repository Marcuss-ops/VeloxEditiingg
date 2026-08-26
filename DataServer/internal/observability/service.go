package observability

import (
	"context"
	"fmt"

	"velox-server/internal/audittrail"
	"velox-server/internal/jobs"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// TaskReader is the read surface the observability service depends on.
type TaskReader interface {
	Get(ctx context.Context, id string) (*taskgraph.Task, error)
	GetByJobID(ctx context.Context, jobID string) (*taskgraph.Task, error)
	List(ctx context.Context, filter taskgraph.Filter) ([]taskgraph.Task, error)
}

// AttemptReader provides attempt queries for aggregation.
type AttemptReader interface {
	Get(ctx context.Context, id string) (*taskattempts.TaskAttempt, error)
	ListByTaskID(ctx context.Context, taskID string) ([]taskattempts.TaskAttempt, error)
	GetPhaseTimings(ctx context.Context, attemptID string) ([]taskattempts.PhaseTiming, error)
	GetMetrics(ctx context.Context, attemptID string) (*taskattempts.AttemptMetrics, error)
	GetCacheStats(ctx context.Context, attemptID string) (*taskattempts.AttemptCacheStats, error)
}

// SegmentReader is optional so deployments with older schemas continue to
// serve job inspection; newer SQLite repositories expose sidecar segment
// telemetry through this read-only seam.
type SegmentReader interface {
	ListSegmentTimings(ctx context.Context, attemptID string) ([]taskattempts.SegmentTiming, error)
}

// LiveAttemptReader is the volatile worker_task_runtime projection.
// It is an overlay only: durable task_attempts history remains authoritative
// for identity, status, errors, timestamps, and final metrics. A live row may
// be exposed temporarily during claim/accept visibility, but is never durable
// history and must not resurrect a terminal attempt.
type LiveAttemptReader interface {
	GetWorkerTaskRuntimeByJob(ctx context.Context, jobID string) (*LiveAttempt, error)
}

// LiveAttemptTaskReader is an optional task-scoped refinement of
// LiveAttemptReader. It prevents a multi-task job from selecting another
// task's newest runtime row while preserving the older job-scoped contract
// for compatibility with existing adapters.
type LiveAttemptTaskReader interface {
	GetWorkerTaskRuntimeByTask(ctx context.Context, taskID, jobID string) (*LiveAttempt, error)
}

// JobReader provides job queries for observability aggregates.
type JobReader interface {
	Get(ctx context.Context, id string) (*jobs.Job, error)
	List(ctx context.Context, filter jobs.Filter) ([]jobs.Job, error)
	Counts(ctx context.Context) (jobs.Counts, error)
}

// WorkerReader provides worker queries for observability.
type WorkerReader interface {
	ListWorkers() ([]map[string]any, error)
	GetWorker(workerID string) (map[string]any, error)
}

// VersionMetricsReader provides per-version metric queries for
// regression comparison. Implemented by the store layer on
// task_attempts + task_attempt_metrics.
type VersionMetricsReader interface {
	// ListMetricsByGitSHA returns metric snapshots for all attempts
	// with the given git_sha. Returns an empty slice when no attempts
	// match (not an error).
	ListMetricsByGitSHA(ctx context.Context, gitSHA string) ([]VersionMetricSnapshot, error)
}

type AuditReader interface {
	ListAuditEvents(context.Context, string, int) ([]audittrail.Event, error)
}

// AssetProgressReader provides job-scoped asset download progress for
// admin read paths. Unlike the M2M-scoped pipeline handler, this does
// not require a clientID — it reads the canonical job_asset_refs join.
type AssetProgressReader interface {
	ListAssetDownloadProgressForJob(ctx context.Context, jobID string) ([]AssetProgressView, error)
}

// AssetProgressView is the admin-level asset progress shape consumed
// by the /live endpoint. It mirrors the store's AssetDownloadProgressView
// but avoids importing the store package from the observability layer.
type AssetProgressView struct {
	State           string  `json:"state"`
	BytesDownloaded int64   `json:"bytes_downloaded"`
	BytesTotal      int64   `json:"bytes_total"`
	BytesPerSecond  float64 `json:"bytes_per_second"`
	ETASeconds      int64   `json:"eta_seconds"`
	CacheHit        bool    `json:"cache_hit"`
}

// JobInspectionReader is the optional read model behind the operator-facing
// job inspection surface. Keeping this as a small local contract means the
// observability package does not depend on a concrete database backend.
type JobInspectionReader interface {
	ListJobEvents(context.Context, string, int) ([]JobEvent, error)
	ListArtifacts(context.Context, string, int) ([]ArtifactSnapshot, error)
	ListDeliveries(context.Context, string) ([]DeliverySnapshot, error)
}

// Service is the read-only observability aggregation service.
type Service struct {
	tasks          TaskReader
	attempts       AttemptReader
	jobs           JobReader
	jobWriter      jobs.Writer
	workers        WorkerReader
	versionMetrics VersionMetricsReader
	audit          AuditReader
	jobInspection  JobInspectionReader
	liveAttempts   LiveAttemptReader
	assetProgress  AssetProgressReader
}

// NewService constructs the observability aggregation service.
// Jobs and workers readers are optional (nil-safe) for backward compatibility
// with existing callers that only need task/attempt summarization.
func NewService(tasks TaskReader, attempts AttemptReader) (*Service, error) {
	if tasks == nil {
		return nil, fmt.Errorf("observability: task reader is required")
	}
	if attempts == nil {
		return nil, fmt.Errorf("observability: attempt reader is required")
	}
	return &Service{tasks: tasks, attempts: attempts}, nil
}

// WithJobs sets the job reader for aggregate queries (Overview).
func (s *Service) WithJobs(r JobReader) *Service { s.jobs = r; return s }

// WithJobWriter wires the canonical job lifecycle mutation surface used by
// operator-only administrative actions. Read projections remain separate
// from mutations, and a missing writer keeps cancellation unavailable rather
// than silently falling back to direct SQL or an in-memory state.
func (s *Service) WithJobWriter(w jobs.Writer) *Service { s.jobWriter = w; return s }

// CancelJob performs the canonical job cancellation transition. It never
// mutates task/attempt tables directly; the task lifecycle and reaper own
// the worker-side convergence after the parent job becomes CANCELLED.
func (s *Service) CancelJob(ctx context.Context, id, reason string) error {
	if s == nil || s.jobWriter == nil {
		return fmt.Errorf("observability: job cancellation is not configured")
	}
	if id == "" {
		return fmt.Errorf("observability: job_id is required")
	}
	if reason == "" {
		reason = "cancelled by admin operator"
	}
	return s.jobWriter.Cancel(ctx, id, reason, -1)
}

// WithWorkers sets the worker reader for worker queries.
func (s *Service) WithWorkers(r WorkerReader) *Service { s.workers = r; return s }

// WithVersionMetrics sets the version metrics reader for regression comparison.
func (s *Service) WithVersionMetrics(r VersionMetricsReader) *Service {
	s.versionMetrics = r
	return s
}

func (s *Service) WithAudit(r AuditReader) *Service { s.audit = r; return s }

// WithJobInspection wires the optional persistence-backed job details.
func (s *Service) WithJobInspection(r JobInspectionReader) *Service {
	s.jobInspection = r
	return s
}

// WithLiveAttempts wires the existing volatile runtime projection into the
// admin read model. It does not create or persist a second tracker.
func (s *Service) WithLiveAttempts(r LiveAttemptReader) *Service {
	s.liveAttempts = r
	return s
}

// WithAssetProgress wires the admin-level asset download progress reader.
func (s *Service) WithAssetProgress(r AssetProgressReader) *Service {
	s.assetProgress = r
	return s
}

func (s *Service) ListAudit(ctx context.Context, resourceID string, limit int) ([]audittrail.Event, error) {
	if s.audit == nil {
		return []audittrail.Event{}, nil
	}
	return s.audit.ListAuditEvents(ctx, resourceID, limit)
}
