// Package observability provides read-only aggregation and diagnostics
// for task execution. It exposes bounded internal diagnostics only;
// no UI. All data is sourced from repositories, never direct SQL.
package observability

import (
	"context"
	"fmt"
	"time"

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

// JobInspectionReader is the optional read model behind the operator-facing
// job inspection surface. Keeping this as a small local contract means the
// observability package does not depend on a concrete database backend.
type JobInspectionReader interface {
	ListJobEvents(context.Context, string, int) ([]JobEvent, error)
	ListArtifacts(context.Context, string, int) ([]ArtifactSnapshot, error)
	ListDeliveries(context.Context, string) ([]DeliverySnapshot, error)
}

// JobEvent is a normalized event suitable for both JSON clients and the
// fleetctl watch command. Payload is intentionally decoded by the adapter so
// the service never exposes raw SQLite blobs.
type JobEvent struct {
	Timestamp string         `json:"timestamp"`
	JobID     string         `json:"job_id"`
	Event     string         `json:"event"`
	Payload   map[string]any `json:"payload,omitempty"`
}

type ArtifactSnapshot struct {
	ID              string  `json:"id"`
	Type            string  `json:"type,omitempty"`
	Status          string  `json:"status"`
	SHA256          string  `json:"sha256,omitempty"`
	SizeBytes       int64   `json:"size_bytes"`
	DurationSeconds float64 `json:"duration_seconds,omitempty"`
	MimeType        string  `json:"mime_type,omitempty"`
	CreatedAt       string  `json:"created_at,omitempty"`
	VerifiedAt      string  `json:"verified_at,omitempty"`
}

type DeliverySnapshot struct {
	DeliveryID       string `json:"delivery_id"`
	ArtifactID       string `json:"artifact_id"`
	DestinationID    string `json:"destination_id"`
	Status           string `json:"status"`
	RemoteID         string `json:"remote_id,omitempty"`
	RemoteURL        string `json:"remote_url,omitempty"`
	AttemptCount     int    `json:"attempt_count"`
	MaxAttempts      int    `json:"max_attempts"`
	LastError        string `json:"last_error,omitempty"`
	LastErrorMessage string `json:"last_error_message,omitempty"`
	CompletedAt      string `json:"completed_at,omitempty"`
}

// JobInspection is the single read model consumed by `fleetctl job inspect`,
// `fleetctl job metrics`, and `fleetctl job watch`.
type JobInspection struct {
	Job        *jobs.Job          `json:"job,omitempty"`
	Execution  *ExecutionSummary  `json:"execution,omitempty"`
	Events     []JobEvent         `json:"events"`
	Artifacts  []ArtifactSnapshot `json:"artifacts"`
	Deliveries []DeliverySnapshot `json:"deliveries"`
}

type DoctorCheck struct {
	WorkerID string `json:"worker_id"`
	Name     string `json:"name,omitempty"`
	Check    string `json:"check"`
	Status   string `json:"status"`
	Detail   string `json:"detail,omitempty"`
}

type ProductionDoctorResult struct {
	Environment string        `json:"environment"`
	GeneratedAt string        `json:"generated_at"`
	Healthy     bool          `json:"healthy"`
	Checks      []DoctorCheck `json:"checks"`
}

// VersionMetricSnapshot is a single attempt's metric values indexed
// by catalog metric name, for regression comparison.
type VersionMetricSnapshot struct {
	AttemptID  string             `json:"attempt_id"`
	WorkerID   string             `json:"worker_id"`
	ExecutorID string             `json:"executor_id"`
	Metrics    map[string]float64 `json:"metrics"`
}

// ExecutionSummary is the aggregated execution diagnostics for a single task.
type ExecutionSummary struct {
	TaskID              string            `json:"task_id"`
	JobID               string            `json:"job_id"`
	TaskStatus          taskgraph.Status  `json:"task_status"`
	AttemptCount        int               `json:"attempt_count"`
	TotalWallTimeMS     int64             `json:"total_wall_time_ms"`
	PhaseTotals         map[string]int64  `json:"phase_totals"`
	TotalInputBytes     int64             `json:"total_input_bytes"`
	TotalOutputBytes    int64             `json:"total_output_bytes"`
	BytesFromDrive      int64             `json:"bytes_from_drive"`
	BytesFromBlobstore  int64             `json:"bytes_from_blobstore"`
	BytesFromLocalCache int64             `json:"bytes_from_local_cache"`
	CPUTimeMS           int64             `json:"cpu_time_ms"`
	GPUTimeMS           int64             `json:"gpu_time_ms"`
	PeakRSSBytes        int64             `json:"peak_rss_bytes"`
	PeakVRAMBytes       int64             `json:"peak_vram_bytes"`
	Cache               CacheSummary      `json:"cache"`
	Retries             int               `json:"retries"`
	Attempts            []AttemptSummary  `json:"attempts"`
	Segments            []SegmentSnapshot `json:"segments,omitempty"`
}

type SegmentSnapshot struct {
	AttemptID        string  `json:"attempt_id"`
	SegmentIndex     int     `json:"segment_index"`
	SceneID          string  `json:"scene_id,omitempty"`
	DurationMS       float64 `json:"duration_ms"`
	AssetDownloadMS  float64 `json:"asset_download_ms"`
	FFmpegEncodeMS   float64 `json:"ffmpeg_encode_ms"`
	FramesEncoded    int64   `json:"frames_encoded"`
	FramesDecoded    int64   `json:"frames_decoded"`
	FramesComposited int64   `json:"frames_composited"`
	FFmpegSpeedX     float64 `json:"ffmpeg_speed_x"`
	Status           string  `json:"status"`
}

// CacheSummary is the job-level rollup of typed cache counters. A zero
// value is meaningful: older workers may have reported byte volume without
// typed hit/miss counters.
type CacheSummary struct {
	Hits        int64   `json:"hits"`
	Misses      int64   `json:"misses"`
	Evictions   int64   `json:"evictions"`
	Corruptions int64   `json:"corruptions"`
	BytesUsed   int64   `json:"bytes_used"`
	Entries     int64   `json:"entries"`
	HitRatio    float64 `json:"hit_ratio"`
}

// AttemptSummary is the aggregated diagnostics for a single attempt.
type AttemptSummary struct {
	AttemptID      string                          `json:"attempt_id"`
	AttemptNumber  int                             `json:"attempt_number"`
	Status         taskattempts.AttemptStatus      `json:"status"`
	WorkerID       string                          `json:"worker_id"`
	DurationMS     int64                           `json:"duration_ms"`
	PhaseBreakdown map[string]int64                `json:"phase_breakdown"`
	Metrics        *taskattempts.AttemptMetrics    `json:"metrics,omitempty"`
	CacheStats     *taskattempts.AttemptCacheStats `json:"cache_stats,omitempty"`
}

// Service is the read-only observability aggregation service.
type Service struct {
	tasks          TaskReader
	attempts       AttemptReader
	jobs           JobReader
	workers        WorkerReader
	versionMetrics VersionMetricsReader
	audit          AuditReader
	jobInspection  JobInspectionReader
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

// WithWorkers sets the worker reader for worker queries.
func (s *Service) WithWorkers(r WorkerReader) *Service { s.workers = r; return s }

// WithVersionMetrics sets the version metrics reader for regression comparison.
func (s *Service) WithVersionMetrics(r VersionMetricsReader) *Service { s.versionMetrics = r; return s }

func (s *Service) WithAudit(r AuditReader) *Service { s.audit = r; return s }

// WithJobInspection wires the optional persistence-backed job details.
func (s *Service) WithJobInspection(r JobInspectionReader) *Service {
	s.jobInspection = r
	return s
}

func (s *Service) ListAudit(ctx context.Context, resourceID string, limit int) ([]audittrail.Event, error) {
	if s.audit == nil {
		return []audittrail.Event{}, nil
	}
	return s.audit.ListAuditEvents(ctx, resourceID, limit)
}

// SummarizeTask returns the aggregated execution diagnostics for a task.
func (s *Service) SummarizeTask(ctx context.Context, taskID string) (*ExecutionSummary, error) {
	task, err := s.tasks.Get(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("observability summarize: %w", err)
	}
	if task == nil {
		return nil, fmt.Errorf("observability summarize: task %s not found", taskID)
	}

	attempts, err := s.attempts.ListByTaskID(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("observability summarize attempts: %w", err)
	}

	summary := &ExecutionSummary{
		TaskID:       task.ID,
		JobID:        task.JobID,
		TaskStatus:   task.Status,
		AttemptCount: task.AttemptCount,
		PhaseTotals:  make(map[string]int64),
	}

	var firstStart *time.Time
	var lastEnd *time.Time

	for _, a := range attempts {
		as := AttemptSummary{
			AttemptID:      a.ID,
			AttemptNumber:  a.AttemptNumber,
			Status:         a.Status,
			WorkerID:       a.WorkerID,
			PhaseBreakdown: make(map[string]int64),
		}

		// Phase timings
		timings, err := s.attempts.GetPhaseTimings(ctx, a.ID)
		if err == nil {
			var totalDur int64
			var attemptFirstStart *time.Time
			var attemptLastEnd *time.Time
			for _, pt := range timings {
				as.PhaseBreakdown[pt.Phase] += pt.DurationMS
				summary.PhaseTotals[pt.Phase] += pt.DurationMS
				totalDur += pt.DurationMS
				if !pt.WallStart.IsZero() && (attemptFirstStart == nil || pt.WallStart.Before(*attemptFirstStart)) {
					start := pt.WallStart
					attemptFirstStart = &start
				}
				if !pt.WallEnd.IsZero() && (attemptLastEnd == nil || pt.WallEnd.After(*attemptLastEnd)) {
					end := pt.WallEnd
					attemptLastEnd = &end
				}
			}
			// Phase timings can overlap (for example render contains
			// compile/encode/audio). Report wall duration for the attempt;
			// retain the sum only for legacy rows without wall bounds.
			if attemptFirstStart != nil && attemptLastEnd != nil {
				as.DurationMS = attemptLastEnd.Sub(*attemptFirstStart).Milliseconds()
			} else {
				as.DurationMS = totalDur
			}

			for _, timing := range timings {
				// Summary rows (for example quality/ffprobe) may carry no
				// wall-clock bounds. Never let a zero timestamp replace a
				// valid execution bound: time.Time.Sub would otherwise
				// saturate and expose MaxInt64-like wall times.
				if !timing.WallStart.IsZero() && (firstStart == nil || timing.WallStart.Before(*firstStart)) {
					start := timing.WallStart
					firstStart = &start
				}
				if !timing.WallEnd.IsZero() && (lastEnd == nil || timing.WallEnd.After(*lastEnd)) {
					end := timing.WallEnd
					lastEnd = &end
				}
			}
		}

		// Metrics
		metrics, err := s.attempts.GetMetrics(ctx, a.ID)
		if err == nil && metrics != nil {
			as.Metrics = metrics
			summary.TotalInputBytes += metrics.InputBytes
			summary.TotalOutputBytes += metrics.OutputBytes
			summary.BytesFromDrive += metrics.BytesFromDrive
			summary.BytesFromBlobstore += metrics.BytesFromBlobstore
			summary.BytesFromLocalCache += metrics.BytesFromLocalCache
			summary.CPUTimeMS += metrics.CPUTimeMS
			summary.GPUTimeMS += metrics.GPUTimeMS
			if metrics.PeakRSSBytes > summary.PeakRSSBytes {
				summary.PeakRSSBytes = metrics.PeakRSSBytes
			}
			if metrics.PeakVRAMBytes > summary.PeakVRAMBytes {
				summary.PeakVRAMBytes = metrics.PeakVRAMBytes
			}
		}

		// Cache counters are a separate typed row because they are not
		// part of the legacy execution-metrics envelope.
		cacheStats, cacheErr := s.attempts.GetCacheStats(ctx, a.ID)
		if cacheErr == nil && cacheStats != nil {
			as.CacheStats = cacheStats
			summary.Cache.Hits += cacheStats.CacheHits
			summary.Cache.Misses += cacheStats.CacheMisses
			summary.Cache.Evictions += cacheStats.CacheEvictions
			summary.Cache.Corruptions += cacheStats.CacheCorruptions
			summary.Cache.BytesUsed += cacheStats.CacheBytesUsed
			summary.Cache.Entries += int64(cacheStats.CacheEntries)
		}

		if segmentReader, ok := s.attempts.(SegmentReader); ok {
			if segments, segmentErr := segmentReader.ListSegmentTimings(ctx, a.ID); segmentErr == nil {
				for _, segment := range segments {
					summary.Segments = append(summary.Segments, SegmentSnapshot{
						AttemptID: segment.AttemptID, SegmentIndex: segment.SegmentIndex,
						SceneID: segment.SceneID, DurationMS: segment.DurationMS,
						AssetDownloadMS: segment.AssetDownloadMS, FFmpegEncodeMS: segment.FfmpegEncodeMS,
						FramesEncoded: segment.FramesEncoded, FramesDecoded: segment.FramesDecoded,
						FramesComposited: segment.FramesComposited, FFmpegSpeedX: segment.FfmpegSpeedX,
						Status: segment.Status,
					})
				}
			}
		}

		summary.Attempts = append(summary.Attempts, as)
	}

	if firstStart != nil && lastEnd != nil {
		summary.TotalWallTimeMS = lastEnd.Sub(*firstStart).Milliseconds()
	}
	if task.AttemptCount > 1 {
		summary.Retries = task.AttemptCount - 1
	}
	cacheTotal := summary.Cache.Hits + summary.Cache.Misses
	if cacheTotal > 0 {
		summary.Cache.HitRatio = float64(summary.Cache.Hits) / float64(cacheTotal)
	}

	return summary, nil
}

// SummarizeJob returns the aggregated diagnostics for the task owning a job.
func (s *Service) SummarizeJob(ctx context.Context, jobID string) (*ExecutionSummary, error) {
	task, err := s.tasks.GetByJobID(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, fmt.Errorf("observability: no task for job %s", jobID)
	}
	return s.SummarizeTask(ctx, task.ID)
}

// ── Aggregate Observability Queries ──────────────────────────────────────

// OverviewResult is the aggregate system health snapshot returned by Overview().
type OverviewResult struct {
	JobsCompleted24h int64        `json:"jobs_completed_24h"`
	JobsFailed24h    int64        `json:"jobs_failed_24h"`
	ErrorRate        float64      `json:"error_rate"`
	P95RenderMS      int64        `json:"p95_render_ms"`
	ActiveWorkers    int          `json:"active_workers"`
	QueueDepth       int          `json:"queue_depth"`
	TopSlowPhases    []PhaseStat  `json:"top_slow_phases"`
	TopSlowWorkers   []WorkerStat `json:"top_slow_workers"`
	TopErrors        []ErrorStat  `json:"top_errors"`
}

// PhaseStat is a single phase aggregate for the overview.
type PhaseStat struct {
	Phase   string `json:"phase"`
	AvgMS   int64  `json:"avg_ms"`
	P95MS   int64  `json:"p95_ms"`
	Samples int    `json:"samples"`
}

// WorkerStat is a single worker aggregate for the overview.
type WorkerStat struct {
	WorkerID  string  `json:"worker_id"`
	JobCount  int     `json:"job_count"`
	AvgMS     int64   `json:"avg_ms"`
	P95MS     int64   `json:"p95_ms"`
	ErrorRate float64 `json:"error_rate"`
}

// ErrorStat is a single error aggregate.
type ErrorStat struct {
	ErrorCode string `json:"error_code"`
	Count     int    `json:"count"`
}

// WorkerPerformance is the per-worker performance summary.
type WorkerPerformance struct {
	WorkerID      string  `json:"worker_id"`
	WorkerName    string  `json:"worker_name"`
	Status        string  `json:"status"`
	JobCount      int     `json:"job_count"`
	SuccessRate   float64 `json:"success_rate"`
	AvgMS         int64   `json:"avg_ms"`
	P95MS         int64   `json:"p95_ms"`
	LastHeartbeat string  `json:"last_heartbeat"`
}

// PhaseTrendResult is the phase timing trend data.
type PhaseTrendResult struct {
	Phase       string               `json:"phase"`
	AvgMS       int64                `json:"avg_ms"`
	P95MS       int64                `json:"p95_ms"`
	Samples     int                  `json:"samples"`
	Trend       string               `json:"trend"`
	DailyPoints []PhaseTrendDayPoint `json:"daily_points,omitempty"`
}

// PhaseTrendDayPoint is a single day's aggregate for phase trends.
type PhaseTrendDayPoint struct {
	Date    string `json:"date"`
	AvgMS   int64  `json:"avg_ms"`
	P95MS   int64  `json:"p95_ms"`
	Samples int    `json:"samples"`
}
