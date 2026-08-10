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

// LiveAttemptReader is the existing volatile worker_task_runtime projection.
// It supplies the current Attempt state while durable attempt metrics are
// still being produced and therefore may not exist yet.
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

type LiveAttempt struct {
	TaskID                 string
	JobID                  string
	AttemptID              string
	AttemptNumber          int
	WorkerID               string
	LeaseID                string
	RuntimeStatus          string
	WorkerConnectionState  string
	ProgressPercent        int
	ProgressPhase          string
	CurrentScene           int
	TotalScenes            int
	CurrentSegment         int
	TotalSegments          int
	FramesEncoded          int64
	FramesDecoded          int64
	FramesComposited       int64
	FFmpegSpeedX           float64
	ElapsedMS              int64
	CumulativeMetrics      map[string]any
	CanonicalAttemptEvents []map[string]any
	StartedAt              string
	LastProgressAt         string
	UpdatedAt              string
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
	QueuedAt         string `json:"queued_at,omitempty"`
	StartedAt        string `json:"started_at,omitempty"`
	CompletedAt      string `json:"completed_at,omitempty"`
	QueueMS          int64  `json:"queue_ms,omitempty"`
	UploadMS         int64  `json:"upload_ms,omitempty"`
	TotalMS          int64  `json:"total_ms,omitempty"`
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
	TaskID       string           `json:"task_id"`
	JobID        string           `json:"job_id"`
	TaskStatus   taskgraph.Status `json:"task_status"`
	AttemptCount int              `json:"attempt_count"`
	// These live fields are an optional top-level projection of the same
	// worker_task_runtime Attempt already overlaid into Attempts below.
	// omitempty preserves the legacy ExecutionSummary shape when no live
	// Attempt exists (including jobs whose runtime row was cleaned up).
	AttemptID           string                `json:"attempt_id,omitempty"`
	WorkerID            string                `json:"worker_id,omitempty"`
	Phase               string                `json:"phase,omitempty"`
	Progress            *ExecutionProgress    `json:"progress,omitempty"`
	LiveMetrics         *ExecutionLiveMetrics `json:"live_metrics,omitempty"`
	LastProgressAt      string                `json:"last_progress_at,omitempty"`
	TotalWallTimeMS     int64                 `json:"total_wall_time_ms"`
	PhaseTotals         map[string]int64      `json:"phase_totals"`
	TotalInputBytes     int64                 `json:"total_input_bytes"`
	TotalOutputBytes    int64                 `json:"total_output_bytes"`
	BytesFromDrive      int64                 `json:"bytes_from_drive"`
	BytesFromBlobstore  int64                 `json:"bytes_from_blobstore"`
	BytesFromLocalCache int64                 `json:"bytes_from_local_cache"`
	CPUTimeMS           int64                 `json:"cpu_time_ms"`
	GPUTimeMS           int64                 `json:"gpu_time_ms"`
	PeakRSSBytes        int64                 `json:"peak_rss_bytes"`
	PeakVRAMBytes       int64                 `json:"peak_vram_bytes"`
	Cache               CacheSummary          `json:"cache"`
	Retries             int                   `json:"retries"`
	Attempts            []AttemptSummary      `json:"attempts"`
	PhaseTimings        []PhaseSnapshot       `json:"phase_timings,omitempty"`
	Segments            []SegmentSnapshot     `json:"segments,omitempty"`
}

// ExecutionProgress is the compact live progress projection exposed by the
// admin job endpoint. It is derived from the canonical worker_task_runtime
// Attempt, not maintained as a second tracker.
type ExecutionProgress struct {
	Percent       int `json:"percent"`
	Scene         int `json:"scene"`
	ScenesTotal   int `json:"scenes_total"`
	Segment       int `json:"segment"`
	SegmentsTotal int `json:"segments_total"`
}

// ExecutionLiveMetrics contains the cumulative counters available while the
// Attempt is RUNNING. The same values are copied into AttemptMetrics when the
// final report arrives; CumulativeMetrics preserves additional typed counters
// without changing this stable envelope.
type ExecutionLiveMetrics struct {
	ElapsedMS         int64          `json:"elapsed_ms"`
	FramesEncoded     int64          `json:"frames_encoded"`
	FramesDecoded     int64          `json:"frames_decoded"`
	FramesComposited  int64          `json:"frames_composited"`
	FFmpegSpeedX      float64        `json:"ffmpeg_speed_x"`
	CumulativeMetrics map[string]any `json:"cumulative_metrics,omitempty"`
}

// PhaseSnapshot is the ordered, persisted phase timeline. PhaseTotals remains
// the compact aggregate; this slice is the detail used by job inspect/watch.
type PhaseSnapshot struct {
	AttemptID  string    `json:"attempt_id"`
	Phase      string    `json:"phase"`
	DurationMS int64     `json:"duration_ms"`
	WallStart  time.Time `json:"wall_start,omitempty"`
	WallEnd    time.Time `json:"wall_end,omitempty"`
}

type SegmentSnapshot struct {
	AttemptID        string  `json:"attempt_id"`
	SegmentIndex     int     `json:"segment_index"`
	SceneID          string  `json:"scene_id,omitempty"`
	SourceType       string  `json:"source_type,omitempty"`
	AssetKey         string  `json:"asset_key,omitempty"`
	SourceURLHash    string  `json:"source_url_hash,omitempty"`
	Codec            string  `json:"codec,omitempty"`
	Preset           string  `json:"preset,omitempty"`
	DurationMS       float64 `json:"duration_ms"`
	AssetDownloadMS  float64 `json:"asset_download_ms"`
	FFmpegEncodeMS   float64 `json:"ffmpeg_encode_ms"`
	SourceBytes      int64   `json:"source_bytes"`
	OutputBytes      int64   `json:"output_bytes"`
	InputDurationMS  float64 `json:"input_duration_ms"`
	OutputDurationMS float64 `json:"output_duration_ms"`
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
	AttemptID              string                          `json:"attempt_id"`
	AttemptNumber          int                             `json:"attempt_number"`
	Status                 taskattempts.AttemptStatus      `json:"status"`
	WorkerID               string                          `json:"worker_id"`
	WorkerName             string                          `json:"worker_name,omitempty"`
	ErrorCode              string                          `json:"error_code,omitempty"`
	ErrorMessage           string                          `json:"error_message,omitempty"`
	StartedAt              string                          `json:"started_at,omitempty"`
	CompletedAt            string                          `json:"completed_at,omitempty"`
	DurationMS             int64                           `json:"duration_ms"`
	PhaseBreakdown         map[string]int64                `json:"phase_breakdown"`
	Metrics                *taskattempts.AttemptMetrics    `json:"metrics,omitempty"`
	CacheStats             *taskattempts.AttemptCacheStats `json:"cache_stats,omitempty"`
	Live                   bool                            `json:"live,omitempty"`
	Phase                  string                          `json:"phase,omitempty"`
	ProgressPercent        int                             `json:"progress_percent,omitempty"`
	CurrentScene           int                             `json:"current_scene,omitempty"`
	TotalScenes            int                             `json:"total_scenes,omitempty"`
	CurrentSegment         int                             `json:"current_segment,omitempty"`
	TotalSegments          int                             `json:"total_segments,omitempty"`
	FramesEncoded          int64                           `json:"frames_encoded,omitempty"`
	FramesDecoded          int64                           `json:"frames_decoded,omitempty"`
	FramesComposited       int64                           `json:"frames_composited,omitempty"`
	FFmpegSpeedX           float64                         `json:"ffmpeg_speed_x,omitempty"`
	ElapsedMS              int64                           `json:"elapsed_ms,omitempty"`
	LastProgressAt         string                          `json:"last_progress_at,omitempty"`
	CumulativeMetrics      map[string]any                  `json:"cumulative_metrics,omitempty"`
	CanonicalAttemptEvents []map[string]any                `json:"canonical_attempt_events,omitempty"`
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
	liveAttempts   LiveAttemptReader
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

// WithLiveAttempts wires the existing volatile runtime projection into the
// admin read model. It does not create or persist a second tracker.
func (s *Service) WithLiveAttempts(r LiveAttemptReader) *Service {
	s.liveAttempts = r
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

	// Durable metrics are final-report data. Overlay the same canonical
	// Attempt with the live worker_task_runtime row while it is RUNNING.
	// Keep the live row aside until durable attempts are loaded so a matching
	// attempt is enriched in place rather than appended twice.
	var live *LiveAttempt
	if s.liveAttempts != nil {
		var candidate *LiveAttempt
		var liveErr error
		if taskReader, ok := s.liveAttempts.(LiveAttemptTaskReader); ok {
			candidate, liveErr = taskReader.GetWorkerTaskRuntimeByTask(ctx, task.ID, task.JobID)
		} else {
			candidate, liveErr = s.liveAttempts.GetWorkerTaskRuntimeByJob(ctx, task.JobID)
		}
		if liveErr == nil && candidate != nil && candidate.TaskID == task.ID && candidate.AttemptID != "" {
			live = candidate
			if live.AttemptNumber > summary.AttemptCount {
				summary.AttemptCount = live.AttemptNumber
			}
		}
	}
	liveActive := liveAttemptIsEligible(live, task, attempts)

	var firstStart *time.Time
	var lastEnd *time.Time

	for _, a := range attempts {
		as := AttemptSummary{
			AttemptID:      a.ID,
			AttemptNumber:  a.AttemptNumber,
			Status:         a.Status,
			WorkerID:       a.WorkerID,
			ErrorCode:      a.ErrorCode,
			ErrorMessage:   a.ErrorMessage,
			PhaseBreakdown: make(map[string]int64),
		}
		as.WorkerName = s.workerDisplayName(as.WorkerID)
		// liveActive is decided once by liveAttemptIsEligible before this
		// loop. That authority includes the matching durable attempt status,
		// so merge precedence cannot depend on the order of durable rows.
		if liveActive && live != nil && live.AttemptID == a.ID && !a.Status.IsTerminal() {
			as.Live = true
			as.Status = liveAttemptStatus(live)
			as.WorkerID = live.WorkerID
			as.WorkerName = s.workerDisplayName(as.WorkerID)
			as.Phase = live.ProgressPhase
			as.ProgressPercent = live.ProgressPercent
			as.CurrentScene = live.CurrentScene
			as.TotalScenes = live.TotalScenes
			as.CurrentSegment = live.CurrentSegment
			as.TotalSegments = live.TotalSegments
			as.FramesEncoded = live.FramesEncoded
			as.FramesDecoded = live.FramesDecoded
			as.FramesComposited = live.FramesComposited
			as.FFmpegSpeedX = live.FFmpegSpeedX
			as.ElapsedMS = live.ElapsedMS
			as.StartedAt = live.StartedAt
			as.LastProgressAt = live.LastProgressAt
			as.CumulativeMetrics = live.CumulativeMetrics
			as.CanonicalAttemptEvents = live.CanonicalAttemptEvents
		}
		if a.StartedAt != nil {
			as.StartedAt = a.StartedAt.UTC().Format(time.RFC3339Nano)
		}
		if a.CompletedAt != nil {
			as.CompletedAt = a.CompletedAt.UTC().Format(time.RFC3339Nano)
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
				summary.PhaseTimings = append(summary.PhaseTimings, PhaseSnapshot{
					AttemptID: pt.AttemptID, Phase: pt.Phase, DurationMS: pt.DurationMS,
					WallStart: pt.WallStart, WallEnd: pt.WallEnd,
				})
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
		} else if liveActive && live != nil && live.AttemptID == a.ID && !a.Status.IsTerminal() {
			// Before final TaskResult ingest, expose the same typed metric
			// shape that the final report will persist. This is a projection
			// of worker_task_runtime, not a second telemetry store; once the
			// durable row exists it remains authoritative above.
			as.Metrics = liveAttemptMetrics(live)
			summary.TotalInputBytes += as.Metrics.InputBytes
			summary.TotalOutputBytes += as.Metrics.OutputBytes
			summary.BytesFromDrive += as.Metrics.BytesFromDrive
			summary.BytesFromBlobstore += as.Metrics.BytesFromBlobstore
			summary.BytesFromLocalCache += as.Metrics.BytesFromLocalCache
			summary.CPUTimeMS += as.Metrics.CPUTimeMS
			summary.GPUTimeMS += as.Metrics.GPUTimeMS
			if as.Metrics.PeakRSSBytes > summary.PeakRSSBytes {
				summary.PeakRSSBytes = as.Metrics.PeakRSSBytes
			}
			if as.Metrics.PeakVRAMBytes > summary.PeakVRAMBytes {
				summary.PeakVRAMBytes = as.Metrics.PeakVRAMBytes
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
						SceneID: segment.SceneID, SourceType: segment.SourceType,
						AssetKey: segment.CacheKey, SourceURLHash: segment.SourceURLHash,
						Codec: segment.Codec, Preset: segment.Preset,
						DurationMS:      segment.DurationMS,
						AssetDownloadMS: segment.AssetDownloadMS, FFmpegEncodeMS: segment.FfmpegEncodeMS,
						SourceBytes: segment.SourceBytes, OutputBytes: segment.OutputBytes,
						InputDurationMS: segment.InputDurationMS, OutputDurationMS: segment.OutputDurationMS,
						FramesEncoded: segment.FramesEncoded, FramesDecoded: segment.FramesDecoded,
						FramesComposited: segment.FramesComposited, FFmpegSpeedX: segment.FfmpegSpeedX,
						Status: segment.Status,
					})
				}
			}
		}

		summary.Attempts = append(summary.Attempts, as)
	}
	if liveActive {
		found := false
		for _, existing := range summary.Attempts {
			if existing.AttemptID == live.AttemptID {
				found = true
				break
			}
		}
		if !found {
			summary.Attempts = append(summary.Attempts, AttemptSummary{
				AttemptID: live.AttemptID, AttemptNumber: live.AttemptNumber,
				Status: liveAttemptStatus(live), WorkerID: live.WorkerID,
				WorkerName: s.workerDisplayName(live.WorkerID),
				Metrics:    liveAttemptMetrics(live),
				Live:       true, Phase: live.ProgressPhase, ProgressPercent: live.ProgressPercent,
				CurrentScene: live.CurrentScene, TotalScenes: live.TotalScenes,
				CurrentSegment: live.CurrentSegment, TotalSegments: live.TotalSegments,
				FramesEncoded: live.FramesEncoded, FramesDecoded: live.FramesDecoded,
				FramesComposited: live.FramesComposited, FFmpegSpeedX: live.FFmpegSpeedX,
				ElapsedMS: live.ElapsedMS, StartedAt: live.StartedAt,
				LastProgressAt: live.LastProgressAt, CumulativeMetrics: live.CumulativeMetrics,
				CanonicalAttemptEvents: live.CanonicalAttemptEvents,
			})
		}
	}

	if liveActive {
		summary.AttemptID = live.AttemptID
		summary.WorkerID = live.WorkerID
		summary.Phase = live.ProgressPhase
		summary.Progress = &ExecutionProgress{
			Percent: live.ProgressPercent, Scene: live.CurrentScene, ScenesTotal: live.TotalScenes,
			Segment: live.CurrentSegment, SegmentsTotal: live.TotalSegments,
		}
		summary.LiveMetrics = &ExecutionLiveMetrics{
			ElapsedMS: live.ElapsedMS, FramesEncoded: live.FramesEncoded,
			FramesDecoded: live.FramesDecoded, FramesComposited: live.FramesComposited,
			FFmpegSpeedX: live.FFmpegSpeedX, CumulativeMetrics: live.CumulativeMetrics,
		}
		summary.LastProgressAt = live.LastProgressAt
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

// liveAttemptIsEligible is the single authority for live overlay precedence.
// Durable terminal state always wins over volatile worker_task_runtime state;
// live data is eligible only for a matching non-terminal attempt (or the
// claim-to-accept visibility window before that durable row exists).
func liveAttemptIsEligible(live *LiveAttempt, task *taskgraph.Task, attempts []taskattempts.TaskAttempt) bool {
	if live == nil || task == nil || task.Status.IsTerminal() || live.AttemptID == "" || live.AttemptNumber <= 0 {
		return false
	}

	// A runtime row is live only while its attempt is in an execution phase.
	// PARTITIONED_SUSPECTED is a disconnect signal, not progress: exposing it
	// as RUNNING would make a dead worker look active until the next retry.
	switch live.RuntimeStatus {
	case "ACCEPTED", "STARTING", "RUNNING", "CANCELLING", "UPLOADING", "FINALIZING":
		// Keep the canonical active execution states below.
	default:
		return false
	}
	// A worker-level partition/disconnect state invalidates the volatile
	// runtime row even if the last heartbeat payload still carried RUNNING.
	// The workers row is the canonical connection-state mirror used by the
	// recovery path, so this prevents stale progress from being presented as
	// active after a heartbeat stream stops entirely.
	switch live.WorkerConnectionState {
	case "", "CONNECTED":
		// Empty preserves compatibility with older adapters/fixtures that do
		// not expose the worker connection-state column.
	default:
		return false
	}

	latestAttemptNumber := 0
	for _, attempt := range attempts {
		if attempt.AttemptNumber > latestAttemptNumber {
			latestAttemptNumber = attempt.AttemptNumber
		}
	}

	// During the Claim→Accept visibility window the durable attempt list can
	// briefly lag the runtime row. Allow a newer live attempt through, but
	// never resurrect an older attempt after a retry has been created.
	if live.AttemptNumber < latestAttemptNumber || live.AttemptNumber < task.AttemptCount {
		return false
	}
	for _, attempt := range attempts {
		if attempt.ID == live.AttemptID {
			// Durable terminal state is strictly authoritative. This check
			// is deliberately independent of task status and row ordering.
			return !attempt.Status.IsTerminal()
		}
	}
	// A runtime row can become visible between claim/accept and durable
	// attempt persistence. Permit that narrow window, but never an older
	// attempt (guarded above by attempt number).
	return true
}

func liveAttemptStatus(live *LiveAttempt) taskattempts.AttemptStatus {
	if live == nil {
		return taskattempts.AttemptStatusRunning
	}
	// Runtime phases such as UPLOADING and FINALIZING are deliberately
	// richer than the durable AttemptStatus enum. The admin summary keeps
	// the durable wire contract and reports every eligible non-terminal
	// runtime phase as RUNNING.
	return taskattempts.AttemptStatusRunning
}

func liveAttemptMetrics(live *LiveAttempt) *taskattempts.AttemptMetrics {
	if live == nil {
		return nil
	}
	metrics := &taskattempts.AttemptMetrics{
		AttemptID:         live.AttemptID,
		FramesEncoded:     live.FramesEncoded,
		FramesDecoded:     live.FramesDecoded,
		FramesComposited:  live.FramesComposited,
		FFmpegSpeedRatio:  live.FFmpegSpeedX,
		SceneCount:        live.TotalScenes,
		SegmentCount:      live.TotalSegments,
		CompletedSegments: live.CurrentSegment,
		WallClockSeconds:  float64(live.ElapsedMS) / 1000,
	}
	for key, value := range live.CumulativeMetrics {
		switch key {
		case "input_bytes":
			metrics.InputBytes = int64Value(value)
		case "output_bytes":
			metrics.OutputBytes = int64Value(value)
		case "bytes_from_drive":
			metrics.BytesFromDrive = int64Value(value)
		case "bytes_from_blobstore":
			metrics.BytesFromBlobstore = int64Value(value)
		case "bytes_from_local_cache":
			metrics.BytesFromLocalCache = int64Value(value)
		case "cpu_time_ms":
			metrics.CPUTimeMS = int64Value(value)
		case "gpu_time_ms":
			metrics.GPUTimeMS = int64Value(value)
		case "peak_rss_bytes":
			metrics.PeakRSSBytes = int64Value(value)
		case "peak_vram_bytes":
			metrics.PeakVRAMBytes = int64Value(value)
		case "frames_encoded":
			metrics.FramesEncoded = int64Value(value)
		case "frames_decoded":
			metrics.FramesDecoded = int64Value(value)
		case "frames_composited":
			metrics.FramesComposited = int64Value(value)
		case "ffmpeg_speed_ratio", "ffmpeg_speed_x":
			metrics.FFmpegSpeedRatio = float64Value(value)
		case "pipeline_render_ms":
			metrics.PipelineRenderMs = int64Value(value)
		case "pipeline_total_ms":
			metrics.PipelineTotalMs = int64Value(value)
		}
	}
	return metrics
}

func int64Value(value any) int64 {
	switch number := value.(type) {
	case int:
		return int64(number)
	case int64:
		return number
	case float64:
		return int64(number)
	case float32:
		return int64(number)
	default:
		return 0
	}
}

func float64Value(value any) float64 {
	switch number := value.(type) {
	case int:
		return float64(number)
	case int64:
		return float64(number)
	case float64:
		return number
	case float32:
		return float64(number)
	default:
		return 0
	}
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
