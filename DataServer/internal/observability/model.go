// Package observability provides read-only aggregation and diagnostics
// for task execution. It exposes bounded internal diagnostics only;
// no UI. All data is sourced from repositories, never direct SQL.
package observability

import (
	"time"

	sharedtelemetry "velox-shared/telemetry"
	"velox-server/internal/jobs"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
)

// LiveAttempt is the volatile worker_task_runtime projection used only as
// an overlay while durable task_attempts data is unavailable or non-terminal.
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
	// AttemptMilestones is the worker's monotonic milestone timeline
	// (elapsed_ms since attempt start), live-only while the runtime row
	// exists. The durable attempt report is the authority after completion.
	AttemptMilestones []sharedtelemetry.AttemptMilestoneSample
	StartedAt         string
	LastProgressAt    string
	UpdatedAt         string
}

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
	DeliveryID       string  `json:"delivery_id"`
	ArtifactID       string  `json:"artifact_id"`
	DestinationID    string  `json:"destination_id"`
	Status           string  `json:"status"`
	RemoteID         string  `json:"remote_id,omitempty"`
	RemoteURL        string  `json:"remote_url,omitempty"`
	AttemptCount     int     `json:"attempt_count"`
	RetryCount       int     `json:"retry_count"`
	MaxAttempts      int     `json:"max_attempts"`
	LastError        string  `json:"last_error,omitempty"`
	LastErrorMessage string  `json:"last_error_message,omitempty"`
	QueuedAt         string  `json:"queued_at,omitempty"`
	StartedAt        string  `json:"started_at,omitempty"`
	CompletedAt      string  `json:"completed_at,omitempty"`
	QueueMS          int64   `json:"queue_ms,omitempty"`
	UploadMS         int64   `json:"upload_ms,omitempty"`
	TotalMS          int64   `json:"total_ms,omitempty"`
	BytesUploaded    int64   `json:"bytes_uploaded,omitempty"`
	UploadMbps       float64 `json:"upload_mbps,omitempty"`
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
// Attempts is the canonical reconciled list; top-level live fields are derived
// from its one eligible Live row and therefore cannot diverge from that list.
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
	Hits                  int64 `json:"hits"`
	Misses                int64 `json:"misses"`
	Evictions             int64 `json:"evictions"`
	Corruptions           int64 `json:"corruptions"`
	BytesUsed             int64 `json:"bytes_used"`
	Entries               int64 `json:"entries"`
	Lookups               int64 `json:"lookups"`
	UniqueAssetsRequested int64 `json:"unique_assets_requested"`
	// Phase A1.5: attempt-scoped asset download volume, hoisted from the
	// worker's CacheResolver into task_attempt_cache_stats (migration 147).
	DownloadCount int64   `json:"download_count"`
	DownloadBytes int64   `json:"download_bytes"`
	HitRatio      float64 `json:"hit_ratio"`
}

// AttemptSummary is the aggregated diagnostics for a single attempt.
// Durable fields remain authoritative; Live marks a temporary overlay of
// volatile progress fields and does not change durable status or final metrics.
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
	AttemptMilestones      []sharedtelemetry.AttemptMilestoneSample `json:"attempt_milestones,omitempty"`
	Waterfall              []WaterfallStage                `json:"waterfall,omitempty"`
	WaterfallValid         bool                            `json:"waterfall_valid,omitempty"`
	AttemptWaterfall       *AttemptWaterfall               `json:"attempt_waterfall,omitempty"`
}

type WaterfallStage struct {
	Name        string    `json:"name"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	DurationMS  int64     `json:"duration_ms"`
	Status      string    `json:"status,omitempty"`
}
