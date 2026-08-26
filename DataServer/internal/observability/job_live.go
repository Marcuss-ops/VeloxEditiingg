package observability

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Stall detection thresholds. Overridable via environment variables for
// deployment-specific tuning.
//
// VELOC_STALL_NO_PROGRESS_MS: when progress_age exceeds this threshold
// AND the worker is alive, the job is considered stalled. Default: 30s.
//
// VELOC_STALL_HEARTBEAT_FRESH_MS: when heartbeat_age is below this
// threshold, the worker is considered alive. Default: 5s.
//
// VELOC_STAGNATION_WATERMARK_MS: when the same phase+percent persists
// for longer than this threshold, the job is considered stagnant even
// if progress_age is being refreshed (e.g., heartbeat-only updates).
// Default: 120s.
const (
	defaultStallNoProgressMS     int64 = 30000  // 30 seconds
	defaultStallHeartbeatFreshMS int64 = 5000   // 5 seconds
	defaultStagnationWatermarkMS int64 = 120000 // 2 minutes
)

// JobLiveStatus is the compact, lightweight live snapshot of a job's current
// state. It is the "tell me what's happening right now" endpoint designed
// for polling by fleetctl, dashboards, and Codex.
//
// All data is sourced from existing projections (worker_task_runtime,
// workers table), never a second telemetry store.
type JobLiveStatus struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`

	Worker *JobLiveWorker `json:"worker,omitempty"`

	Execution *JobLiveExecution `json:"execution,omitempty"`

	Assets *JobLiveAssets `json:"assets,omitempty"`

	Publication *JobLivePublication `json:"publication,omitempty"`

	Stalled           bool         `json:"stalled"`
	StallReason       string       `json:"stall_reason,omitempty"`
	StallDetail       *StallDetail `json:"stall_detail,omitempty"`
	ProgressAgeMS     int64        `json:"progress_age_ms"`
	HeartbeatAgeMS    int64        `json:"heartbeat_age_ms,omitempty"`
	LastProgressAgeMS int64        `json:"last_progress_age_ms"`
}

// StallDetail provides granular stall diagnosis. It is populated when
// stalled=true and explains WHY the job is stuck, for how long, and
// what the worker's state is.
type StallDetail struct {
	// Reason is the machine-readable stall classification:
	//   "no_progress"     — worker alive, job progress frozen
	//   "worker_offline"  — worker heartbeat stale/disconnected
	//   "stagnation"      — same phase+percent for too long
	Reason string `json:"reason"`

	// ProgressFrozenMS is how long progress has been stuck at the
	// current value (derived from last_progress_at).
	ProgressFrozenMS int64 `json:"progress_frozen_ms"`

	// HeartbeatAgeMS is the worker's heartbeat age at stall detection time.
	HeartbeatAgeMS int64 `json:"heartbeat_age_ms"`

	// StagnationMS is how long the current phase+percent combo has
	// persisted. Only set when reason="stagnation".
	StagnationMS int64 `json:"stagnation_ms,omitempty"`

	// FrozenPhase is the phase that is stuck.
	FrozenPhase string `json:"frozen_phase"`

	// FrozenPercent is the percent that is stuck.
	FrozenPercent int `json:"frozen_percent"`
}

// JobLiveWorker contains the worker identity and liveness signal.
type JobLiveWorker struct {
	WorkerID       string `json:"worker_id"`
	WorkerName     string `json:"worker_name,omitempty"`
	Connection     string `json:"connection"`
	HeartbeatAgeMS int64  `json:"heartbeat_age_ms"`
}

// JobLiveExecution contains the current execution progress.
type JobLiveExecution struct {
	Phase            string  `json:"phase"`
	OperationalPhase string  `json:"operational_phase,omitempty"`
	Percent          int     `json:"percent"`
	Scene            int     `json:"scene"`
	ScenesTotal      int     `json:"scenes_total"`
	Segment          int     `json:"segment"`
	SegmentsTotal    int     `json:"segments_total"`
	ElapsedMS        int64   `json:"elapsed_ms"`
	FramesDecoded    int64   `json:"frames_decoded"`
	FramesComposited int64   `json:"frames_composited"`
	FramesEncoded    int64   `json:"frames_encoded"`
	SpeedX           float64 `json:"speed_x"`
}

// JobLiveAssets contains the aggregated asset download progress for the job.
type JobLiveAssets struct {
	Percent         float64 `json:"percent"`
	BytesDownloaded int64   `json:"bytes_downloaded"`
	BytesTotal      int64   `json:"bytes_total"`
	ThroughputBPS   float64 `json:"throughput_bytes_per_second"`
	ETASeconds      int64   `json:"eta_seconds"`
	Ready           int     `json:"ready"`
	Downloading     int     `json:"downloading"`
	Queued          int     `json:"queued"`
	Failed          int     `json:"failed"`
	CacheHits       int     `json:"cache_hits"`
	Total           int     `json:"total"`
}

// JobLivePublication contains the upload/commit progress for the job.
// All fields are derived from the upload progress metrics stored in
// cumulative_metrics_json by the worker's UpdateOperationalPhase and
// updateUploadProgress helpers.
type JobLivePublication struct {
	State            string  `json:"state"` // NOT_STARTED | UPLOADING | COMMITTED
	UploadBytes      int64   `json:"upload_bytes,omitempty"`
	UploadTotalBytes int64   `json:"upload_total_bytes,omitempty"`
	UploadPercent    float64 `json:"upload_percent,omitempty"`
	UploadBPS        float64 `json:"upload_bytes_per_second,omitempty"`
	UploadETASeconds float64 `json:"upload_eta_seconds,omitempty"`
	ArtifactIndex    int     `json:"upload_artifact_index,omitempty"`
	ArtifactTotal    int     `json:"upload_artifact_total,omitempty"`
}

// JobLive returns the compact live status for a job. It reads from the
// volatile worker_task_runtime projection and the workers table, with no
// persistence or side effects.
func (s *Service) JobLive(ctx context.Context, jobID string) (*JobLiveStatus, error) {
	if s == nil || s.jobs == nil {
		return nil, fmt.Errorf("observability: job reader not configured")
	}
	if jobID == "" {
		return nil, fmt.Errorf("observability: job id is required")
	}

	job, err := s.jobs.Get(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("observability: get job: %w", err)
	}
	if job == nil {
		return nil, fmt.Errorf("observability: job %s not found", jobID)
	}

	task, err := s.tasks.GetByJobID(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("observability: get task: %w", err)
	}

	result := &JobLiveStatus{
		JobID:  jobID,
		Status: string(job.Status),
	}

	if task == nil {
		return result, nil
	}

	// Read the volatile worker_task_runtime row.
	var live *LiveAttempt
	if s.liveAttempts != nil {
		if taskReader, ok := s.liveAttempts.(LiveAttemptTaskReader); ok {
			live, err = taskReader.GetWorkerTaskRuntimeByTask(ctx, task.ID, jobID)
		} else {
			live, err = s.liveAttempts.GetWorkerTaskRuntimeByJob(ctx, jobID)
		}
		if err != nil {
			return nil, fmt.Errorf("observability: live runtime: %w", err)
		}
	}

	if live == nil || live.AttemptID == "" {
		return result, nil
	}

	now := time.Now()

	// Worker liveness.
	result.Worker = &JobLiveWorker{
		WorkerID:   live.WorkerID,
		WorkerName: s.workerDisplayName(live.WorkerID),
		Connection: live.WorkerConnectionState,
	}
	if live.UpdatedAt != "" {
		if t, ok := parseLiveTime(live.UpdatedAt); ok {
			result.Worker.HeartbeatAgeMS = now.Sub(t).Milliseconds()
			result.HeartbeatAgeMS = result.Worker.HeartbeatAgeMS
		}
	}

	// Execution progress.
	result.Execution = &JobLiveExecution{
		Phase:            live.ProgressPhase,
		Percent:          live.ProgressPercent,
		Scene:            live.CurrentScene,
		ScenesTotal:      live.TotalScenes,
		Segment:          live.CurrentSegment,
		SegmentsTotal:    live.TotalSegments,
		ElapsedMS:        live.ElapsedMS,
		FramesDecoded:    live.FramesDecoded,
		FramesComposited: live.FramesComposited,
		FramesEncoded:    live.FramesEncoded,
		SpeedX:           live.FFmpegSpeedX,
	}

	// Operational phase and upload progress from CumulativeMetrics.
	if live.CumulativeMetrics != nil {
		if opPhase, ok := live.CumulativeMetrics["operational_phase"].(string); ok && opPhase != "" {
			result.Execution.OperationalPhase = opPhase
		}
		// Upload / publication progress.
		uploadBytes := int64(cumulativeFloat(live.CumulativeMetrics, "upload_bytes"))
		uploadTotal := int64(cumulativeFloat(live.CumulativeMetrics, "upload_total_bytes"))
		if uploadTotal > 0 || uploadBytes > 0 {
			result.Publication = &JobLivePublication{
				State:            "UPLOADING",
				UploadBytes:      uploadBytes,
				UploadTotalBytes: uploadTotal,
				UploadPercent:    cumulativeFloat(live.CumulativeMetrics, "upload_percent"),
				UploadBPS:        cumulativeFloat(live.CumulativeMetrics, "upload_bytes_per_second"),
				UploadETASeconds: cumulativeFloat(live.CumulativeMetrics, "upload_eta_seconds"),
				ArtifactIndex:    int(cumulativeFloat(live.CumulativeMetrics, "upload_artifact_index")),
				ArtifactTotal:    int(cumulativeFloat(live.CumulativeMetrics, "upload_artifact_total")),
			}
		}
	}

	// ── Enhanced stall detection ─────────────────────────────────
	stallNoProgressMS := stallThreshold("VELOC_STALL_NO_PROGRESS_MS", defaultStallNoProgressMS)
	stallFreshMS := stallThreshold("VELOC_STALL_HEARTBEAT_FRESH_MS", defaultStallHeartbeatFreshMS)
	stagnationMS := stallThreshold("VELOC_STAGNATION_WATERMARK_MS", defaultStagnationWatermarkMS)

	result.Stalled, result.StallReason, result.StallDetail = detectStall(
		live, result.HeartbeatAgeMS, now,
		stallNoProgressMS, stallFreshMS, stagnationMS,
	)

	// Asset download progress.
	if s.assetProgress != nil {
		assets, assetErr := s.assetProgress.ListAssetDownloadProgressForJob(ctx, jobID)
		if assetErr == nil && len(assets) > 0 {
			var downloaded, total int64
			var throughput float64
			var etaMax int64
			ready, downloading, queued, failed, cacheHits := 0, 0, 0, 0, 0
			for _, a := range assets {
				total += a.BytesTotal
				downloaded += a.BytesDownloaded
				switch a.State {
				case "READY", "CACHE_HIT":
					ready++
					if a.CacheHit {
						cacheHits++
					}
					if a.BytesTotal > a.BytesDownloaded {
						downloaded += a.BytesTotal - a.BytesDownloaded
					}
				case "DOWNLOADING":
					downloading++
					if a.BytesPerSecond > 0 {
						throughput += a.BytesPerSecond
					}
					if a.ETASeconds > etaMax {
						etaMax = a.ETASeconds
					}
				case "QUEUED", "CACHE_CHECK", "RETRY_WAIT":
					queued++
				case "FAILED", "CANCELLED":
					failed++
				}
			}
			var percent float64
			if total > 0 {
				percent = float64(downloaded) / float64(total) * 100
			}
			result.Assets = &JobLiveAssets{
				Percent:         percent,
				BytesDownloaded: downloaded,
				BytesTotal:      total,
				ThroughputBPS:   throughput,
				ETASeconds:      etaMax,
				Ready:           ready,
				Downloading:     downloading,
				Queued:          queued,
				Failed:          failed,
				CacheHits:       cacheHits,
				Total:           len(assets),
			}
		}
	}

	return result, nil
}

// detectStall evaluates three stall conditions and returns the first
// one that fires, in priority order:
//
//  1. Worker offline: heartbeat stale AND progress frozen → "worker_offline"
//  2. No progress: worker alive but progress frozen → "no_progress"
//  3. Stagnation: same phase+percent for too long → "stagnation"
//
// When none fire, stalled=false.
func detectStall(
	live *LiveAttempt,
	heartbeatAgeMS int64,
	now time.Time,
	stallNoProgressMS, stallFreshMS, stagnationMS int64,
) (bool, string, *StallDetail) {
	if live == nil || live.LastProgressAt == "" {
		return false, "", nil
	}

	progressTime, ok := parseLiveTime(live.LastProgressAt)
	if !ok {
		return false, "", nil
	}
	progressAgeMS := now.Sub(progressTime).Milliseconds()
	if progressAgeMS <= 0 {
		return false, "", nil
	}

	frozenPhase := live.ProgressPhase
	frozenPercent := live.ProgressPercent

	// 1. Worker offline: heartbeat stale AND progress frozen.
	workerAlive := heartbeatAgeMS > 0 && heartbeatAgeMS <= stallFreshMS
	workerConnected := live.WorkerConnectionState == "CONNECTED"

	if progressAgeMS > stallNoProgressMS && !workerAlive {
		return true, "worker_offline", &StallDetail{
			Reason:           "worker_offline",
			ProgressFrozenMS: progressAgeMS,
			HeartbeatAgeMS:   heartbeatAgeMS,
			FrozenPhase:      frozenPhase,
			FrozenPercent:    frozenPercent,
		}
	}

	// 2. No progress: worker alive but progress frozen.
	if progressAgeMS > stallNoProgressMS && (workerConnected || workerAlive) {
		return true, "no_progress", &StallDetail{
			Reason:           "no_progress",
			ProgressFrozenMS: progressAgeMS,
			HeartbeatAgeMS:   heartbeatAgeMS,
			FrozenPhase:      frozenPhase,
			FrozenPercent:    frozenPercent,
		}
	}

	// 3. Stagnation: same phase+percent for too long.
	// Even if progress_age is being refreshed by heartbeat-only updates,
	// the actual work may be stuck at the same phase+percent.
	if stagnationMS > 0 && progressAgeMS > stagnationMS {
		return true, "stagnation", &StallDetail{
			Reason:           "stagnation",
			ProgressFrozenMS: progressAgeMS,
			HeartbeatAgeMS:   heartbeatAgeMS,
			StagnationMS:     progressAgeMS,
			FrozenPhase:      frozenPhase,
			FrozenPercent:    frozenPercent,
		}
	}

	return false, "", nil
}

// cumulativeFloat extracts a float64 from a CumulativeMetrics map,
// tolerating float64, int, int64 values. Returns 0 for missing keys.
func cumulativeFloat(m map[string]any, key string) float64 {
	if m == nil {
		return 0
	}
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	}
	return 0
}

// parseLiveTime attempts to parse a timestamp string produced by the
// worker heartbeat pipeline. It handles RFC3339Nano (primary), common
// SQL datetime formats, and falls back gracefully.
func parseLiveTime(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// stallThreshold reads an environment variable as milliseconds, falling
// back to the provided default. Returns the default when the env var is
// unset, empty, or not a valid positive integer.
func stallThreshold(envKey string, defaultMS int64) int64 {
	if v := os.Getenv(envKey); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return defaultMS
}
