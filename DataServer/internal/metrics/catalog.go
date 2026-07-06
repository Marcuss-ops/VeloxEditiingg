// Package metrics / catalog.go
//
// MetricCatalog is the central, single-source-of-truth registry for every
// canonical metric name Velox emits. No other package may invent metric
// names — all producers (workers, pipeline runners, engine sidecar, master
// supervisors) MUST use names from this catalog. The registry is enforced
// at registration time: unknown names are rejected.
//
// Adding a new metric:
//   1. Add an entry to MetricCatalog below with a unique key.
//   2. Run tests — TestCatalog_NoDuplicateNames catches collisions.
//   3. Add the validation test in TestCatalog_RequiredMetricsExist.
//   4. Update docs/metrics-catalog.md.
//
// Naming convention:
//   - Lowercase, dot-separated: <component>.<metric>[_unit]
//   - Unit suffix is part of the name (e.g. _ms, _bytes, _ratio)
//   - Components: engine, pipeline, native, output, cache, blob, ffmpeg,
//     video, queue, lease, resource, input, waste, error, worker, taskrunner
package metrics

import "sort"

// MetricDefinition is the canonical descriptor for one metric name.
// Every metric Velox emits MUST have a corresponding entry in MetricCatalog.
type MetricDefinition struct {
	// Name is the canonical dotted-key name (e.g. "engine.segment_build_ms").
	Name string
	// Unit is the SI-suffixed unit (ms, bytes, ratio, count, fps, seconds, items, tracks).
	Unit string
	// Component is the subsystem that produces this metric (engine, pipeline, native, etc.).
	Component string
	// Description is a human-readable explanation of what this metric measures.
	Description string
	// Kind indicates the metric type for the catalog consumer (counter, gauge, histogram).
	Kind CatalogMetricKind
}

// CatalogMetricKind mirrors the Prometheus family type for catalog
// consumers that need to know if a metric is cumulative or instantaneous.
type CatalogMetricKind string

const (
	KindCounter   CatalogMetricKind = "counter"
	KindGauge     CatalogMetricKind = "gauge"
	KindHistogram CatalogMetricKind = "histogram"
)

// Component constants for the MetricCatalog entries.
const (
	CompEngine     = "engine"
	CompPipeline   = "pipeline"
	CompNative     = "native"
	CompOutput     = "output"
	CompCache      = "cache"
	CompBlob       = "blob"
	CompFFmpeg     = "ffmpeg"
	CompVideo      = "video"
	CompQueue      = "queue"
	CompLease      = "lease"
	CompResource   = "resource"
	CompInput      = "input"
	CompWaste      = "waste"
	CompError      = "error"
	CompWorker     = "worker"
	CompTaskRunner = "taskrunner"
	CompMaster     = "master"
	CompCost       = "cost"
	CompPlacement  = "placement"
	CompConflict   = "conflict"
	CompReconcile  = "reconcile"
	CompScorecard  = "scorecard"
)

// MetricCatalog is the central registry of every canonical metric name.
// The key is the dotted metric name; the value is its full definition.
// Tests verify no duplicates and that required families are present.
var MetricCatalog = map[string]MetricDefinition{
	// ── Engine phases (C++ sidecar) ──────────────────────────────────────
	"engine.asset_download_ms": {
		Name: "engine.asset_download_ms", Unit: "ms", Component: CompEngine, Kind: KindHistogram,
		Description: "Time spent downloading assets (clips, images, audio) in the C++ engine",
	},
	"engine.segment_build_ms": {
		Name: "engine.segment_build_ms", Unit: "ms", Component: CompEngine, Kind: KindHistogram,
		Description: "Time spent building video segments (FFmpeg encode per segment) in the C++ engine",
	},
	"engine.concat_ms": {
		Name: "engine.concat_ms", Unit: "ms", Component: CompEngine, Kind: KindHistogram,
		Description: "Time spent concatenating segments into the final output",
	},
	"engine.mux_audio_ms": {
		Name: "engine.mux_audio_ms", Unit: "ms", Component: CompEngine, Kind: KindHistogram,
		Description: "Time spent muxing audio tracks into the final output",
	},
	"engine.copy_final_ms": {
		Name: "engine.copy_final_ms", Unit: "ms", Component: CompEngine, Kind: KindHistogram,
		Description: "Time spent copying the final rendered file to the output destination",
	},
	"engine.audio_download_ms": {
		Name: "engine.audio_download_ms", Unit: "ms", Component: CompEngine, Kind: KindHistogram,
		Description: "Time spent downloading audio-only assets",
	},
	"engine.frames": {
		Name: "engine.frames", Unit: "count", Component: CompEngine, Kind: KindCounter,
		Description: "Total frames processed by the C++ engine in this attempt",
	},
	"engine.fps": {
		Name: "engine.fps", Unit: "fps", Component: CompEngine, Kind: KindGauge,
		Description: "Last-observed frames-per-second from the C++ engine",
	},
	"engine.speed_x": {
		Name: "engine.speed_x", Unit: "ratio", Component: CompEngine, Kind: KindGauge,
		Description: "Render speed multiplier vs realtime (>1 means faster than realtime)",
	},
	"engine.encode_passes": {
		Name: "engine.encode_passes", Unit: "count", Component: CompEngine, Kind: KindCounter,
		Description: "Number of encode passes performed by the C++ engine",
	},
	"engine.temp_bytes": {
		Name: "engine.temp_bytes", Unit: "bytes", Component: CompEngine, Kind: KindGauge,
		Description: "Temporary bytes written by the C++ engine during rendering",
	},
	"engine.duration_seconds": {
		Name: "engine.duration_seconds", Unit: "seconds", Component: CompEngine, Kind: KindGauge,
		Description: "Total media duration processed by the C++ engine",
	},
	"engine.concat_mode": {
		Name: "engine.concat_mode", Unit: "string", Component: CompEngine, Kind: KindGauge,
		Description: "Concat mode used (stream_copy, reencode, or n/a)",
	},
	"engine.bitrate": {
		Name: "engine.bitrate", Unit: "bps", Component: CompEngine, Kind: KindGauge,
		Description: "Output bitrate from the C++ engine",
	},
	"engine.dup_frames": {
		Name: "engine.dup_frames", Unit: "count", Component: CompEngine, Kind: KindCounter,
		Description: "Duplicate frames detected during encoding",
	},
	"engine.drop_frames": {
		Name: "engine.drop_frames", Unit: "count", Component: CompEngine, Kind: KindCounter,
		Description: "Dropped frames during encoding",
	},
	"engine.segment_duration_ms": {
		Name: "engine.segment_duration_ms", Unit: "ms", Component: CompEngine, Kind: KindHistogram,
		Description: "Per-segment duration from the C++ engine sidecar segments[] array",
	},

	// ── Pipeline phases (Go pipeline runner) ─────────────────────────────
	"pipeline.id": {
		Name: "pipeline.id", Unit: "string", Component: CompPipeline, Kind: KindGauge,
		Description: "Pipeline identifier selected for this task (e.g. hybrid.v1)",
	},
	"pipeline.resolve_ms": {
		Name: "pipeline.resolve_ms", Unit: "ms", Component: CompPipeline, Kind: KindHistogram,
		Description: "Time spent resolving the pipeline for the given task spec",
	},
	"pipeline.validate_ms": {
		Name: "pipeline.validate_ms", Unit: "ms", Component: CompPipeline, Kind: KindHistogram,
		Description: "Time spent validating pipeline input parameters",
	},
	"pipeline.compile_ms": {
		Name: "pipeline.compile_ms", Unit: "ms", Component: CompPipeline, Kind: KindHistogram,
		Description: "Time spent compiling the render plan from the pipeline spec",
	},
	"pipeline.render_ms": {
		Name: "pipeline.render_ms", Unit: "ms", Component: CompPipeline, Kind: KindHistogram,
		Description: "Time spent in the render phase (orchestrating the C++ engine)",
	},
	"pipeline.total_ms": {
		Name: "pipeline.total_ms", Unit: "ms", Component: CompPipeline, Kind: KindHistogram,
		Description: "Total wall-clock time spent in the pipeline (resolve + compile + render)",
	},
	"pipeline.timeline_items": {
		Name: "pipeline.timeline_items", Unit: "items", Component: CompPipeline, Kind: KindCounter,
		Description: "Number of timeline items (clips, images, entities) in the render plan",
	},
	"pipeline.audio_tracks": {
		Name: "pipeline.audio_tracks", Unit: "tracks", Component: CompPipeline, Kind: KindCounter,
		Description: "Number of audio tracks processed in the pipeline",
	},

	// ── Native process (C++ engine subprocess) ───────────────────────────
	"native.total_ms": {
		Name: "native.total_ms", Unit: "ms", Component: CompNative, Kind: KindHistogram,
		Description: "Total time the native C++ engine process was running",
	},
	"native.plan_write_ms": {
		Name: "native.plan_write_ms", Unit: "ms", Component: CompNative, Kind: KindHistogram,
		Description: "Time spent writing the render plan to disk for the native process",
	},
	"native.process_wait_ms": {
		Name: "native.process_wait_ms", Unit: "ms", Component: CompNative, Kind: KindHistogram,
		Description: "Time spent waiting for the native C++ engine process to exit",
	},

	// ── Output metrics ───────────────────────────────────────────────────
	"output.bytes": {
		Name: "output.bytes", Unit: "bytes", Component: CompOutput, Kind: KindCounter,
		Description: "Total output bytes produced by this attempt",
	},
	"output.hash_ms": {
		Name: "output.hash_ms", Unit: "ms", Component: CompOutput, Kind: KindHistogram,
		Description: "Time spent computing SHA-256 hash of the output file",
	},
	"output.ffprobe_valid": {
		Name: "output.ffprobe_valid", Unit: "boolean", Component: CompOutput, Kind: KindGauge,
		Description: "Whether ffprobe validation passed on the output file",
	},
	"output.duration_diff_sec": {
		Name: "output.duration_diff_sec", Unit: "seconds", Component: CompOutput, Kind: KindGauge,
		Description: "Absolute difference between expected and actual output duration",
	},
	"output.has_video_stream": {
		Name: "output.has_video_stream", Unit: "boolean", Component: CompOutput, Kind: KindGauge,
		Description: "Whether the output contains a video stream",
	},
	"output.has_audio_stream": {
		Name: "output.has_audio_stream", Unit: "boolean", Component: CompOutput, Kind: KindGauge,
		Description: "Whether the output contains an audio stream",
	},
	"output.file_size": {
		Name: "output.file_size", Unit: "bytes", Component: CompOutput, Kind: KindGauge,
		Description: "File size of the rendered output in bytes",
	},
	"output.black_frame_ratio": {
		Name: "output.black_frame_ratio", Unit: "ratio", Component: CompOutput, Kind: KindGauge,
		Description: "Ratio of black frames detected in the output (quality check)",
	},
	"output.audio_sync_offset_ms": {
		Name: "output.audio_sync_offset_ms", Unit: "ms", Component: CompOutput, Kind: KindGauge,
		Description: "Audio-video sync offset measured in the output",
	},

	// ── Cache metrics ────────────────────────────────────────────────────
	"cache.hits": {
		Name: "cache.hits", Unit: "count", Component: CompCache, Kind: KindCounter,
		Description: "Number of local cache hit events",
	},
	"cache.misses": {
		Name: "cache.misses", Unit: "count", Component: CompCache, Kind: KindCounter,
		Description: "Number of local cache miss events",
	},
	"cache.evictions": {
		Name: "cache.evictions", Unit: "count", Component: CompCache, Kind: KindCounter,
		Description: "Number of cache eviction events",
	},
	"cache.corruptions": {
		Name: "cache.corruptions", Unit: "count", Component: CompCache, Kind: KindCounter,
		Description: "Number of cache corruption events detected",
	},
	"cache.entries": {
		Name: "cache.entries", Unit: "count", Component: CompCache, Kind: KindGauge,
		Description: "Current number of entries in the local cache",
	},
	"cache.bytes": {
		Name: "cache.bytes", Unit: "bytes", Component: CompCache, Kind: KindGauge,
		Description: "Current size of the local cache in bytes",
	},
	"cache.pinned": {
		Name: "cache.pinned", Unit: "count", Component: CompCache, Kind: KindGauge,
		Description: "Number of pinned (non-evictable) entries in the local cache",
	},
	"cache.asset_hit_count": {
		Name: "cache.asset_hit_count", Unit: "count", Component: CompCache, Kind: KindCounter,
		Description: "Number of asset cache hits (granular per-category counter)",
	},
	"cache.asset_miss_count": {
		Name: "cache.asset_miss_count", Unit: "count", Component: CompCache, Kind: KindCounter,
		Description: "Number of asset cache misses (granular per-category counter)",
	},
	"cache.blob_hit_count": {
		Name: "cache.blob_hit_count", Unit: "count", Component: CompCache, Kind: KindCounter,
		Description: "Number of blob cache hits (granular per-category counter)",
	},
	"cache.blob_miss_count": {
		Name: "cache.blob_miss_count", Unit: "count", Component: CompCache, Kind: KindCounter,
		Description: "Number of blob cache misses (granular per-category counter)",
	},
	"cache.render_hit_count": {
		Name: "cache.render_hit_count", Unit: "count", Component: CompCache, Kind: KindCounter,
		Description: "Number of render cache hits (granular per-category counter)",
	},
	"cache.byte_hit_ratio": {
		Name: "cache.byte_hit_ratio", Unit: "ratio", Component: CompCache, Kind: KindGauge,
		Description: "Ratio of bytes served from local cache vs total bytes downloaded",
	},
	"cache.requests_total": {
		Name: "cache.requests_total", Unit: "count", Component: CompCache, Kind: KindCounter,
		Description: "Total cache requests by result (hit, miss, corrupt)",
	},
	"cache.bytes_total": {
		Name: "cache.bytes_total", Unit: "bytes", Component: CompCache, Kind: KindCounter,
		Description: "Total cache bytes by result (hit, miss)",
	},

	// ── Blob store metrics ───────────────────────────────────────────────
	"blob.publish": {
		Name: "blob.publish", Unit: "count", Component: CompBlob, Kind: KindCounter,
		Description: "Number of successful blob publish operations",
	},
	"blob.publish_failed": {
		Name: "blob.publish_failed", Unit: "count", Component: CompBlob, Kind: KindCounter,
		Description: "Number of failed blob publish operations",
	},
	"blob.fetch": {
		Name: "blob.fetch", Unit: "count", Component: CompBlob, Kind: KindCounter,
		Description: "Number of successful blob fetch operations",
	},
	"blob.fetch_miss": {
		Name: "blob.fetch_miss", Unit: "count", Component: CompBlob, Kind: KindCounter,
		Description: "Number of blob fetch misses (blob not found)",
	},
	"blob.fetch_corruption": {
		Name: "blob.fetch_corruption", Unit: "count", Component: CompBlob, Kind: KindCounter,
		Description: "Number of blob fetch corruption events detected",
	},
	"blob.entries": {
		Name: "blob.entries", Unit: "count", Component: CompBlob, Kind: KindGauge,
		Description: "Current number of entries in the blob store",
	},
	"blob.bytes": {
		Name: "blob.bytes", Unit: "bytes", Component: CompBlob, Kind: KindGauge,
		Description: "Current size of the blob store in bytes",
	},

	// ── FFmpeg metrics ───────────────────────────────────────────────────
	"ffmpeg.speed_ratio": {
		Name: "ffmpeg.speed_ratio", Unit: "ratio", Component: CompFFmpeg, Kind: KindGauge,
		Description: "FFmpeg encoding speed vs realtime (>1 means faster than realtime)",
	},
	"ffmpeg.fps": {
		Name: "ffmpeg.fps", Unit: "fps", Component: CompFFmpeg, Kind: KindGauge,
		Description: "Last-observed FFmpeg frames-per-second encoding rate",
	},
	"ffmpeg.frames_processed": {
		Name: "ffmpeg.frames_processed", Unit: "count", Component: CompFFmpeg, Kind: KindCounter,
		Description: "Total frames processed by FFmpeg as observed from -progress",
	},
	"ffmpeg.encode_duration_ms": {
		Name: "ffmpeg.encode_duration_ms", Unit: "ms", Component: CompFFmpeg, Kind: KindHistogram,
		Description: "FFmpeg encode duration per segment or pass",
	},
	"ffmpeg.decode_duration_ms": {
		Name: "ffmpeg.decode_duration_ms", Unit: "ms", Component: CompFFmpeg, Kind: KindHistogram,
		Description: "FFmpeg decode duration per segment",
	},
	"ffmpeg.dropped_frames": {
		Name: "ffmpeg.dropped_frames", Unit: "count", Component: CompFFmpeg, Kind: KindCounter,
		Description: "Dropped frames as observed from FFmpeg -progress",
	},
	"ffmpeg.duplicated_frames": {
		Name: "ffmpeg.duplicated_frames", Unit: "count", Component: CompFFmpeg, Kind: KindCounter,
		Description: "Duplicated frames as observed from FFmpeg -progress",
	},
	"ffmpeg.exit_code": {
		Name: "ffmpeg.exit_code", Unit: "code", Component: CompFFmpeg, Kind: KindCounter,
		Description: "FFmpeg process exit codes by value",
	},
	"ffmpeg.restarts": {
		Name: "ffmpeg.restarts", Unit: "count", Component: CompFFmpeg, Kind: KindCounter,
		Description: "FFmpeg process restart count",
	},
	"ffmpeg.processes_active": {
		Name: "ffmpeg.processes_active", Unit: "count", Component: CompFFmpeg, Kind: KindGauge,
		Description: "Number of currently active FFmpeg processes",
	},

	// ── Video metrics ────────────────────────────────────────────────────
	"video.encode_passes": {
		Name: "video.encode_passes", Unit: "count", Component: CompVideo, Kind: KindCounter,
		Description: "Total encode passes performed (1 for single-pass, 2 for two-pass)",
	},
	"video.frames_encoded": {
		Name: "video.frames_encoded", Unit: "count", Component: CompVideo, Kind: KindCounter,
		Description: "Total frames encoded across all passes",
	},
	"video.output_frames": {
		Name: "video.output_frames", Unit: "count", Component: CompVideo, Kind: KindCounter,
		Description: "Output frames published (lower-bound dedup of frames_encoded)",
	},
	"video.stream_copy_operations": {
		Name: "video.stream_copy_operations", Unit: "count", Component: CompVideo, Kind: KindCounter,
		Description: "Stream-copy concat operations (cheap path, no re-encoding)",
	},
	"video.reencode_operations": {
		Name: "video.reencode_operations", Unit: "count", Component: CompVideo, Kind: KindCounter,
		Description: "Re-encode concat operations (expensive path, resolution mismatch etc.)",
	},

	// ── Queue / wait-time metrics ────────────────────────────────────────
	"queue.ms": {
		Name: "queue.ms", Unit: "ms", Component: CompQueue, Kind: KindHistogram,
		Description: "Time the task spent in the queue before being dispatched to a worker",
	},
	"lease_wait.ms": {
		Name: "lease_wait.ms", Unit: "ms", Component: CompLease, Kind: KindHistogram,
		Description: "Time spent waiting for a lease to be granted",
	},
	"time_to_first_worker.ms": {
		Name: "time_to_first_worker.ms", Unit: "ms", Component: CompQueue, Kind: KindHistogram,
		Description: "Time from job submission to first worker assignment",
	},

	// ── Task-level input context ─────────────────────────────────────────
	"input.scene_count": {
		Name: "input.scene_count", Unit: "count", Component: CompInput, Kind: KindCounter,
		Description: "Number of scenes in the input timeline",
	},
	"input.segment_count": {
		Name: "input.segment_count", Unit: "count", Component: CompInput, Kind: KindCounter,
		Description: "Number of timeline segments in the render plan",
	},
	"input.total_duration_sec": {
		Name: "input.total_duration_sec", Unit: "seconds", Component: CompInput, Kind: KindGauge,
		Description: "Total input media duration in seconds",
	},
	"input.resolution_width": {
		Name: "input.resolution_width", Unit: "pixels", Component: CompInput, Kind: KindGauge,
		Description: "Output resolution width in pixels",
	},
	"input.resolution_height": {
		Name: "input.resolution_height", Unit: "pixels", Component: CompInput, Kind: KindGauge,
		Description: "Output resolution height in pixels",
	},
	"input.fps": {
		Name: "input.fps", Unit: "fps", Component: CompInput, Kind: KindGauge,
		Description: "Output frames per second",
	},
	"input.audio_track_count": {
		Name: "input.audio_track_count", Unit: "tracks", Component: CompInput, Kind: KindCounter,
		Description: "Number of audio tracks in the input",
	},
	"input.subtitle_count": {
		Name: "input.subtitle_count", Unit: "count", Component: CompInput, Kind: KindCounter,
		Description: "Number of subtitle tracks in the input",
	},

	// ── Per-attempt resource snapshot ────────────────────────────────────
	"resource.cpu_percent_peak": {
		Name: "resource.cpu_percent_peak", Unit: "percent", Component: CompResource, Kind: KindGauge,
		Description: "Peak CPU utilization during the attempt (0-100)",
	},
	"resource.rss_peak_bytes": {
		Name: "resource.rss_peak_bytes", Unit: "bytes", Component: CompResource, Kind: KindGauge,
		Description: "Peak RSS memory usage during the attempt",
	},
	"resource.disk_read_bytes": {
		Name: "resource.disk_read_bytes", Unit: "bytes", Component: CompResource, Kind: KindCounter,
		Description: "Total disk bytes read during the attempt",
	},
	"resource.disk_write_bytes": {
		Name: "resource.disk_write_bytes", Unit: "bytes", Component: CompResource, Kind: KindCounter,
		Description: "Total disk bytes written during the attempt",
	},
	"resource.network_rx_bytes": {
		Name: "resource.network_rx_bytes", Unit: "bytes", Component: CompResource, Kind: KindCounter,
		Description: "Total network bytes received during the attempt",
	},
	"resource.network_tx_bytes": {
		Name: "resource.network_tx_bytes", Unit: "bytes", Component: CompResource, Kind: KindCounter,
		Description: "Total network bytes transmitted during the attempt",
	},
	"resource.iowait_ms": {
		Name: "resource.iowait_ms", Unit: "ms", Component: CompResource, Kind: KindCounter,
		Description: "Total IO wait time during the attempt",
	},
	"resource.open_fds_peak": {
		Name: "resource.open_fds_peak", Unit: "count", Component: CompResource, Kind: KindGauge,
		Description: "Peak number of open file descriptors during the attempt",
	},

	// ── Waste / cost attribution ─────────────────────────────────────────
	"waste.retry_count": {
		Name: "waste.retry_count", Unit: "count", Component: CompWaste, Kind: KindCounter,
		Description: "Number of retries for this task (wasted attempts)",
	},
	"waste.wasted_cpu_ms": {
		Name: "waste.wasted_cpu_ms", Unit: "ms", Component: CompWaste, Kind: KindCounter,
		Description: "CPU time wasted on failed/retried attempts",
	},
	"waste.wasted_download_bytes": {
		Name: "waste.wasted_download_bytes", Unit: "bytes", Component: CompWaste, Kind: KindCounter,
		Description: "Download bytes wasted on failed/retried attempts",
	},
	"waste.wasted_cost_estimate": {
		Name: "waste.wasted_cost_estimate", Unit: "eur", Component: CompWaste, Kind: KindCounter,
		Description: "Estimated cost in EUR of wasted compute resources",
	},

	// ── Error classification ─────────────────────────────────────────────
	"error.component": {
		Name: "error.component", Unit: "string", Component: CompError, Kind: KindCounter,
		Description: "Component where the error originated (engine, pipeline, cache, etc.)",
	},
	"error.phase": {
		Name: "error.phase", Unit: "string", Component: CompError, Kind: KindCounter,
		Description: "Canonical phase where the error occurred",
	},
	"error.retryable": {
		Name: "error.retryable", Unit: "boolean", Component: CompError, Kind: KindGauge,
		Description: "Whether the error is classified as retryable",
	},
	"error.message_hash": {
		Name: "error.message_hash", Unit: "hash", Component: CompError, Kind: KindCounter,
		Description: "Stable hash of the error message for deduplication",
	},

	// ── TaskRunner phases ────────────────────────────────────────────────
	"taskrunner.cache_lookup_ms": {
		Name: "taskrunner.cache_lookup_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
		Description: "Time spent in the TaskRunner cache_lookup phase",
	},
	"taskrunner.prefetch_ms": {
		Name: "taskrunner.prefetch_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
		Description: "Time spent in the TaskRunner prefetch phase",
	},
	"taskrunner.execute_ms": {
		Name: "taskrunner.execute_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
		Description: "Time spent in the TaskRunner execute phase (pipeline + engine)",
	},
	"taskrunner.upload_ms": {
		Name: "taskrunner.upload_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
		Description: "Time spent in the TaskRunner upload phase",
	},
	"taskrunner.report_ms": {
		Name: "taskrunner.report_ms", Unit: "ms", Component: CompTaskRunner, Kind: KindHistogram,
		Description: "Time spent in the TaskRunner report phase",
	},

	// ── Worker resource gauges (heartbeat) ───────────────────────────────
	"worker.cpu_utilization_ratio": {
		Name: "worker.cpu_utilization_ratio", Unit: "ratio", Component: CompWorker, Kind: KindGauge,
		Description: "Worker CPU utilization ratio (0-1)",
	},
	"worker.cpu_iowait_ratio": {
		Name: "worker.cpu_iowait_ratio", Unit: "ratio", Component: CompWorker, Kind: KindGauge,
		Description: "Worker CPU iowait ratio (0-1)",
	},
	"worker.cpu_steal_ratio": {
		Name: "worker.cpu_steal_ratio", Unit: "ratio", Component: CompWorker, Kind: KindGauge,
		Description: "Worker CPU steal time ratio (virtualized env)",
	},
	"worker.process_rss_bytes": {
		Name: "worker.process_rss_bytes", Unit: "bytes", Component: CompWorker, Kind: KindGauge,
		Description: "Worker agent process resident set size",
	},
	"worker.process_rss_peak_bytes": {
		Name: "worker.process_rss_peak_bytes", Unit: "bytes", Component: CompWorker, Kind: KindGauge,
		Description: "Worker agent process peak RSS",
	},
	"worker.memory_used_bytes": {
		Name: "worker.memory_used_bytes", Unit: "bytes", Component: CompWorker, Kind: KindGauge,
		Description: "Worker system memory used",
	},
	"worker.disk_free_bytes": {
		Name: "worker.disk_free_bytes", Unit: "bytes", Component: CompWorker, Kind: KindGauge,
		Description: "Worker free disk space on the working volume",
	},
	"worker.temp_bytes": {
		Name: "worker.temp_bytes", Unit: "bytes", Component: CompWorker, Kind: KindGauge,
		Description: "Worker temp space used at heartbeat time",
	},
	"worker.active_tasks": {
		Name: "worker.active_tasks", Unit: "count", Component: CompWorker, Kind: KindGauge,
		Description: "Number of currently active tasks on the worker",
	},
	"worker.task_slots": {
		Name: "worker.task_slots", Unit: "count", Component: CompWorker, Kind: KindGauge,
		Description: "Total task slots available on the worker",
	},
	"worker.load1": {
		Name: "worker.load1", Unit: "ratio", Component: CompWorker, Kind: KindGauge,
		Description: "Worker 1-minute load average",
	},
	"worker.run_queue": {
		Name: "worker.run_queue", Unit: "count", Component: CompWorker, Kind: KindGauge,
		Description: "Worker OS run queue depth",
	},
	"worker.network_receive_bytes": {
		Name: "worker.network_receive_bytes", Unit: "bytes", Component: CompWorker, Kind: KindCounter,
		Description: "Worker total network bytes received (cumulative delta per heartbeat)",
	},
	"worker.network_transmit_bytes": {
		Name: "worker.network_transmit_bytes", Unit: "bytes", Component: CompWorker, Kind: KindCounter,
		Description: "Worker total network bytes transmitted (cumulative delta per heartbeat)",
	},
	"worker.heartbeat_age_seconds": {
		Name: "worker.heartbeat_age_seconds", Unit: "seconds", Component: CompWorker, Kind: KindGauge,
		Description: "Seconds since last worker heartbeat",
	},

	// ── Master-side health ───────────────────────────────────────────────
	"master.memory_rss_bytes": {
		Name: "master.memory_rss_bytes", Unit: "bytes", Component: CompMaster, Kind: KindGauge,
		Description: "Master process RSS memory",
	},
	"master.goroutines": {
		Name: "master.goroutines", Unit: "count", Component: CompMaster, Kind: KindGauge,
		Description: "Number of active goroutines on the master",
	},
	"master.outbox_pending": {
		Name: "master.outbox_pending", Unit: "count", Component: CompMaster, Kind: KindGauge,
		Description: "Number of pending outbox events",
	},

	// ── Scorecard / compute outcomes ─────────────────────────────────────
	"scorecard.render_speed_ratio": {
		Name: "scorecard.render_speed_ratio", Unit: "ratio", Component: CompScorecard, Kind: KindGauge,
		Description: "Ratio of media duration to wall clock time (>1 means faster than realtime)",
	},
	"scorecard.compute_seconds_total": {
		Name: "scorecard.compute_seconds_total", Unit: "seconds", Component: CompScorecard, Kind: KindCounter,
		Description: "Total compute seconds classified by outcome (useful, failed, cancelled, stale)",
	},
	"scorecard.failure_reasons_total": {
		Name: "scorecard.failure_reasons_total", Unit: "count", Component: CompScorecard, Kind: KindCounter,
		Description: "Number of failed compute attempts by reason code",
	},

	// ── Cost metrics ─────────────────────────────────────────────────────
	"cost.cpu_core_seconds_per_output_minute": {
		Name: "cost.cpu_core_seconds_per_output_minute", Unit: "eur_per_min", Component: CompCost, Kind: KindGauge,
		Description: "CPU cost per output minute by worker class",
	},
	"cost.network_gb_per_output_minute": {
		Name: "cost.network_gb_per_output_minute", Unit: "eur_per_min", Component: CompCost, Kind: KindGauge,
		Description: "Network egress cost per output minute by worker class",
	},
	"cost.storage_gb_per_output_minute": {
		Name: "cost.storage_gb_per_output_minute", Unit: "eur_per_min", Component: CompCost, Kind: KindGauge,
		Description: "Storage cost per output minute by worker class",
	},
	"cost.total_per_output_minute": {
		Name: "cost.total_per_output_minute", Unit: "eur_per_min", Component: CompCost, Kind: KindGauge,
		Description: "Total cost per output minute by worker class",
	},

	// ── Placement metrics ────────────────────────────────────────────────
	"placement.rejections_total": {
		Name: "placement.rejections_total", Unit: "count", Component: CompPlacement, Kind: KindCounter,
		Description: "Placement rejections by reason code (capacity_full, unsupported_executor, etc.)",
	},

	// ── Completion / reconcile ───────────────────────────────────────────
	"reconcile.total": {
		Name: "reconcile.total", Unit: "count", Component: CompReconcile, Kind: KindCounter,
		Description: "Reconcile supervisor dispatch counts by case × action",
	},
	"reconcile.commit_deadline_exceeded": {
		Name: "reconcile.commit_deadline_exceeded", Unit: "count", Component: CompReconcile, Kind: KindCounter,
		Description: "Attempts whose commit_deadline_at crossed without a terminal transition",
	},

	// ── Conflict budget ──────────────────────────────────────────────────
	"conflict.streak_reset_total": {
		Name: "conflict.streak_reset_total", Unit: "count", Component: CompConflict, Kind: KindCounter,
		Description: "ConflictBudget streak resets on successful CAS operations",
	},
	"conflict.escalations_total": {
		Name: "conflict.escalations_total", Unit: "count", Component: CompConflict, Kind: KindCounter,
		Description: "ConflictBudget escalations when the consecutive-conflict threshold is crossed",
	},
	"conflict.stayed_under_threshold_total": {
		Name: "conflict.stayed_under_threshold_total", Unit: "count", Component: CompConflict, Kind: KindCounter,
		Description: "ConflictBudget observations that stayed under the escalation threshold",
	},
	"conflict.streak_length": {
		Name: "conflict.streak_length", Unit: "count", Component: CompConflict, Kind: KindHistogram,
		Description: "Distribution of consecutive-conflict streak lengths on the CAS path",
	},
}

// ValidateMetricName reports whether name is a valid entry in the catalog.
// Returns the definition and true if found, zero-value and false otherwise.
func ValidateMetricName(name string) (MetricDefinition, bool) {
	def, ok := MetricCatalog[name]
	return def, ok
}

// MetricNames returns all registered metric names in sorted order.
// Useful for iteration, documentation generation, and validation.
func MetricNames() []string {
	names := make([]string, 0, len(MetricCatalog))
	for name := range MetricCatalog {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// MetricNamesByComponent returns all metric names belonging to the given component.
// Results are sorted for deterministic iteration.
func MetricNamesByComponent(component string) []string {
	var names []string
	for name, def := range MetricCatalog {
		if def.Component == component {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}
